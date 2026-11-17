"""Query service read model.

Every query runs inside a single read transaction (a consistent WAL snapshot).
Rows are resolved in two independent steps:

1. raw event -> serving window (by event_time) -> generation
2. (generation, raw_event) -> published clean result

Because step 1 reads *one* snapshot of the window table, a switch that flips a
range from v1 to v2 is observed in full or not at all. Clean results for a new
generation are written long before publication and never read through the
`published` pointer until the approving transaction commits.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from . import common


def _windows(conn: sqlite3.Connection) -> list[dict]:
    rows = conn.execute("SELECT * FROM serving_window ORDER BY created_at, rowid").fetchall()
    return [dict(r) for r in rows]


def resolve_window(windows: list[dict], event_time: str) -> dict | None:
    match = None
    for w in windows:
        if w["from_time"] <= event_time and (w["to_time"] is None or event_time < w["to_time"]):
            match = w  # ordered by created_at, so later rows win
    return match

def _row_for(conn: sqlite3.Connection, raw: sqlite3.Row, gen_id: str) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT p.clean_result_id, c.status, c.output, c.rule_trace, c.quality_flags,
               c.computed_at, c.output_hash
        FROM published p JOIN clean_result c ON c.id = p.clean_result_id
        WHERE p.generation_id = ? AND p.raw_event_id = ?
        """,
        (gen_id, raw["id"]),
    ).fetchone()


def latest_revision(conn: sqlite3.Connection, event_key: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM raw_event WHERE event_key = ? AND is_current = 1", (event_key,)
    ).fetchone()


def query(
    conn: sqlite3.Connection,
    *,
    from_time: str | None = None,
    to_time: str | None = None,
    limit: int = 200,
    statuses: list[str] | None = None,
) -> dict:
    from_time = common.norm_time(from_time) if from_time else "0000-01-01T00:00:00.000000Z"
    to_time = common.norm_time(to_time) if to_time else "9999-12-31T23:59:59.999999Z"
    limit = max(1, min(int(limit), 1000))

    conn.execute("BEGIN")
    try:
        windows = _windows(conn)
        gens = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM generation").fetchall()}
        raws = conn.execute(
            "SELECT * FROM raw_event WHERE is_current = 1 AND event_time >= ? AND event_time < ? "
            "ORDER BY event_time, seq LIMIT ?",
            (from_time, to_time, limit + 1),
        ).fetchall()
        truncated = len(raws) > limit
        raws = raws[:limit]

        rows: list[dict] = []
        gaps: list[dict] = []
        for raw in raws:
            w = resolve_window(windows, raw["event_time"])
            if w is None:
                gaps.append(
                    {
                        "event_key": raw["event_key"],
                        "event_time": raw["event_time"],
                        "reason": "no serving window",
                    }
                )
                continue
            if w["kind"] == "suspended":
                gaps.append(
                    {
                        "event_key": raw["event_key"],
                        "event_time": raw["event_time"],
                        "reason": "interval suspended",
                        "window_id": w["id"],
                        "from_time": w["from_time"],
                        "to_time": w["to_time"],
                    }
                )
                continue
            gen = gens.get(w["generation_id"])
            if gen is None:
                gaps.append(
                    {"event_key": raw["event_key"], "event_time": raw["event_time"],
                     "reason": "unknown generation"}
                )
                continue
            res = _row_for(conn, raw, gen["id"])
            if res is None:
                gaps.append(
                    {
                        "event_key": raw["event_key"],
                        "event_time": raw["event_time"],
                        "reason": "not yet published for serving generation",
                        "generation_id": gen["id"],
                    }
                )
                continue
            output = json.loads(res["output"])
            item = {
                "event_key": raw["event_key"],
                "source": raw["source"],
                "event_time": raw["event_time"],
                "revision": raw["revision"],
                "raw_event_id": raw["id"],
                "received_at": raw["received_at"],
                "result_id": res["clean_result_id"],
                "result_status": res["status"],
                "output_hash": res["output_hash"],
                "computed_at": res["computed_at"],
                "serving": {
                    "generation_id": gen["id"],
                    "version": gen["version"],
                    "label": gen["label"],
                    "window_kind": w["kind"],
                    "window_id": w["id"],
                },
                **output,
            }
            if statuses and res["status"] not in statuses:
                continue
            rows.append(item)

        result = {
            "from": from_time,
            "to": to_time,
            "limit": limit,
            "truncated": truncated,
            "read_at": common.utcnow(),
            "rows": rows,
            "gaps": gaps,
            "serving_generations": sorted({r["serving"]["version"] for r in rows}),
            "windows": windows,
        }
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return result


def provenance(conn: sqlite3.Connection, event_key: str) -> dict:
    """Expand one event to its inputs, every generation's derivation, and publication."""
    conn.execute("BEGIN")
    try:
        revisions = conn.execute(
            "SELECT * FROM raw_event WHERE event_key = ? OR (source, event_key) IN "
            "(SELECT source, event_key FROM raw_event WHERE event_key = ?) ORDER BY seq",
            (event_key, event_key),
        ).fetchall()
        if not revisions:
            conn.execute("COMMIT")
            return {"event_key": event_key, "found": False}
        raw_ids = [r["id"] for r in revisions]
        marks = ",".join("?" for _ in raw_ids)
        results = conn.execute(
            f"""SELECT c.*, g.version, g.label, g.definition_hash, g.status AS generation_status
                FROM clean_result c JOIN generation g ON g.id = c.generation_id
                WHERE c.raw_event_id IN ({marks}) ORDER BY g.ordinal, c.computed_at""",
            raw_ids,
        ).fetchall()
        published = conn.execute(
            f"SELECT * FROM published WHERE raw_event_id IN ({marks})", raw_ids
        ).fetchall()
        windows = _windows(conn)
        out_results = []
        for r in results:
            out_results.append(
                {
                    "generation_id": r["generation_id"],
                    "generation_version": r["version"],
                    "generation_label": r["label"],
                    "generation_status": r["generation_status"],
                    "definition_hash": r["definition_hash"],
                    "raw_event_id": r["raw_event_id"],
                    "result_id": r["id"],
                    "status": r["status"],
                    "output": json.loads(r["output"]),
                    "output_hash": r["output_hash"],
                    "rule_trace": json.loads(r["rule_trace"]),
                    "quality_flags": json.loads(r["quality_flags"]),
                    "computed_at": r["computed_at"],
                    "worker": r["worker"],
                    "published": any(
                        p["generation_id"] == r["generation_id"]
                        and p["raw_event_id"] == r["raw_event_id"]
                        and p["clean_result_id"] == r["id"]
                        for p in published
                    ),
                }
            )
        out_revisions = []
        for r in revisions:
            w = resolve_window(windows, r["event_time"])
            out_revisions.append(
                {
                    "raw_event_id": r["id"],
                    "revision": r["revision"],
                    "seq": r["seq"],
                    "event_time": r["event_time"],
                    "received_at": r["received_at"],
                    "is_current": bool(r["is_current"]),
                    "supersedes": r["supersedes"],
                    "payload": json.loads(r["payload"]),
                    "payload_hash": r["payload_hash"],
                    "served_by_window": w,
                }
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "event_key": event_key,
        "found": True,
        "revisions": out_revisions,
        "results": out_results,
        "windows": windows,
    }


def coverage(conn: sqlite3.Connection) -> dict:
    """Coverage of the serving surface, expressed in raw revisions."""
    conn.execute("BEGIN")
    try:
        windows = _windows(conn)
        gens = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM generation").fetchall()}
        total = conn.execute("SELECT COUNT(*) AS n FROM raw_event WHERE is_current = 1").fetchone()["n"]

        def count_where(pred) -> int:
            n = 0
            last = None
            for raw in conn.execute(
                "SELECT event_time FROM raw_event WHERE is_current = 1 ORDER BY event_time"
            ):
                w = resolve_window(windows, raw["event_time"])
                if pred(w):
                    n += 1
            del last
            return n

        suspended = count_where(lambda w: w is not None and w["kind"] == "suspended")
        unserved = count_where(lambda w: w is None)
        per_gen = {gid: 0 for gid in gens}
        rows = conn.execute(
            "SELECT event_time FROM raw_event WHERE is_current = 1 ORDER BY event_time"
        ).fetchall()
        for raw in rows:
            w = resolve_window(windows, raw["event_time"])
            if w and w["generation_id"] in per_gen and w["kind"] != "suspended":
                per_gen[w["generation_id"]] += 1
        served = total - suspended - unserved
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {
        "total_current_events": total,
        "served": served,
        "suspended": suspended,
        "unserved": unserved,
        "pct": round(100.0 * served / total, 1) if total else 100.0,
        "by_generation": [
            {
                "generation_id": gid,
                "version": gens[gid]["version"],
                "label": gens[gid]["label"],
                "status": gens[gid]["status"],
                "events": n,
            }
            for gid, n in per_gen.items()
            if n
        ],
        "windows": windows,
    }
