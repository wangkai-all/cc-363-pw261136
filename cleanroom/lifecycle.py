"""Generation lifecycle: backfill progress, shadow diff, approval and switch.

The visibility boundary is explicit:

* A generation in `building` receives incremental arrivals and cleans them on
  top of its backfill, but readers never see any of it.
* When the backfill is complete the generation can be compared against the
  serving one. During the comparison it stays `building` (paused or running):
  readers keep getting the old generation, whole.
* `approve` copies every clean result into the `published` table *inside one
  write transaction* and flips the serving windows (or a subset of them). A
  query opens one read transaction and resolves each event to exactly one
  generation, so it sees either the entire old set or the entire new set -
  never a splice of the two.
* `pause` stops the worker and marks affected intervals `suspended`, which is
  visible in the query response. It is the alternative to serving unverified
  rows; correction arrivals are still queued while paused.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from . import common


class LifecycleError(ValueError):
    pass


def get_generation(conn: sqlite3.Connection, gid: str) -> sqlite3.Row:
    row = conn.execute("SELECT * FROM generation WHERE id = ?", (gid,)).fetchone()
    if row is None:
        raise LifecycleError(f"no such generation: {gid}")
    return row


def active_generation(conn: sqlite3.Connection) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM generation WHERE status = 'active' "
        "ORDER BY COALESCE(activated_at, '') DESC, ordinal DESC LIMIT 1"
    ).fetchone()


def candidate_generations(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM generation WHERE status IN ('building','ready') ORDER BY ordinal"
    ).fetchall()


def start_backfill(conn: sqlite3.Connection, gid: str, *, from_time: str | None = None) -> dict:
    row = get_generation(conn, gid)
    if row["status"] not in ("building",):
        raise LifecycleError(f"generation is {row['status']}, not building")
    conn.execute("BEGIN IMMEDIATE")
    try:
        if from_time is not None:
            from_time = common.norm_time(from_time)
            ids = conn.execute(
                "SELECT id FROM raw_event WHERE is_current = 1 AND event_time >= ?", (from_time,)
            ).fetchall()
            conn.executemany(
                "INSERT INTO work_item(generation_id, raw_event_id, status) VALUES(?,?,'pending') "
                "ON CONFLICT(generation_id, raw_event_id) DO NOTHING",
                [(gid, r["id"]) for r in ids],
            )
            conn.execute("UPDATE generation SET backfill_from = ? WHERE id = ?", (from_time, gid))
        conn.execute("UPDATE generation SET paused = 0 WHERE id = ?", (gid,))
        common.audit(conn, "operator", "backfill.start", {"generation_id": gid, "from": from_time})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return progress(conn, gid)


def pause_backfill(conn: sqlite3.Connection, gid: str, reason: str = "") -> dict:
    get_generation(conn, gid)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("UPDATE generation SET paused = 1 WHERE id = ?", (gid,))
        common.audit(conn, "operator", "backfill.pause", {"generation_id": gid, "reason": reason})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return progress(conn, gid)


def resume_backfill(conn: sqlite3.Connection, gid: str) -> dict:
    get_generation(conn, gid)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute("UPDATE generation SET paused = 0 WHERE id = ?", (gid,))
        common.audit(conn, "operator", "backfill.resume", {"generation_id": gid})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return progress(conn, gid)


def progress(conn: sqlite3.Connection, gid: str) -> dict:
    from .ingest import progress as _p

    row = get_generation(conn, gid)
    p = _p(conn, gid)
    p.update(
        {
            "generation_id": gid,
            "version": row["version"],
            "label": row["label"],
            "status": row["status"],
            "paused": bool(row["paused"]),
            "backfill_from": row["backfill_from"],
        }
    )
    return p


def diff_summary(conn: sqlite3.Connection, candidate_id: str, base_id: str | None = None) -> dict:
    """Compare a candidate generation with the one currently serving.

    Only the *serving* intersection is compared, so the numbers answer the
    question an approver actually asks: "what changes for readers if I switch?"
    """
    if base_id is None:
        base = active_generation(conn)
        base_id = base["id"] if base else None
    if base_id is None:
        raise LifecycleError("no active generation to compare against")
    if base_id == candidate_id:
        prior = conn.execute(
            "SELECT id FROM generation WHERE id != ? AND status IN ('retired','building','ready') "
            "ORDER BY ordinal DESC LIMIT 1",
            (candidate_id,),
        ).fetchone()
        if prior is None:
            raise LifecycleError("nothing to compare against")
        base_id = prior["id"]

    rows = conn.execute(
        """
        SELECT r.id AS raw_id, r.event_time, r.event_key,
               b.status AS base_status, b.output AS base_output,
               c.status AS cand_status, c.output AS cand_output,
               c.quality_flags AS cand_flags, c.rule_trace AS cand_trace,
               b.id AS base_result_id, c.id AS cand_result_id
        FROM work_item w
        JOIN raw_event r ON r.id = w.raw_event_id
        LEFT JOIN clean_result b ON b.generation_id = ? AND b.raw_event_id = r.id
        LEFT JOIN clean_result c ON c.generation_id = ? AND c.raw_event_id = r.id
        WHERE w.generation_id = ? AND r.is_current = 1
        """,
        (base_id, candidate_id, candidate_id),
    ).fetchall()

    counts = {
        "compared": 0,
        "same": 0,
        "changed": 0,
        "candidate_only": 0,
        "base_only": 0,
        "status_flip": 0,
        "field_changes": 0,
    }
    by_field: dict[str, int] = {}
    by_status: dict[str, int] = {}
    samples: list[dict] = []
    for r in rows:
        if r["cand_status"] is None and r["base_status"] is None:
            continue
        counts["compared"] += 1
        if r["base_status"] is None:
            counts["candidate_only"] += 1
            continue
        if r["cand_status"] is None:
            counts["base_only"] += 1
            continue
        b = json.loads(r["base_output"])
        c = json.loads(r["cand_output"])
        if r["base_status"] != r["cand_status"]:
            counts["status_flip"] += 1
        changed_fields = [k for k in sorted(set(b) | set(c)) if b.get(k) != c.get(k)]
        if changed_fields:
            counts["changed"] += 1
            counts["field_changes"] += len(changed_fields)
            for f in changed_fields:
                by_field[f] = by_field.get(f, 0) + 1
            transitional = f"{r['base_status']}→{r['cand_status']}"
            by_status[transitional] = by_status.get(transitional, 0) + 1
            if len(samples) < 40:
                samples.append(
                    {
                        "raw_event_id": r["raw_id"],
                        "event_key": r["event_key"],
                        "event_time": r["event_time"],
                        "base_status": r["base_status"],
                        "candidate_status": r["cand_status"],
                        "changed_fields": changed_fields,
                        "base_output": b,
                        "candidate_output": c,
                        "candidate_flags": json.loads(r["cand_flags"] or "[]"),
                        "base_result_id": r["base_result_id"],
                        "candidate_result_id": r["cand_result_id"],
                    }
                )
        else:
            counts["same"] += 1

    counts["changed_pct"] = round(100.0 * counts["changed"] / counts["compared"], 1) if counts["compared"] else 0.0
    cand_row = conn.execute("SELECT backfill_from FROM generation WHERE id = ?", (candidate_id,)).fetchone()
    return {
        "base_generation_id": base_id,
        "candidate_generation_id": candidate_id,
        "scope_from": cand_row["backfill_from"] if cand_row else None,
        "counts": counts,
        "by_field": dict(sorted(by_field.items(), key=lambda kv: -kv[1])),
        "by_status": dict(sorted(by_status.items(), key=lambda kv: -kv[1])),
        "samples": samples,
    }


def approve(
    conn: sqlite3.Connection,
    gid: str,
    *,
    from_time: str | None = None,
    to_time: str | None = None,
    actor: str = "operator",
    allow_incomplete: bool = False,
) -> dict:
    """Publish a generation's clean results and switch the serving windows.

    Everything happens in one IMMEDIATE transaction: the published rows and the
    window flips commit together. Readers use a single read snapshot, so they
    cannot observe a half-switched interval.
    """
    gen = get_generation(conn, gid)
    if gen["status"] not in ("building", "ready"):
        raise LifecycleError(f"generation is {gen['status']}; only building/ready can be approved")

    from_time = common.norm_time(from_time) if from_time else gen["backfill_from"]
    to_time = common.norm_time(to_time) if to_time else None
    if gen["backfill_from"] and from_time and from_time < gen["backfill_from"]:
        raise LifecycleError(
            f"switch range starts {from_time}, before this generation's backfill scope "
            f"({gen['backfill_from']}); those events were never cleaned by it"
        )
    if to_time and from_time and to_time <= from_time:
        raise LifecycleError("switch range is empty (to <= from)")

    # Continuous ingestion means "everything is done" is a moving target.
    # Approval is refused only if unfinished work falls *inside* the range
    # being switched. Newer in-flight arrivals stay on the base generation
    # until a later approval, which is a visible, explicit boundary.
    where_pending = ["w.status IN ('pending','failed','running')"]
    params_pending: list[Any] = [gid]
    if from_time:
        where_pending.append("r.event_time >= ?")
        params_pending.append(from_time)
    if to_time:
        where_pending.append("r.event_time < ?")
        params_pending.append(to_time)
    unfinished = conn.execute(
        f"SELECT COUNT(*) n FROM work_item w JOIN raw_event r ON r.id = w.raw_event_id "
        f"WHERE w.generation_id = ? AND {' AND '.join(where_pending)}",
        params_pending,
    ).fetchone()["n"]
    if unfinished and not allow_incomplete:
        scope = f"in {from_time} → {to_time}" if to_time else "and no end bound was given"
        raise LifecycleError(
            f"{unfinished} work item(s) still unfinished {scope}; "
            "finish them, narrow the switch range, or pass allow_incomplete explicitly"
        )

    conn.execute("BEGIN IMMEDIATE")
    try:
        # 1. Publish derivations. For a partial interval switch, only rows in
        #    the interval are repointed; the rest of the range keeps its old
        #    generation.
        where = ["r.is_current = 1"]
        params: list[Any] = []
        if from_time:
            where.append("r.event_time >= ?")
            params.append(from_time)
        if to_time:
            where.append("r.event_time < ?")
            params.append(to_time)
        rows = conn.execute(
            f"""
            SELECT w.raw_event_id, c.id AS result_id
            FROM work_item w
            JOIN raw_event r ON r.id = w.raw_event_id
            JOIN clean_result c ON c.generation_id = w.generation_id AND c.raw_event_id = w.raw_event_id
            WHERE w.generation_id = ? AND {' AND '.join(where)}
            """,
            [gid, *params],
        ).fetchall()
        if not rows:
            raise LifecycleError("nothing to publish in that range for this generation")
        conn.executemany(
            "INSERT INTO published(generation_id, raw_event_id, clean_result_id) VALUES(?,?,?) "
            "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id = excluded.clean_result_id",
            [(gid, r["raw_event_id"], r["result_id"]) for r in rows],
        )

        # 2. Retire any window fully covered by the new interval, and split a
        #    straddling window so the new range wins cleanly.
        existing = conn.execute("SELECT * FROM serving_window").fetchall()
        for w in existing:
            w_from, w_to = w["from_time"], w["to_time"]
            overlaps = True
            if to_time and w_from >= to_time:
                overlaps = False
            if from_time and w_to and w_to <= from_time:
                overlaps = False
            if not overlaps:
                continue
            conn.execute("DELETE FROM serving_window WHERE id = ?", (w["id"],))
            if from_time and w_from < from_time:
                conn.execute(
                    "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (common.new_id("win_"), w["generation_id"], w_from, from_time, w["kind"], common.utcnow()),
                )
            if to_time and (w_to is None or w_to > to_time):
                conn.execute(
                    "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
                    "VALUES(?,?,?,?,?,?)",
                    (common.new_id("win_"), w["generation_id"], to_time, w_to, w["kind"], common.utcnow()),
                )
        conn.execute(
            "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
            "VALUES(?,?,?,?,?,?)",
            (common.new_id("win_"), gid, from_time or "0000-01-01T00:00:00.000000Z", to_time, "backfill",
             common.utcnow()),
        )

        # 3. Retire the old active generation only if it has no serving windows.
        prior_active = active_generation(conn)
        if prior_active and prior_active["id"] != gid:
            left = conn.execute(
                "SELECT COUNT(*) AS n FROM serving_window WHERE generation_id = ?", (prior_active["id"],)
            ).fetchone()["n"]
            if left == 0:
                conn.execute(
                    "UPDATE generation SET status = 'retired', retired_at = ? WHERE id = ?",
                    (common.utcnow(), prior_active["id"]),
                )

        conn.execute(
            "UPDATE generation SET status = 'active', activated_at = COALESCE(activated_at, ?) WHERE id = ?",
            (common.utcnow(), gid),
        )
        common.audit(
            conn, actor, "generation.approve",
            {"generation_id": gid, "from": from_time, "to": to_time, "published": len(rows)},
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"generation_id": gid, "published": len(rows), "from_time": from_time, "to_time": to_time}


def suspend(conn: sqlite3.Connection, gid: str, *, from_time: str, to_time: str,
            actor: str = "operator", reason: str = "") -> dict:
    """Mark an interval as suspended: queries return a visible gap, not a guess."""
    from_time = common.norm_time(from_time)
    to_time = common.norm_time(to_time)
    conn.execute("BEGIN IMMEDIATE")
    try:
        conn.execute(
            "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
            "VALUES(?,?,?,?,'suspended',?)",
            (common.new_id("win_"), gid, from_time, to_time, common.utcnow()),
        )
        common.audit(conn, actor, "interval.suspend",
                     {"generation_id": gid, "from": from_time, "to": to_time, "reason": reason})
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return {"suspended": {"from": from_time, "to": to_time}}


def switch_windows(conn: sqlite3.Connection, gid: str, *, from_time: str | None = None,
                   to_time: str | None = None, actor: str = "operator") -> dict:
    """Flip serving windows for an already-published generation."""
    gen = get_generation(conn, gid)
    if gen["status"] != "active":
        raise LifecycleError(f"generation is {gen['status']}, not active; approve it first")
    return approve(conn, gid, from_time=from_time, to_time=to_time, actor=actor)
