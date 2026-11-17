"""Ingestion: append raw events and pin each arrival to a generation.

Key design choice (documented in README): generation assignment happens **at
arrival time**, not at clean time. The arrival writes one `work_item` row per
generation that currently needs the event. A generation that is building or
shadowing receives arrivals on top of its backfill, so an event that lands
while v2 is backfilling is cleaned by both v1 and v2 and can never be stranded
on the old generation. Corrections append a new raw revision and supersede the
old one for *every* generation that ever held it, which is why a correction
during a backfill cannot be lost.
"""
from __future__ import annotations

import json
import sqlite3
from typing import Any

from . import common


class IngestError(ValueError):
    pass


REQUIRED_RAW = ("source", "event_key", "event_time")


def generations_for_arrival(conn: sqlite3.Connection) -> list[str]:
    """Generations that should receive a newly arrived raw event.

    active    - the generation currently serving queries
    building  - a backfill in progress; incremental arrivals must join it
    shadow    - fully cleaned and awaiting approval; still tracks arrivals
    """
    rows = conn.execute(
        "SELECT id FROM generation WHERE status IN ('active','building','shadow') ORDER BY ordinal"
    ).fetchall()
    return [r["id"] for r in rows]


def ingest(
    conn: sqlite3.Connection,
    event: dict,
    *,
    received_at: str | None = None,
    actor: str = "queue",
    commit: bool = True,
) -> dict:
    for key in REQUIRED_RAW:
        if not event.get(key):
            raise IngestError(f"event missing {key!r}")
    if not isinstance(event.get("payload"), dict):
        raise IngestError("event.payload must be an object")

    received_at = received_at or common.utcnow()
    source = str(event["source"])
    event_key = str(event["event_key"])
    event_time = common.norm_time(event["event_time"])
    payload = event["payload"]
    payload_hash = common.canonical_hash(payload)
    corrupt = 1 if event.get("corrupt") else 0

    conn.execute("BEGIN IMMEDIATE")
    try:
        prev = conn.execute(
            "SELECT id, revision, payload_hash FROM raw_event "
            "WHERE source = ? AND event_key = ? AND is_current = 1",
            (source, event_key),
        ).fetchone()
        if prev and prev["payload_hash"] == payload_hash and not corrupt:
            conn.execute("COMMIT")
            return {
                "status": "duplicate",
                "raw_event_id": prev["id"],
                "revision": prev["revision"],
                "generations": [],
            }
        revision = (prev["revision"] + 1) if prev else 1
        seq_row = conn.execute("SELECT COALESCE(MAX(seq), 0) + 1 AS n FROM raw_event").fetchone()
        raw_id = common.new_id("raw_")
        if prev:
            conn.execute("UPDATE raw_event SET is_current = 0 WHERE id = ?", (prev["id"],))
        conn.execute(
            "INSERT INTO raw_event(id, seq, source, event_key, event_time, revision, payload, "
            "payload_hash, received_at, corrupt, supersedes, is_current) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,1)",
            (
                raw_id,
                seq_row["n"],
                source,
                event_key,
                event_time,
                revision,
                common.canonical(payload),
                payload_hash,
                received_at,
                corrupt,
                prev["id"] if prev else None,
            ),
        )

        gens = generations_for_arrival(conn)
        for gid in gens:
            conn.execute(
                "INSERT INTO work_item(generation_id, raw_event_id, status) VALUES(?,?,'pending') "
                "ON CONFLICT(generation_id, raw_event_id) DO NOTHING",
                (gid, raw_id),
            )

        # Corrections must also re-open the superseded revision for any
        # generation that already cleaned it: the published row for that
        # revision must be replaced by the corrected one.
        superseded_recleaned = []
        if prev:
            # Re-clean the new revision for every generation that holds the old
            # one, whether it is active, backfilling or ready to switch. The
            # candidate must not inherit a pre-correction derivation.
            rows = conn.execute(
                "SELECT DISTINCT generation_id FROM work_item WHERE raw_event_id = ?", (prev["id"],)
            ).fetchall()
            for r in rows:
                conn.execute(
                    "INSERT INTO work_item(generation_id, raw_event_id, status) VALUES(?,?,'pending') "
                    "ON CONFLICT(generation_id, raw_event_id) DO NOTHING",
                    (r["generation_id"], raw_id),
                )
                superseded_recleaned.append(r["generation_id"])

        common.audit(
            conn,
            actor,
            "ingest",
            {
                "raw_event_id": raw_id,
                "source": source,
                "event_key": event_key,
                "revision": revision,
                "generations": gens,
                "correction": bool(prev),
            },
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return {
        "status": "corrected" if prev else "accepted",
        "raw_event_id": raw_id,
        "revision": revision,
        "generations": gens,
        "supersedes": prev["id"] if prev else None,
        "recleaned_generations": superseded_recleaned,
    }


def add_generation(
    conn: sqlite3.Connection,
    version: str,
    *,
    label: str | None = None,
    definition: dict | None = None,
    backfill_from: str | None = None,
    actor: str = "operator",
) -> str:
    """Register a new immutable generation and open its work set.

    Its work set is every current raw revision at or after `backfill_from`.
    Later arrivals are added by `ingest` because 'building' is an accepting
    status. The work set is only a *starting point*: once inserted, work items
    never vanish, so an event that arrives later and supersedes one of these
    gets its own item.
    """
    from . import rules

    if definition is None:
        definition = rules.definition_for(version)
    definition = json.loads(json.dumps(definition))  # deep copy
    if "rules" not in definition or not isinstance(definition["rules"], list):
        raise IngestError("generation definition needs a rules list")
    dhash = common.canonical_hash(definition)
    backfill_from = common.norm_time(backfill_from) if backfill_from else None

    conn.execute("BEGIN IMMEDIATE")
    try:
        existing = conn.execute(
            "SELECT id FROM generation WHERE version = ? AND definition_hash = ?",
            (version, dhash),
        ).fetchone()
        if existing:
            conn.execute("ROLLBACK")
            return existing["id"]
        ord_row = conn.execute("SELECT COALESCE(MAX(ordinal), 0) + 1 AS n FROM generation").fetchone()
        gid = common.new_id("gen_")
        conn.execute(
            "INSERT INTO generation(id, ordinal, label, version, definition, definition_hash, status, "
            "created_at, backfill_from, notes) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                gid,
                ord_row["n"],
                label or definition.get("label") or version,
                version,
                common.canonical(definition),
                dhash,
                "building",
                common.utcnow(),
                backfill_from,
                "",
            ),
        )
        where = "is_current = 1"
        params: list[Any] = []
        if backfill_from:
            where += " AND event_time >= ?"
            params.append(backfill_from)
        ids = conn.execute(f"SELECT id FROM raw_event WHERE {where}", params).fetchall()
        conn.executemany(
            "INSERT INTO work_item(generation_id, raw_event_id, status) VALUES(?,?,'pending')",
            [(gid, r["id"]) for r in ids],
        )
        common.audit(
            conn,
            actor,
            "generation.create",
            {"generation_id": gid, "version": version, "work_items": len(ids), "backfill_from": backfill_from},
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return gid


def progress(conn: sqlite3.Connection, generation_id: str) -> dict:
    row = conn.execute(
        "SELECT COUNT(*) AS total, "
        "SUM(CASE WHEN status = 'done' THEN 1 ELSE 0 END) AS done, "
        "SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending, "
        "SUM(CASE WHEN status = 'skipped' THEN 1 ELSE 0 END) AS skipped, "
        "SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed "
        "FROM work_item WHERE generation_id = ?",
        (generation_id,),
    ).fetchone()
    total = row["total"] or 0
    done = row["done"] or 0
    pending = row["pending"] or 0
    failed = row["failed"] or 0
    return {
        "total": total,
        "done": done,
        "pending": pending,
        "failed": failed,
        "skipped": row["skipped"] or 0,
        "pct": round(100.0 * done / total, 1) if total else 100.0,
        "remaining": pending + failed,
    }
