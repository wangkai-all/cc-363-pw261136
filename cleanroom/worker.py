"""Cleaning worker: drains work items for a generation.

Runs as its own process (`python -m cleanroom.worker`). Multiple workers can
run in parallel; claiming is a single UPDATE ... RETURNING, so two workers can
never clean the same item. Results are inserted with UNIQUE(generation_id,
raw_event_id) and are never overwritten in place: a re-open (new revision)
creates a fresh work item and the insert replaces the derivation row for that
exact revision only.

Crucially, writing clean results does NOT change what readers see. Publication
is a separate, generation-wide, atomic step performed at approval time.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time

from . import common, rules


class WorkerError(RuntimeError):
    pass


def _stale_cutoff(seconds: float) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


class Worker:
    def __init__(self, conn: sqlite3.Connection, owner: str, *, pause_flag: str | None = None,
                 stale_after: float = 30.0):
        self.conn = conn
        self.owner = owner
        self.pause_flag = pause_flag
        self.stale_after = stale_after

    def paused(self) -> bool:
        if self.pause_flag and self.conn:
            return common.get_meta(self.conn, self.pause_flag, "0") == "1"
        return False

    def claim(self, generation_id: str | None) -> sqlite3.Row | None:
        """Claim one work item, including items abandoned by a killed worker.

        An item left in 'running' for longer than `stale_after` seconds is
        treated as abandoned. Cleaning is idempotent (the result row is keyed
        by generation + revision), so a double clean after a crash is safe.
        """
        cutoff = _stale_cutoff(self.stale_after)
        sql = (
            "UPDATE work_item SET status = 'running', claim_owner = ?, claim_at = ?, attempt = attempt + 1 "
            "WHERE rowid = ("
            "  SELECT rowid FROM work_item "
            "  WHERE status = 'pending' OR (status = 'running' AND claim_at < ?)"
        )
        params: list = [self.owner, common.utcnow(), cutoff]
        if generation_id:
            sql += " AND generation_id = ?"
            params.append(generation_id)
        sql += " ORDER BY rowid LIMIT 1) RETURNING generation_id, raw_event_id, attempt"
        return self.conn.execute(sql, params).fetchone()

    def clean_one(self) -> dict | None:
        row = self.claim(None)
        if row is None:
            return None
        return self._clean_claimed(row)

    def _clean_claimed(self, row: sqlite3.Row) -> dict:
        gid = row["generation_id"]
        raw_id = row["raw_event_id"]
        gen = self.conn.execute("SELECT * FROM generation WHERE id = ?", (gid,)).fetchone()
        raw = self.conn.execute("SELECT * FROM raw_event WHERE id = ?", (raw_id,)).fetchone()
        started = time.perf_counter()

        if gen is None or raw is None:
            self.conn.execute(
                "UPDATE work_item SET status='failed', finished_at=?, last_error='missing generation or raw event' "
                "WHERE generation_id=? AND raw_event_id=?",
                (common.utcnow(), gid, raw_id),
            )
            return {"generation_id": gid, "raw_event_id": raw_id, "status": "failed"}

        # A stale revision (already superseded) is still cleaned for its own
        # generation: the derivation belongs to the revision it read.
        if raw["corrupt"]:
            result = {
                "status": "dropped",
                "output": {"drop_reason": "corrupt_payload"},
                "trace": [],
                "flags": ["corrupt_payload"],
            }
        else:
            try:
                definition = json.loads(gen["definition"])
                payload = json.loads(raw["payload"])
                result = rules.apply(definition, payload)
            except Exception as exc:  # noqa: BLE001 - a bad rule must not kill the worker
                self.conn.execute(
                    "UPDATE work_item SET status='failed', finished_at=?, last_error=? "
                    "WHERE generation_id=? AND raw_event_id=?",
                    (common.utcnow(), f"{type(exc).__name__}: {exc}", gid, raw_id),
                )
                common.audit(
                    self.conn, self.owner, "clean.failed",
                    {"generation_id": gid, "raw_event_id": raw_id, "error": str(exc)},
                )
                return {"generation_id": gid, "raw_event_id": raw_id, "status": "failed"}

        out_hash = common.canonical_hash(result["output"])
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            self.conn.execute(
                "INSERT INTO clean_result(id, generation_id, raw_event_id, status, output, output_hash, "
                "rule_trace, quality_flags, computed_at, worker) VALUES(?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET "
                "status = excluded.status, output = excluded.output, output_hash = excluded.output_hash, "
                "rule_trace = excluded.rule_trace, quality_flags = excluded.quality_flags, "
                "computed_at = excluded.computed_at, worker = excluded.worker",
                (
                    common.new_id("res_"),
                    gid,
                    raw_id,
                    result["status"],
                    common.canonical(result["output"]),
                    out_hash,
                    common.canonical(result["trace"]),
                    common.canonical(result["flags"]),
                    common.utcnow(),
                    self.owner,
                ),
            )
            # A serving generation publishes its own corrections immediately:
            # the reader swaps *this event* to the new revision, atomically and
            # for its own generation only. A building generation never
            # publishes; its rows wait for approval. A generation only
            # publishes events that fall inside one of its own serving
            # windows, so a partially-switched v1 cannot quietly write a
            # pointer for a range that v2 serves.
            if gen["status"] == "active" and self._serves(raw["event_time"], gid):
                res_id = self.conn.execute(
                    "SELECT id FROM clean_result WHERE generation_id = ? AND raw_event_id = ?",
                    (gid, raw_id),
                ).fetchone()["id"]
                self.conn.execute(
                    "INSERT INTO published(generation_id, raw_event_id, clean_result_id) VALUES(?,?,?) "
                    "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id = excluded.clean_result_id",
                    (gid, raw_id, res_id),
                )
            self.conn.execute(
                "UPDATE work_item SET status='done', finished_at=?, last_error=NULL "
                "WHERE generation_id=? AND raw_event_id=?",
                (common.utcnow(), gid, raw_id),
            )
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        return {
            "generation_id": gid,
            "raw_event_id": raw_id,
            "status": "done",
            "result": result["status"],
            "ms": round((time.perf_counter() - started) * 1000, 2),
        }

    def _serves(self, event_time: str, generation_id: str) -> bool:
        row = self.conn.execute(
            "SELECT 1 FROM serving_window WHERE generation_id = ? AND kind != 'suspended' "
            "AND from_time <= ? AND (to_time IS NULL OR to_time > ?) LIMIT 1",
            (generation_id, event_time, event_time),
        ).fetchone()
        return row is not None

    def heartbeat(self, processed: int, generation_id: str | None) -> None:
        try:
            self.conn.execute(
                "INSERT INTO worker_heartbeat(owner, at, generation_id, processed) VALUES(?,?,?,?) "
                "ON CONFLICT(owner) DO UPDATE SET at = excluded.at, "
                "generation_id = COALESCE(excluded.generation_id, worker_heartbeat.generation_id), "
                "processed = excluded.processed",
                (self.owner, common.utcnow(), generation_id, processed),
            )
        except sqlite3.Error:
            pass

    def run(self, *, generation_id: str | None = None, max_items: int | None = None,
            idle_sleep: float = 0.25) -> int:
        processed = 0
        last_beat = 0.0
        self.heartbeat(0, generation_id)
        try:
            while True:
                if self.paused():
                    return processed
                if max_items is not None and processed >= max_items:
                    return processed
                now = time.monotonic()
                if now - last_beat > 2.0:
                    self.heartbeat(processed, generation_id)
                    last_beat = now
                row = self.claim(generation_id)
                if row is None:
                    if max_items is not None:
                        return processed
                    time.sleep(idle_sleep)
                    continue
                self._clean_claimed(row)
                processed += 1
        finally:
            try:
                self.conn.execute("DELETE FROM worker_heartbeat WHERE owner = ?", (self.owner,))
            except sqlite3.Error:
                pass


def main(argv: list[str] | None = None) -> int:
    import signal

    ap = argparse.ArgumentParser(description="Cleanroom cleaning worker")
    ap.add_argument("--db", default=None)
    ap.add_argument("--generation", default=None, help="only drain this generation id")
    ap.add_argument("--max-items", type=int, default=None)
    ap.add_argument("--owner", default=None)
    ap.add_argument("--once", action="store_true", help="drain everything then exit")
    args = ap.parse_args(argv)

    conn = common.connect(args.db)
    common.init_db(conn)
    owner = args.owner or f"worker-{common.new_id()[:6]}"
    worker = Worker(conn, owner)

    def stop(*_):
        # Unwind through run() so the finally block clears the heartbeat and
        # any in-flight claim rows are left for the reclaim endpoint.
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        if args.once or args.max_items is not None:
            n = worker.run(generation_id=args.generation, max_items=args.max_items)
        else:
            n = worker.run(generation_id=args.generation)
    except (KeyboardInterrupt, SystemExit):
        print(f"{owner}: interrupted", file=sys.stderr)
        return 130
    print(f"{owner}: processed {n} item(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
