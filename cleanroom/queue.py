"""Continuous ingestion queue process.

Independent of the workers and the query service. It appends synthetic
arrivals at a configurable rate, including late corrections, and never touches
clean results. Being a separate process is what makes the "stop a process and
come back" requirement meaningful: arrivals keep landing while the worker is
down and are picked up when it starts again.
"""
from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime, timezone

from . import common, ingest, rules  # noqa: F401  (rules keeps import parity)
from .generator import _base_event


class Queue:
    def __init__(self, conn, *, source: str = "live-feed", rate: float = 1.0, seed: int = None):
        self.conn = conn
        self.source = source
        self.rate = max(0.05, rate)
        self.rng = random.Random(seed)
        self.stopped = False
        self.sent = 0

    def enabled(self) -> bool:
        try:
            return common.get_meta(self.conn, "queue.enabled", "1") == "1"
        except Exception:  # noqa: BLE001
            return True

    def beat(self) -> None:
        try:
            common.set_meta(self.conn, "queue.beat", common.utcnow())
        except Exception:  # noqa: BLE001
            pass

    def tick(self) -> dict:
        # Arrivals are timestamped now: they land in the newest serving range
        # and are pinned to every accepting generation at arrival time.
        now = datetime.now(timezone.utc)
        seq = self.conn.execute("SELECT COALESCE(MAX(seq), 0) AS n FROM raw_event").fetchone()["n"] + 1
        event = _base_event(self.rng, seq, now)
        event["source"] = self.source
        event["event_key"] = f"live-{seq:06d}-{self.rng.randrange(1000):03d}"
        event["payload"]["order_id"] = event["event_key"]
        result = ingest.ingest(self.conn, event, actor="queue")

        # Occasionally emit a correction for a random earlier key.
        if self.rng.random() < 0.08:
            row = self.conn.execute(
                "SELECT * FROM raw_event WHERE is_current = 1 ORDER BY seq DESC LIMIT 1 OFFSET ?",
                (self.rng.randrange(1, 40),),
            ).fetchone()
            if row and row["event_key"] != event["event_key"]:
                payload = json.loads(row["payload"])
                payload["status"] = self.rng.choice(["refunded", "cancelled", "completed"])
                payload["amount"] = self.rng.choice(["12.50", "1,99", " 87.00 "])
                payload["_correction_note"] = "late revision from source"
                corr = ingest.ingest(
                    self.conn,
                    {
                        "source": row["source"],
                        "event_key": row["event_key"],
                        "event_time": row["event_time"],
                        "payload": payload,
                    },
                    actor="correction",
                )
                result["correction"] = corr
        self.sent += 1
        return result

    def run(self, *, max_events: int | None = None, duration: float | None = None,
            quiet: bool = False) -> int:
        started = time.monotonic()
        while not self.stopped:
            if max_events is not None and self.sent >= max_events:
                break
            if duration is not None and time.monotonic() - started >= duration:
                break
            self.beat()
            if self.enabled():
                self.tick()
                if not quiet:
                    print(f"queue: sent {self.sent}", flush=True)
            time.sleep(1.0 / self.rate)
        return self.sent


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Cleanroom ingestion queue")
    ap.add_argument("--db", default=None)
    ap.add_argument("--rate", type=float, default=2.0, help="events per second")
    ap.add_argument("--max-events", type=int, default=None)
    ap.add_argument("--duration", type=float, default=None, help="seconds")
    ap.add_argument("--seed", type=int, default=None)
    args = ap.parse_args(argv)

    conn = common.connect(args.db)
    common.init_db(conn)
    q = Queue(conn, rate=args.rate, seed=args.seed)

    def stop(*_):
        q.stopped = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    q.beat()
    n = q.run(max_events=args.max_events, duration=args.duration)
    print(f"queue: stopped after {n} events")
    return 0


if __name__ == "__main__":
    sys.exit(main())
