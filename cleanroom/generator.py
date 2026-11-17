"""Synthetic, deterministic event stream for the demo.

The generated stream deliberately contains the messy shapes that separate the
two rule generations: case-varied statuses, currency words/symbols, decimal
commas, odd spacing, invalid email, and a few corrections that arrive late.
"""
from __future__ import annotations

import argparse
import json
import random
import sys
from datetime import datetime, timedelta, timezone

from . import common, ingest, rules


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"


SOURCES = ["storefront", "partner-feed", "manual-ops"]
STATUS_VARIANTS = [
    "COMPLETED", "completed", "Complete", "complete", "done",
    "PENDING", "pending", "awaiting",
    "CANCELLED", "cancelled", "canceled", "cancel",
    "REFUNDED", "refunded", "refund",
    "failed", "declined",
]
CURRENCY_VARIANTS = [
    "USD", "usd", "US Dollar", "dollar", "us$",
    "EUR", "eur", "euro", "€",
    "GBP", "gbp", "pound", "£",
    "JPY", "jpy", "yen", "¥",
]
REGION_VARIANTS = [
    "us-east", "US East", "east", "na-east",
    "us-west", "US West", "west", "na-west",
    "eu-central", "EU Central", "central", "eu",
    "apac", "APAC", "ap-southeast", "asia",
]
EMAIL_DOMAINS = ["example.com", "mail.example.org", "shop.example.net", "corp.example.co"]


def _money(rng: random.Random, amount_cents: int) -> str:
    style = rng.randrange(8)
    whole, cents = divmod(amount_cents, 100)
    if style == 0:
        return f"${whole}.{cents:02d}"
    if style == 1:
        return f"{whole}.{cents:02d} USD"
    if style == 2:
        return f"{whole},{cents:02d} EUR"
    if style == 3:
        return f"{whole:,}.{cents:02d}"
    if style == 4:
        return f" {whole}.{cents:02d} "
    if style == 5:
        return f"{whole}.{cents:02d}"
    if style == 6:
        return f"{whole}.{cents:02d} USD"
    return f"{whole:,}.{cents:02d}".replace(",", " ")


def _amount(rng: random.Random) -> int:
    bucket = rng.random()
    if bucket < 0.7:
        return rng.randrange(500, 25000)
    if bucket < 0.95:
        return rng.randrange(25000, 450000)
    return rng.randrange(-40000, -100)


def _base_event(rng: random.Random, seq: int, event_time: datetime) -> dict:
    order = f"ord-{seq:05d}"
    status = rng.choice(STATUS_VARIANTS)
    currency = rng.choice(CURRENCY_VARIANTS)
    region = rng.choice(REGION_VARIANTS)
    count = rng.choice([1, 1, 2, 2, 3, 4, 6, "2", "3", " 5 "])
    amount = _amount(rng)
    email = f"user{seq}@{rng.choice(EMAIL_DOMAINS)}"
    if rng.random() < 0.05:
        email = email.replace("@", " at ")
    payload: dict = {
        "order_id": order,
        "customer_email": email,
        "currency": currency,
        "region": region,
        "status": status,
        "item_count": count,
        "amount": _money(rng, amount),
    }
    return {
        "source": rng.choice(SOURCES),
        "event_key": order,
        "event_time": _iso(event_time),
        "payload": payload,
    }


def generate(count: int = 200, *, seed: int = 7, start: datetime | None = None,
             span_hours: float = 72.0, order_prefix: str = "ord") -> list[dict]:
    rng = random.Random(seed)
    start = start or datetime(2026, 10, 1, tzinfo=timezone.utc)
    events = []
    for i in range(count):
        when = start + timedelta(hours=span_hours * i / max(1, count - 1))
        ev = _base_event(rng, i + 1, when)
        ev["event_key"] = f"{order_prefix}-{i + 1:05d}"
        ev["payload"]["order_id"] = ev["event_key"]
        events.append(ev)
    return events


def corrections(events: list[dict], base: dict[str, dict] | None = None) -> list[dict]:
    """A few later revisions to earlier keys (with changed payload)."""
    out = []
    for idx, new_status, new_amount in (
        (12, "refunded", "62.74"),
        (57, "completed", "118.00"),
        (99, "cancelled", "42.10"),
        (141, "pending", "9.99"),
    ):
        if idx >= len(events):
            continue
        event = json.loads(json.dumps(events[idx]))
        key = event["event_key"]
        original = (base or {}).get(key, event["payload"])
        payload = json.loads(json.dumps(original))
        payload["status"] = new_status
        payload["amount"] = new_amount
        out.append(
            {
                "source": event["source"],
                "event_key": key,
                "event_time": event["event_time"],
                "payload": payload,
                "corrupt": idx == 57,
            }
        )
    return out


def seed_database(conn, events: list[dict]) -> int:
    """Install v1 as active over the whole range, then ingest the stream."""
    existing = conn.execute("SELECT COUNT(*) AS n FROM generation").fetchone()["n"]
    if existing == 0:
        gid = ingest.add_generation(conn, "1.0.0", label=rules.BUILTIN_GENERATIONS["1.0.0"]["label"])
        conn.execute(
            "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
            "VALUES(?,?,?,NULL,'baseline',?)",
            (common.new_id("win_"), gid, "0000-01-01T00:00:00.000000Z", common.utcnow()),
        )
        conn.execute(
            "UPDATE generation SET status='active', activated_at=? WHERE id=?",
            (common.utcnow(), gid),
        )
        from .worker import Worker

        w = Worker(conn, "bootstrap")
        w.run(generation_id=gid, max_items=10**9)
        conn.execute(
            "INSERT INTO published(generation_id, raw_event_id, clean_result_id) "
            "SELECT c.generation_id, c.raw_event_id, c.id FROM clean_result c WHERE c.generation_id = ?",
            (gid,),
        )

    n = 0
    for ev in events:
        r = ingest.ingest(conn, ev, actor="generator")
        if r["status"] == "accepted":
            n += 1
    # drain whatever arrived after bootstrap (active generation only)
    from .worker import Worker

    Worker(conn, "bootstrap").run(max_items=10**9)
    gid = conn.execute("SELECT id FROM generation WHERE status='active'").fetchone()["id"]
    conn.execute(
        "INSERT INTO published(generation_id, raw_event_id, clean_result_id) "
        "SELECT c.generation_id, c.raw_event_id, c.id FROM clean_result c WHERE c.generation_id = ? "
        "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id=excluded.clean_result_id",
        (gid,),
    )
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Seed a cleanroom database with synthetic events")
    ap.add_argument("--db", default=None)
    ap.add_argument("--count", type=int, default=200)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--span-hours", type=float, default=72.0)
    ap.add_argument("--reset", action="store_true", help="delete the DB file first")
    ap.add_argument("--with-corrections", action="store_true", default=True)
    args = ap.parse_args(argv)

    path = args.db or str(common.db_path())
    if args.reset:
        import os

        for suffix in ("", "-wal", "-shm"):
            try:
                os.remove(path + suffix)
            except FileNotFoundError:
                pass
    conn = common.connect(path)
    common.init_db(conn)
    events = generate(args.count, seed=args.seed, span_hours=args.span_hours)
    n = seed_database(conn, events)
    extra = 0
    if args.with_corrections:
        from .worker import Worker

        current = {
            r["event_key"]: json.loads(r["payload"])
            for r in conn.execute("SELECT event_key, payload FROM raw_event WHERE is_current = 1")
        }
        for ev in corrections(events, current):
            if ingest.ingest(conn, ev, actor="generator")["status"] != "duplicate":
                extra += 1
        Worker(conn, "bootstrap").run(max_items=10**9)
        gid = conn.execute("SELECT id FROM generation WHERE status='active'").fetchone()["id"]
        conn.execute(
            "INSERT INTO published(generation_id, raw_event_id, clean_result_id) "
            "SELECT c.generation_id, c.raw_event_id, c.id FROM clean_result c WHERE c.generation_id = ? "
            "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id=excluded.clean_result_id",
            (gid,),
        )
    print(json.dumps({"db": path, "ingested": n, "corrections": extra}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
