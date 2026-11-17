"""End-to-end behaviour tests for the cleanroom pipeline.

Run with:  python3 -m unittest discover -s tests -v
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from cleanroom import common, generator, ingest, lifecycle, query, rules  # noqa: E402
from cleanroom.worker import Worker  # noqa: E402


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def payload(order_id="a-1", status="COMPLETED", amount="$10.00", **kw):
    base = {
        "order_id": order_id,
        "customer_email": "user@example.com",
        "currency": "USD",
        "region": "us-east",
        "status": status,
        "item_count": 1,
        "amount": amount,
    }
    base.update(kw)
    return base


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "test.db"
        self.conn = common.connect(self.path)
        common.init_db(self.conn)
        self.t0 = datetime(2026, 10, 1, tzinfo=timezone.utc)
        self.gid1 = self._bootstrap_v1()

    def tearDown(self):
        self.conn.close()
        self.tmp.cleanup()

    def _bootstrap_v1(self) -> str:
        gid = ingest.add_generation(self.conn, "1.0.0")
        self.conn.execute(
            "INSERT INTO serving_window(id, generation_id, from_time, to_time, kind, created_at) "
            "VALUES(?,?,?,NULL,'baseline',?)",
            (common.new_id("win_"), gid, "0000-01-01T00:00:00.000000Z", common.utcnow()),
        )
        self.conn.execute(
            "UPDATE generation SET status='active', activated_at=? WHERE id=?",
            (common.utcnow(), gid),
        )
        return gid

    def add(self, key, when, **kw):
        return ingest.ingest(
            self.conn,
            {"source": "test", "event_key": key, "event_time": iso(when), "payload": payload(order_id=key, **kw)},
        )

    def drain(self, gid=None):
        return Worker(self.conn, "test-worker").run(generation_id=gid, max_items=10_000)

    def approve(self, gid, **kw):
        return lifecycle.approve(self.conn, gid, actor="test", **kw)

    def publish_active(self):
        gid = lifecycle.active_generation(self.conn)["id"]
        self.conn.execute(
            "INSERT INTO published(generation_id, raw_event_id, clean_result_id) "
            "SELECT generation_id, raw_event_id, id FROM clean_result WHERE generation_id = ? "
            "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id=excluded.clean_result_id",
            (gid,),
        )
        self.conn.commit()
        return gid


class TestRuleEngines(unittest.TestCase):
    def test_v2_parses_decimal_comma_exactly(self):
        p = payload(amount="1.234,56", currency="eur", status="Complete", region="EU")
        r = rules.apply(rules.definition_for("2.0.0"), p)
        self.assertEqual(r["output"]["amount_cents"], 123456)
        self.assertEqual(r["output"]["currency"], "EUR")
        self.assertEqual(r["output"]["status"], "completed")
        self.assertEqual(r["output"]["region"], "eu-central")

    def test_v1_float_path_loses_precision_but_v2_does_not(self):
        p = payload(amount="0.29")
        v1 = rules.apply(rules.definition_for("1.0.0"), p)["output"]["amount_cents"]
        v2 = rules.apply(rules.definition_for("2.0.0"), p)["output"]["amount_cents"]
        self.assertEqual(v2, 29)
        self.assertEqual(v1, 29)  # both agree here; separator handling differs
        p2 = payload(amount="1,234.56")
        self.assertEqual(rules.apply(rules.definition_for("2.0.0"), p2)["output"]["amount_cents"], 123456)

    def test_v2_drops_rows_missing_required_fields(self):
        r = rules.apply(rules.definition_for("2.0.0"), payload(amount="not money"))
        self.assertEqual(r["status"], "dropped")
        self.assertIn("missing_required", r["flags"])

    def test_trace_records_rule_ids(self):
        r = rules.apply(rules.definition_for("2.0.0"), payload(order_id=" z-9 ", status="done"))
        ids = [t["rule"] for t in r["trace"]]
        self.assertIn("id.upper", ids)
        self.assertIn("status.alias", ids)
        self.assertEqual(r["output"]["order_id"], "Z-9")


class TestPinning(Base):
    def test_arrival_during_backfill_is_pinned_to_both_generations(self):
        self.add("e1", self.t0)
        self.drain()
        self.publish_active()

        gid2 = ingest.add_generation(self.conn, "2.0.0")
        # v1's initial item and v2's backfill item both exist; now a new arrival:
        r = self.add("e2", self.t0 + timedelta(hours=1))
        gens = {
            row["generation_id"]
            for row in self.conn.execute(
                "SELECT generation_id FROM work_item WHERE raw_event_id = ?", (r["raw_event_id"],)
            )
        }
        self.assertEqual(gens, {self.gid1, gid2})

        self.drain()
        for gid in gens:
            row = self.conn.execute(
                "SELECT status FROM work_item WHERE generation_id=? AND raw_event_id=?",
                (gid, r["raw_event_id"]),
            ).fetchone()
            self.assertEqual(row["status"], "done")

    def test_correction_during_backfill_reaches_candidate(self):
        r1 = self.add("e1", self.t0, status="COMPLETED")
        self.drain()
        self.publish_active()

        gid2 = ingest.add_generation(self.conn, "2.0.0")
        # correct the event while v2 is still backfilling
        corr = ingest.ingest(
            self.conn,
            {
                "source": "test",
                "event_key": "e1",
                "event_time": iso(self.t0),
                "payload": payload(order_id="e1", status="REFUNDED", amount="12.00"),
            },
        )
        self.assertEqual(corr["revision"], 2)
        self.assertIn(gid2, corr["recleaned_generations"])

        self.drain()
        # candidate derivation exists for revision 2 and is not published yet
        row = self.conn.execute(
            "SELECT c.output, c.status FROM clean_result c WHERE c.generation_id=? AND c.raw_event_id=?",
            (gid2, corr["raw_event_id"]),
        ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(json.loads(row["output"])["status"], "refunded")
        pub = self.conn.execute(
            "SELECT COUNT(*) n FROM published WHERE generation_id=? AND raw_event_id=?",
            (gid2, corr["raw_event_id"]),
        ).fetchone()["n"]
        self.assertEqual(pub, 0, "candidate must not publish before approval")

    def test_correction_to_active_generation_republishes_immediately(self):
        r1 = self.add("e1", self.t0)
        self.drain()
        self.publish_active()
        corr = ingest.ingest(
            self.conn,
            {
                "source": "test",
                "event_key": "e1",
                "event_time": iso(self.t0),
                "payload": payload(order_id="e1", status="CANCELLED", amount="99.99"),
            },
        )
        self.drain()
        served = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=1)))
        self.assertEqual(len(served["rows"]), 1)
        self.assertEqual(served["rows"][0]["revision"], 2)
        self.assertEqual(served["rows"][0]["amount_cents"], 9999)
        older = self.conn.execute(
            "SELECT COUNT(*) n FROM clean_result WHERE raw_event_id=?", (r1["raw_event_id"],)
        ).fetchone()["n"]
        self.assertEqual(older, 1, "superseded derivation is retained, not destroyed")


class TestApproval(Base):
    def test_approve_refused_while_work_pending(self):
        for i in range(5):
            self.add(f"e{i}", self.t0 + timedelta(minutes=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        with self.assertRaises(lifecycle.LifecycleError):
            self.approve(gid2)
        self.drain()
        out = self.approve(gid2)
        self.assertEqual(out["published"], 5)

    def test_pending_outside_the_switch_range_does_not_block_it(self):
        """Continuous ingestion means work is always arriving. A pending item
        past the range's end must not block switching the finished slice."""
        for i in range(6):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        Worker(self.conn, "w").run(generation_id=gid2, max_items=3)
        pending = self.conn.execute(
            "SELECT COUNT(*) n FROM work_item WHERE generation_id=? AND status='pending'", (gid2,)
        ).fetchone()["n"]
        self.assertEqual(pending, 3)

        out = self.approve(
            gid2, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=3))
        )
        self.assertEqual(out["published"], 3)
        self.drain(gid2)
        early = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=3)))
        late = query.query(self.conn, from_time=iso(self.t0 + timedelta(hours=3)),
                           to_time=iso(self.t0 + timedelta(hours=6)))
        self.assertEqual(early["serving_generations"], ["2.0.0"])
        self.assertEqual(late["serving_generations"], ["1.0.0"])

    def test_pending_inside_the_switch_range_still_blocks(self):
        for i in range(6):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        Worker(self.conn, "w").run(generation_id=gid2, max_items=3)
        with self.assertRaises(lifecycle.LifecycleError):
            self.approve(gid2, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=6)))

    def test_partial_switch_only_moves_the_slice(self):
        for i in range(6):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()
        self.approve(gid2, from_time=iso(self.t0 + timedelta(hours=2)), to_time=iso(self.t0 + timedelta(hours=4)))

        early = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=2)))
        late = query.query(
            self.conn,
            from_time=iso(self.t0 + timedelta(hours=2)),
            to_time=iso(self.t0 + timedelta(hours=4)),
        )
        after = query.query(
            self.conn, from_time=iso(self.t0 + timedelta(hours=4)), to_time=iso(self.t0 + timedelta(hours=6))
        )
        self.assertEqual(early["serving_generations"], ["1.0.0"])
        self.assertEqual(late["serving_generations"], ["2.0.0"])
        self.assertEqual(after["serving_generations"], ["1.0.0"])

    def test_retired_generation_still_serves_its_remaining_windows(self):
        for i in range(4):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()
        self.approve(gid2, from_time=iso(self.t0 + timedelta(hours=2)))
        statuses = {
            row["version"]: row["status"] for row in self.conn.execute("SELECT version,status FROM generation")
        }
        self.assertEqual(statuses["2.0.0"], "active")
        self.assertEqual(statuses["1.0.0"], "active", "v1 keeps its early window and must stay serving-capable")


class TestIsolation(Base):
    def test_reader_never_sees_mixed_generation_sets_across_switch(self):
        n = 40
        for i in range(n):
            self.add(f"e{i:03d}", self.t0 + timedelta(minutes=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()

        stop = threading.Event()
        seen: list[set[str]] = []
        errors: list[str] = []

        def reader():
            conn = common.connect(self.path)
            try:
                while not stop.is_set():
                    q = query.query(conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(days=1)),
                                    limit=1000)
                    gens = {r["serving"]["generation_id"] for r in q["rows"]}
                    seen.append(gens)
            except Exception as exc:  # noqa: BLE001
                errors.append(str(exc))
            finally:
                conn.close()

        t = threading.Thread(target=reader)
        t.start()
        time.sleep(0.15)
        self.approve(gid2)
        time.sleep(0.15)
        stop.set()
        t.join(5)

        self.assertEqual(errors, [])
        self.assertTrue(seen)
        for gens in seen:
            self.assertLessEqual(len(gens), 1, f"mixed generations observed: {gens}")

    def test_queries_before_approval_are_all_old(self):
        for i in range(10):
            self.add(f"e{i}", self.t0 + timedelta(minutes=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()
        before = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(days=1)))
        self.assertEqual(before["serving_generations"], ["1.0.0"])
        self.approve(gid2)
        after = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(days=1)))
        self.assertEqual(after["serving_generations"], ["2.0.0"])


class TestSuspension(Base):
    def test_suspended_interval_returns_explicit_gaps(self):
        for i in range(4):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        lifecycle.suspend(
            self.conn, self.gid1,
            from_time=iso(self.t0 + timedelta(hours=1)),
            to_time=iso(self.t0 + timedelta(hours=2)),
            reason="under review",
        )
        q = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=4)))
        served = {r["event_key"] for r in q["rows"]}
        self.assertEqual(served, {"e0", "e2", "e3"})
        self.assertEqual(len(q["gaps"]), 1)
        self.assertEqual(q["gaps"][0]["reason"], "interval suspended")
        self.assertIn("from_time", q["gaps"][0])

    def test_coverage_counts_suspension(self):
        self.add("e0", self.t0)
        self.drain()
        self.publish_active()
        lifecycle.suspend(
            self.conn, self.gid1,
            from_time=iso(self.t0 - timedelta(minutes=1)),
            to_time=iso(self.t0 + timedelta(minutes=1)),
        )
        cov = query.coverage(self.conn)
        self.assertEqual(cov["suspended"], 1)
        self.assertEqual(cov["served"], 0)


class TestRecovery(Base):
    def test_recleaning_same_revision_is_idempotent(self):
        r = self.add("e1", self.t0)
        self.drain()
        before = self.conn.execute(
            "SELECT COUNT(*) n FROM clean_result WHERE raw_event_id=?", (r["raw_event_id"],)
        ).fetchone()["n"]
        self.conn.execute(
            "UPDATE work_item SET status='pending' WHERE raw_event_id=?", (r["raw_event_id"],)
        )
        self.drain()
        after = self.conn.execute(
            "SELECT COUNT(*) n FROM clean_result WHERE raw_event_id=?", (r["raw_event_id"],)
        ).fetchone()["n"]
        self.assertEqual(before, after)

    def test_stale_claim_is_reclaimed(self):
        self.add("e1", self.t0)
        self.conn.execute(
            "UPDATE work_item SET status='running', claim_owner='dead', claim_at=?",
            ((datetime.now(timezone.utc) - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),),
        )
        n = Worker(self.conn, "rescuer", stale_after=30).run(max_items=10)
        self.assertEqual(n, 1)
        row = self.conn.execute("SELECT status FROM work_item").fetchone()
        self.assertEqual(row["status"], "done")

    def test_worker_pauses_via_flag(self):
        for i in range(3):
            self.add(f"e{i}", self.t0 + timedelta(minutes=i))
        common.set_meta(self.conn, "work.pause", "1")
        w = Worker(self.conn, "paused-worker", pause_flag="work.pause")
        self.assertEqual(w.run(max_items=10), 0)
        common.set_meta(self.conn, "work.pause", "0")
        self.assertEqual(w.run(max_items=10), 3)


class TestProvenance(Base):
    def test_provenance_expands_to_input_and_versions(self):
        r = self.add("e1", self.t0, status="COMPLETED", amount="$10.00")
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()
        prov = query.provenance(self.conn, "e1")
        self.assertTrue(prov["found"])
        self.assertEqual(len(prov["revisions"]), 1)
        versions = {x["generation_version"] for x in prov["results"]}
        self.assertEqual(versions, {"1.0.0", "2.0.0"})
        self.assertEqual(prov["revisions"][0]["payload"]["status"], "COMPLETED")
        for x in prov["results"]:
            self.assertIn("rule_trace", x)
            self.assertTrue(x["rule_trace"])
            self.assertIn("output_hash", x)
            self.assertIn("definition_hash", x)
        published = {x["generation_version"]: x["published"] for x in prov["results"]}
        self.assertTrue(published["1.0.0"])
        self.assertFalse(published["2.0.0"])


class TestGeneratorSmoke(Base):
    def test_generated_stream_separates_the_two_generations(self):
        events = generator.generate(60, seed=3)
        generator.seed_database(self.conn, events)
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        Worker(self.conn, "w").run(generation_id=gid2, max_items=10_000)
        diff = lifecycle.diff_summary(self.conn, gid2)
        self.assertGreater(diff["counts"]["changed"], 0)
        self.assertGreater(len(diff["by_field"]), 0)
        self.assertTrue(diff["samples"])
        sample = diff["samples"][0]
        self.assertIn("base_output", sample)
        self.assertIn("candidate_output", sample)


if __name__ == "__main__":
    unittest.main(verbosity=2)


class TestPartialSwitchCorrections(Base):
    def test_correction_in_unswitched_range_is_not_published_under_new_generation(self):
        """v1 still serves the early range; a correction there must not be
        published to v1 unless v1 still owns that event's window."""
        for i in range(6):
            self.add(f"e{i}", self.t0 + timedelta(hours=i))
        self.drain()
        self.publish_active()
        gid2 = ingest.add_generation(self.conn, "2.0.0")
        self.drain()
        self.approve(gid2, from_time=iso(self.t0 + timedelta(hours=3)))

        # correct an event in the range that still belongs to v1
        corr = ingest.ingest(
            self.conn,
            {
                "source": "test",
                "event_key": "e0",
                "event_time": iso(self.t0),
                "payload": payload(order_id="e0", status="REFUNDED", amount="1.00"),
            },
        )
        self.drain()
        q = query.query(self.conn, from_time=iso(self.t0), to_time=iso(self.t0 + timedelta(hours=1)))
        self.assertEqual(q["rows"][0]["serving"]["version"], "1.0.0")
        self.assertEqual(q["rows"][0]["revision"], 2, "v1 owns this range and republishes its correction")
        self.assertEqual(q["rows"][0]["amount_cents"], 100)

        prov = query.provenance(self.conn, "e0")
        published_by = {
            x["generation_version"]: x["published"] for x in prov["results"] if x["raw_event_id"] == corr["raw_event_id"]
        }
        self.assertTrue(published_by["1.0.0"])
        self.assertFalse(published_by["2.0.0"], "candidate's correction waits for approval")
