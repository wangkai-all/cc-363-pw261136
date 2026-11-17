"""HTTP service: read API, operator API, and the static operator console.

Run with `python -m cleanroom.server --port 8765`. The query endpoints and the
operator endpoints share the SQLite database but not a process with the
workers; the web UI only ever calls these endpoints.
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import sys
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from . import common, generator, ingest, lifecycle, query, rules
from .worker import Worker

STATIC = Path(__file__).resolve().parent.parent / "static"
DB_HOLDER: dict[str, str] = {}


def _conn():
    return common.connect(DB_HOLDER.get("db"))


def _svc_backfill(body: dict) -> dict:
    conn = _conn()
    try:
        version = body.get("version")
        definition = body.get("definition")
        if definition is None and version not in rules.BUILTIN_GENERATIONS:
            raise ValueError(f"need a built-in version {sorted(rules.BUILTIN_GENERATIONS)} or a definition")
        gid = ingest.add_generation(
            conn,
            version or str(definition.get("version")),
            label=body.get("label"),
            definition=definition,
            backfill_from=body.get("from_time"),
        )
        p = lifecycle.progress(conn, gid)
        return {"generation_id": gid, "progress": p}
    finally:
        conn.close()


def _svc_work(body: dict) -> dict:
    conn = _conn()
    try:
        n = int(body.get("items", 50))
        gid = body.get("generation_id")
        worker = Worker(conn, body.get("owner") or f"api-{common.new_id()[:6]}")
        done = worker.run(generation_id=gid, max_items=n)
        out = {"processed": done}
        if gid:
            out["progress"] = lifecycle.progress(conn, gid)
        return out
    finally:
        conn.close()


def _svc_drain(body: dict) -> dict:
    """Remove stale claims left by a worker that was killed mid-item."""
    conn = _conn()
    try:
        cut = _seconds_ago(int(body.get("stale_seconds", 30)))
        n = conn.execute(
            "SELECT COUNT(*) n FROM work_item WHERE status = 'running' AND claim_at < ?", (cut,)
        ).fetchone()["n"]
        conn.execute(
            "UPDATE work_item SET status = 'pending', claim_owner = NULL WHERE status = 'running' AND claim_at < ?",
            (cut,),
        )
        conn.execute("DELETE FROM worker_heartbeat WHERE at < ?", (_seconds_ago(15),))
        common.audit(conn, "operator", "work.reclaim", {"reclaimed": n})
        return {"reclaimed": n}
    finally:
        conn.close()


def _need(body: dict, key: str):
    if not body.get(key):
        raise ValueError(f"missing {key!r}")
    return body[key]


def _svc_approve(body: dict) -> dict:
    conn = _conn()
    try:
        return lifecycle.approve(
            conn,
            _need(body, "generation_id"),
            from_time=body.get("from_time"),
            to_time=body.get("to_time"),
            allow_incomplete=bool(body.get("allow_incomplete")),
        )
    finally:
        conn.close()


def _svc_pause(body: dict) -> dict:
    conn = _conn()
    try:
        gid = _need(body, "generation_id")
        if gid == "active":
            gid = (lifecycle.active_generation(conn) or {"id": None})["id"]
            if gid is None:
                raise ValueError("no active generation")
        return lifecycle.pause_backfill(conn, gid, body.get("reason", ""))
    finally:
        conn.close()


def _svc_resume(body: dict) -> dict:
    conn = _conn()
    try:
        gid = _need(body, "generation_id")
        return lifecycle.resume_backfill(conn, gid)
    finally:
        conn.close()


def _svc_suspend(body: dict) -> dict:
    conn = _conn()
    try:
        return lifecycle.suspend(
            conn, _need(body, "generation_id"),
            from_time=_need(body, "from_time"), to_time=_need(body, "to_time"),
            reason=body.get("reason", ""),
        )
    finally:
        conn.close()


def _svc_suspend_interval(body: dict) -> dict:
    """Suspend [from_time, to_time) under whichever generation currently serves its start."""
    conn = _conn()
    try:
        from_time = common.norm_time(_need(body, "from_time"))
        to_time = common.norm_time(_need(body, "to_time"))
        windows = query._windows(conn)
        w = query.resolve_window(windows, from_time)
        gid = w["generation_id"] if w else (lifecycle.active_generation(conn) or {"id": None})["id"]
        if gid is None:
            raise ValueError("no serving generation")
        return lifecycle.suspend(
            conn, gid, from_time=from_time, to_time=to_time, reason=body.get("reason", "")
        )
    finally:
        conn.close()


def _svc_lift(body: dict) -> dict:
    """Remove suspension windows overlapping [from_time, to_time)."""
    conn = _conn()
    try:
        from_time = common.norm_time(_need(body, "from_time"))
        to_time = common.norm_time(_need(body, "to_time"))
        conn.execute("BEGIN IMMEDIATE")
        rows = conn.execute(
            "SELECT * FROM serving_window WHERE kind = 'suspended' AND from_time >= ? AND "
            "COALESCE(to_time, '9999') <= ?",
            (from_time, to_time),
        ).fetchall()
        for r in rows:
            conn.execute("DELETE FROM serving_window WHERE id = ?", (r["id"],))
        common.audit(conn, "operator", "interval.unsuspend",
                     {"from": from_time, "to": to_time, "removed": len(rows)})
        conn.execute("COMMIT")
        return {"removed": len(rows)}
    finally:
        conn.close()


def _svc_ingest(body: dict) -> dict:
    conn = _conn()
    try:
        event = body.get("event")
        if event is None:
            raise ValueError("missing 'event'")
        return ingest.ingest(conn, event, actor=body.get("actor", "queue"))
    finally:
        conn.close()


def _svc_add_arrivals(body: dict) -> dict:
    """Generate `count` new synthetic arrivals, optionally correct an older key."""
    from .queue import Queue

    conn = _conn()
    try:
        count = int(body.get("count", 10))
        q = Queue(conn, source=body.get("source", "manual-feed"), seed=body.get("seed"))
        out: dict = {"accepted": 0, "duplicates": 0, "corrections": []}
        for _ in range(count):
            r = q.tick()
            if r["status"] == "accepted":
                out["accepted"] += 1
            else:
                out["duplicates"] += 1
            if "correction" in r:
                out["corrections"].append(r["correction"])

        correct_key = body.get("correct_event_key")
        if correct_key:
            row = conn.execute(
                "SELECT * FROM raw_event WHERE event_key = ? AND is_current = 1", (correct_key,)
            ).fetchone()
            if row is None:
                raise ValueError(f"no current event with key {correct_key!r}")
            payload = json.loads(row["payload"])
            payload["status"] = body.get("new_status", "refunded")
            payload["amount"] = body.get("new_amount", "0.01")
            out["corrections"].append(
                ingest.ingest(
                    conn,
                    {
                        "source": row["source"],
                        "event_key": row["event_key"],
                        "event_time": row["event_time"],
                        "payload": payload,
                    },
                    actor="correction",
                )
            )
        return out
    finally:
        conn.close()


def _svc_demo_reset(body: dict) -> dict:
    path = DB_HOLDER.get("db")
    if not path:
        raise ValueError("no db configured")
    conn = _conn()
    conn.close()
    import os

    for suffix in ("", "-wal", "-shm"):
        try:
            os.remove(path + suffix)
        except FileNotFoundError:
            pass
    conn = _conn()
    common.init_db(conn)
    events = generator.generate(int(body.get("count", 200)), span_hours=72.0)
    n = generator.seed_database(conn, events)
    from .worker import Worker as W

    current = {
        r["event_key"]: json.loads(r["payload"])
        for r in conn.execute("SELECT event_key, payload FROM raw_event WHERE is_current = 1")
    }
    for ev in generator.corrections(events, current):
        ingest.ingest(conn, ev, actor="generator")
    W(conn, "bootstrap").run(max_items=10**9)
    gid = conn.execute("SELECT id FROM generation WHERE status='active'").fetchone()["id"]
    conn.execute(
        "INSERT INTO published(generation_id, raw_event_id, clean_result_id) "
        "SELECT c.generation_id, c.raw_event_id, c.id FROM clean_result c WHERE c.generation_id = ? "
        "ON CONFLICT(generation_id, raw_event_id) DO UPDATE SET clean_result_id=excluded.clean_result_id",
        (gid,),
    )
    conn.close()
    return {"seeded": n}


def _svc_diff(params: dict) -> dict:
    conn = _conn()
    try:
        cand = params.get("generation_id", [None])[0]
        if not cand:
            rows = lifecycle.candidate_generations(conn)
            if not rows:
                return {"error": "no candidate generation"}
            cand = rows[-1]["id"]
        return lifecycle.diff_summary(conn, cand, params.get("base", [None])[0])
    finally:
        conn.close()


def _svc_proc(body: dict) -> dict:
    from . import procs

    action = body.get("action")
    name = body.get("name")
    db = DB_HOLDER.get("db")
    if action == "start":
        out = procs.start(name, db)
        common.audit(_conn(), "operator", "process.start", out)
        return out
    if action == "stop":
        out = procs.stop(name)
        common.audit(_conn(), "operator", "process.stop", out)
        return out
    raise ValueError("action must be start or stop")


def _svc_procs() -> dict:
    from . import procs

    return procs.state()


def _svc_toggle_queue(body: dict) -> dict:
    conn = _conn()
    try:
        on = bool(body.get("on"))
        common.set_meta(conn, "queue.enabled", "1" if on else "0")
        common.audit(conn, "operator", "queue.toggle", {"on": on})
        return {"queue_enabled": on}
    finally:
        conn.close()


def _svc_status() -> dict:
    conn = _conn()
    try:
        gens = []
        for g in conn.execute("SELECT * FROM generation ORDER BY ordinal").fetchall():
            p = lifecycle.progress(conn, g["id"])
            gens.append({**dict(g), "progress": p})
        cut = common.utcnow()
        workers = [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM worker_heartbeat WHERE at >= ? ORDER BY at DESC",
                (_seconds_ago(10),),
            )
        ]
        recent = conn.execute(
            "SELECT COUNT(*) n FROM raw_event WHERE received_at >= ?", (_seconds_ago(300),)
        ).fetchone()["n"]
        queue_on = common.get_meta(conn, "queue.enabled", "1") == "1"
        queue_beat = common.get_meta(conn, "queue.beat")
        queue_alive = bool(queue_beat and queue_beat >= _seconds_ago(10))
        span = conn.execute(
            "SELECT MIN(event_time) lo, MAX(event_time) hi FROM raw_event WHERE is_current = 1"
        ).fetchone()
        return {
            "generations": gens,
            "active": (lifecycle.active_generation(conn) or {})["id"]
            if lifecycle.active_generation(conn) else None,
            "coverage": query.coverage(conn),
            "workers": workers,
            "supervised": _svc_procs(),
            "queue": {"enabled": queue_on, "alive": queue_alive, "last_beat": queue_beat},
            "arrivals_5m": recent,
            "counts": {
                "raw_current": conn.execute("SELECT COUNT(*) n FROM raw_event WHERE is_current=1").fetchone()["n"],
                "raw_total": conn.execute("SELECT COUNT(*) n FROM raw_event").fetchone()["n"],
                "corrections": conn.execute("SELECT COUNT(*) n FROM raw_event WHERE revision > 1").fetchone()["n"],
                "pending_work": conn.execute(
                    "SELECT COUNT(*) n FROM work_item WHERE status = 'pending'"
                ).fetchone()["n"],
            },
            "audit": [
                dict(r)
                for r in conn.execute("SELECT * FROM audit ORDER BY at DESC, rowid DESC LIMIT 25")
            ],
            "event_span": {"from": span["lo"], "to": span["hi"]},
            "server_time": cut,
        }
    finally:
        conn.close()


def _seconds_ago(seconds: int) -> str:
    from datetime import datetime, timedelta, timezone

    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


ROUTES = {
    "/api/status": lambda body, params: _svc_status(),
    "/api/query": lambda body, params: query.query(
        _conn(),
        from_time=params.get("from", [None])[0],
        to_time=params.get("to", [None])[0],
        limit=int(params.get("limit", ["50"])[0]),
        statuses=params.get("status") or None,
    ),
    "/api/provenance": lambda body, params: query.provenance(
        _conn(), params.get("event_key", [""])[0]
    ),
    "/api/diff": lambda body, params: _svc_diff(params),
    "/api/backfill": lambda body, params: _svc_backfill(body),
    "/api/work": lambda body, params: _svc_work(body),
    "/api/reclaim": lambda body, params: _svc_drain(body),
    "/api/queue": lambda body, params: _svc_toggle_queue(body),
    "/api/proc": lambda body, params: _svc_proc(body),
    "/api/procs": lambda body, params: _svc_procs(),
    "/api/pause": lambda body, params: _svc_pause(body),
    "/api/resume": lambda body, params: _svc_resume(body),
    "/api/approve": lambda body, params: _svc_approve(body),
    "/api/suspend": lambda body, params: _svc_suspend(body),
    "/api/suspend_interval": lambda body, params: _svc_suspend_interval(body),
    "/api/unsuspend": lambda body, params: _svc_lift(body),
    "/api/ingest": lambda body, params: _svc_ingest(body),
    "/api/arrivals": lambda body, params: _svc_add_arrivals(body),
    "/api/demo/reset": lambda body, params: _svc_demo_reset(body),
}


class Handler(BaseHTTPRequestHandler):
    server_version = "Cleanroom/1.0"

    def log_message(self, fmt, *args):  # keep the demo console quiet
        if self.path.startswith("/api/"):
            sys.stderr.write("%s %s\n" % (self.command, self.path))

    def _send(self, code: int, payload, ctype: str = "application/json") -> None:
        if isinstance(payload, (dict, list)):
            body = json.dumps(payload).encode()
            ctype = "application/json"
        elif isinstance(payload, str):
            body = payload.encode()
        else:
            body = payload
        self.send_response(code)
        self.send_header("Content-Type", ctype + "; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _serve_static(self, path: str) -> None:
        rel = "index.html" if path in ("/", "") else path.lstrip("/")
        target = (STATIC / rel).resolve()
        if not str(target).startswith(str(STATIC.resolve())) or not target.is_file():
            self._send(404, {"error": "not found"})
            return
        ctype = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        self._send(200, target.read_bytes(), ctype)

    def do_GET(self):
        u = urlparse(self.path)
        if u.path.startswith("/api/"):
            params = parse_qs(u.query)
            fn = ROUTES.get(u.path)
            if fn is None:
                self._send(404, {"error": f"unknown endpoint {u.path}"})
                return
            try:
                self._send(200, fn({}, params))
            except Exception as exc:  # noqa: BLE001
                traceback.print_exc()
                self._send(400, {"error": str(exc)})
            return
        self._serve_static(u.path)

    def do_POST(self):
        u = urlparse(self.path)
        fn = ROUTES.get(u.path)
        if fn is None:
            self._send(404, {"error": f"unknown endpoint {u.path}"})
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length else b"{}"
            body = json.loads(raw or b"{}")
            if not isinstance(body, dict):
                raise ValueError("body must be a JSON object")
            self._send(200, fn(body, parse_qs(u.query)))
        except Exception as exc:  # noqa: BLE001
            traceback.print_exc()
            self._send(400, {"error": str(exc)})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Cleanroom query + operator service")
    ap.add_argument("--db", default=None)
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8765)
    args = ap.parse_args(argv)

    DB_HOLDER["db"] = str(Path(args.db) if args.db else common.db_path())
    conn = common.connect(DB_HOLDER["db"])
    common.init_db(conn)
    conn.close()

    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"cleanroom serving http://{args.host}:{args.port}  (db {DB_HOLDER['db']})")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        from . import procs

        procs.shutdown()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
