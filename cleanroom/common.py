"""Shared helpers: DB connection, clock, ids, canonical JSON."""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

DB_ENV = "CLEANROOM_DB"
DEFAULT_DB = Path(__file__).resolve().parent.parent / "var" / "cleanroom.db"


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def norm_time(value) -> str:
    """Canonical UTC timestamp so string comparison equals time comparison."""
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def new_id(prefix: str = "") -> str:
    raw = uuid.uuid4().hex[:16]
    return f"{prefix}{raw}" if prefix else raw


def db_path() -> Path:
    return Path(os.environ.get(DB_ENV, DEFAULT_DB))


def connect(path: Path | str | None = None, *, readonly: bool = False) -> sqlite3.Connection:
    p = Path(path) if path is not None else db_path()
    if readonly:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True, timeout=30, isolation_level=None)
    else:
        p.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(p, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    if not readonly:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
    return conn


def canonical(obj) -> str:
    return json.dumps(obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def canonical_hash(obj) -> str:
    import hashlib

    return hashlib.sha256(canonical(obj).encode("utf-8")).hexdigest()


SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

-- A rule generation is an immutable, versioned cleaning algorithm.
CREATE TABLE IF NOT EXISTS generation (
  id TEXT PRIMARY KEY,
  ordinal INTEGER NOT NULL UNIQUE,
  label TEXT NOT NULL,
  version TEXT NOT NULL,
  definition TEXT NOT NULL,          -- canonical JSON of rules
  definition_hash TEXT NOT NULL,
  status TEXT NOT NULL,              -- building | ready | active | retired | rejected
  paused INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  activated_at TEXT,
  retired_at TEXT,
  backfill_from TEXT,                -- ISO from-time, NULL = full history
  notes TEXT NOT NULL DEFAULT ''
);

-- Immutable raw inputs. Every arrival is a row; corrections create new rows
-- that supersede older ones by (source, event_key).
CREATE TABLE IF NOT EXISTS raw_event (
  id TEXT PRIMARY KEY,
  seq INTEGER NOT NULL UNIQUE,       -- ingestion order, monotonic
  source TEXT NOT NULL,
  event_key TEXT NOT NULL,
  event_time TEXT NOT NULL,
  revision INTEGER NOT NULL,
  payload TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  received_at TEXT NOT NULL,
  corrupt INTEGER NOT NULL DEFAULT 0,
  supersedes TEXT,
  is_current INTEGER NOT NULL DEFAULT 1
);
CREATE INDEX IF NOT EXISTS raw_event_current ON raw_event(source, event_key) WHERE is_current = 1;
CREATE INDEX IF NOT EXISTS raw_event_time ON raw_event(event_time);

-- Pointwise membership: which raw revisions a generation must clean.
-- generation x raw_event. Revisions never disappear once assigned.
CREATE TABLE IF NOT EXISTS work_item (
  generation_id TEXT NOT NULL REFERENCES generation(id),
  raw_event_id TEXT NOT NULL REFERENCES raw_event(id),
  status TEXT NOT NULL DEFAULT 'pending',  -- pending | done | skipped | failed
  attempt INTEGER NOT NULL DEFAULT 0,
  claim_owner TEXT,
  claim_at TEXT,
  finished_at TEXT,
  last_error TEXT,
  PRIMARY KEY (generation_id, raw_event_id)
);
CREATE INDEX IF NOT EXISTS work_item_pending ON work_item(generation_id, status, claim_at);

-- Cleaned derivations. Never overwritten: a re-clean writes a new row.
CREATE TABLE IF NOT EXISTS clean_result (
  id TEXT PRIMARY KEY,
  generation_id TEXT NOT NULL REFERENCES generation(id),
  raw_event_id TEXT NOT NULL REFERENCES raw_event(id),
  status TEXT NOT NULL,              -- clean | corrected | dropped
  output TEXT NOT NULL,              -- canonical JSON
  output_hash TEXT NOT NULL,
  rule_trace TEXT NOT NULL,          -- JSON list of applied rule ids
  quality_flags TEXT NOT NULL,       -- JSON list
  computed_at TEXT NOT NULL,
  worker TEXT,
  UNIQUE (generation_id, raw_event_id)
);
CREATE INDEX IF NOT EXISTS clean_result_gen ON clean_result(generation_id, status);

-- Which clean result is published for a raw event, per generation.
-- Publish is generation-wide and atomic, so readers never straddle.
CREATE TABLE IF NOT EXISTS published (
  generation_id TEXT NOT NULL,
  raw_event_id TEXT NOT NULL,
  clean_result_id TEXT NOT NULL,
  PRIMARY KEY (generation_id, raw_event_id)
);

-- Which generation serves each event-time interval. Readers resolve raw ->
-- interval -> generation, so a switch only flips the pointer row.
CREATE TABLE IF NOT EXISTS serving_window (
  id TEXT PRIMARY KEY,
  generation_id TEXT NOT NULL REFERENCES generation(id),
  from_time TEXT NOT NULL,
  to_time TEXT,                       -- NULL = open-ended
  kind TEXT NOT NULL,                 -- baseline | backfill | shadow
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS serving_window_gen ON serving_window(generation_id);

-- Audit trail of operations that change what readers see.
CREATE TABLE IF NOT EXISTS audit (
  id TEXT PRIMARY KEY,
  at TEXT NOT NULL,
  actor TEXT NOT NULL,
  action TEXT NOT NULL,
  detail TEXT NOT NULL
);

-- Live worker presence, refreshed while a cleaning process is running.
CREATE TABLE IF NOT EXISTS worker_heartbeat (
  owner TEXT PRIMARY KEY,
  at TEXT NOT NULL,
  generation_id TEXT,
  processed INTEGER NOT NULL DEFAULT 0
);
"""


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def get_meta(conn, key: str, default: str | None = None) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_meta(conn, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def audit(conn, actor: str, action: str, detail: dict) -> None:
    conn.execute(
        "INSERT INTO audit(id, at, actor, action, detail) VALUES(?,?,?,?,?)",
        (new_id("aud_"), utcnow(), actor, action, canonical(detail)),
    )
