"""Process supervisor for the operator console.

The queue, cleaning worker and query service are separate OS processes. When
the console starts a process it owns the child (`subprocess.Popen`); when a
process was started from a shell instead, the console only observes it through
heartbeats. Stopping a supervised child sends SIGTERM and waits, so the UI's
"exit and resume" control is a real process exit, not a paused loop.
"""
from __future__ import annotations

import os
import signal
import subprocess
import sys
import threading

from . import common

LOCK = threading.Lock()
CHILDREN: dict[str, subprocess.Popen] = {}

COMMANDS = {
    "queue": lambda db: [sys.executable, "-m", "cleanroom.queue", "--db", db, "--rate", "2"],
    "worker": lambda db: [sys.executable, "-m", "cleanroom.worker", "--db", db],
}


def _popen(name: str, db: str) -> subprocess.Popen:
    env = dict(os.environ)
    env["CLEANROOM_DB"] = db
    kwargs = {}
    if os.name == "posix":
        kwargs["start_new_session"] = True
    return subprocess.Popen(COMMANDS[name](db), env=env, **kwargs)


def start(name: str, db: str) -> dict:
    if name not in COMMANDS:
        raise ValueError(f"unknown process {name!r}")
    with LOCK:
        proc = CHILDREN.get(name)
        if proc is not None and proc.poll() is None:
            return {"name": name, "state": "already running", "pid": proc.pid}
        CHILDREN[name] = _popen(name, db)
        return {"name": name, "state": "started", "pid": CHILDREN[name].pid}


def stop(name: str, timeout: float = 8.0) -> dict:
    if name not in COMMANDS:
        raise ValueError(f"unknown process {name!r}")
    with LOCK:
        proc = CHILDREN.get(name)
        if proc is None or proc.poll() is not None:
            CHILDREN.pop(name, None)
            return {"name": name, "state": "not running (not started by this console)"}
        proc.terminate()
        try:
            proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
        CHILDREN.pop(name, None)
        return {"name": name, "state": "stopped", "pid": proc.pid, "code": proc.returncode}


def state() -> dict:
    out = {}
    with LOCK:
        for name in COMMANDS:
            proc = CHILDREN.get(name)
            running = proc is not None and proc.poll() is None
            out[name] = {
                "supervised": True,
                "running": running,
                "pid": proc.pid if proc else None,
                "exit_code": None if running or proc is None else proc.returncode,
            }
    return out


def shutdown() -> None:
    for name in list(CHILDREN):
        try:
            stop(name, timeout=3)
        except Exception:  # noqa: BLE001
            pass
