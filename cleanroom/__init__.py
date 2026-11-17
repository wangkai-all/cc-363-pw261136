"""Cleanroom: streaming data cleaning with generational rollout.

Modules are intentionally independent entry points so the queue, the worker
and the query service can run as separate processes against one SQLite file:

    python3 -m cleanroom.queue  --db var/cleanroom.db
    python3 -m cleanroom.worker --db var/cleanroom.db
    python3 -m cleanroom.server --db var/cleanroom.db --port 8765
"""
