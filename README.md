# Cleanroom — streaming cleaning with generational rollout

Cleanroom is a small, runnable operations application for a data-cleaning
pipeline that never stops ingesting. It answers three questions concretely:

1. **How do I roll out new cleaning rules without readers seeing a splice of
   old and new algorithms?** Each rule set is an immutable *generation*. It
   cleans into its own result rows; nothing reaches readers until a single
   approving transaction publishes it and flips the serving windows.
2. **How does an event that arrives mid-backfill land on the right generation?**
   Generation assignment happens **at arrival**, not at clean time. An event
   that lands while v2 is building is queued for both v1 and v2.
3. **What happens when a source corrects an event during a backfill?** A
   correction is a new immutable revision. It is queued for every generation
   that could ever serve that key — including the building one — so it cannot
   be lost, and it cannot be "cleaned by v1 and silently inherited by v2".

Everything is implemented from scratch on Python 3 + SQLite (WAL) and vanilla
HTML/CSS/JS. There are no third-party runtime dependencies.

---

## Quick start

```bash
# 1. create and seed the demo database (synthetic events, v1.0.0 serving)
python3 -m cleanroom.generator --db var/cleanroom.db --reset --count 200

# 2. run the three independent processes (three terminals)
python3 -m cleanroom.queue  --db var/cleanroom.db --rate 2
python3 -m cleanroom.worker --db var/cleanroom.db
python3 -m cleanroom.server --db var/cleanroom.db --port 8765

# 3. open http://127.0.0.1:8765
```

Or start the queue and worker from the console's **Processes** card — the
server supervises those children and can stop them for real (SIGTERM), which
is how the "exit and resume" demo works.

### Demo script (5 minutes)

1. **Prepare v2.** In *Generations → Prepare a new rule generation*, leave the
   backfill range empty and click **Start backfill**. The candidate card shows
   progress; the serving KPI still says v1.0.0.
2. **Watch the shadow difference.** The *Shadow difference* card compares the
   candidate against the serving generation: changed event count, change rate,
   field-level edits, and sample before/after values.
3. **Pause / resume.** Pause the backfill. The badge changes to *backfill
   paused*, the worker stops claiming items, and the query service keeps
   serving v1. Resume and the worker picks up the remaining items.
4. **Let arrivals land during the backfill.** Keep the queue running (or press
   **Start queue**). Each arrival is pinned to *both* generations. The switch
   control refuses while work inside the requested switch range is still
   pending; arrivals after the range's end stay on the old generation until a
   later approval.
5. **Approve a slice, not the world.** Set a From/To in *Switch control* and
   approve. Query explorer for an early window still shows v1.0.0; a window
   inside the slice shows v2.0.0. Each query is one snapshot, so no page shows
   half of each.
6. **Corrections during the switch.** Use the console's arrivals API, or send
   one with `curl`:

   ```bash
   curl -s -X POST localhost:8765/api/arrivals \
     -d '{"count":0,"correct_event_key":"ord-00010","new_status":"refunded"}'
   ```

   Click the row in *Query explorer* to expand it: the provenance panel shows
   every raw revision, every generation's derivation (including the un-published
   v1 and v2 rows), the rule trace, and which generation re-served the revised
   event. The replacement is a new revision with a new result — the old
   derivation is retained, never overwritten.
7. **Exit and resume a process.** Click **Stop worker (exit)**. Arrivals keep
   queueing; the *work queue* row grows. Click **Start worker** and it drains
   from where the queue left off. In-flight claims from a killed worker are
   reclaimed automatically after 30 seconds (or immediately with **Reclaim stale
   claims**).
8. **Pause a range instead of serving it.** *Suspend this range* turns the
   interval into explicit gaps: queries return `gaps[]` with the window id and
   the reason, and coverage drops visibly. *Lift* removes the suspension.
   This is the visible-cost alternative to double-serving.

---

## Architecture

```
  arrivals ──▶ queue process ──▶ raw_event (append-only, revisions)
                                     │
                                     │ arrival-time pinning
                                     ▼
                              work_item (generation × revision)
                                     │
                          ┌──────────┴──────────┐
                          ▼                     ▼
                    worker process        worker process      ← claim with UPDATE … RETURNING
                          │                     │
                          ▼                     ▼
                     clean_result (per generation, NEVER overwritten)
                          │
              approval: one IMMEDIATE transaction
                          ▼
                     published (generation × revision → result)  +  serving_window flips
                          │
                          ▼
                  query service (single read snapshot)
```

### Data model

| Table | Why it exists |
|---|---|
| `raw_event` | Immutable arrivals. Every correction is a new row with `revision+1` and `supersedes`; `is_current` marks the head. Nothing is updated in place except the head flag. |
| `generation` | Immutable rule definition + status (`building`/`ready`/`active`/`retired`), `paused` flag, and backfill scope. |
| `work_item` | Pointwise membership: one row per (generation, raw revision). This is the queue. Items are never deleted, so a revision that was assigned at arrival is always cleaned eventually. |
| `clean_result` | One derivation per (generation, revision): output, output hash, rule trace, quality flags, worker, timestamp. `UNIQUE(generation_id, raw_event_id)`, replaced only when a re-clean of the *same* revision happens (idempotent retry). |
| `published` | What readers may see for a generation. Written only by approval (or by the worker for the currently *active* generation's own live corrections). |
| `serving_window` | Event-time → generation intervals. `baseline` / `backfill` / `suspended`. Readers resolve through these. |
| `audit` | Every reader-visible change, with actor and detail JSON. |
| `worker_heartbeat` | Liveness for the console; removed on clean shutdown. |

### Why a query cannot stitch old and new

A query opens one `BEGIN` (a consistent WAL snapshot) and reads:

1. `serving_window` rows to map `event_time → generation_id`
2. `published` joined to `clean_result` for `(generation, raw_event)`

Approval writes the `published` rows and replaces the window rows in a **single
`BEGIN IMMEDIATE` transaction**. Since the read snapshot predates or postdates
that commit as a whole, the reader sees the old window set with old published
pointers, or the new window set with new published pointers. Partial-interval
switches split straddling windows and insert one new window, all inside the
same transaction. `tests/test_pipeline.py::TestIsolation` hammers a switch from
a reader thread and asserts it never sees a mixed result set.

### Who owns which state

- **Queue**: writes `raw_event`, `work_item`, `queue.beat`. Never touches
  results.
- **Worker**: claims `work_item`, writes `clean_result`. Publishes a live
  correction only for a generation that is already `active` (its serving set is
  its own and no approval is pending for it).
- **Server**: serves reads; operator actions write `generation.paused`,
  `published`, `serving_window`, `audit`; it can spawn/signal queue and worker
  children.
- **Generator** (`cleanroom.generator`): seeds a demo database only.

### The visibility boundary, and its cost

Two acceptable strategies were considered. This app implements both and makes
the choice explicit:

| Strategy | What readers see | Cost | Where it is |
|---|---|---|---|
| **Double-clean, single-publish** (default) | Old generation, whole, until approval. The candidate is computed in parallel and published atomically. | Extra cleaning compute and storage (one result row per generation per revision). No reader-visible degradation. | Automatic: `clean_result` rows for the candidate exist but are invisible to readers. |
| **Suspend the affected range** | Explicit gaps: `gaps[]` entries with window id + reason, lower coverage %, no fabricated values. | An outage for that interval; downstream consumers must handle gaps. | *Suspend this range* in the console, or `POST /api/suspend_interval`. |

The console surfaces both: shadow diff tells you *what* would change before you
pay for the switch, and coverage tells you *what would be lost* if you choose
to suspend an interval instead. There is no silent third option.

### Generation pinning semantics (the requirement that usually breaks)

- `ingest()` appends a raw revision and inserts a `work_item` for every
  generation in `active`, `building`, or `ready` **at that moment**.
- A correction also re-queues the new revision for every one of those
  generations (deduplicated via `ON CONFLICT DO NOTHING`). It never modifies
  `clean_result` in place.
- When the worker cleans a revision for the active generation, it atomically
  updates `published` for that one `(generation, revision)`. Readers of that
  generation therefore pick up a correction within one query, while readers of
  a building generation keep seeing the old revision until approval.
- A candidate's `pending` count includes arrivals that landed after the initial
  backfill snapshot. Approval is refused when unfinished work falls **inside the
  range being switched**; work after the range end (or before a bounded switch)
  stays on the generation that serves it until a later approval. `allow_incomplete`
  overrides across the whole range and the UI never sends it.

### Rules and provenance

Rules are declarative JSON (trim, case, money, int, alias, email, required,
range). Each generation stores the full definition and its SHA-256. Each result
stores the applied rule trace and quality flags. The *Query explorer* "expand"
panel shows, for one event: the current raw payload, earlier revisions, the
serving window, per-generation outputs (including unpublished ones), rule ids
with outcomes/details, the output hash, and the worker/time that computed it.

v1.0.0 is a deliberately naive "legacy" normalizer (float money, exact-case
statuses, no validation). v2.0.0 fixes the money parser (exact decimal, decimal
comma, thousands separators), folds currency/region/status aliases, validates
e-mail, upper-cases order ids, and drops rows missing core fields. That is what
produces the meaningful shadow diff.

---

## HTTP API

| Method | Path | Purpose |
|---|---|---|
| GET | `/api/status` | Generations + progress, coverage, workers, queue heartbeat, audit, event span. |
| GET | `/api/query?from&to&limit&status` | Read served rows + explicit `gaps[]` + serving windows. |
| GET | `/api/provenance?event_key=` | Raw revisions, all derivations, publication flags, windows. |
| GET | `/api/diff?generation_id=` | Shadow diff of a candidate against the serving generation. |
| POST | `/api/backfill` | `{version|definition, label, from_time}` → new generation + work items. |
| POST | `/api/work` | `{items, generation_id?}` → run an inline worker batch. |
| POST | `/api/pause`, `/api/resume` | `{generation_id}` → backfill pause flag. |
| POST | `/api/approve` | `{generation_id, from_time?, to_time?, allow_incomplete?}` → publish + switch window(s). |
| POST | `/api/suspend_interval` | `{from_time, to_time, reason}` → explicit gaps. |
| POST | `/api/unsuspend` | `{from_time, to_time}` → remove suspension windows. |
| POST | `/api/arrivals` | `{count, correct_event_key?, new_status?, new_amount?}` → synthetic arrivals/correction. |
| POST | `/api/ingest` | `{event}` → append an arbitrary raw event. |
| POST | `/api/queue` | `{on}` → ingest on/off (process liveness separate). |
| POST | `/api/proc` | `{action: start\|stop, name: queue\|worker}` → supervised process control. |
| POST | `/api/reclaim` | `{stale_seconds}` → reclaim abandoned claims. |
| POST | `/api/demo/reset` | Reseed the demo database. |

## Tests

```bash
python3 -m unittest discover -s tests -v
```

The suite uses temporary databases and covers:

- arrival-time pinning of consecutive generations,
- a correction arriving during a backfill reaching both generations,
- approval publishing only after the work set is complete,
- a partial-interval switch leaving the rest of the range on the old generation,
- a concurrent reader never observing a mixed generation set across a switch,
- suspension producing explicit gaps in query output,
- idempotent re-cleaning and stale-claim recovery,
- provenance exposing raw input, rule trace, and version for a value.
