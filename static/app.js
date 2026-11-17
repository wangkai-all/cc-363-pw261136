"use strict";

const state = {
  status: null,
  diff: null,
  diffView: "chart",
  query: null,
  openKey: null,
  provenance: new Map(),
  busy: new Set(),
  timer: null,
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function el(tag, attrs = {}, ...kids) {
  const node = document.createElement(tag);
  for (const [k, v] of Object.entries(attrs || {})) {
    if (v === null || v === undefined || v === false) continue;
    if (k === "class") node.className = v;
    else if (k === "text") node.textContent = v;
    else if (k.startsWith("on") && typeof v === "function") node.addEventListener(k.slice(2), v);
    else node.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of kids.flat()) {
    if (kid === null || kid === undefined || kid === false) continue;
    node.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return node;
}

function svg(tag, attrs = {}, ...kids) {
  const node = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const [k, v] of Object.entries(attrs)) node.setAttribute(k, v);
  for (const kid of kids.flat()) if (kid) node.append(kid);
  return node;
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  let body = null;
  try { body = await res.json(); } catch { body = null; }
  if (!res.ok) {
    const msg = (body && body.error) || `${res.status} ${res.statusText}`;
    throw new Error(msg);
  }
  return body;
}

const post = (path, body) => api(path, { method: "POST", body: JSON.stringify(body || {}) });

/* ---------------- formatting ---------------- */
const nf = new Intl.NumberFormat("en-US");
const compact = (n) => {
  if (n === null || n === undefined) return "–";
  const abs = Math.abs(n);
  if (abs >= 1e6) return (n / 1e6).toFixed(1).replace(/\.0$/, "") + "M";
  if (abs >= 1e4) return (n / 1e3).toFixed(1).replace(/\.0$/, "") + "K";
  return nf.format(n);
};
const money = (cents) => cents === null || cents === undefined ? "–"
  : new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" }).format(cents / 100);
const pct = (n) => `${(n ?? 0).toFixed(1)}%`;
const clock = (iso) => {
  if (!iso) return "–";
  const d = new Date(iso);
  if (Number.isNaN(d.valueOf())) return "–";
  return d.toLocaleString(undefined, { month: "short", day: "2-digit", hour: "2-digit", minute: "2-digit" });
};
const relTime = (iso) => {
  if (!iso) return "–";
  const s = Math.round((Date.now() - new Date(iso).valueOf()) / 1000);
  if (s < 5) return "just now";
  if (s < 60) return `${s}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
};
const localInput = (iso) => {
  if (!iso) return "";
  const d = new Date(iso);
  const pad = (x) => String(x).padStart(2, "0");
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
};
const isoOf = (value) => value ? new Date(value).toISOString() : null;

function banner(message, kind = "info", sticky = false) {
  const node = $("#banner");
  node.textContent = message;
  node.className = `banner ${kind}`;
  node.hidden = false;
  if (!sticky) {
    clearTimeout(banner._t);
    banner._t = setTimeout(() => { node.hidden = true; }, 7000);
  }
}

function withBusy(button, fn) {
  return async (...args) => {
    if (!button) return fn(...args);
    if (button.getAttribute("aria-busy") === "true") return;
    button.setAttribute("aria-busy", "true");
    button.disabled = true;
    try { await fn(...args); }
    catch (err) { banner(err.message || String(err), "err"); }
    finally {
      button.removeAttribute("aria-busy");
      button.disabled = false;
    }
  };
}

/* ---------------- status / KPI ---------------- */
function renderStatus(s) {
  state.status = s;
  const active = s.generations.find((g) => g.id === s.active) || null;
  $("#kpi-active").textContent = active ? active.version : "none";
  $("#kpi-active").classList.add("sm");
  $("#kpi-active-foot").textContent = active
    ? `${active.label} · ${active.progress.done.toLocaleString()} of ${active.progress.total.toLocaleString()} published`
    : "no serving generation";

  $("#kpi-events").textContent = compact(s.counts.raw_current);
  $("#kpi-events-foot").textContent =
    `${compact(s.counts.raw_total)} raw revisions · ${compact(s.counts.corrections)} corrections`;

  const q = s.queue || {};
  $("#kpi-arrivals").textContent = compact(s.arrivals_5m);
  const qdot = el("span", { class: "dot" });
  qdot.style.background = q.alive ? "var(--good)" : "var(--critical)";
  $("#kpi-arrivals-foot").replaceChildren(
    qdot,
    q.alive ? `queue running${q.enabled ? "" : " · paused by operator"}` : "queue process down · arrivals queued only via manual trigger"
  );

  const cov = s.coverage;
  $("#kpi-coverage").textContent = pct(cov.pct);
  $("#kpi-coverage-foot").textContent =
    `${compact(cov.served)} served · ${compact(cov.suspended)} suspended · ${compact(cov.unserved)} unserved`;

  renderGenerations(s.generations);
  renderProcesses(s);
  renderAudit(s.audit || []);
  renderTimeline(s);
}

const GEN_COLORS = { "1": "var(--series-1)", "2": "var(--series-3)" };

function renderTimeline(s) {
  const host = $("#timeline");
  const legend = $("#timeline-legend");
  host.replaceChildren();
  legend.replaceChildren();
  const span = s.event_span || {};
  const windows = (s.coverage && s.coverage.windows) || [];
  if (!span.from || !span.to || !windows.length) {
    host.append(el("div", { class: "empty", text: "No serving windows yet." }));
    return;
  }
  const lo = new Date(span.from).valueOf();
  const hi = new Date(span.to).valueOf();
  const pad = Math.max(1, (hi - lo) * 0.02);
  const start = lo - pad;
  const total = (hi + pad) - start;
  const gens = Object.fromEntries(s.generations.map((g) => [g.id, g]));

  const bar = el("div", { class: "cov-bar", role: "img", "aria-label": "Serving windows by generation" });
  const entries = windows
    .map((w) => ({ ...w, s: Math.max(0, new Date(w.from_time).valueOf()), e: w.to_time ? new Date(w.to_time).valueOf() : hi + pad }))
    .filter((w) => w.e > w.s)
    .sort((a, b) => a.s - b.s);

  const seenGens = new Set();
  for (const w of entries) {
    const s = Math.max(w.s, start);
    const e = Math.min(w.e, hi + pad);
    if (e <= s) continue;
    const width = Math.max(0.4, ((e - s) / total) * 100);
    const g = gens[w.generation_id];
    let color = "var(--text-muted)";
    let label = "unknown";
    if (w.kind === "suspended") { color = "var(--warning)"; label = "suspended"; }
    else if (g) {
      const major = g.version.split(".")[0];
      color = GEN_COLORS[major] || "var(--series-4)";
      label = `v${g.version}`;
      seenGens.add(`v${g.version}`);
    }
    const fromLabel = w.s < start ? clock(span.from) : clock(w.from_time);
    const toLabel = !w.to_time ? "now" : clock(w.to_time);
    const seg = el("div", {
      class: "cov-seg", tabindex: "0", role: "img",
      "aria-label": `${label} from ${fromLabel} to ${toLabel}`,
      style: `flex:0 0 ${width}%; background:${color}`,
    });
    seg.title = `${label} · ${fromLabel} → ${toLabel}`;
    bar.append(seg);
  }
  host.append(bar);

  const rows = el("div", { class: "cov-legend" });
  for (const v of [...seenGens].sort()) {
    const major = v.replace("v", "").split(".")[0];
    rows.append(el("span", { class: "k" },
      el("span", { class: "sw", style: `background:${GEN_COLORS[major] || "var(--series-4)"}` }), v));
  }
  rows.append(el("span", { class: "k" }, el("span", { class: "sw", style: "background:var(--warning)" }), "suspended"));
  legend.append(rows);

  const suspended = windows.filter((w) => w.kind === "suspended");
  if (suspended.length) {
    host.append(el("div", { class: "meter-caption" },
      ...suspended.slice(0, 5).map((w) => el("span", {},
        "gap ", el("b", { text: `${clock(w.from_time)} → ${clock(w.to_time)}` })))));
  }

  // Table twin of the bar: every interval with its generation and kind.
  const table = $("#timeline-table");
  if (table) {
    table.replaceChildren();
    if (!$("#timeline-table-toggle")?.checked) {
      table.hidden = true;
    } else {
      table.hidden = false;
      table.append(el("table", {},
        el("thead", {}, el("tr", {},
          el("th", { text: "From" }), el("th", { text: "To" }), el("th", { text: "Generation" }), el("th", { text: "Kind" }))),
        el("tbody", {}, ...entries.map((w) => {
          const g = gens[w.generation_id];
          return el("tr", {},
            el("td", { class: "mono", text: clock(w.from_time) }),
            el("td", { class: "mono", text: w.kind === "suspended" ? clock(w.to_time) : (w.to_time ? clock(w.to_time) : "open-ended") }),
            el("td", { text: w.kind === "suspended" ? "—" : g ? `v${g.version}` : "unknown" }),
            el("td", { text: w.kind })
          );
        }))
      ));
    }
  }
}

function statusBadge(g) {
  const p = g.progress;
  if (g.status === "active" && p.remaining > 0)
    return el("span", { class: "badge building" }, el("span", { class: "glyph", text: "◐" }), "serving · catching up");
  if (g.status === "active") return el("span", { class: "badge active" }, el("span", { class: "glyph", text: "●" }), "serving");
  if (g.status === "building" && g.paused) return el("span", { class: "badge paused" }, el("span", { class: "glyph", text: "❙❙" }), "backfill paused");
  if (g.status === "building") return el("span", { class: "badge building" }, el("span", { class: "glyph", text: "◐" }), "backfilling");
  if (g.status === "ready") return el("span", { class: "badge building" }, el("span", { class: "glyph", text: "✓" }), "ready");
  if (g.status === "retired") return el("span", { class: "badge retired" }, el("span", { class: "glyph", text: "○" }), "retired");
  return el("span", { class: "badge rejected" }, el("span", { class: "glyph", text: "×" }), g.status);
}

function renderGenerations(gens) {
  const host = $("#gens");
  host.replaceChildren();
  if (!gens.length) {
    host.append(el("div", { class: "empty", text: "No generations yet." }));
    return;
  }
  for (const g of gens) {
    const p = g.progress;
    const fillClass = p.remaining === 0 ? "good" : "warn";
    const card = el("div", { class: `gen ${g.status === "active" ? "serving" : ""}` },
      el("div", { class: "gen-head" },
        el("span", { class: "gen-ver", text: `v${g.version}` }),
        el("span", { class: "gen-label", text: g.label }),
        statusBadge(g),
        el("span", { class: "gen-meta", text: `created ${relTime(g.created_at)}` })
      ),
      el("div", { class: "meter-row" },
        el("div", { class: "meter", role: "progressbar", "aria-valuemin": 0, "aria-valuemax": 100, "aria-valuenow": p.pct, "aria-label": `Backfill progress v${g.version}` },
          el("div", { class: `fill ${fillClass}` }),
        ),
        el("span", { class: "meter-val", text: `${pct(p.pct)}` })
      ),
      el("div", { class: "meter-caption" },
        el("span", {}, "done ", el("b", { text: compact(p.done) })),
        el("span", {}, "pending ", el("b", { text: compact(p.pending) })),
        el("span", {}, "failed ", el("b", { text: compact(p.failed) })),
        el("span", {}, "scope ", el("b", { text: g.backfill_from ? `from ${clock(g.backfill_from)}` : "full history" }))
      )
    );
    card.querySelector(".fill").style.width = `${Math.max(p.pct, p.total ? 2 : 0)}%`;

    if (g.status === "building") {
      const actions = el("div", { class: "gen-actions" });
      const pauseLabel = g.paused ? "Resume backfill" : "Pause backfill";
      const pauseBtn = el("button", {
        class: "btn", type: "button", text: pauseLabel,
        onclick: () => withBusy(pauseBtn, () => g.paused
          ? act("/api/resume", { generation_id: g.id }, `Resumed v${g.version}`)
          : act("/api/pause", { generation_id: g.id, reason: "operator paused" }, `Paused v${g.version}`))(),
      });
      const drainBtn = el("button", {
        class: "btn ghost", type: "button", text: "Worker · drain 100",
        onclick: () => withBusy(drainBtn, () => act("/api/work", { items: 100, generation_id: g.id }, `Drained work for v${g.version}`))(),
      });
      actions.append(pauseBtn, drainBtn);
      card.append(actions);
    }
    host.append(card);
  }
}

function renderProcesses(s) {
  const host = $("#procs");
  host.replaceChildren();
  const active = s.generations.find((g) => g.id === s.active);
  const q = s.queue || {};
  const qOn = q.alive && q.enabled;
  const sup = s.supervised || {};
  const supSuffix = (name) => {
    const p = sup[name];
    if (!p || !p.supervised) return "";
    return p.running ? ` · pid ${p.pid}` : p.exit_code !== null && p.exit_code !== undefined ? ` · exited (${p.exit_code})` : "";
  };
  host.append(procRow(q.alive ? (qOn ? "good" : "warn") : "bad",
    "queue", (q.alive ? (qOn ? "ingesting" : "idle · paused by operator") : "not running") + supSuffix("queue"),
    q.last_beat ? `last beat ${relTime(q.last_beat)}` : "no heartbeat"));

  const workers = s.workers || [];
  host.append(procRow(workers.length ? "good" : "bad", "worker",
    (workers.length ? `${workers.length} cleaning` : "not running") + supSuffix("worker"),
    workers.length
      ? workers.map((w) => `${w.owner} · ${w.processed} items`).join(" · ")
      : "arrivals wait in work_item until a worker runs"));
  host.append(procRow("good", "query service", "serving snapshots",
    active ? `v${active.version} · coverage ${pct(s.coverage.pct)}` : "no active generation"));

  const p = s.counts.pending_work;
  host.append(procRow(p > 0 ? "warn" : "good", "work queue",
    p > 0 ? `${compact(p)} pending` : "empty",
    p > 0 ? "each pending item is one (generation, revision) pair" : "all arrivals cleaned"));
}
function procRow(led, name, detail, state) {
  return el("div", { class: "proc" },
    el("span", { class: `proc-led ${led}` }),
    el("span", { class: "proc-name" }, name, el("span", { class: "proc-detail", style: "margin-left:8px", text: detail })),
    el("span", { class: "proc-state", text: state })
  );
}

function renderAudit(items) {
  const host = $("#audit");
  host.replaceChildren();
  if (!items.length) { host.append(el("li", { class: "muted", text: "No activity yet." })); return; }
  for (const a of items) {
    let detail = {};
    try { detail = JSON.parse(a.detail); } catch { detail = { raw: a.detail }; }
    const summary = shortDetail(detail);
    host.append(el("li", {},
      el("time", { text: new Date(a.at).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" }) }),
      el("div", {},
        el("div", { class: "act" }, el("b", { text: a.action }), ` · ${a.actor}`),
        summary ? el("div", { class: "detail", text: summary }) : null
      )
    ));
  }
}

function shortDetail(d) {
  const bits = [];
  for (const k of ["version", "from", "to", "published", "work_items", "generation_id", "reason", "reclaimed", "on", "correction", "revision", "event_key"]) {
    if (d[k] === undefined || d[k] === null || d[k] === "" || d[k] === false) continue;
    let v = d[k];
    if (k === "generation_id") v = String(v).slice(0, 12);
    if (k === "from" || k === "to") v = clock(v);
    if (Array.isArray(v)) v = v.length + " ids";
    bits.push(`${k}=${v}`);
  }
  return bits.join("  ");
}

/* ---------------- diff ---------------- */
function renderDiff(d) {
  state.diff = d;
  const host = $("#diff-body");
  host.replaceChildren();
  if (!d || d.error) {
    host.append(el("div", { class: "empty", text: (d && d.error) || "No candidate generation to compare." }));
    $("#btn-approve") && ($("#btn-approve").disabled = true);
    return;
  }
  const c = d.counts;
  const cand = (state.status?.generations || []).find((g) => g.id === d.candidate_generation_id);
  const base = (state.status?.generations || []).find((g) => g.id === d.base_generation_id);

  if (state.diffView === "table") return renderDiffTable(host, d, cand, base);

  const head = el("div", { class: "diff-nums" },
    el("div", {},
      el("div", { class: "diff-hero" },
        el("span", { class: "hero", text: compact(c.changed) }),
        el("span", { class: "hero-unit", text: `of ${compact(c.compared)} events change` }),
      ),
      el("div", { class: "diff-mini" },
        mini("Change rate", pct(c.changed_pct)),
        mini("Field edits", compact(c.field_changes)),
        mini("Status flips", compact(c.status_flip)),
        mini("Unchanged", compact(c.same))
      ),
      el("p", { class: "muted", style: "margin-top:12px" },
        `Candidate `, el("b", { text: `v${cand ? cand.version : "?"}` }), " would replace serving ",
        el("b", { text: `v${base ? base.version : "?"}` }), ` across ${compact(c.compared)} current events in the comparable scope.`)
    ),
    el("div", {},
      el("h3", { style: "margin-bottom:8px", text: "Events changed, by field" }),
      el("div", { class: "bars" }, Object.entries(d.by_field).map(([field, n]) => barRow(field, n, c.compared)))
    )
  );
  host.append(head);

  if (Object.keys(d.by_status).length) {
    host.append(el("div", { class: "meter-caption", style: "font-size:12px" },
      el("span", { text: "Status transitions: " }),
      ...Object.entries(d.by_status).map(([k, n]) => el("span", {}, k.replace("→", " → "), " ", el("b", { text: compact(n) })))));
  }

  const samples = el("div", { class: "diff-samples" },
    el("h3", { text: `Sample changed events (${d.samples.length} shown)` }),
    el("div", { class: "sample-list" },
      ...d.samples.slice(0, 12).map((s) => el("div", { class: "sample" },
        el("span", { class: "sample-key", text: s.event_key }),
        el("div", { class: "sample-vals" },
          ...s.changed_fields.map((f) => el("span", { class: "sample-line" },
            el("span", { class: "chip was", text: `${f}: ${fmtVal(f, s.base_output[f])}` }),
            el("span", { class: "muted", text: "→ " }),
            el("span", { class: "chip now", text: `${f}: ${fmtVal(f, s.candidate_output[f])}` })
          ))
        )
      ))
    )
  );
  host.append(samples);
}

function mini(label, value) {
  return el("div", { class: "m" }, el("span", { class: "l", text: label }), el("span", { class: "v", text: value }));
}

function barRow(field, n, max) {
  const w = Math.max(2, Math.round((n / Math.max(1, max)) * 100));
  const bar = el("div", {
    class: "bar", style: `width:${w}%`, tabindex: "0", role: "img",
    "aria-label": `${field}: ${n} events changed`,
    title: `${field}: ${nf.format(n)}`,
  });
  return el("div", { class: "bar-row" },
    el("span", { class: "bar-name", text: field }),
    el("div", { class: "bar-track" }, bar),
    el("span", { class: "bar-val", text: compact(n) })
  );
}

function fmtVal(field, v) {
  if (v === null || v === undefined) return "∅";
  if (field === "amount_cents") return money(v);
  if (typeof v === "string" && v.length > 28) return v.slice(0, 27) + "…";
  return String(v);
}

function renderDiffTable(host, d, cand, base) {
  const c = d.counts;
  const rows = Object.entries(d.by_field).map(([f, n]) =>
    el("tr", {},
      el("td", { class: "mono", text: f }),
      el("td", { class: "num", text: nf.format(n) }),
      el("td", { class: "num", text: c.compared ? pct((100 * n) / c.compared) : "0%" })
    ));
  host.append(el("div", { class: "table-wrap" },
    el("table", {},
      el("thead", {}, el("tr", {},
        el("th", { text: "Field" }), el("th", { class: "num", text: "Events changed" }), el("th", { class: "num", text: "Share" }))),
      el("tbody", {}, ...rows))
  ));
  host.append(el("div", { class: "table-wrap" },
    el("table", {},
      el("thead", {}, el("tr", {},
        el("th", { text: "Event" }), el("th", { text: "Triggered by" }), el("th", { text: "Base output" }), el("th", { text: "Candidate output" }))),
      el("tbody", {},
        ...d.samples.slice(0, 40).map((s) => el("tr", {},
          el("td", { class: "mono", text: s.event_key }),
          el("td", {}, ...s.changed_fields.map((f) => el("span", { class: "chip", text: f }))),
          el("td", { class: "mono", text: s.changed_fields.map((f) => fmtVal(f, s.base_output[f])).join(", ") }),
          el("td", { class: "mono", text: s.changed_fields.map((f) => fmtVal(f, s.candidate_output[f])).join(", ") })
        )))
    )
  ));
  host.append(el("p", { class: "muted", text: `Candidate v${cand ? cand.version : "?"} vs serving v${base ? base.version : "?"} · ${compact(c.compared)} events compared · ${compact(c.changed)} changed.` }));
}

/* ---------------- switch control ---------------- */
function renderSwitch(s, diff) {
  const host = $("#switch-body");
  host.replaceChildren();
  const candidates = s.generations.filter((g) => g.status === "building" || g.status === "ready");
  const active = s.generations.find((g) => g.id === s.active);

  if (!candidates.length) {
    host.append(el("div", { class: "empty" },
      active ? `No candidate is being prepared. Serving v${active.version} with ${pct(s.coverage.pct)} coverage.`
             : "No generation is serving."));
    if (active) {
      host.append(renderSuspendControls(active));
    }
    return;
  }
  const cand = candidates[candidates.length - 1];
  const p = cand.progress;
  const ready = p.remaining === 0 && !cand.paused;
  const d = diff && !diff.error ? diff : null;

  const box = el("div", { class: "switch-ctl" });
  box.append(el("div", { class: "gen-head" },
    el("span", { class: "gen-ver", text: `v${cand.version}` }),
    statusBadge(cand),
    el("span", { class: "gen-meta", text: p.remaining ? `${compact(p.pending)} of ${compact(p.total)} pending` : `${compact(p.done)} of ${compact(p.total)} cleaned` })
  ));

  if (d) {
    box.append(el("div", { class: "diff-mini" },
      mini("Would change", `${compact(d.counts.changed)} events`),
      mini("Field edits", compact(d.counts.field_changes)),
      mini("Status flips", compact(d.counts.status_flip))
    ));
  }

  if (!ready) {
    box.append(el("div", { class: "risk" },
      el("span", { class: "glyph", text: "!" }),
      el("span", {}, `Backfill still has ${compact(p.remaining)} item(s) to clean${cand.paused ? " and is paused" : ""}. The server refuses a switch range that still contains unfinished work — narrow the range to a finished slice, or let the rest drain first.`)));
  } else {
    box.append(el("div", { class: "risk", style: "border-left-color:var(--good);background:rgba(12,163,12,.06)" },
      el("span", { class: "glyph", style: "color:var(--good)", text: "✓" }),
      el("span", {}, `Backfill complete. Approval publishes ${compact(p.done)} results and flips serving windows in one transaction.`)));
  }

  const from = el("input", { type: "datetime-local", step: "60", id: "sw-from" });
  const to = el("input", { type: "datetime-local", step: "60", id: "sw-to" });
  if (cand.backfill_from) {
    from.value = localInput(cand.backfill_from);
    from.min = localInput(cand.backfill_from);
  }
  if (diffScope(state.diff)) {
    from.value = localInput(diffScope(state.diff));
  }

  box.append(el("div", { class: "switch-row" },
    el("label", { class: "field" }, "From (serve v" + cand.version + ")", from),
    el("label", { class: "field" }, "To (empty = open-ended)", to),
  ));
  box.append(el("p", { class: "switch-hint" },
    "Leave both empty to switch the whole range. A narrower range switches only that slice; the rest keeps the serving generation. Query readers see one snapshot, so the boundary never appears mid-page."));

  const approveBtn = el("button", {
    class: "btn primary", type: "button", id: "btn-approve",
    text: `Approve & switch to v${cand.version}`,
    onclick: () => withBusy(approveBtn, async () => {
      const body = { generation_id: cand.id };
      if (from.value) body.from_time = isoOf(from.value);
      if (to.value) body.to_time = isoOf(to.value);
      const r = await post("/api/approve", body);
      banner(`Switched to v${cand.version} · ${compact(r.published)} results published${r.from_time ? ` from ${clock(r.from_time)}` : ""}.`, "ok");
      await refreshAll();
    })(),
  });
  const rediffBtn = el("button", {
    class: "btn ghost", type: "button", text: "Re-run shadow diff",
    onclick: () => withBusy(rediffBtn, async () => { await loadDiff(); banner("Shadow diff recomputed.", "info"); })(),
  });
  box.append(el("div", { class: "switch-row" }, approveBtn, rediffBtn));
  host.append(box);
  if (active) host.append(renderSuspendControls(active));
}

function diffScope(diff) {
  return diff && !diff.error ? diff.scope_from || null : null;
}

function renderSuspendControls(active) {
  const wrap = el("div", { class: "switch-ctl" });
  wrap.append(el("h3", { text: "Pause a range instead of serving it" }));
  const from = el("input", { type: "datetime-local", step: "60" });
  const to = el("input", { type: "datetime-local", step: "60" });
  const q = state.query;
  if (q && q.rows.length) {
    from.value = localInput(q.rows[0].event_time);
    to.value = localInput(q.rows[q.rows.length - 1].event_time);
  }
  wrap.append(el("div", { class: "switch-row" },
    el("label", { class: "field" }, "From", from),
    el("label", { class: "field" }, "To", to)
  ));
  wrap.append(el("p", { class: "switch-hint", text: "Suspended events are returned as explicit gaps with the window id — never silently omitted or silently guessed." }));
  const btn = el("button", {
    class: "btn", type: "button", text: "Suspend this range",
    onclick: () => withBusy(btn, async () => {
      if (!from.value || !to.value) throw new Error("Suspend needs a from and a to time");
      await post("/api/suspend_interval", { from_time: isoOf(from.value), to_time: isoOf(to.value), reason: "operator suspended affected range" });
      banner("Range suspended. Queries in that range now return explicit gaps.", "info");
      await refreshAll();
    })(),
  });
  wrap.append(el("div", { class: "switch-row" }, btn));

  const suspended = (state.status?.coverage?.windows || []).filter((w) => w.kind === "suspended");
  if (suspended.length) {
    wrap.append(el("h3", { style: "margin-top:10px", text: "Active suspensions" }));
    wrap.append(el("div", { class: "gap-list" },
      ...suspended.map((w) => {
        const liftBtn = el("button", {
          class: "btn ghost", type: "button", text: "Lift",
          onclick: () => withBusy(liftBtn, async () => {
            const r = await post("/api/unsuspend", { from_time: w.from_time, to_time: w.to_time });
            banner(`${r.removed} suspension window(s) lifted.`, "ok");
            await refreshAll();
          })(),
        });
        return el("div", { class: "gap-item" },
          el("span", {}, el("span", { class: "glyph", text: "!" }), `${clock(w.from_time)} → ${clock(w.to_time)}`),
          liftBtn
        );
      })
    ));
  }
  return wrap;
}

/* ---------------- query explorer ---------------- */
function renderQuery(q) {
  state.query = q;
  const host = $("#query-body");
  host.replaceChildren();
  if (!q) { host.append(el("div", { class: "empty", text: "Run a query to see results." })); return; }

  host.append(el("div", { class: "q-summary" },
    el("span", {}, el("b", { text: compact(q.rows.length) }), " rows"),
    el("span", {}, "serving ", el("b", { text: q.serving_generations.length ? q.serving_generations.map((v) => `v${v}`).join(", ") : "none" })),
    el("span", {}, "read at ", el("b", { text: clock(q.read_at) })),
    q.truncated ? el("span", { class: "st-corrected", text: "result truncated at limit" }) : null,
  ));

  if (q.gaps.length) {
    host.append(el("div", { class: "gaps" },
      el("div", {}, el("span", { class: "glyph", text: "!" }), `${q.gaps.length} event(s) in this range have no served value:`),
      ...q.gaps.slice(0, 6).map((g) => el("div", { class: "mono", text: `${g.event_key} · ${g.event_time} · ${g.reason}` }))
    ));
  }

  if (!q.rows.length) { host.append(el("div", { class: "empty", text: "No rows in this range." })); return; }

  const body = el("tbody");
  for (const row of q.rows) {
    const isOpen = state.openKey === row.event_key;
    const tr = el("tr", {
      class: `clickable ${isOpen ? "open" : ""}`,
      tabindex: "0",
      role: "button",
      "aria-expanded": isOpen ? "true" : "false",
      onclick: () => toggleRow(row.event_key),
      onkeydown: (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); toggleRow(row.event_key); } },
    },
      el("td", { class: "mono", text: row.event_key }),
      el("td", { class: "mono", text: clock(row.event_time) }),
      el("td", { class: "num", text: row.revision }),
      el("td", {
        class: `st-${row.result_status}`, text: row.result_status,
      }),
      el("td", {},
        el("span", { class: `gen-pill ${row.serving.version.startsWith("1.") ? "v1" : row.serving.version.startsWith("2.") ? "v2" : "other"}` },
          el("span", { class: "sw" }), `v${row.serving.version}`)),
      el("td", { class: "mono", text: row.output_hash.slice(0, 10) }),
      el("td", { class: "mono", text: fmtVal("status", row.status) }),
      el("td", { class: "num", text: money(row.amount_cents) }),
      el("td", { class: "mono", text: fmtVal("currency", row.currency) }),
      el("td", { class: "mono", text: fmtVal("region", row.region) }),
      el("td", { class: "num", text: row.item_count === null ? "∅" : row.item_count }),
    );
    body.append(tr);
    if (isOpen) {
      const det = el("tr", {}, el("td", { colspan: "11", style: "padding:0" }));
      det.firstChild.append(renderDetail(row));
      body.append(det);
    }
  }
  const thead = el("thead", {},
    el("tr", {},
      el("th", { text: "Event" }),
      el("th", { text: "Event time" }),
      el("th", { class: "num", text: "Rev" }),
      el("th", { text: "Clean status" }),
      el("th", { text: "Generation" }),
      el("th", { text: "Result hash" }),
      el("th", { text: "Order status" }),
      el("th", { class: "num", text: "Amount" }),
      el("th", { text: "Currency" }),
      el("th", { text: "Region" }),
      el("th", { class: "num", text: "Items" })
    )
  );
  host.append(el("div", { class: "table-wrap" }, el("table", {}, thead, body)));
}

async function toggleRow(key) {
  state.openKey = state.openKey === key ? null : key;
  renderQuery(state.query);
  if (state.openKey && !state.provenance.has(key)) {
    try {
      const p = await api(`/api/provenance?event_key=${encodeURIComponent(key)}`);
      if (p.found) state.provenance.set(key, p);
      renderQuery(state.query);
    } catch (err) {
      banner(err.message, "err");
    }
  }
}

function renderDetail(row) {
  const wrap = el("div", { class: "detail" });
  const p = state.provenance.get(row.event_key);
  if (!p) {
    wrap.append(el("div", { class: "muted", text: "Loading provenance…" }));
    return wrap;
  }
  const rev = p.revisions.find((r) => r.is_current) || p.revisions[p.revisions.length - 1];
  const results = p.results.slice().reverse();

  wrap.append(el("div", { class: "detail-grid" },
    el("div", {},
      el("h4", { text: "Raw input (revision " + rev.revision + (rev.is_current ? ", current" : ", superseded") + ")" }),
      el("pre", { class: "json", text: JSON.stringify(rev.payload, null, 2) })
    ),
    el("div", {},
      el("h4", { text: "Serving window" }),
      el("dl", { class: "kv" },
        el("dt", { text: "window" }), el("dd", { text: row.serving.window_id }),
        el("dt", { text: "kind" }), el("dd", { text: row.serving.window_kind }),
        el("dt", { text: "generation" }), el("dd", { text: `${row.serving.generation_id} · v${row.serving.version}` }),
        el("dt", { text: "raw row" }), el("dd", { text: row.raw_event_id }),
        el("dt", { text: "result row" }), el("dd", { text: row.result_id }),
        el("dt", { text: "payload hash" }), el("dd", { text: rev.payload_hash.slice(0, 18) + "…" }),
        el("dt", { text: "output hash" }), el("dd", { text: row.output_hash.slice(0, 18) + "…" }),
        el("dt", { text: "received" }), el("dd", { text: clock(row.received_at) }),
        el("dt", { text: "computed" }), el("dd", { text: clock(row.computed_at) })
      ),
      el("h4", { style: "margin-top:14px", text: "Earlier revisions" }),
      p.revisions.length <= 1
        ? el("p", { class: "muted", text: "None — this is the first arrival." })
        : el("ul", { class: "trace" }, ...p.revisions.map((r) => el("li", {},
            el("span", { class: "r", text: `rev ${r.revision}` }),
            el("span", { class: r.is_current ? "o-changed" : "o-ok", text: r.is_current ? "current" : "superseded" }),
            el("span", { class: "r", text: `seq ${r.seq} · ${clock(r.received_at)}` })
          )))
    )
  ));

  const genBlock = el("div", {});
  genBlock.append(el("h4", { text: "Derivations by generation" }));
  for (const r of results) {
    const applied = r.rule_trace.filter((t) => t.outcome !== "ok");
    const published = r.published ? el("span", { class: "badge active" }, el("span", { class: "glyph", text: "●" }), "published") : el("span", { class: "badge retired", text: "not published" });
    genBlock.append(el("div", { class: "gen", style: "margin-bottom:8px" },
      el("div", { class: "gen-head" },
        el("span", { class: "gen-ver", text: `v${r.generation_version}` }),
        published,
        el("span", { class: "gen-meta", text: `def ${r.definition_hash.slice(0, 12)}` })
      ),
      el("div", { class: "meter-caption" },
        el("span", {}, "status ", el("b", { class: `st-${r.status}`, text: r.status })),
        ...r.quality_flags.map((f) => el("span", { class: "flag", text: f }))
      ),
      el("ul", { class: "trace" }, ...applied.map((t) => el("li", {},
        el("span", { class: "r", text: t.rule }),
        el("span", { class: `o-${t.outcome}`, text: t.outcome }),
        el("span", { class: "r", text: t.detail ? JSON.stringify(t.detail) : "" })
      ))),
      el("pre", { class: "json", text: JSON.stringify(r.output, null, 2) })
    ));
  }
  wrap.append(genBlock);
  return wrap;
}

/* ---------------- actions ---------------- */
async function act(path, body, message) {
  const r = await post(path, body);
  if (message) banner(message, "ok");
  await refreshAll();
  return r;
}

async function loadStatus() {
  try {
    const s = await api("/api/status");
    renderStatus(s);
    if (!$("#q-from").value && s.event_span?.from) {
      $("#q-from").value = localInput(s.event_span.from);
      $("#q-to").value = localInput(s.event_span.to);
    }
    const link = $("#link-state");
    link.textContent = "live";
    link.className = "link-state ok";
    return s;
  } catch (err) {
    const link = $("#link-state");
    link.textContent = "offline";
    link.className = "link-state bad";
    throw err;
  }
}

async function loadDiff() {
  try {
    const d = await api("/api/diff");
    renderDiff(d);
    if (state.status) renderSwitch(state.status, d);
  } catch (err) {
    renderDiff({ error: err.message });
  }
}

async function loadQuery() {
  const from = $("#q-from").value;
  const to = $("#q-to").value;
  const limit = $("#q-limit").value || 12;
  const qs = new URLSearchParams({ limit });
  if (from) qs.set("from", isoOf(from));
  if (to) qs.set("to", isoOf(to));
  const q = await api(`/api/query?${qs}`);
  renderQuery(q);
}

async function refreshAll() {
  const s = await loadStatus();
  await loadDiff();
  await loadQuery().catch(() => {});
  renderSwitch(s, state.diff);
}

/* ---------------- wiring ---------------- */
function wire() {
  $("#btn-refresh").addEventListener("click", withBusy($("#btn-refresh"), refreshAll));
  $("#btn-reset").addEventListener("click", withBusy($("#btn-reset"), async () => {
    if (!confirm("Reset the demo database and reseed synthetic events?")) return;
    await post("/api/demo/reset", { count: 200 });
    state.provenance.clear();
    state.openKey = null;
    banner("Demo reset. v1.0.0 serving full history.", "ok");
    await refreshAll();
  }));

  $("#btn-backfill").addEventListener("click", withBusy($("#btn-backfill"), async () => {
    const version = $("#new-version").value;
    const from = $("#backfill-from").value;
    const r = await post("/api/backfill", { version, from_time: from ? isoOf(from) : null });
    banner(`Backfill for v${version} opened with ${compact(r.progress.total)} work items. Workers clean it; readers still see the serving generation.`, "ok");
    await refreshAll();
  }));

  $("#btn-queue")?.addEventListener("click", withBusy($("#btn-queue"), async () => {
    const alive = state.status?.queue?.alive;
    if (!alive) {
      banner("The queue process is not running. Use Start queue, or launch python -m cleanroom.queue.", "err");
      return;
    }
    const on = !(state.status?.queue?.enabled);
    await post("/api/queue", { on });
    banner(`Queue ingest ${on ? "resumed" : "paused"} (process still alive).`, "info");
    await refreshAll();
  }));

  const procBtn = (id, name, action, label) => {
    const b = $(id);
    b?.addEventListener("click", withBusy(b, async () => {
      const r = await post("/api/proc", { action, name });
      const detail = r.pid ? ` (pid ${r.pid})` : "";
      banner(`${label}: ${r.state}${detail}.`, action === "stop" ? "info" : "ok");
      await refreshAll();
    }));
  };
  procBtn("#btn-proc-queue-start", "queue", "start", "Queue start");
  procBtn("#btn-proc-queue-stop", "queue", "stop", "Queue stop");
  procBtn("#btn-proc-worker-start", "worker", "start", "Worker start");
  procBtn("#btn-proc-worker-stop", "worker", "stop", "Worker stopped");

  $("#btn-work").addEventListener("click", withBusy($("#btn-work"), async () => {
    const r = await post("/api/work", { items: 40 });
    banner(`Inline worker cleaned ${r.processed} item(s).`, "ok");
    await refreshAll();
  }));

  $("#btn-reclaim").addEventListener("click", withBusy($("#btn-reclaim"), async () => {
    const r = await post("/api/reclaim", {});
    banner(`Reclaimed ${r.reclaimed} stale claim(s).`, "info");
    await refreshAll();
  }));

  $("#btn-query").addEventListener("click", withBusy($("#btn-query"), async () => {
    await loadQuery();
  }));

  $("#diff-toggle").addEventListener("click", (e) => {
    const btn = e.target.closest(".seg-btn");
    if (!btn) return;
    state.diffView = btn.dataset.view;
    $$("#diff-toggle .seg-btn").forEach((b) => b.classList.toggle("is-on", b === btn));
    renderDiff(state.diff);
  });

  $("#timeline-table-toggle")?.addEventListener("change", () => {
    if (state.status) renderTimeline(state.status);
  });
}

async function tick() {
  try { await loadStatus(); } catch { /* keep last render */ }
  try { await loadDiff(); } catch { /* keep last render */ }
  renderSwitch(state.status, state.diff);
}

function startPolling() {
  clearInterval(state.timer);
  state.timer = setInterval(tick, 4000);
}

(async function init() {
  wire();
  const now = new Date();
  $("#clock").textContent = `session ${now.toLocaleDateString()}`;
  try {
    await refreshAll();
    banner("Connected. Use Prepare a new rule generation to open a v2 backfill.", "info");
  } catch (err) {
    banner(`Could not reach the service: ${err.message}`, "err", true);
  }
  startPolling();
})();
