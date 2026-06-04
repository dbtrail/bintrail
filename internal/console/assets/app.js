"use strict";

// ── token bootstrap ────────────────────────────────────────────────────────
// The page itself loads without a token; the API requires one. We read it from
// the ?token= query param the CLI prints, keep it in memory, and strip it from
// the visible URL so it isn't left sitting in the address bar / history.
const TOKEN = new URLSearchParams(location.search).get("token") || "";
if (TOKEN) {
  history.replaceState(null, "", location.pathname);
}

let lastSQL = "";

// ── tiny DOM helper (builds nodes; never injects data as HTML) ───────────────
function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v === null || v === undefined) continue;
      if (k === "class") n.className = v;
      else if (k === "text") n.textContent = v;
      else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v);
    }
  }
  for (const kid of kids) {
    if (kid === null || kid === undefined) continue;
    n.appendChild(typeof kid === "string" ? document.createTextNode(kid) : kid);
  }
  return n;
}

function opt(value, label) {
  const o = el("option", { value });
  o.textContent = label;
  return o;
}

// clear removes all children of a node. Used instead of innerHTML="" so no
// markup is ever assigned from a string.
function clear(n) {
  n.replaceChildren();
}

function valueToString(v) {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function toast(msg) {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { t.hidden = true; }, 2000);
}

// ── API wrapper ──────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const headers = { Authorization: "Bearer " + TOKEN };
  if (opts.body) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const text = await res.text();
  let data = null;
  if (text) {
    try { data = JSON.parse(text); } catch { data = { error: text }; }
  }
  if (!res.ok) {
    throw new Error((data && data.error) || "HTTP " + res.status);
  }
  return data;
}

// ── shared rendering ─────────────────────────────────────────────────────────
function renderError(container, err) {
  clear(container);
  container.appendChild(el("div", { class: "error-box" }, String(err.message || err)));
}

function renderWarnings(node, warnings) {
  clear(node);
  (warnings || []).forEach((w) => node.appendChild(el("div", { class: "warn-item" }, w)));
}

function formParams(form) {
  const out = {};
  for (const [k, v] of new FormData(form).entries()) {
    if (String(v).trim() !== "") out[k] = v;
  }
  return out;
}

function renderDiff(ev) {
  const before = ev.row_before || {};
  const after = ev.row_after || {};
  const cols = Array.from(new Set([...Object.keys(before), ...Object.keys(after)]));
  const changed = new Set(ev.changed_columns || []);
  const wholeRow = ev.event_type === "INSERT" || ev.event_type === "DELETE";
  const grid = el("div", { class: "diff" },
    el("div", { class: "dh" }, "column"),
    el("div", { class: "dh" }, "before"),
    el("div", { class: "dh" }, "after"),
  );
  if (cols.length === 0) {
    grid.appendChild(el("div", { class: "col" }, "(no row image)"));
    grid.appendChild(el("div", { class: "before" }, ""));
    grid.appendChild(el("div", { class: "after" }, ""));
  }
  cols.forEach((c) => {
    const isCh = wholeRow || changed.has(c);
    const mark = isCh ? " changed" : "";
    grid.appendChild(el("div", { class: "col" + mark }, c));
    grid.appendChild(el("div", { class: "before" + mark }, c in before ? valueToString(before[c]) : ""));
    grid.appendChild(el("div", { class: "after" + mark }, c in after ? valueToString(after[c]) : ""));
  });
  return grid;
}

function renderEvents(container, data) {
  clear(container);
  container.appendChild(el("div", { class: "meta-line" },
    `${data.count} event(s) · limit ${data.limit}`));
  if (!data.events || data.events.length === 0) {
    container.appendChild(el("div", { class: "empty" }, "No events matched these filters."));
    return;
  }
  const tbody = el("tbody");
  data.events.forEach((ev) => {
    const row = el("tr", { class: "event-row" },
      el("td", null, ev.event_timestamp),
      el("td", null, `${ev.schema_name}.${ev.table_name}`),
      el("td", null, el("span", { class: "badge " + ev.event_type }, ev.event_type)),
      el("td", null, ev.pk_values),
      el("td", null, (ev.changed_columns || []).join(", ")),
    );
    const detail = el("tr", { class: "detail" }, el("td", { colspan: "5" }, renderDiff(ev)));
    detail.hidden = true;
    row.addEventListener("click", () => { detail.hidden = !detail.hidden; });
    tbody.appendChild(row);
    tbody.appendChild(detail);
  });
  const table = el("table", { class: "events" },
    el("thead", null, el("tr", null,
      el("th", null, "time"), el("th", null, "table"), el("th", null, "type"),
      el("th", null, "pk"), el("th", null, "changed columns"))),
    tbody,
  );
  container.appendChild(table);
}

// ── events tab ───────────────────────────────────────────────────────────────
async function runEvents(e) {
  e.preventDefault();
  const container = document.getElementById("events-result");
  const warns = document.getElementById("events-warnings");
  try {
    const params = new URLSearchParams(formParams(e.target));
    const data = await api("/api/events?" + params.toString());
    renderWarnings(warns, data.warnings);
    renderEvents(container, data);
  } catch (err) {
    clear(warns);
    renderError(container, err);
  }
}

// ── recover tab ──────────────────────────────────────────────────────────────
async function previewRecover() {
  const container = document.getElementById("recover-preview");
  try {
    const params = new URLSearchParams(formParams(document.getElementById("recover-form")));
    const data = await api("/api/events?" + params.toString());
    renderEvents(container, data);
  } catch (err) {
    renderError(container, err);
  }
}

async function generateUndo(e) {
  e.preventDefault();
  const warns = document.getElementById("recover-warnings");
  const wrap = document.getElementById("recover-sql-wrap");
  try {
    const body = formParams(e.target);
    if (body.limit) body.limit = Number(body.limit);
    const data = await api("/api/recover", { method: "POST", body });
    renderWarnings(warns, data.warnings);
    lastSQL = data.sql || "";
    document.getElementById("recover-sql").textContent = lastSQL;
    document.getElementById("recover-meta").textContent =
      `${data.statement_count} statement(s) from ${data.row_count} event(s)`;
    wrap.hidden = false;
  } catch (err) {
    clear(warns);
    wrap.hidden = true;
    renderError(document.getElementById("recover-preview"), err);
  }
}

function copySQL() {
  navigator.clipboard.writeText(lastSQL)
    .then(() => toast("SQL copied to clipboard"))
    .catch(() => toast("copy failed"));
}

function downloadSQL() {
  const blob = new Blob([lastSQL], { type: "application/sql" });
  const a = el("a", { href: URL.createObjectURL(blob), download: "bintrail-undo.sql" });
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(a.href);
}

// ── status tab ───────────────────────────────────────────────────────────────
function kv(k, v) {
  return el("div", { class: "kv" },
    el("span", { class: "k" }, k),
    el("span", { class: "v" }, v === null || v === undefined ? "—" : String(v)));
}

async function refreshStatus() {
  const c = document.getElementById("status-result");
  try {
    const s = await api("/api/status");
    clear(c);
    const grid = el("div", { class: "status-grid" });
    grid.appendChild(el("div", { class: "card" },
      el("h3", null, "Summary"),
      kv("total events (est.)", s.total_events_estimate),
      kv("indexed files", (s.files || []).length),
      kv("partitions", (s.partitions || []).length)));
    if (s.coverage) {
      grid.appendChild(el("div", { class: "card" },
        el("h3", null, "Coverage"),
        kv("earliest event", s.coverage.earliest_event),
        kv("latest event", s.coverage.latest_event),
        kv("total events", s.coverage.total_events),
        kv("schema changes", s.coverage.schema_changes)));
    }
    if (s.stream) {
      grid.appendChild(el("div", { class: "card" },
        el("h3", null, "Stream"),
        kv("mode", s.stream.mode),
        kv("binlog file", s.stream.binlog_file),
        kv("position", s.stream.binlog_position),
        kv("events indexed", s.stream.events_indexed)));
    }
    if (s.archives) {
      grid.appendChild(el("div", { class: "card" },
        el("h3", null, "Archives"),
        kv("files", s.archives.total_files),
        kv("rows", s.archives.total_rows),
        kv("size", s.archives.total_size_human)));
    }
    c.appendChild(grid);
  } catch (err) {
    renderError(c, err);
  }
}

// ── schema / table dropdowns ─────────────────────────────────────────────────
async function populateSchemas() {
  let schemas = [];
  try {
    const data = await api("/api/schemas");
    schemas = data.schemas || [];
  } catch (err) {
    document.querySelectorAll(".schema-select").forEach((sel) => {
      clear(sel);
      sel.appendChild(opt("", "(error: " + (err.message || err) + ")"));
    });
    return;
  }
  document.querySelectorAll(".schema-select").forEach((sel) => {
    clear(sel);
    sel.appendChild(opt("", "— select —"));
    schemas.forEach((s) => sel.appendChild(opt(s, s)));
  });
}

async function loadTables(form) {
  const schema = form.querySelector(".schema-select").value;
  const tsel = form.querySelector(".table-select");
  clear(tsel);
  tsel.appendChild(opt("", "— any —"));
  if (!schema) return;
  try {
    const data = await api("/api/schemas?schema=" + encodeURIComponent(schema));
    (data.tables || []).forEach((t) => tsel.appendChild(opt(t, t)));
  } catch (err) {
    // Surface the failure (like populateSchemas) so an empty dropdown isn't
    // mistaken for "this schema has no tables". Table is still optional.
    tsel.appendChild(opt("", "(error loading tables)"));
    toast("failed to load tables: " + (err.message || err));
  }
}

// ── wiring ───────────────────────────────────────────────────────────────────
function init() {
  document.querySelectorAll(".tab").forEach((t) => {
    t.addEventListener("click", () => {
      document.querySelectorAll(".tab").forEach((x) => x.classList.remove("active"));
      document.querySelectorAll(".panel").forEach((x) => x.classList.remove("active"));
      t.classList.add("active");
      document.getElementById(t.dataset.panel).classList.add("active");
    });
  });

  document.getElementById("events-form").addEventListener("submit", runEvents);
  document.getElementById("recover-form").addEventListener("submit", generateUndo);
  document.querySelector("#recover-form .preview-btn").addEventListener("click", previewRecover);
  document.getElementById("copy-sql").addEventListener("click", copySQL);
  document.getElementById("download-sql").addEventListener("click", downloadSQL);
  document.getElementById("status-refresh").addEventListener("click", refreshStatus);

  document.querySelectorAll(".schema-select").forEach((sel) => {
    sel.addEventListener("change", () => loadTables(sel.closest("form")));
  });

  if (!TOKEN) {
    toast("No token in URL — open the link printed by `bintrail console`.");
  }
  populateSchemas();
}

document.addEventListener("DOMContentLoaded", init);
