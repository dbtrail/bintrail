"use strict";

// ── token bootstrap ────────────────────────────────────────────────────────
// The page itself loads without a token; the API requires one. We read it from
// the ?token= query param the CLI prints, persist it in sessionStorage, and
// strip it from the visible URL so it isn't left sitting in the address bar /
// history. On a reload the param is gone, so we recover the token from
// sessionStorage — otherwise a refresh would drop it and every request would
// 401. sessionStorage is per-tab and cleared when the tab closes.
const TOKEN_KEY = "bintrail_console_token";
let TOKEN = new URLSearchParams(location.search).get("token") || "";
if (TOKEN) {
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (e) { /* storage unavailable */ }
  history.replaceState(null, "", location.pathname);
} else {
  try { TOKEN = sessionStorage.getItem(TOKEN_KEY) || ""; } catch (e) { TOKEN = ""; }
}

let lastSQL = "";

// ── server selection ─────────────────────────────────────────────────────────
// currentServer is the id of the server every API call targets, sent as the
// X-Bintrail-Server header (captured at dispatch time inside api(), so an
// in-flight request keeps the server it was fired for even if the operator
// switches mid-flight). Empty = the backend's default (the --index-dsn entry,
// else the first saved server). Persisted per-tab like the token, so two tabs
// can watch two different servers.
const SERVER_KEY = "bintrail_console_server";
let currentServer = "";
try { currentServer = sessionStorage.getItem(SERVER_KEY) || ""; } catch (e) { /* storage unavailable */ }
let defaultServerId = "";

function setCurrentServer(id) {
  currentServer = id || "";
  try { sessionStorage.setItem(SERVER_KEY, currentServer); } catch (e) { /* storage unavailable */ }
}

// serverGen invalidates in-flight renders across server switches. api()
// captures the header at dispatch, so a slow request keeps QUERYING the right
// server — but its response must not repaint a panel that now shows another
// server. Handlers snapshot the generation before awaiting and drop the render
// when a switch happened underneath them.
let serverGen = 0;

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
  if (currentServer) headers["X-Bintrail-Server"] = currentServer;
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
  const meta = el("div", { class: "meta-line" }, `${data.count} event(s) · limit ${data.limit}`);
  if (!data.events || data.events.length === 0) {
    container.appendChild(meta);
    container.appendChild(el("div", { class: "empty" }, "No events matched these filters."));
    return;
  }
  // Export toolbar — client-side, over the already-fetched (redacted) DTOs, so
  // it carries no connection_id and only the capped result set.
  meta.appendChild(el("span", { class: "export-bar" },
    el("button", { type: "button", class: "export-btn", onclick: () => downloadEventsJSON(data.events) }, "Download JSON"),
    el("button", { type: "button", class: "export-btn", onclick: () => downloadEventsCSV(data.events) }, "Download CSV")));
  container.appendChild(meta);
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
  const gen = serverGen;
  const container = document.getElementById("events-result");
  const warns = document.getElementById("events-warnings");
  try {
    const params = new URLSearchParams(formParams(e.target));
    const data = await api("/api/events?" + params.toString());
    if (gen !== serverGen) return; // switched servers mid-flight
    renderWarnings(warns, data.warnings);
    renderEvents(container, data);
  } catch (err) {
    if (gen !== serverGen) return;
    clear(warns);
    renderError(container, err);
  }
}

// ── recover tab ──────────────────────────────────────────────────────────────
async function previewRecover() {
  const gen = serverGen;
  const container = document.getElementById("recover-preview");
  try {
    const params = new URLSearchParams(formParams(document.getElementById("recover-form")));
    const data = await api("/api/events?" + params.toString());
    if (gen !== serverGen) return; // switched servers mid-flight
    renderEvents(container, data);
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(container, err);
  }
}

async function generateUndo(e) {
  e.preventDefault();
  const gen = serverGen;
  const warns = document.getElementById("recover-warnings");
  const wrap = document.getElementById("recover-sql-wrap");
  try {
    const body = formParams(e.target);
    if (body.limit) body.limit = Number(body.limit);
    const data = await api("/api/recover", { method: "POST", body });
    if (gen !== serverGen) return; // an undo script must never show under another server
    renderWarnings(warns, data.warnings);
    lastSQL = data.sql || "";
    document.getElementById("recover-sql").textContent = lastSQL;
    document.getElementById("recover-meta").textContent =
      `${data.statement_count} statement(s) from ${data.row_count} event(s)`;
    wrap.hidden = false;
  } catch (err) {
    if (gen !== serverGen) return;
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

// downloadBlob triggers a client-side file download, surfacing failures as a
// toast (parity with copySQL) rather than only an uncaught console exception.
function downloadBlob(filename, content, mime) {
  try {
    const a = el("a", {
      href: URL.createObjectURL(new Blob([content], { type: mime })),
      download: filename,
    });
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(a.href);
  } catch (err) {
    toast("download failed: " + (err.message || err));
  }
}

function downloadSQL() {
  downloadBlob("bintrail-undo.sql", lastSQL, "application/sql");
}

// ── events export (client-side, over the redacted eventDTOs) ──────────────────
// Flat CSV columns; row_before/row_after and changed_columns are emitted as JSON
// strings. connection_id is intentionally absent — these DTOs never carried it.
const EVENT_CSV_COLUMNS = [
  "event_id", "event_timestamp", "schema_name", "table_name", "event_type",
  "pk_values", "changed_columns", "gtid", "binlog_file", "start_pos", "end_pos",
  "row_before", "row_after",
];

function csvCell(v) {
  let s;
  if (v === null || v === undefined) s = "";
  else if (typeof v === "object") s = JSON.stringify(v); // arrays + maps
  else s = String(v);
  if (/[",\r\n]/.test(s)) s = '"' + s.replace(/"/g, '""') + '"';
  return s;
}

function downloadEventsJSON(events) {
  downloadBlob("bintrail-events.json", JSON.stringify(events, null, 2), "application/json");
}

function downloadEventsCSV(events) {
  const lines = [EVENT_CSV_COLUMNS.join(",")];
  events.forEach((ev) => lines.push(EVENT_CSV_COLUMNS.map((c) => csvCell(ev[c])).join(",")));
  downloadBlob("bintrail-events.csv", lines.join("\r\n"), "text/csv");
}

// ── status tab ───────────────────────────────────────────────────────────────
function kv(k, v) {
  return el("div", { class: "kv" },
    el("span", { class: "k" }, k),
    el("span", { class: "v" }, v === null || v === undefined ? "—" : String(v)));
}

async function refreshStatus() {
  const gen = serverGen;
  const c = document.getElementById("status-result");
  try {
    const s = await api("/api/status");
    if (gen !== serverGen) return; // switched servers mid-flight
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
    if (gen !== serverGen) return;
    renderError(c, err);
  }
}

// ── schema / table dropdowns ─────────────────────────────────────────────────
async function populateSchemas() {
  const gen = serverGen;
  let schemas = [];
  try {
    const data = await api("/api/schemas");
    schemas = data.schemas || [];
  } catch (err) {
    if (gen !== serverGen) return;
    document.querySelectorAll(".schema-select").forEach((sel) => {
      clear(sel);
      sel.appendChild(opt("", "(error: " + (err.message || err) + ")"));
    });
    return;
  }
  if (gen !== serverGen) return; // a newer switch's populateSchemas owns the dropdowns
  document.querySelectorAll(".schema-select").forEach((sel) => {
    clear(sel);
    sel.appendChild(opt("", "— select —"));
    schemas.forEach((s) => sel.appendChild(opt(s, s)));
  });
}

async function loadTables(form) {
  const gen = serverGen;
  const schema = form.querySelector(".schema-select").value;
  const tsel = form.querySelector(".table-select");
  clear(tsel);
  tsel.appendChild(opt("", "— any —"));
  if (!schema) return;
  try {
    const data = await api("/api/schemas?schema=" + encodeURIComponent(schema));
    if (gen !== serverGen) return; // switched servers mid-flight
    (data.tables || []).forEach((t) => tsel.appendChild(opt(t, t)));
  } catch (err) {
    if (gen !== serverGen) return;
    // Surface the failure (like populateSchemas) so an empty dropdown isn't
    // mistaken for "this schema has no tables". Table is still optional.
    tsel.appendChild(opt("", "(error loading tables)"));
    toast("failed to load tables: " + (err.message || err));
  }
}

// ── reconstruct (time-travel) tab ────────────────────────────────────────────
async function runReconstruct(history) {
  const form = document.getElementById("reconstruct-form");
  const warns = document.getElementById("reconstruct-warnings");
  const container = document.getElementById("reconstruct-result");
  const p = formParams(form);
  if (!p.schema || !p.table || !p.pk) {
    clear(warns);
    renderError(container, new Error("schema, table, and pk are required"));
    return;
  }
  const params = new URLSearchParams({ schema: p.schema, table: p.table, pk: p.pk });
  if (p.at) params.set("at", p.at);
  if (p.allow_gaps) params.set("allow_gaps", "true");
  if (history) params.set("history", "true");
  const gen = serverGen;
  try {
    const data = await api("/api/reconstruct?" + params.toString());
    if (gen !== serverGen) return; // switched servers mid-flight
    renderWarnings(warns, data.warnings);
    if (history) renderReconstructHistory(container, data);
    else renderReconstructState(container, data);
  } catch (err) {
    if (gen !== serverGen) return;
    clear(warns);
    renderError(container, err);
  }
}

function reconstructMeta(data, label) {
  return el("div", { class: "meta-line" },
    `${data.schema}.${data.table} pk=${data.pk} · ${label} · baseline ${data.baseline_time} · ${data.event_count} event(s)`);
}

function stateTable(state) {
  const tbody = el("tbody");
  Object.keys(state || {}).forEach((k) => {
    tbody.appendChild(el("tr", null, el("th", null, k), el("td", null, valueToString(state[k]))));
  });
  return el("table", { class: "statetable" }, tbody);
}

function renderReconstructState(container, data) {
  clear(container);
  container.appendChild(reconstructMeta(data, "as of " + data.at));
  if (!data.found) {
    container.appendChild(el("div", { class: "deleted-note" },
      "No row with this primary key existed at or before the selected time."));
    return;
  }
  if (data.deleted) {
    container.appendChild(el("div", { class: "deleted-note" }, "Row was deleted as of " + data.at + "."));
    return;
  }
  container.appendChild(stateTable(data.state));
}

function compactState(state) {
  return Object.keys(state || {}).map((k) => `${k}=${valueToString(state[k])}`).join("   ");
}

function renderReconstructHistory(container, data) {
  clear(container);
  const entries = data.history || [];
  container.appendChild(reconstructMeta(data, `history through ${data.at} · ${entries.length} state(s)`));
  if (!data.found) {
    container.appendChild(el("div", { class: "deleted-note" },
      "No row with this primary key existed at or before the selected time."));
    return;
  }
  const tl = el("div", { class: "timeline" });
  entries.forEach((e) => {
    const badgeClass = "badge " + (e.source === "baseline" ? "baseline" : e.source);
    const head = el("div", { class: "tl-head" },
      el("span", { class: badgeClass }, e.source),
      el("span", { class: "tl-time" }, e.time));
    const body = e.deleted
      ? el("div", { class: "tl-state" }, "(row deleted)")
      : el("div", { class: "tl-state" }, compactState(e.state));
    tl.appendChild(el("div", { class: "tl-entry" }, head, body));
  });
  container.appendChild(tl);
}

// gateCapabilities toggles capability-gated tabs/panels per the SELECTED
// server's report. Gated elements keep their data-capability attribute and are
// shown/hidden via the cap-on class, so the gate re-evaluates on every server
// switch (a server with Time-travel can be followed by one without). Anything
// not enabled stays hidden (display:none via CSS), so an un-configured surface
// never flashes on screen.
async function gateCapabilities() {
  const gen = serverGen;
  let caps = {};
  try {
    caps = await api("/api/capabilities");
  } catch {
    caps = {}; // unreachable/unknown server → no optional surfaces
  }
  if (gen !== serverGen) return; // a newer switch's gate owns the tabs
  document.querySelectorAll("[data-capability]").forEach((node) => {
    node.classList.toggle("cap-on", !!caps[node.dataset.capability]);
  });
  // If the active tab just lost its capability on a switch, fall back to the
  // landing tab rather than leaving a blank main area.
  const active = document.querySelector(".tab.active");
  if (active && active.dataset.capability && !caps[active.dataset.capability]) {
    const home = document.querySelector('.tab[data-panel="recover"]');
    if (home) home.click();
  }
}

// ── server switcher + management modal ───────────────────────────────────────
function serverLabel(s) {
  return s.kind === "ephemeral" ? s.name + " (cli)" : s.name;
}

// loadServers refreshes the header dropdown from /api/servers and reconciles a
// stale per-tab selection (e.g. the server was deleted from another tab).
async function loadServers() {
  const data = await api("/api/servers");
  defaultServerId = data.default_id || "";
  const servers = data.servers || [];
  if (currentServer && !servers.some((s) => s.id === currentServer)) {
    setCurrentServer(""); // deleted under us → fall back to the default
  }
  const sel = document.getElementById("server-select");
  clear(sel);
  servers.forEach((s) => sel.appendChild(opt(s.id, serverLabel(s))));
  sel.value = currentServer || defaultServerId;
  return servers;
}

// clearResults wipes every per-server result so nothing from the previous
// server lingers after a switch (stale rows would read as the new server's).
function clearResults() {
  ["events-result", "events-warnings", "recover-preview", "recover-warnings",
    "reconstruct-result", "reconstruct-warnings", "status-result"].forEach((id) => {
    const n = document.getElementById(id);
    if (n) clear(n);
  });
  document.getElementById("recover-sql-wrap").hidden = true;
  lastSQL = "";
}

async function switchServer(id) {
  setCurrentServer(id);
  serverGen++; // anything still in flight for the previous server must not render
  clearResults();
  await gateCapabilities();
  populateSchemas();
}

function openServersModal() {
  document.getElementById("servers-modal").hidden = false;
  hideServerForm();
  refreshServersList();
}

function closeServersModal() {
  document.getElementById("servers-modal").hidden = true;
}

async function refreshServersList() {
  const list = document.getElementById("servers-list");
  let servers = [];
  try {
    servers = await loadServers();
  } catch (err) {
    renderError(list, err);
    return;
  }
  clear(list);
  if (servers.length === 0) {
    list.appendChild(el("div", { class: "empty" }, "No servers yet — add your first connection."));
    return;
  }
  servers.forEach((s) => {
    const desc = s.host ? `${s.user}@${s.host}:${s.port || "3306"}/${s.dbname}` : s.dbname || "";
    const row = el("div", { class: "server-row" },
      el("span", { class: "health-dot" + (s.connected ? " ok" : ""), title: s.connected ? "connected" : "not connected yet" }),
      el("span", { class: "srv-name" }, serverLabel(s)),
      s.kind === "ephemeral" ? el("span", { class: "badge cli", title: "From --index-dsn; managed by the command line" }, "CLI") : null,
      s.reconstruct ? el("span", { class: "badge tt", title: "Baseline configured: Time-travel available" }, "TT") : null,
      el("span", { class: "srv-desc" }, desc),
      el("span", { class: "srv-status", id: "srv-status-" + s.id }),
      el("button", { type: "button", class: "row-btn", onclick: () => testServerRow(s.id) }, "Test"),
      el("button", { type: "button", class: "row-btn", disabled: s.editable ? null : "", onclick: () => editServer(s.id) }, "Edit"),
      el("button", { type: "button", class: "row-btn danger", disabled: s.deletable ? null : "", onclick: () => deleteServer(s) }, "Delete"),
    );
    list.appendChild(row);
  });
}

function formMsg(text, isError) {
  const n = document.getElementById("server-form-msg");
  n.className = "form-msg " + (isError ? "err" : "ok");
  n.textContent = text;
}

function showServerForm(prefill) {
  const f = document.getElementById("server-form");
  f.reset();
  formMsg("", false);
  f.elements.id.value = prefill ? prefill.id : "";
  if (prefill) {
    f.elements.name.value = prefill.name || "";
    f.elements.host.value = prefill.host || "";
    f.elements.port.value = prefill.port || "";
    f.elements.user.value = prefill.user || "";
    f.elements.dbname.value = prefill.dbname || "";
    f.elements.baseline_dir.value = prefill.baseline_dir || "";
    f.elements.baseline_s3.value = prefill.baseline_s3 || "";
    f.elements.no_archive.checked = !!prefill.no_archive;
    f.elements.password.placeholder = prefill.has_password ? "(unchanged — leave blank to keep)" : "(none)";
  } else {
    f.elements.password.placeholder = "";
  }
  document.getElementById("server-add-wrap").hidden = true;
  f.hidden = false;
  f.elements.name.focus();
}

function hideServerForm() {
  document.getElementById("server-form").hidden = true;
  document.getElementById("server-add-wrap").hidden = false;
}

async function editServer(id) {
  try {
    const s = await api("/api/servers/" + encodeURIComponent(id));
    showServerForm(s);
  } catch (err) {
    toast("failed to load server: " + (err.message || err));
  }
}

// serverFormBody collects the form into a request body. The password is only
// included when the operator typed one — an omitted password means "keep the
// stored secret" on edit (the server merges it; the browser never sees it).
function serverFormBody(f) {
  const body = {
    name: f.elements.name.value.trim(),
    host: f.elements.host.value.trim(),
    port: f.elements.port.value.trim(),
    user: f.elements.user.value.trim(),
    dbname: f.elements.dbname.value.trim(),
    baseline_dir: f.elements.baseline_dir.value.trim(),
    baseline_s3: f.elements.baseline_s3.value.trim(),
    no_archive: f.elements.no_archive.checked,
  };
  if (f.elements.password.value !== "") body.password = f.elements.password.value;
  return body;
}

async function saveServer(e) {
  e.preventDefault();
  const f = e.target;
  const id = f.elements.id.value;
  try {
    if (id) {
      await api("/api/servers/" + encodeURIComponent(id), { method: "PUT", body: serverFormBody(f) });
    } else {
      await api("/api/servers", { method: "POST", body: serverFormBody(f) });
    }
    hideServerForm();
    await refreshServersList();
    toast(id ? "Server updated" : "Server added");
  } catch (err) {
    formMsg(err.message || String(err), true);
  }
}

async function deleteServer(s) {
  if (!window.confirm(`Remove server "${s.name}"? This only removes the saved connection — nothing happens to the server itself.`)) return;
  try {
    await api("/api/servers/" + encodeURIComponent(s.id), { method: "DELETE" });
    if (currentServer === s.id) {
      await switchServer(""); // deleted the selected server → back to default
      document.getElementById("server-select").value = defaultServerId;
    }
    await refreshServersList();
    toast("Server removed");
  } catch (err) {
    toast("delete failed: " + (err.message || err));
  }
}

function testResultText(res) {
  if (!res.ok) return "✗ " + (res.error || "unreachable");
  let txt = `✓ ok · ${res.latency_ms} ms`;
  if (res.server_version) txt += " · MySQL " + res.server_version;
  // has_index/schema_current are tri-state: absent means the metadata lookup
  // itself failed (unknown) — never render that as the confident negative.
  if (res.has_index === false) txt += " · no binlog_events table (not a bintrail index?)";
  else if (res.has_index === undefined || res.schema_current === undefined) txt += " · index metadata unavailable";
  else if (res.schema_current === false) txt += " · index schema outdated (run bintrail index/stream once)";
  return txt;
}

// testServerForm probes the (possibly unsaved) form values; with an id the
// backend merges the stored password when the field was left blank.
async function testServerForm() {
  const f = document.getElementById("server-form");
  const id = f.elements.id.value;
  const path = id ? "/api/servers/" + encodeURIComponent(id) + "/test" : "/api/servers/test";
  formMsg("testing…", false);
  try {
    const res = await api(path, { method: "POST", body: serverFormBody(f) });
    formMsg(testResultText(res), !res.ok);
  } catch (err) {
    formMsg(err.message || String(err), true);
  }
}

// testServerRow probes a saved entry as-is (no body → stored DSN).
async function testServerRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) slot.textContent = "testing…";
  try {
    const res = await api("/api/servers/" + encodeURIComponent(id) + "/test", { method: "POST", body: {} });
    if (slot) {
      slot.textContent = testResultText(res);
      slot.className = "srv-status " + (res.ok ? "ok" : "err");
    }
  } catch (err) {
    if (slot) {
      slot.textContent = "✗ " + (err.message || err);
      slot.className = "srv-status err";
    }
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

  document.getElementById("reconstruct-form").addEventListener("submit", (e) => {
    e.preventDefault();
    runReconstruct(false);
  });
  document.querySelector("#reconstruct-form .preview-btn").addEventListener("click", () => runReconstruct(true));

  document.querySelectorAll(".schema-select").forEach((sel) => {
    sel.addEventListener("change", () => loadTables(sel.closest("form")));
  });

  // Server switcher + management modal.
  document.getElementById("server-select").addEventListener("change", (e) => {
    switchServer(e.target.value);
  });
  document.getElementById("manage-servers").addEventListener("click", openServersModal);
  document.getElementById("servers-close").addEventListener("click", closeServersModal);
  document.getElementById("servers-modal").addEventListener("click", (e) => {
    if (e.target === e.currentTarget) closeServersModal(); // click on the backdrop
  });
  document.getElementById("server-add").addEventListener("click", () => showServerForm(null));
  document.getElementById("server-form").addEventListener("submit", saveServer);
  document.getElementById("server-cancel").addEventListener("click", hideServerForm);
  document.getElementById("server-test").addEventListener("click", testServerForm);

  if (!TOKEN) {
    toast("No token in URL — open the link printed by `bintrail console`.");
  }
  // Load the server list first so a stale per-tab selection is reconciled
  // before the capability gate and schema dropdowns fire against it.
  (async () => {
    try { await loadServers(); } catch (e) { /* default server still works */ }
    gateCapabilities();
    populateSchemas();
  })();
}

document.addEventListener("DOMContentLoaded", init);
