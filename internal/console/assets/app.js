// dbtrail console — vanilla-JS SPA over the read-only JSON API.
//
// No frameworks, no bundler, no third-party code (see assets/VENDOR.md). The
// design system lives in style.css; this file renders the sidebar-shell UI into
// #view, talks to /api/* with a bearer token, and selects a server per-request
// via the X-Bintrail-Server header.
//
// Security invariants kept from the prior frontend (do not regress):
//  1. The token comes from ?token= on first load, is stashed in sessionStorage,
//     and is stripped from the URL — it never lingers in the address bar.
//  2. The X-Bintrail-Server header is captured INSIDE api() at dispatch time, so
//     an in-flight request keeps the server it was fired for.
//  3. Every async render captures `serverGen` before its await and bails if a
//     server switch happened mid-flight (no cross-server repaint).
//  4. Capability gating toggles the `.cap-on` class on [data-capability] nodes.
//  5. Exports stay connection_id-free: CSV via the EVENT_CSV_COLUMNS allowlist;
//     JSON only because the server's eventDTO omits connection_id (it serializes
//     rows as-is). The open-core line is enforced server-side — don't add it.
//  6. The DOM is built with el()/textContent only — no innerHTML anywhere. The
//     one string→DOM path is svgEl(), which DOMParses STATIC icon constants
//     (never data) into SVG nodes. No value from the API touches markup.
"use strict";

// ── constants ──────────────────────────────────────────────────────────────

const TOKEN_KEY = "bintrail_console_token";
const SERVER_KEY = "bintrail_console_server";
const ONBOARD_KEY = "bintrail_console_onboarded";

// Export columns. connection_id is DELIBERATELY ABSENT — it is paid-forensics
// data and the console (query_explorer surface) never exposes it.
const EVENT_CSV_COLUMNS = [
  "event_id", "event_timestamp", "schema_name", "table_name", "event_type",
  "pk_values", "changed_columns", "gtid", "binlog_file", "start_pos", "end_pos",
  "row_before", "row_after",
];

// event_type → badge modifier class.
const BADGE_CLASS = { UPDATE: "b-update", INSERT: "b-insert", DELETE: "b-delete" };
function badgeClass(t) { return BADGE_CLASS[t] || "b-baseline"; }

const ROUTES = ["overview", "events", "timetravel", "recover", "status", "storage"];

const MON_STATE_TITLES = {
  failed: "stream failing — retrying with backoff; press Start for details",
  stalled: "stream is connected but has made no progress for several minutes",
  lost_position: "binlogs were purged past the saved position — events in the gap are permanently lost; current changes are still streaming",
};

// Static decorative SVGs (module constants — parsed by svgEl via DOMParser).
const ICONS = {
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"><circle cx="11" cy="11" r="7"></circle><path d="M21 21l-4.3-4.3"></path></svg>`,
  caret: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"></path></svg>`,
  file: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"></path><path d="M14 3v5h5"></path></svg>`,
  warn: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/></svg>`,
};

// ── module state ─────────────────────────────────────────────────────────────

let TOKEN = "";
let currentServer = "";       // X-Bintrail-Server target ("" = backend default)
let defaultServerId = "";
let serverGen = 0;            // bumped on every server switch (staleness guard)
let capsCache = {};           // last /api/capabilities for the selected server
let lastSQL = "";             // last generated undo SQL (for copy/download)
let lastEvents = [];          // last rendered (filtered, capped) event page
let pendingRecover = null;    // event context carried into Recover via "Undo"
let schemaCache = null;       // cached schema list for the selected server
const tablesCache = new Map();// schema → tables[]
let cursorIdx = -1;           // keyboard cursor row on Events
let serversEmpty = false;     // no listed servers (hidden-boot fresh install)

// ── token bootstrap ────────────────────────────────────────────────────────

(function bootstrapToken() {
  const urlToken = new URLSearchParams(location.search).get("token");
  if (urlToken) {
    // Assign and strip the URL BEFORE touching storage: with storage
    // disabled the old ordering threw first, leaving the token both unused
    // and sitting in the address bar.
    TOKEN = urlToken;
    history.replaceState(null, "", location.pathname);
    try { sessionStorage.setItem(TOKEN_KEY, urlToken); } catch (_) { /* in-memory only */ }
  } else {
    try { TOKEN = sessionStorage.getItem(TOKEN_KEY) || ""; } catch (_) {}
  }
  try { currentServer = sessionStorage.getItem(SERVER_KEY) || ""; } catch (_) {}
})();

function setCurrentServer(id) {
  currentServer = id || "";
  try { sessionStorage.setItem(SERVER_KEY, currentServer); } catch (_) {}
}

// ── tiny DOM helpers (no data ever assigned as HTML) ─────────────────────────

function el(tag, attrs, ...kids) {
  const n = document.createElement(tag);
  if (attrs) {
    for (const [k, v] of Object.entries(attrs)) {
      if (v == null || v === false) continue;
      if (k === "class") n.className = v;
      else if (k === "text") n.textContent = v;
      else if (k.startsWith("on")) n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v);
    }
  }
  for (const kid of kids) {
    if (kid == null) continue;
    n.append(kid.nodeType ? kid : document.createTextNode(String(kid)));
  }
  return n;
}
function opt(value, label) { const o = el("option", { value }); o.textContent = label; return o; }
function clear(n) { if (n) n.replaceChildren(); }
function $(sel, root = document) { return root.querySelector(sel); }
function $all(sel, root = document) { return Array.from(root.querySelectorAll(sel)); }
// svgEl parses a STATIC, trusted SVG constant into a detached SVG node. It is
// NOT an innerHTML sink: image/svg+xml parsing never executes script, and the
// argument is always a module constant — never server or user data.
function svgEl(s) {
  const doc = new DOMParser().parseFromString(s, "image/svg+xml");
  return document.importNode(doc.documentElement, true);
}
// icon(name) → a <span> wrapping the named static SVG.
function icon(name, cls) {
  const span = el("span", { class: cls || "" });
  if (ICONS[name]) span.append(svgEl(ICONS[name]));
  return span;
}

function valueToString(v) {
  if (v === null || v === undefined) return "NULL";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

const VIEW = () => document.getElementById("view");

// ── api ──────────────────────────────────────────────────────────────────────

async function api(path, opts = {}) {
  const headers = { Authorization: "Bearer " + TOKEN };
  if (currentServer) headers["X-Bintrail-Server"] = currentServer; // captured at dispatch
  if (opts.body) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  const text = await res.text();
  let data = null;
  if (text) {
    try {
      data = JSON.parse(text);
    } catch (_) {
      // A non-JSON body is the server's error text on a non-OK response, or a
      // server malfunction on an OK one — surface it; never let a stray HTML
      // error page render as an empty success. (An EMPTY body stays null: the
      // 204 from DELETE /api/servers/{id} is a legitimate no-content success.)
      if (!res.ok) throw apiError(res.status, text || "HTTP " + res.status);
      throw new Error("malformed response from " + path);
    }
  }
  if (!res.ok) {
    // 401 = the bearer credential is dead (expired session, rotated token).
    // One central chokepoint clears state and raises the sign-in gate; every
    // in-flight render bails on the serverGen bump.
    if (res.status === 401) handleUnauthorized();
    throw apiError(res.status, (data && data.error) || "HTTP " + res.status);
  }
  return data;
}

function apiError(status, message) {
  const err = new Error(message);
  err.status = status;
  return err;
}

// ── auth: login overlay, logout, password dialog ─────────────────────────────
//
// Password login is OPTIONAL (configured with `bintrail-console user
// set-password`); the ?token= bootstrap stays the default. A successful login
// returns a session token that drops into the SAME `TOKEN` slot the static
// token uses — api() and the X-Bintrail-Server flow never know the difference.
// The server reports how this tab authenticated (capabilities.auth.auth_kind),
// which gates the logout affordance via [data-auth]/.auth-on.

let unauthorizedHandled = false; // first 401 wins; later ones no-op
let loginGateRaised = false;     // the sign-in gate owns the screen — ⌘K and onboarding stay inert

// fetchAuthInfo asks the unauthenticated probe whether password login exists.
// Raw fetch: no bearer yet, and its failure must not recurse into the 401
// chokepoint.
async function fetchAuthInfo() {
  const res = await fetch("/api/auth");
  if (!res.ok) throw new Error("HTTP " + res.status);
  return res.json();
}

// handleUnauthorized is api()'s 401 chokepoint: the bearer is dead, so clear
// every credential-scoped cache (same hygiene as switchServer), invalidate
// in-flight renders, and raise the sign-in gate.
async function handleUnauthorized() {
  if (unauthorizedHandled) return;
  unauthorizedHandled = true;
  clearAuthState();
  let auth = {};
  try { auth = await fetchAuthInfo(); } catch (_) {}
  // After a `user remove` the console can be back in first-run setup.
  if (auth.setup) { showLoginOverlay({ setup: true }); return; }
  // Token mode has no session to expire and no form to "sign in" to — say what
  // actually happened (the stored token is no longer accepted).
  const msg = auth.password_login ? "Session expired — sign in again." : "This access token is no longer valid.";
  showLoginOverlay({ passwordLogin: !!auth.password_login, message: msg });
}

// clearAuthState drops every credential-scoped cache on sign-out. capsCache
// MUST be cleared too: a stale capsCache.auth would keep the command palette
// offering "Change console password…"/"Log out" in a signed-out tab, and
// running either evicts the gate from #login-mount.
function clearAuthState() {
  TOKEN = "";
  try { sessionStorage.removeItem(TOKEN_KEY); } catch (_) {}
  serverGen++;
  schemaCache = null;
  tablesCache.clear();
  pendingRecover = null;
  lastSQL = "";
  lastEvents = [];
  capsCache = {};
  applyAuthGate();
}

// showLoginOverlay raises the sign-in gate in #login-mount. Three modes:
//   - setup: true        → first-run "create your password" form (no auth yet)
//   - passwordLogin: true → the username/password sign-in form
//   - passwordLogin: false → a pointer at the printed ?token= URL (token mode)
// The scrim deliberately does NOT close on outside-click — it is a gate.
function showLoginOverlay(opts) {
  opts = opts || { passwordLogin: true };
  loginGateRaised = true;
  // Clear the workspace: the prior session's events/recover SQL must not stay
  // readable behind the blurred scrim (or after the gate is dismissed by a
  // dialog mounted in the same slot).
  clear(VIEW());
  const mount = document.getElementById("login-mount");
  const scrim = el("div", { class: "modal-scrim show" });
  const panel = el("div", { class: "modal login-panel", role: "dialog", "aria-label": opts.setup ? "Set up console" : "Sign in" });
  panel.append(el("h2", { class: "modal-title", text: "dbtrail console" }));

  if (opts.setup) {
    panel.append(el("p", { class: "modal-desc", text: "First run — create a username and password to access this console." }));
    const form = el("form", { class: "login-form", id: "login-form" });
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Username" }),
      el("input", { class: "input", name: "username", value: "admin", autocomplete: "username", spellcheck: "false" })));
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Password" }),
      el("input", { class: "input", name: "password", type: "password", autocomplete: "new-password" })));
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Confirm password" }),
      el("input", { class: "input", name: "confirm", type: "password", autocomplete: "new-password" })));
    const msg = el("div", { class: "form-msg", id: "login-msg" });
    const foot = el("div", { class: "modal-foot" });
    foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Create & sign in" }));
    form.append(foot);
    form.append(msg);
    form.addEventListener("submit", (e) => { e.preventDefault(); submitSetup(form, msg); });
    panel.append(form);
    scrim.append(panel);
    mount.replaceChildren(scrim);
    form.elements.password.focus();
    return;
  }

  if (!opts.passwordLogin) {
    panel.append(el("p", { class: "modal-desc", text: opts.message || "" }));
    panel.append(el("p", { class: "modal-desc", text: "Open the access link printed by bintrail-console — it carries the token this page needs." }));
    scrim.append(panel);
    mount.replaceChildren(scrim);
    return;
  }

  panel.append(el("p", { class: "modal-desc", text: "Sign in to the read-only console." }));
  const form = el("form", { class: "login-form", id: "login-form" });
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Username" }),
    el("input", { class: "input", name: "username", value: "admin", autocomplete: "username", spellcheck: "false" })));
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Password" }),
    el("input", { class: "input", name: "password", type: "password", autocomplete: "current-password" })));
  // Its own message node: formMsg() is hard-wired to #server-form-msg.
  const msg = el("div", { class: "form-msg", id: "login-msg" });
  if (opts.message) { msg.classList.add("err"); msg.textContent = opts.message; }
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Sign in" }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitLogin(form, msg); });
  panel.append(form);
  scrim.append(panel);
  mount.replaceChildren(scrim);
  form.elements.password.focus();
}

// submitSetup posts the first-run credential to /api/auth/setup (raw fetch:
// no bearer yet) and, on success, drops the gate on the returned session.
async function submitSetup(form, msg) {
  const password = form.elements.password.value;
  if (password !== form.elements.confirm.value) { loginMsg(msg, "Passwords do not match."); return; }
  const body = { username: form.elements.username.value.trim(), password };
  let res;
  try {
    res = await fetch("/api/auth/setup", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error — is the console still running?"); return; }
  if (res.status === 403) {
    // Setup closed under us (a concurrent `user set-password`, another tab, or
    // a CLI set it first). Unlike login, the setup endpoint self-disables —
    // re-probe and switch the gate to the sign-in form instead of leaving the
    // operator stuck re-posting to a now-closed endpoint.
    let pw = false;
    try { pw = !!(await fetchAuthInfo()).password_login; } catch (_) {}
    showLoginOverlay({ passwordLogin: pw, message: "A password was already created — sign in." });
    return;
  }
  if (!res.ok) {
    let m = "Could not set the password.";
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts — wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Malformed response."); return; }
  TOKEN = data.token || "";
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  unauthorizedHandled = false;
  loginGateRaised = false;
  closeLoginOverlay();
  await bootSequence();
}

// closeLoginOverlay empties the #login-mount slot. It is used both to dismiss
// the password dialog (authenticated; the gate was never up) and, after a
// successful login, by submitLogin. It does NOT lower loginGateRaised on its
// own — only authenticating does, so the password dialog can never be the
// thing that drops the gate: showPasswordDialog bails on `if (loginGateRaised)
// return`, and it is only reachable from ⌘K, which itself no-ops while the gate
// is up and is only offered once authenticated (capsCache.auth populated).
function closeLoginOverlay() { document.getElementById("login-mount").replaceChildren(); }

function loginMsg(node, text) { node.classList.add("err"); node.textContent = text; }

// submitLogin posts the credentials with a RAW fetch: there is no bearer yet,
// and a 401 here means "wrong password", which must not recurse into
// handleUnauthorized.
async function submitLogin(form, msg) {
  const body = {
    username: form.elements.username.value.trim(),
    password: form.elements.password.value,
  };
  let res;
  try {
    res = await fetch("/api/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error — is the console still running?"); return; }
  if (res.status === 429) {
    const retry = res.headers.get("Retry-After");
    loginMsg(msg, "Too many attempts — wait " + (retry ? retry + "s" : "a minute") + " and retry.");
    return;
  }
  if (!res.ok) {
    let m = "Invalid username or password.";
    if (res.status !== 401) { try { m = (await res.json()).error || m; } catch (_) {} }
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Malformed login response."); return; }
  TOKEN = data.token || "";
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  unauthorizedHandled = false;
  loginGateRaised = false; // authenticating is the ONLY thing that drops the gate
  closeLoginOverlay();
  await bootSequence();
}

async function doLogout() {
  try { await api("/api/auth/logout", { method: "POST" }); } catch (_) { /* dead session = already out */ }
  clearAuthState();
  unauthorizedHandled = false;
  // Logout is only reachable for session auth, which implies password login.
  showLoginOverlay({ passwordLogin: true, message: "Signed out." });
}

// applyAuthGate mirrors the [data-capability]/.cap-on pattern for auth-kind
// gated surfaces (the logout button). Server-derived: capabilities.auth tells
// this tab how it authenticated, so the affordance survives reloads.
function applyAuthGate() {
  const kind = (capsCache.auth && capsCache.auth.auth_kind) || "";
  $all("[data-auth]").forEach((n) => n.classList.toggle("auth-on", n.dataset.auth === kind));
}

// showPasswordDialog sets (token bootstrap) or rotates the console password.
// Mounted in #login-mount — never coexists with the login gate: it requires an
// authenticated tab, so it refuses while the gate is up (defense in depth —
// clearAuthState also strips the cmdk entries that could reach it signed-out).
function showPasswordDialog() {
  if (loginGateRaised) return;
  const firstSet = !(capsCache.auth && capsCache.auth.password_set);
  const mount = document.getElementById("login-mount");
  const scrim = el("div", { class: "modal-scrim show" });
  const panel = el("div", { class: "modal login-panel", role: "dialog", "aria-label": "Console password" });
  panel.append(el("h2", { class: "modal-title", text: firstSet ? "Set console password" : "Change console password" }));
  panel.append(el("p", { class: "modal-desc", text: firstSet
    ? "Enables username+password sign-in next to the access token."
    : "Rotating the password signs out every other session." }));

  const form = el("form", { class: "login-form" });
  if (!firstSet) {
    form.append(el("label", { class: "field" },
      el("span", { class: "field-label", text: "Current password" }),
      el("input", { class: "input", name: "current", type: "password", autocomplete: "current-password" })));
  }
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "New password" }),
    el("input", { class: "input", name: "next", type: "password", autocomplete: "new-password" })));
  form.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Retype new password" }),
    el("input", { class: "input", name: "confirm", type: "password", autocomplete: "new-password" })));
  const msg = el("div", { class: "form-msg" });
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: firstSet ? "Set password" : "Change password" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Cancel", onclick: closeLoginOverlay }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitPasswordChange(form, msg, firstSet); });
  panel.append(form);
  scrim.append(panel);
  mount.replaceChildren(scrim);
}

// submitPasswordChange uses a RAW fetch with the live bearer: the endpoint
// answers 401 for "wrong current password", which must not trip api()'s
// dead-credential chokepoint.
async function submitPasswordChange(form, msg, firstSet) {
  const next = form.elements.next.value;
  if (next !== form.elements.confirm.value) { loginMsg(msg, "Passwords do not match."); return; }
  const body = {
    current_password: firstSet ? "" : form.elements.current.value,
    new_password: next,
  };
  let res;
  try {
    res = await fetch("/api/auth/password", {
      method: "POST",
      headers: { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error — is the console still running?"); return; }
  if (!res.ok) {
    let m = "HTTP " + res.status;
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts — wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Malformed response."); return; }
  // Every other session just died; this tab continues on the fresh one.
  TOKEN = data.token || TOKEN;
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  closeLoginOverlay();
  toast("Password " + (firstSet ? "set" : "updated"));
  // password_set (and possibly auth_kind) changed server-side.
  try { await gateCapabilities(); } catch (_) {}
}

// ── toast / errors / warnings ─────────────────────────────────────────────────

function toast(msg) {
  const t = document.getElementById("toast");
  if (!t) return;
  t.textContent = msg;
  t.hidden = false;
  clearTimeout(toast._t);
  toast._t = setTimeout(() => { t.hidden = true; }, 2200);
}

function renderError(container, err) {
  if (!container) return;
  clear(container);
  container.append(el("div", { class: "error-box", text: String((err && err.message) || err) }));
}

function renderWarnings(node, warnings) {
  if (!node) return;
  clear(node);
  (warnings || []).forEach((w) => node.append(
    el("div", { class: "warn-item" }, icon("warn"), el("span", { text: w }))
  ));
}

// ── badge / page-head builders ────────────────────────────────────────────────

function badge(type) { return el("span", { class: "badge " + badgeClass(type), text: type }); }

function pageHead(title, subNode) {
  const head = el("div", { class: "page-head" }, el("h1", { class: "page-title", text: title }));
  if (subNode) head.append(subNode);
  return head;
}

function viewLoading() {
  const v = VIEW();
  clear(v);
  v.append(el("div", { class: "view-loading", text: "Loading…" }));
  v.classList.remove("view-enter");
}
function viewEnter() { const v = VIEW(); v.classList.remove("view-enter"); void v.offsetWidth; v.classList.add("view-enter"); }

// ── router ─────────────────────────────────────────────────────────────────

function routeFromLocation() {
  const path = location.pathname.replace(/^\//, "").split("/")[0] || "overview";
  return ROUTES.includes(path) ? path : "overview";
}

function navigate(route, params, push = true) {
  if (!ROUTES.includes(route)) route = "overview";
  // Reconstruct surface is gated; never navigate to a disabled Time-travel.
  if (route === "timetravel" && !capsCache.reconstruct) route = "overview";
  // Storage is a watch-daemon surface (rotation + archiving live there).
  if (route === "storage" && !capsCache.monitor) route = "overview";
  const qs = params && Object.keys(params).length
    ? "?" + new URLSearchParams(params).toString() : "";
  if (push) history.pushState({ route }, "", "/" + route + qs);
  renderRoute();
}

function renderRoute() {
  const route = routeFromLocation();
  setActiveNav(route);
  cursorIdx = -1;
  const params = Object.fromEntries(new URLSearchParams(location.search));
  switch (route) {
    case "overview": return renderOverview();
    case "events": return renderEvents(params);
    case "timetravel": return renderTimetravel(params);
    case "recover": return renderRecover(params);
    case "status": return renderStatus();
    case "storage": return renderStorage();
    default: return renderOverview();
  }
}

function setActiveNav(route) {
  $all(".nav-item").forEach((a) => a.classList.toggle("active", a.dataset.route === route));
}

// ── Overview ─────────────────────────────────────────────────────────────────

async function renderOverview() {
  const gen = serverGen;
  viewLoading();
  try {
    const [status, eventsData] = await Promise.all([
      api("/api/status").catch(() => null),
      api("/api/events?limit=200&order=DESC"),
    ]);
    if (gen !== serverGen) return;
    buildOverview(status, eventsData); // render INSIDE the try: a throw here shows an error, never a stuck "Loading…"
  } catch (err) {
    if (gen !== serverGen) return;
    const v = VIEW(); clear(v); v.append(pageHead("Overview", null)); renderError(v, err);
  }
}

// buildOverview renders the dashboard from the two fetched payloads. status may
// be null (its fetch is best-effort); when it is, we do NOT claim a global
// total — `events` is only the fetched window (limit 200), never the index size.
function buildOverview(status, eventsData) {
  if (status) updateSideMeta(status);

  const events = (eventsData && eventsData.events) || [];
  const cov = (status && status.coverage) || {};
  const total = status ? (status.total_events_estimate || cov.total_events || "—") : "—";
  const deletes = events.filter((e) => e.event_type === "DELETE").length;

  // Aggregate the fetched window by table.
  const byTable = new Map();
  for (const e of events) {
    const k = e.schema_name + "." + e.table_name;
    let s = byTable.get(k);
    if (!s) { s = { key: k, insert: 0, update: 0, delete: 0, total: 0 }; byTable.set(k, s); }
    s.total++;
    if (e.event_type === "INSERT") s.insert++;
    else if (e.event_type === "UPDATE") s.update++;
    else if (e.event_type === "DELETE") s.delete++;
  }
  const tables = Array.from(byTable.values()).sort((a, b) => b.total - a.total);
  const latest = cov.latest_event || (events[0] && events[0].event_timestamp) || "—";
  const earliest = cov.earliest_event || (events.length ? events[events.length - 1].event_timestamp : "—");

  const v = VIEW(); clear(v);

  const sub = el("p", { class: "page-sub" },
    "What changed recently, and where — your starting point. ",
    el("b", { text: deletes + " delete(s)" }),
    " in the latest window: the ones worth a look first.");
  v.append(pageHead("Overview", sub));

  // stats
  const stats = el("div", { class: "ov-stats" });
  stats.append(ovStat(String(total), "changes indexed"));
  stats.append(ovStat(String(deletes), "deletes", deletes > 0 ? "danger" : ""));
  stats.append(ovStat(String(tables.length), "tables touched"));
  const wide = el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v small", text: latest }),
    el("div", { class: "ov-stat-k", text: "most recent change" }));
  stats.append(wide);
  v.append(stats);

  // grid
  const grid = el("div", { class: "ov-grid" });

  // recent changes
  const recentPanel = el("section", { class: "ov-panel" });
  const rHead = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Recent changes" }),
    el("a", { class: "btn btn-sm btn-ghost", href: "/events",
      onclick: (e) => { e.preventDefault(); navigate("events"); }, text: "Browse all events ›" }));
  recentPanel.append(rHead);
  const evlist = el("div", { class: "ov-evlist" });
  if (!events.length) {
    evlist.append(el("div", { class: "ev-empty", text: "No changes indexed yet." }));
  } else {
    events.slice(0, 8).forEach((e) => evlist.append(ovEventRow(e)));
  }
  recentPanel.append(evlist);
  grid.append(recentPanel);

  // activity by table
  const tablesPanel = el("section", { class: "ov-panel" });
  tablesPanel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Activity by table" })));
  const tbox = el("div", { class: "ov-tables" });
  tables.slice(0, 12).forEach((s) => tbox.append(ovTableRow(s)));
  tablesPanel.append(tbox);
  tablesPanel.append(el("div", { class: "ov-coverage" },
    el("span", { text: "coverage" }), " ",
    el("b", { text: earliest }), " → ", el("b", { text: latest })));
  grid.append(tablesPanel);

  v.append(grid);
  viewEnter();
}

function ovStat(value, key, mod) {
  return el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v" + (mod ? " " + mod : ""), text: value }),
    el("div", { class: "ov-stat-k", text: key }));
}

function colsSummary(cols, highlight) {
  cols = cols || [];
  if (cols.length > 2) return [el("span", { text: cols.length + " cols" })];
  const out = [];
  cols.forEach((c, i) => {
    if (i) out.push(document.createTextNode(", "));
    out.push(highlight ? el("span", { class: "hl", text: c }) : el("span", { text: c }));
  });
  return out;
}

function ovEventRow(e) {
  const row = el("div", { class: "ov-ev",
    onclick: () => navigate("events", { q: "pk:" + e.pk_values }) });
  row.append(el("span", { class: "ov-ev-time", text: e.event_timestamp }));
  row.append(badge(e.event_type));
  const tbl = el("span", { class: "ov-ev-tbl", text: e.schema_name + "." + e.table_name + " " });
  tbl.append(el("span", { class: "ov-ev-pk", text: "#" + e.pk_values }));
  row.append(tbl);
  row.append(el("span", { class: "ov-ev-cols" }, ...colsSummary(e.changed_columns, false)));
  const undo = el("a", { class: "btn btn-sm ov-ev-undo", text: "Undo",
    onclick: (ev) => { ev.stopPropagation(); undoEvent(e); } });
  row.append(undo);
  return row;
}

function ovTableRow(s) {
  const row = el("a", { class: "ov-tablerow",
    onclick: () => navigate("events", { q: s.key }) });
  row.append(el("span", { class: "ov-tname", text: s.key }));
  const bar = el("span", { class: "ov-bar" });
  if (s.insert) bar.append(el("span", { class: "ov-seg i", style: "flex:" + s.insert }));
  if (s.update) bar.append(el("span", { class: "ov-seg u", style: "flex:" + s.update }));
  if (s.delete) bar.append(el("span", { class: "ov-seg d", style: "flex:" + s.delete }));
  row.append(bar);
  row.append(el("span", { class: "ov-total", text: String(s.total) }));
  return row;
}

// ── Events ─────────────────────────────────────────────────────────────────

function renderEvents(params) {
  const v = VIEW(); clear(v);
  v.append(pageHead("Events", el("p", { class: "page-sub", text: "Browse indexed row events with full before / after images." })));

  const form = el("form", { id: "ev-form" });
  // search bar
  const searchwrap = el("div", { class: "ev-searchwrap" });
  searchwrap.append(icon("search", "ev-search-ic"));
  const search = el("input", { class: "ev-search", id: "ev-search", name: "q",
    autocomplete: "off", spellcheck: "false",
    placeholder: 'Search changes — try "orders", "type:delete", "pk:1006", "col:email"' });
  if (params && params.q) search.value = params.q;
  searchwrap.append(search);
  const advBtn = el("button", { class: "ev-advbtn", type: "button", text: "Filters",
    onclick: () => { const a = $("#ev-advanced", VIEW()); a.toggleAttribute("hidden"); advBtn.classList.toggle("on"); } });
  searchwrap.append(advBtn);
  form.append(searchwrap);

  // advanced panel
  const adv = el("div", { class: "ev-advanced", id: "ev-advanced", hidden: "" });
  adv.append(fieldSelect("Schema", "schema", "md", true));
  adv.append(fieldSelect("Table", "table", "md", false, true));
  adv.append(fieldInput("PK", "pk", "sm", "1006"));
  adv.append(fieldSelect("Type", "event_type", "sm", false, false, ["", "INSERT", "UPDATE", "DELETE"], "any"));
  adv.append(fieldInput("Changed column", "changed_column", "md", "email"));
  adv.append(fieldInput("Since", "since", "md", "YYYY-MM-DD HH:MM"));
  adv.append(fieldInput("Until", "until", "md", "YYYY-MM-DD HH:MM"));
  adv.append(fieldInput("Limit", "limit", "sm", "100"));
  form.append(adv);
  v.append(form);

  // result bar
  const bar = el("div", { class: "result-bar" });
  bar.append(el("span", { class: "result-count" }, el("b", { id: "ev-count", text: "…" }), " event(s)"));
  bar.append(el("span", { class: "spacer" }));
  bar.append(el("span", { class: "kbd-hint" },
    el("b", { text: "j" }), "/", el("b", { text: "k" }), " move · ",
    el("b", { text: "↵" }), " expand · ", el("b", { text: "u" }), " undo"));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "JSON",
    onclick: () => downloadEventsJSON(lastEvents) }));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "CSV",
    onclick: () => downloadEventsCSV(lastEvents) }));
  v.append(bar);

  // events list
  const list = el("div", { class: "events", id: "events-list" });
  const head = el("div", { class: "ev-head" });
  ["time", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
  list.append(head);
  list.append(el("div", { id: "ev-rows" }));
  v.append(list);

  // wire search (debounced)
  let t = null;
  const run = () => runEventsQuery(form);
  form.addEventListener("input", () => { clearTimeout(t); t = setTimeout(run, 200); });
  form.addEventListener("change", run);
  form.addEventListener("submit", (e) => { e.preventDefault(); run(); });
  wireSchemaCascade(form);

  populateSchemas(form);
  run();
  viewEnter();
}

function fieldInput(label, name, size, placeholder) {
  return el("div", { class: "field field--" + size },
    el("label", { class: "field-label", text: label }),
    el("input", { class: "input", name, placeholder: placeholder || "" }));
}
function fieldSelect(label, name, size, isSchema, isTable, options, anyLabel) {
  const sel = el("select", { class: "select" + (isSchema ? " schema-select" : "") + (isTable ? " table-select" : ""), name });
  if (options) options.forEach((o) => sel.append(opt(o, o === "" ? (anyLabel || "any") : o)));
  else sel.append(opt("", "any"));
  return el("div", { class: "field field--" + size },
    el("label", { class: "field-label", text: label }), sel);
}

// parseSmartQuery turns "type:delete pk:1006 orders" into structured filters +
// leftover free terms. Mirrors the prototype's parseQuery intent.
function parseSmartQuery(q) {
  const c = { terms: [] };
  const known = { table: 1, pk: 1, type: 1, col: 1, column: 1, schema: 1, since: 1, until: 1, gtid: 1, limit: 1 };
  for (const tok of (q || "").trim().split(/\s+/).filter(Boolean)) {
    const i = tok.indexOf(":");
    const k = i > 0 ? tok.slice(0, i).toLowerCase() : "";
    if (k && known[k]) {
      const val = tok.slice(i + 1);
      if (k === "col" || k === "column") c.changed_column = val;
      else if (k === "type") c.event_type = val.toUpperCase();
      else c[k] = val;
    } else if (tok.includes(".") && !c.schema && !c.table) {
      const [s, tb] = tok.split(".");
      if (s) c.schema = s;
      if (tb) c.table = tb;
    } else {
      // A bare word is a free-text term, refined client-side over the fetched
      // page (like the prototype). We do NOT map it to an exact table filter —
      // that would silently return 0 rows for a value/column search.
      c.terms.push(tok.toLowerCase());
    }
  }
  return c;
}

async function runEventsQuery(form) {
  const gen = serverGen;
  const rowsEl = $("#ev-rows", VIEW());
  const countEl = $("#ev-count", VIEW());
  if (!rowsEl) return;

  // Merge smart-search tokens with the advanced panel (structured fields win).
  const parsed = parseSmartQuery(form.elements.q ? form.elements.q.value : "");
  const f = Object.fromEntries(new FormData(form).entries());
  const merged = Object.assign({}, parsed);
  ["schema", "table", "pk", "event_type", "changed_column", "since", "until", "gtid", "limit"].forEach((k) => {
    if (f[k] && f[k].trim() && f[k] !== "any") merged[k] = f[k].trim();
  });

  // Build API params. pk / changed_column require schema+table server-side, so
  // when they are not both present we apply them client-side instead of 400ing.
  const apiParams = {};
  const hasScope = merged.schema && merged.table;
  ["schema", "table", "event_type", "since", "until", "gtid", "limit"].forEach((k) => {
    if (merged[k]) apiParams[k] = merged[k];
  });
  if (hasScope) {
    if (merged.pk) apiParams.pk = merged.pk;
    if (merged.changed_column) apiParams.changed_column = merged.changed_column;
  }

  let data;
  try {
    data = await api("/api/events?" + new URLSearchParams(apiParams).toString());
  } catch (err) {
    if (gen !== serverGen) return;
    clear(rowsEl); renderError(rowsEl, err);
    if (countEl) countEl.textContent = "0";
    return;
  }
  if (gen !== serverGen) return;

  // Client-side refine: unscoped pk/col + free terms.
  let events = data.events || [];
  const refine = [];
  if (!hasScope && merged.pk) refine.push(merged.pk.toLowerCase());
  if (!hasScope && merged.changed_column) refine.push(merged.changed_column.toLowerCase());
  refine.push(...parsed.terms);
  if (refine.length) {
    events = events.filter((e) => {
      const hay = (e.schema_name + "." + e.table_name + " " + e.event_type + " " + e.pk_values + " " +
        (e.changed_columns || []).join(" ") + " " +
        valueToString(e.row_before) + " " + valueToString(e.row_after)).toLowerCase();
      return refine.every((t) => hay.includes(t));
    });
  }

  lastEvents = events;
  if (countEl) countEl.textContent = String(events.length);
  buildEventRows(rowsEl, events);
}

function buildEventRows(container, events) {
  clear(container);
  if (!events.length) {
    const empty = el("div", { class: "ev-empty" }, "No changes match your search. ",
      el("b", { text: "Clear it", style: "cursor:pointer",
        onclick: () => { const s = $("#ev-search", VIEW()); if (s) { s.value = ""; runEventsQuery($("#ev-form", VIEW())); } } }),
      " to see everything.");
    container.append(empty);
    return;
  }
  events.forEach((e, i) => {
    const row = el("div", { class: "ev-row", "data-ev": i, tabindex: "0" });
    row.append(icon("caret", "ev-caret"));
    row.append(el("span", { class: "ev-time", text: e.event_timestamp }));
    row.append(el("span", { class: "ev-table", text: e.schema_name + "." + e.table_name }));
    row.append(el("span", {}, badge(e.event_type)));
    row.append(el("span", { class: "ev-pk", text: e.pk_values }));
    row.append(el("span", { class: "ev-cols" }, ...colsSummary(e.changed_columns, true)));
    const wrap = el("div", { class: "diff-wrap", id: "diff-" + i });
    let loaded = false;
    row.addEventListener("click", () => {
      const open = row.classList.toggle("open");
      if (open && !loaded) { clear(wrap); wrap.append(renderDiff(e)); loaded = true; }
    });
    container.append(row);
    container.append(wrap);
  });
}

function renderDiff(ev) {
  const before = ev.row_before || {};
  const after = ev.row_after || {};
  const cols = Array.from(new Set([...Object.keys(before), ...Object.keys(after)])).sort();
  const changed = new Set(ev.changed_columns || []);
  const wholeRow = ev.event_type === "INSERT" || ev.event_type === "DELETE";

  const grid = el("div", { class: "diff-grid" });
  grid.append(el("div", { class: "diff-h", text: "column" }));
  grid.append(el("div", { class: "diff-h", text: "before" }));
  grid.append(el("div", { class: "diff-h", text: "after" }));
  if (!cols.length) {
    grid.append(el("div", { class: "diff-col", text: "(no row image)" }));
    grid.append(el("div", { class: "diff-before" }));
    grid.append(el("div", { class: "diff-after" }));
  } else {
    cols.forEach((c) => {
      const isCh = wholeRow || changed.has(c);
      const m = isCh ? " diff-row-changed" : "";
      grid.append(el("div", { class: "diff-col" + m, text: c }));
      grid.append(el("div", { class: "diff-before" + m, text: c in before ? valueToString(before[c]) : "∅" }));
      grid.append(el("div", { class: "diff-after" + m, text: c in after ? valueToString(after[c]) : "∅" }));
    });
  }

  const foot = el("div", { class: "diff-foot" });
  foot.append(el("span", { class: "diff-foot-note", text: "Generates reversal SQL — nothing runs automatically." }));
  const label = ev.event_type === "DELETE" ? "Restore this row" : ev.event_type === "INSERT" ? "Undo this insert" : "Undo this change";
  foot.append(el("a", { class: "btn btn-sm btn-primary", text: label, onclick: () => undoEvent(ev) }));

  return el("div", { class: "diff" }, grid, foot);
}

// ── keyboard cursor on Events (j/k/↵/u) ──────────────────────────────────────

function moveCursor(delta) {
  const rows = $all(".ev-row", VIEW());
  if (!rows.length) return;
  if (cursorIdx >= 0 && rows[cursorIdx]) rows[cursorIdx].classList.remove("cursor");
  cursorIdx = Math.max(0, Math.min(rows.length - 1, cursorIdx + delta));
  const row = rows[cursorIdx];
  row.classList.add("cursor");
  row.scrollIntoView({ block: "nearest" });
}

// ── exports (connection_id-free) ──────────────────────────────────────────────

function downloadBlob(filename, content, mime) {
  try {
    const url = URL.createObjectURL(new Blob([content], { type: mime }));
    const a = el("a", { href: url, download: filename });
    document.body.append(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  } catch (err) { toast("download failed: " + ((err && err.message) || err)); }
}
function csvCell(v) {
  if (v === null || v === undefined) return "";
  const s = typeof v === "object" ? JSON.stringify(v) : String(v);
  return /[",\r\n]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
}
function downloadEventsJSON(events) {
  downloadBlob("dbtrail-events.json", JSON.stringify(events || [], null, 2), "application/json");
}
function downloadEventsCSV(events) {
  const lines = [EVENT_CSV_COLUMNS.join(",")];
  (events || []).forEach((ev) => lines.push(EVENT_CSV_COLUMNS.map((c) => csvCell(ev[c])).join(",")));
  downloadBlob("dbtrail-events.csv", lines.join("\r\n"), "text/csv");
}

// ── Recover ────────────────────────────────────────────────────────────────

function renderRecover(params) {
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Filter the changes you want to undo, preview the affected rows, then generate reversal SQL. ",
    el("b", { text: "Nothing is ever executed" }),
    " — copy or download the script and apply it yourself after review.");
  v.append(pageHead("Recover", sub));

  // Context banner when arriving via an event "Undo" (pendingRecover).
  const ctx = pendingRecover;
  if (ctx) {
    const banner = el("div", { class: "ctx-banner" });
    banner.append(el("span", { class: "badge " + badgeClass(ctx.type), text: ctx.type }));
    banner.append(el("div", { class: "ctx-main" },
      el("span", { class: "ctx-eyebrow", text: "Reverting this row to before this point" }),
      el("span", { class: "ctx-title", text: ctx.schema + "." + ctx.table + " · pk " + ctx.pk }),
      el("span", { class: "ctx-detail", text: "undoing changes up to " + ctx.type + " at " + ctx.time })));
    banner.append(el("span", { class: "spacer" }));
    banner.append(el("a", { class: "btn btn-sm btn-ghost", text: "Clear",
      onclick: () => { pendingRecover = null; navigate("recover"); } }));
    v.append(banner);
  }

  // Manual filter form
  const form = el("form", { class: "filters", id: "recover-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  form.append(fieldInput("Table", "table", "md", "orders"));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7"));
  form.append(fieldInput("Since", "since", "md", "YYYY-MM-DD HH:MM"));
  form.append(fieldInput("Until", "until", "md", "YYYY-MM-DD HH:MM"));
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Preview rows",
    onclick: () => previewRecover(form) }));
  actions.append(el("button", { class: "btn btn-primary", type: "submit", text: "Generate undo SQL" }));
  form.append(actions);
  v.append(form);

  v.append(el("div", { id: "recover-warnings", class: "warnings" }));
  v.append(el("div", { id: "recover-preview" }));
  v.append(el("div", { id: "recover-out" }));

  form.addEventListener("submit", (e) => { e.preventDefault(); generateUndo(form); });
  wireSchemaCascade(form);
  populateSchemas(form);

  // Prefill from context and auto-generate.
  if (ctx) {
    setSelectWhenReady(form, "schema", ctx.schema, () => {
      form.elements.table.value = ctx.table;
      form.elements.pk.value = ctx.pk;
      if (ctx.time) form.elements.until.value = ctx.time;
      generateUndo(form);
    });
  }
  viewEnter();
}

// setSelectWhenReady fills a schema select once its options have loaded, then
// loads its tables and runs cb. Lets the undo-bridge prefill survive the async
// schema fetch without racing it.
function setSelectWhenReady(form, name, value, cb) {
  const gen = serverGen;
  const sel = form.elements[name];
  const tryset = () => {
    if (Array.from(sel.options).some((o) => o.value === value)) {
      sel.value = value;
      loadTables(form).then(() => cb && cb());
      return true;
    }
    return false;
  };
  if (tryset()) return;
  let n = 0;
  const iv = setInterval(() => {
    // Bail if the operator switched servers or navigated away — otherwise a
    // late tick would auto-generate undo SQL against a detached/other form.
    if (gen !== serverGen || !document.contains(form)) { clearInterval(iv); return; }
    if (tryset() || ++n > 40) clearInterval(iv);
  }, 50);
}

async function previewRecover(form) {
  const gen = serverGen;
  const container = $("#recover-preview", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const params = {};
  ["schema", "table", "pk", "since", "until"].forEach((k) => { if (f[k] && f[k].trim()) params[k] = f[k].trim(); });
  try {
    const data = await api("/api/events?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    clear(container);
    container.append(el("div", { class: "meta-line" }, el("b", { text: String(data.count) }), " affected event(s) · limit " + data.limit));
    const list = el("div", { class: "events" });
    const head = el("div", { class: "ev-head" });
    ["time", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
    list.append(head);
    const rows = el("div");
    buildEventRows(rows, data.events || []);
    list.append(rows);
    container.append(list);
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(container, err);
  }
}

async function generateUndo(form) {
  const gen = serverGen;
  const warns = $("#recover-warnings", VIEW());
  const out = $("#recover-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const body = {};
  ["schema", "table", "pk", "since", "until"].forEach((k) => { if (f[k] && f[k].trim()) body[k] = f[k].trim(); });
  if (!body.schema) { renderError(out, "recover requires at least a schema filter"); return; }
  try {
    const data = await api("/api/recover", { method: "POST", body });
    if (gen !== serverGen) return;
    renderWarnings(warns, data.warnings);
    lastSQL = data.sql || "";
    clear(out);
    out.append(codePanel(lastSQL, data.statement_count + " statement(s) from " + data.row_count + " event(s)"));
  } catch (err) {
    if (gen !== serverGen) return;
    clear(warns); renderError(out, err);
  }
}

function codePanel(sql, metaLabel) {
  const panel = el("div", { class: "codepanel", id: "sql-panel" });
  const head = el("div", { class: "code-head" });
  head.append(icon("file"));
  const lbl = el("span", { class: "lbl" }, el("b", { text: "reversal.sql" }), " · " + (metaLabel || "read-only preview"));
  head.append(lbl);
  head.append(el("span", { class: "spacer" }));
  head.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: copySQL }));
  head.append(el("button", { class: "btn btn-sm", type: "button", text: "Download", onclick: downloadSQL }));
  panel.append(head);
  panel.append(el("pre", { class: "code", text: sql }));
  return panel;
}
function copySQL() {
  navigator.clipboard.writeText(lastSQL).then(() => toast("SQL copied to clipboard"), () => toast("copy failed"));
}
function downloadSQL() { downloadBlob("dbtrail-undo.sql", lastSQL, "application/sql"); }

// Bridge: an event → Recover, scoped to that row up to the event's timestamp.
function undoEvent(e) {
  pendingRecover = {
    schema: e.schema_name, table: e.table_name, pk: e.pk_values,
    type: e.event_type, time: e.event_timestamp,
  };
  navigate("recover");
}

// ── Time-travel (reconstruct) ─────────────────────────────────────────────────

function renderTimetravel(params) {
  // Landed on /timetravel but reconstruct is gated off (direct URL or Back).
  // Redirect by REWRITING the URL (replaceState) then re-dispatching — calling
  // navigate(push=false) here would leave the URL on /timetravel and re-resolve
  // straight back into this guard (infinite recursion).
  if (!capsCache.reconstruct) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Reconstruct a single row's full state as of a point in time — baseline snapshot + binlog deltas. Pick a row and a moment; see its value then, or its complete change history.");
  v.append(pageHead("Time-travel", sub));

  const form = el("form", { class: "filters", id: "tt-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  form.append(fieldSelect("Table", "table", "md", false, true));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7"));
  form.append(fieldInput("As of", "at", "md", "YYYY-MM-DD HH:MM (default: now)"));
  const gapsField = el("div", { class: "field", style: "justify-content:flex-end" },
    el("label", { class: "check" }, el("input", { type: "checkbox", name: "allow_gaps" }), el("span", { text: "Allow coverage gaps" })));
  form.append(gapsField);
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Full history", onclick: () => runReconstruct(form, true) }));
  actions.append(el("button", { class: "btn btn-primary", type: "submit", text: "Value as of T" }));
  form.append(actions);
  v.append(form);

  v.append(el("div", { id: "tt-warnings", class: "warnings" }));
  const out = el("div", { id: "tt-out" });
  out.append(el("div", { class: "tt-meta", text: "Run a query to reconstruct this row's state." }));
  v.append(out);

  form.addEventListener("submit", (e) => { e.preventDefault(); runReconstruct(form, false); });
  wireSchemaCascade(form);
  populateSchemas(form);
  viewEnter();
}

async function runReconstruct(form, history) {
  const gen = serverGen;
  const warns = $("#tt-warnings", VIEW());
  const out = $("#tt-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  if (!f.schema || !f.table || !f.pk) { clear(warns); renderError(out, "schema, table, and pk are required"); return; }
  const params = { schema: f.schema, table: f.table, pk: f.pk };
  if (f.at && f.at.trim()) params.at = f.at.trim();
  if (form.elements.allow_gaps && form.elements.allow_gaps.checked) params.allow_gaps = "true";
  if (history) params.history = "true";
  try {
    const data = await api("/api/reconstruct?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    renderWarnings(warns, data.warnings);
    clear(out);
    if (history) renderTimeline(out, data);
    else renderStateAt(out, data);
  } catch (err) {
    if (gen !== serverGen) return;
    clear(warns); renderError(out, err);
  }
}

function reconstructMeta(data, label) {
  return el("div", { class: "meta-line" },
    el("b", { text: data.schema + "." + data.table + " pk=" + data.pk }),
    " · " + label + " · baseline " + data.baseline_time + " · " + data.event_count + " event(s)");
}

function renderStateAt(container, data) {
  container.append(reconstructMeta(data, "as of " + data.at));
  if (!data.found) { container.append(el("div", { class: "deleted-note", text: "No row with this primary key existed at or before the selected time." })); return; }
  if (data.deleted) { container.append(el("div", { class: "deleted-note", text: "Row was deleted as of " + data.at + "." })); return; }
  container.append(stateTable(data.state || {}));
}

function stateTable(state) {
  const table = el("table", { class: "statetable" });
  Object.keys(state).forEach((k) => {
    table.append(el("tr", {}, el("th", { text: k }), el("td", { text: valueToString(state[k]) })));
  });
  return table;
}

function renderTimeline(container, data) {
  const entries = data.history || [];
  container.append(reconstructMeta(data, "history through " + data.at + " · " + entries.length + " state(s)"));
  if (!entries.length) { container.append(el("div", { class: "deleted-note", text: "No history for this primary key in the covered window." })); return; }

  const tl = el("div", { class: "timeline", id: "timeline" });
  let prev = null;
  entries.forEach((e) => {
    const node = el("div", { class: "tl-node" });
    const kind = e.source === "baseline" ? "baseline" : e.source.toLowerCase();
    node.append(el("span", { class: "tl-dot " + kind }));
    const head = el("div", { class: "tl-head" });
    head.append(el("span", { class: "badge " + (e.source === "baseline" ? "b-baseline" : badgeClass(e.source)), text: e.source }));
    head.append(el("span", { class: "tl-time", text: e.time }));
    node.append(head);

    const body = el("div", { class: "tl-body" });
    if (e.deleted || !e.state) {
      body.append(el("span", { class: "pair" }, el("span", { class: "pk", text: "(row deleted)" })));
    } else {
      const changed = new Set();
      if (prev) for (const k of Object.keys(e.state)) if (valueToString(e.state[k]) !== valueToString(prev[k])) changed.add(k);
      Object.keys(e.state).forEach((k) => {
        body.append(el("span", { class: "pair" },
          el("span", { class: "pk", text: k + "=" }),
          el("span", { class: "pv" + (e.source !== "baseline" && changed.has(k) ? " changed" : ""), text: valueToString(e.state[k]) })));
      });
      prev = e.state;
    }
    node.append(body);

    if (e.source !== "baseline") {
      const acts = el("div", { class: "tl-actions" });
      acts.append(el("a", { class: "btn btn-sm tl-restore", text: "Restore to this state",
        onclick: () => undoEvent({ schema_name: data.schema, table_name: data.table, pk_values: data.pk, event_type: e.source, event_timestamp: e.time }) }));
      node.append(acts);
    }
    tl.append(node);
  });
  container.append(tl);

  // "draws itself" — progressive-enhancement reveal (decorative).
  requestAnimationFrame(() => {
    tl.classList.add("drawn");
    $all(".tl-node", tl).forEach((n, i) => setTimeout(() => n.classList.add("in"), 60 + i * 55));
  });
}

// ── Status ─────────────────────────────────────────────────────────────────

async function renderStatus() {
  const gen = serverGen;
  viewLoading();
  let data;
  try { data = await api("/api/status"); }
  catch (err) { if (gen !== serverGen) return; const v = VIEW(); clear(v); v.append(pageHead("Status", null)); renderError(v, err); return; }
  if (gen !== serverGen) return;
  updateSideMeta(data);

  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub", text: "Index health at a glance — coverage window, stream position, and what's been captured." });
  v.append(pageHead("Status", sub));

  const cards = el("div", { class: "cards" });
  const cov = data.coverage || {};
  const stream = data.stream || null;
  const arch = data.archives || null;

  cards.append(statusCard("Summary", [
    ["total events (est.)", data.total_events_estimate, true],
    ["indexed files", (data.files || []).length],
    ["partitions", (data.partitions || []).length],
  ]));
  cards.append(statusCard("Coverage", [
    ["earliest event", cov.earliest_event],
    ["latest event", cov.latest_event],
    ["total events", cov.total_events],
    ["schema changes", cov.schema_changes],
  ]));
  if (stream) cards.append(statusCard("Stream", [
    ["mode", stream.mode],
    ["binlog file", stream.binlog_file],
    ["position", stream.binlog_position],
    ["events indexed", stream.events_indexed],
  ]));
  if (arch) cards.append(statusCard("Archives", [
    ["files", arch.total_files],
    ["rows", arch.total_rows],
    ["size", arch.total_size_human],
  ]));
  v.append(cards);
  viewEnter();
}

function statusCard(title, rows) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: title }));
  rows.forEach(([k, val, big]) => {
    card.append(el("div", { class: "kv" },
      el("span", { class: "kv-k", text: k }),
      el("span", { class: "kv-v" + (big ? " big" : ""), text: val === null || val === undefined ? "—" : String(val) })));
  });
  return card;
}

function updateSideMeta(status) {
  const servers = status.servers || [];
  const s0 = servers[0];
  const conn = s0 ? (s0.username + "@" + s0.host + ":" + s0.port) : "—";
  const connEl = document.getElementById("meta-conn");
  if (connEl) connEl.textContent = serversEmpty && capsCache.monitor ? "internal index" : conn;
  const streamEl = document.getElementById("meta-stream");
  if (streamEl) {
    clear(streamEl);
    streamEl.append("stream ");
    if (status.stream) {
      streamEl.append(el("b", { text: status.stream.mode }));
      if (status.stream.binlog_file) streamEl.append(" · " + status.stream.binlog_file);
    } else {
      streamEl.append(el("b", { text: "—" }));
    }
  }
}

// ── Storage (rotation · S3 archiving · baselines · credentials) ──────────────

async function renderStorage() {
  // Gated like Time-travel: a direct URL / Back with the capability off must
  // REWRITE the URL (replaceState) before re-dispatching — see renderTimetravel.
  if (!capsCache.monitor) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const gen = serverGen;
  viewLoading();
  // Each fetch degrades independently: a panel renders its own failure note
  // instead of one error wiping the whole page. (A 401 inside api() raises the
  // sign-in gate and bumps serverGen, so the stale-render guard below bails.)
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  const [serversRes, rotation, storage, baselines] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/rotation").catch(asErr),
    api("/api/storage").catch(asErr),
    api("/api/baselines").catch(asErr),
  ]);
  if (gen !== serverGen) return;
  // Same guard as renderOverview: a throw inside the build must show an
  // error, never leave the "Loading…" skeleton up forever.
  try {
    buildStorage(serversRes, rotation, storage, baselines);
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Storage", null)); renderError(v, err);
  }
}

function buildStorage(serversRes, rotation, storage, baselines) {
  // serversRes is the raw /api/servers payload or {error} — archivingPanel
  // must be able to tell "failed to load" from "genuinely no sources", or a
  // transient 500 would render the affirmative "No monitored sources yet" lie.
  const servers = (serversRes && serversRes.servers) || [];
  const serversErr = serversRes && serversRes.error;
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "S3 archiving, rotation, and the baselines Time-travel reads. ",
    el("b", { text: "No credentials are stored here." }));
  v.append(pageHead("Storage", sub));

  const cards = el("div", { class: "cards" });
  const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
  cards.append(rotationCard(rotation));
  cards.append(credentialsCard(storage));
  cards.append(baselineSummaryCard(baselines, cur));
  v.append(cards);

  const grid = el("div", { class: "ov-grid", style: "margin-top:18px" });
  grid.append(archivingPanel(servers, serversErr));
  grid.append(baselinesPanel(baselines, servers));
  v.append(grid);
  viewEnter();
}

function kvRow(card, k, val) {
  card.append(el("div", { class: "kv" },
    el("span", { class: "kv-k", text: k }),
    el("span", { class: "kv-v", text: val === null || val === undefined || val === "" ? "—" : String(val) })));
}

function rotationCard(rot) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Rotation" }));
  if (!rot || rot.error) {
    card.append(el("p", { class: "form-hint", text: "Could not load the rotation policy" + (rot && rot.error ? ": " + rot.error : ".") }));
    return card;
  }
  kvRow(card, "retention", rot.retain);
  kvRow(card, "interval", rot.interval);
  kvRow(card, "future partitions", rot.add_future);
  kvRow(card, "policy", rot.source === "override" ? "console override (live)" : "daemon defaults");
  if (!rot.enabled) card.append(el("p", { class: "form-hint", text: "Rotation is OFF at the daemon (--rotate-retain off) — saved changes need a restart." }));
  card.append(el("div", { class: "stg-cardfoot" },
    el("button", { class: "btn btn-sm", type: "button", text: "Edit rotation…", onclick: showRotationDialog })));
  return card;
}

function credentialsCard(storage) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "AWS credentials" }));
  const aws = storage && storage.aws;
  if (!aws) {
    card.append(el("p", { class: "form-hint", text: "Could not read the daemon's credential signals" + (storage && storage.error ? ": " + storage.error : ".") }));
    return card;
  }
  kvRow(card, "access keys (env)", aws.access_key_env ? "set" : "not set");
  kvRow(card, "profile (env)", aws.profile || "—");
  kvRow(card, "region (env)", aws.region_env || "—");
  kvRow(card, "~/.aws config", aws.shared_config ? "present" : "absent");
  if (aws.container_creds) kvRow(card, "ECS task role", "detected");
  if (aws.web_identity) kvRow(card, "EKS IRSA", "detected");
  card.append(el("p", { class: "form-hint stg-hint", text:
    "Resolved from the daemon's AWS credential chain. IAM roles work even when nothing shows as set here." }));
  return card;
}

function baselineSummaryCard(b, cur) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Baselines" }));
  if (!b || b.error) {
    card.append(el("p", { class: "form-hint", text: "Could not list baselines: " + ((b && b.error) || "unavailable") }));
    return card;
  }
  if (!b.configured) {
    kvRow(card, "source", "not configured");
    kvRow(card, "time-travel", "off");
    return card;
  }
  const snaps = b.snapshots || [];
  kvRow(card, "source", b.source);
  kvRow(card, "snapshots", String(snaps.length) + (b.truncated ? "+" : ""));
  kvRow(card, "latest", snaps.length ? snaps[0].time : "none yet");
  if (snaps.length) kvRow(card, "age", formatAge(snaps[0].age_hours));
  kvRow(card, "time-travel", b.reconstruct ? "enabled" : "off (archives disabled)");
  return card;
}

// baselineConfigHint: the boot (cli) entry is not editable from the UI — its
// baseline comes only from --baseline-dir/--baseline-s3 (or the BINTRAIL_
// CONSOLE_BASELINE_DIR/_S3 env; BASELINE_DIR in the compose stack) — so the
// "edit the server" instruction would point it at a dead end.
function baselineConfigHint(cur) {
  if (!cur) return "Add a server first (Manage servers).";
  if (cur.kind === "ephemeral") {
    return "Restart the daemon with --baseline-dir or --baseline-s3 (compose: BASELINE_DIR in .env).";
  }
  return "Set Baseline dir or S3 under Manage servers → Edit → Advanced.";
}

function formatAge(hours) {
  if (hours == null) return "—";
  if (hours < 1) return Math.max(1, Math.round(hours * 60)) + " min";
  if (hours < 48) return Math.round(hours) + " h";
  return Math.round(hours / 24) + " day(s)";
}

function archivingPanel(servers, serversErr) {
  const panel = el("section", { class: "ov-panel" });
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "S3 archiving per source" }),
    el("a", { class: "btn btn-sm btn-ghost", text: "Manage servers ›", onclick: openServersModal })));
  const list = el("div", { class: "stg-list" });
  const sources = servers.filter((s) => s.has_source);
  if (serversErr) {
    list.append(el("div", { class: "ev-empty", text: "Could not load servers: " + serversErr }));
  } else if (!sources.length) {
    list.append(el("div", { class: "ev-empty", text: "No monitored sources yet — add one under Manage servers." }));
  } else {
    sources.forEach((s) => {
      const row = el("div", { class: "stg-row" });
      row.append(el("span", { class: "stg-name", text: s.name }));
      row.append(el("span", { class: "stg-dest" + (s.archive_s3 ? "" : " muted"), text: s.archive_s3 || "drop-only (no S3 destination)" }));
      if (s.monitor_state) row.append(el("span", { class: "chip chip-mon", text: s.monitor_state.replace("_", " ").toUpperCase(), title: MON_STATE_TITLES[s.monitor_state] || ("monitoring " + s.monitor_state) }));
      row.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Configure",
        onclick: () => { openServersModal(); editServer(s.id); } }));
      list.append(row);
    });
  }
  panel.append(list);
  panel.append(el("p", { class: "form-hint stg-foot", text:
    "Rotated partitions upload as Parquet before being dropped, so history survives retention." }));
  return panel;
}

function baselinesPanel(b, servers) {
  const panel = el("section", { class: "ov-panel" });
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  let owner = cur ? serverLabel(cur) : "";
  if (!owner && b && !b.error && b.configured) owner = "daemon (--baseline-dir / --baseline-s3)";
  panel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Baseline snapshots" + (owner ? " — " + owner : "") })));
  const list = el("div", { class: "stg-list" });
  if (!b || b.error) {
    list.append(el("div", { class: "ev-empty", text: "Could not list baselines: " + ((b && b.error) || "unavailable") }));
  } else if (!b.configured) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "No baselines configured." }),
      el("p", { class: "stg-empty-sub", text: "A baseline is a full-table snapshot — with one, Time-travel reconstructs complete rows, not just rows with recent changes." }),
      el("p", { class: "stg-empty-sub", text: "1. Create snapshots:" }),
      el("code", { class: "stg-code", text: "docker compose --profile baseline run --rm baseline" }),
      el("p", { class: "stg-empty-sub", text: "2. " + baselineConfigHint(cur) })));
  } else if (!(b.snapshots || []).length) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "Source configured, no snapshots found." }),
      el("code", { class: "stg-code", text: b.source }),
      el("p", { class: "stg-empty-sub", text: "Run bintrail dump + bintrail baseline to create the first one. The path must point at the snapshots' parent directory (<timestamp>/<schema>/<table>.parquet)." })));
  } else {
    b.snapshots.forEach((sn) => {
      const row = el("div", { class: "stg-row" });
      row.append(el("span", { class: "stg-name mono", text: sn.time }));
      row.append(el("span", { class: "stg-dest", text:
        (sn.tables || []).length + " table(s)" + (sn.binlog_file ? " · " + sn.binlog_file + ":" + sn.binlog_pos : "") }));
      row.append(el("span", { class: "stg-age", text: formatAge(sn.age_hours) + " ago" }));
      list.append(row);
    });
    if (b.truncated) list.append(el("div", { class: "ev-empty", text: "…older snapshots not shown." }));
  }
  panel.append(list);
  return panel;
}

// ── schemas / tables cascade ──────────────────────────────────────────────────

async function loadSchemas() {
  if (schemaCache) return schemaCache;
  const gen = serverGen;
  const data = await api("/api/schemas");
  const schemas = data.schemas || [];
  // Guard the cache WRITE, not just the render: a response in flight when the
  // operator switches servers must not poison the freshly-cleared cache with
  // the previous server's schemas.
  if (gen === serverGen) schemaCache = schemas;
  return schemas;
}

async function populateSchemas(root) {
  const gen = serverGen;
  const selects = $all(".schema-select", root || document);
  if (!selects.length) return;
  let schemas;
  try { schemas = await loadSchemas(); }
  catch (err) {
    if (gen !== serverGen) return;
    selects.forEach((sel) => { const keep = sel.value; clear(sel); sel.append(opt("", "(error: " + ((err && err.message) || err) + ")")); sel.value = keep; });
    return;
  }
  if (gen !== serverGen) return;
  selects.forEach((sel) => {
    const keep = sel.value;
    clear(sel);
    sel.append(opt("", "— select —"));
    schemas.forEach((s) => sel.append(opt(s, s)));
    if (keep) sel.value = keep;
  });
}

async function loadTables(form) {
  const gen = serverGen;
  const sel = form.querySelector(".schema-select");
  const tsel = form.querySelector(".table-select");
  if (!tsel) return;
  const schema = sel ? sel.value : "";
  clear(tsel);
  tsel.append(opt("", "— any —"));
  if (!schema) return;
  let tables = tablesCache.get(schema);
  try {
    if (!tables) {
      const data = await api("/api/schemas?schema=" + encodeURIComponent(schema));
      tables = data.tables || [];
      if (gen === serverGen) tablesCache.set(schema, tables); // don't cache under a server we've since switched away from
    }
  } catch (err) {
    if (gen !== serverGen) return;
    tsel.append(opt("", "(error loading tables)"));
    toast("failed to load tables: " + ((err && err.message) || err));
    return;
  }
  if (gen !== serverGen) return;
  tables.forEach((t) => tsel.append(opt(t, t)));
}

function wireSchemaCascade(root) {
  $all(".schema-select", root).forEach((sel) => sel.addEventListener("change", () => loadTables(sel.closest("form"))));
}

// updateSrvNote labels where header-less data comes from when no servers are
// listed. The hidden boot index is NOT guaranteed empty — a daemon restarted
// without its previous SOURCE_DSN, or pointed at a reused index DB, renders
// real history here — so the origin must be attributed right under the
// "no servers yet" switcher, not only in the docs. Monitor-gated: on a
// registry-only serve an empty list 404s instead, where this label would lie.
function updateSrvNote() {
  const n = document.getElementById("srv-note");
  if (n) n.hidden = !(serversEmpty && capsCache.monitor);
}

// ── capabilities gating ────────────────────────────────────────────────────

async function gateCapabilities() {
  const gen = serverGen;
  let caps = {};
  // Degrading to {} hides capability-gated UI (Time-travel tab, the source
  // section of the server form) — warn so a wrongly-shaped UI is diagnosable.
  // A 401 is NOT capability loss: rethrow so session expiry surfaces as the
  // sign-in gate (api() already raised it), never as silently vanished tabs.
  try { caps = await api("/api/capabilities"); } catch (err) {
    if (err && err.status === 401) throw err;
    console.warn("capabilities check failed; UI degrades to no-capability gating", err);
    caps = {};
  }
  if (gen !== serverGen) return;
  capsCache = caps || {};
  $all("[data-capability]").forEach((node) => node.classList.toggle("cap-on", !!capsCache[node.dataset.capability]));
  applyAuthGate();
  updateSrvNote(); // capsCache.monitor may have just changed
}

// ── server registry: switcher + modal CRUD ──────────────────────────────────

// serverLabel: the ephemeral boot entry's name is the reserved id "default",
// which reads as meaningless in the switcher — label it by its database name
// (what the entry actually is: the daemon's own index DB from --index-dsn).
function serverLabel(s) {
  if (s.kind === "ephemeral") return (s.dbname || s.name) + " (cli)";
  return s.name;
}

async function loadServers() {
  const data = await api("/api/servers");
  defaultServerId = data.default_id || "";
  const servers = data.servers || [];
  // Reconcile a stale selection (server deleted elsewhere).
  if (currentServer && !servers.some((s) => s.id === currentServer)) setCurrentServer("");
  serversEmpty = !servers.length;
  updateSrvNote();
  const sel = document.getElementById("server-select");
  if (sel) {
    clear(sel);
    if (!servers.length) {
      // No listed servers: a hidden-boot fresh install (source-less watch),
      // or a registry-only console whose last entry was deleted.
      const o = opt("", "no servers yet");
      o.disabled = true;
      sel.append(o);
      sel.value = "";
    } else {
      // Registry servers first; the ephemeral boot entry goes last (it shows
      // only where it carries data: serve, or watch with --source-dsn).
      const ordered = servers.filter((s) => s.kind !== "ephemeral")
        .concat(servers.filter((s) => s.kind === "ephemeral"));
      ordered.forEach((s) => {
        const o = opt(s.id, serverLabel(s));
        if (s.kind === "ephemeral") o.title = "The daemon's own index database (from --index-dsn); managed by the command line";
        sel.append(o);
      });
      sel.value = currentServer || defaultServerId;
    }
  }
  return servers;
}

async function switchServer(id) {
  setCurrentServer(id);
  serverGen++;
  schemaCache = null;
  tablesCache.clear();
  // A switch is a fresh context: drop any carried undo-context and SQL so they
  // can never be auto-applied against a different server's index.
  pendingRecover = null;
  lastSQL = "";
  try { await gateCapabilities(); } catch (err) {
    if (err && err.status === 401) return; // chokepoint already raised the sign-in gate
    throw err;
  }
  renderRoute(); // re-render the current screen for the new server
}

// modal -----------------------------------------------------------------------

function buildServersModal() {
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal", role: "dialog", "aria-label": "Servers" });

  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: "Servers" }));
  const desc = el("p", { class: "modal-desc" },
    "Named connections to dbtrail index databases, stored in a local file on the console host. ");
  desc.append(el("span", { "data-capability": "monitor" },
    "This process can also ", el("b", { text: "monitor" }),
    " a source MySQL: add one below and dbtrail runs the preflight checks, provisions an index for it, and starts streaming — no terminal needed."));
  head.append(desc);
  head.append(el("button", { class: "modal-x", type: "button", text: "✕", onclick: closeServersModal }));
  modal.append(head);

  const body = el("div", { class: "modal-body" });
  body.append(el("div", { class: "srv-list", id: "servers-list" }));
  body.append(el("div", { id: "server-add-wrap", style: "margin-top:18px" },
    el("button", { class: "btn btn-primary", type: "button", id: "server-add", text: "+ Add server", onclick: () => showServerForm(null) })));
  body.append(el("div", { id: "server-form-mount" }));
  modal.append(body);

  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeServersModal(); });
  return scrim;
}

function openServersModal() {
  const mount = document.getElementById("modal");
  clear(mount);
  mount.append(buildServersModal());
  // re-apply capability gating to the freshly-mounted [data-capability] nodes
  $all("[data-capability]", mount).forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
  refreshServersList();
}
function closeServersModal() { document.getElementById("modal").replaceChildren(); }

// ── rotation settings ────────────────────────────────────────────────────────

// showRotationDialog edits the daemon-global built-in rotation policy (retain /
// interval / future partitions). Changes apply live — the watch loop re-reads
// them on its next cycle. Only reachable when this process is a supervisor
// (capsCache.monitor gates the ⌘K entry); the read-only console has no loop to
// tune and the PUT 403s there anyway.
async function showRotationDialog() {
  if (loginGateRaised) return;
  let cur;
  try { cur = await api("/api/rotation"); }
  catch (err) { toast("failed to load rotation settings: " + ((err && err.message) || err)); return; }

  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal", role: "dialog", "aria-label": "Rotation" });

  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: "Rotation" }));
  head.append(el("p", { class: "modal-desc", text:
    "The daemon drops index partitions older than the retention window every interval, keeping a few future partitions ready. One shared schedule covers every monitored source; changes take effect on the next cycle." }));
  head.append(el("button", { class: "modal-x", type: "button", text: "✕", onclick: closeRotationDialog }));
  modal.append(head);

  const form = el("form", { class: "filters", style: "display:block" });
  const grid = el("div", { class: "form-grid" });
  grid.append(srvField("Retention", "retain", { placeholder: "e.g. 30d, 24h" }));
  grid.append(srvField("Interval", "interval", { placeholder: "e.g. 1h, 30m" }));
  grid.append(srvField("Future partitions", "add_future", { placeholder: "e.g. 3" }));
  form.append(grid);
  form.elements.retain.value = cur.retain || "";
  form.elements.interval.value = cur.interval || "";
  form.elements.add_future.value = (cur.add_future != null ? cur.add_future : "");

  const note = el("p", { class: "form-hint", style: "margin-top:10px" });
  if (!cur.enabled) note.textContent = "Rotation is OFF at the daemon (--rotate-retain off). Saved changes need a daemon restart to take effect.";
  else if (cur.source === "default") note.textContent = "Currently using the daemon defaults (--rotate-* / BINTRAIL_ROTATE_*). Saving creates a console override that applies live.";
  else note.textContent = "A console override is active, applied live each cycle.";
  form.append(note);

  const msg = el("div", { class: "form-msg" });
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Save" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Cancel", onclick: closeRotationDialog }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitRotation(form, msg, cur.enabled); });
  modal.append(form);

  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeRotationDialog(); });
  mount.replaceChildren(scrim);
}

function closeRotationDialog() { document.getElementById("modal").replaceChildren(); }

async function submitRotation(form, msg, wasEnabled) {
  const body = {
    retain: form.elements.retain.value.trim(),
    interval: form.elements.interval.value.trim(),
    add_future: parseInt(form.elements.add_future.value, 10) || 0,
  };
  try {
    await api("/api/rotation", { method: "PUT", body });
  } catch (err) {
    msg.textContent = (err && err.message) || String(err);
    msg.className = "form-msg err";
    return;
  }
  closeRotationDialog();
  // When the daemon booted with rotation off the loop isn't running, so the
  // save is inert until a restart — say so rather than implying it took effect.
  toast(wasEnabled ? "Rotation settings saved" : "Saved — rotation is off at the daemon; restart to apply");
}

async function refreshServersList() {
  const list = document.getElementById("servers-list");
  if (!list) return;
  let servers;
  try { servers = await loadServers(); }
  catch (err) { renderError(list, err); return; }
  clear(list);
  if (!servers.length) { list.append(el("div", { class: "ev-empty", text: "No servers yet — add your first connection." })); return; }
  servers.forEach((s) => list.append(serverRow(s)));
}

function isLiveMonitorState(st) { return st === "running" || st === "pending" || st === "stalled" || st === "lost_position"; }

function serverRow(s) {
  const item = el("div", { class: "srv-item" });
  const nm = el("span", { class: "nm" },
    el("span", { class: "health-dot" + (s.connected ? " ok" : ""), title: s.connected ? "connected" : "not connected yet" }),
    serverLabel(s));
  item.append(nm);
  if (s.kind === "ephemeral") item.append(el("span", { class: "chip chip-cli", text: "CLI", title: "From --index-dsn; managed by the command line" }));
  if (s.reconstruct) item.append(el("span", { class: "chip chip-tt", text: "TT", title: "Baseline configured: Time-travel available" }));
  if (s.monitor_state) item.append(el("span", { class: "chip chip-mon", text: s.monitor_state.replace("_", " ").toUpperCase(), title: MON_STATE_TITLES[s.monitor_state] || ("monitoring " + s.monitor_state) }));

  let desc;
  if (s.has_source && s.source_host) desc = "watching " + s.source_user + "@" + s.source_host + ":" + (s.source_port || "3306") + (s.schemas ? " [" + s.schemas + "]" : "");
  else if (s.host) desc = s.user + "@" + s.host + ":" + (s.port || "3306") + "/" + s.dbname;
  else desc = s.dbname || "";
  item.append(el("span", { class: "srv-desc conn", text: desc }));

  item.append(el("span", { class: "srv-status", id: "srv-status-" + s.id }));

  const acts = el("span", { class: "acts row-acts" });
  const monitorable = capsCache.monitor && s.has_source && s.kind !== "ephemeral";
  if (monitorable) {
    const running = isLiveMonitorState(s.monitor_state);
    acts.append(el("button", { class: "btn btn-sm" + (running ? "" : " btn-primary"), type: "button", text: running ? "Stop" : "Start",
      onclick: () => running ? stopMonitorRow(s.id) : startMonitorRow(s.id) }));
  }
  acts.append(el("button", { class: "btn btn-sm", type: "button", text: "Test", onclick: () => testServerRow(s.id) }));
  acts.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Edit", disabled: !s.editable, onclick: () => editServer(s.id) }));
  acts.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Delete", disabled: !s.deletable, onclick: () => deleteServer(s) }));
  item.append(acts);
  return item;
}

// server form (add/edit) ------------------------------------------------------

// srvField builds a labeled input row: <label class="field"><span/><input/></label>.
function srvField(label, name, opts) {
  opts = opts || {};
  return el("label", { class: "field" },
    el("span", { class: "field-label", text: label }),
    el("input", { class: "input", name, type: opts.type || "text", placeholder: opts.placeholder || "", autocomplete: opts.autocomplete }));
}

function buildServerForm() {
  const form = el("form", { class: "filters", id: "server-form", style: "display:block;margin-top:18px" });
  form.append(el("input", { type: "hidden", name: "id" }));

  // Name is the one field every entry needs, whatever the path — keep it out
  // of both sections so the index section reads as fully optional.
  const top = el("div", { class: "form-grid" });
  top.append(srvField("Name", "name", { placeholder: "prod-db-01" }));
  form.append(top);

  const mon = el("fieldset", { class: "form-section", "data-capability": "monitor" });
  mon.append(el("legend", { class: "form-legend", text: "Monitor a source MySQL" }));
  mon.append(el("p", { class: "form-hint", text: "Paste the server to watch — dbtrail runs the preflight checks, provisions an index database for it automatically, and starts streaming. Nothing else to fill in beyond a name." }));
  const monGrid = el("div", { class: "form-grid" });
  monGrid.append(srvField("Source host", "source_host", { placeholder: "db.example.com" }));
  monGrid.append(srvField("Source port", "source_port", { placeholder: "3306" }));
  monGrid.append(srvField("Source user", "source_user", { placeholder: "repl" }));
  monGrid.append(srvField("Source password", "source_password", { type: "password", autocomplete: "new-password" }));
  monGrid.append(srvField("Schemas", "schemas", { placeholder: "(optional) shop,billing" }));
  monGrid.append(srvField("Archive to S3", "archive_s3", { placeholder: "(optional) s3://bucket/prefix/" }));
  mon.append(monGrid);
  mon.append(el("p", { class: "form-hint", style: "margin-top:10px", text: "Archive to S3: rotated partitions upload here as Parquet before they're dropped, so the history survives retention and stays queryable. Needs AWS credentials on the daemon (env / role)." }));
  form.append(mon);

  // BYO index is the advanced path — collapsed behind a <details> so the
  // monitor-first form stays one field + source. Open/close rules live in
  // showServerForm.
  const adv = el("details", { class: "form-advanced", id: "server-advanced" });
  adv.append(el("summary", { class: "form-adv-summary", text: "Advanced — bring your own index (optional)" }));
  const idx = el("fieldset", { class: "form-section" });
  idx.append(el("legend", { class: "form-legend", text: "Index connection" }));
  const idxGrid = el("div", { class: "form-grid" });
  idxGrid.append(srvField("Host", "host", { placeholder: "127.0.0.1" }));
  idxGrid.append(srvField("Port", "port", { placeholder: "3306" }));
  idxGrid.append(srvField("User", "user", { placeholder: "bintrail" }));
  idxGrid.append(srvField("Password", "password", { type: "password", autocomplete: "new-password" }));
  idxGrid.append(srvField("Index database", "dbname", { placeholder: "binlog_index" }));
  idxGrid.append(srvField("Baseline dir", "baseline_dir", { placeholder: "(optional) enables Time-travel" }));
  idxGrid.append(srvField("Baseline S3", "baseline_s3", { placeholder: "s3://bucket/prefix/" }));
  idx.append(idxGrid);
  idx.append(el("label", { class: "check", style: "margin-top:10px" },
    el("input", { type: "checkbox", name: "no_archive" }), el("span", { text: "Disable archive auto-discovery" })));
  adv.append(idx);
  form.append(adv);

  const foot = el("div", { class: "modal-foot filter-actions" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Save" }));
  foot.append(el("button", { class: "btn", type: "button", id: "server-test", text: "Test connection" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", id: "server-cancel", text: "Cancel" }));
  form.append(foot);
  form.append(el("div", { id: "server-form-msg", class: "form-msg" }));
  form.append(el("div", { id: "doctor-cards", class: "doctor-cards" }));
  return form;
}

function showServerForm(prefill) {
  document.getElementById("server-add-wrap").hidden = true;
  const mountEl = document.getElementById("server-form-mount");
  const form = buildServerForm();
  mountEl.replaceChildren(form);
  $all("[data-capability]", form).forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
  $("#server-cancel", form).addEventListener("click", hideServerForm);
  $("#server-test", form).addEventListener("click", () => testServerForm(form));
  form.addEventListener("submit", (e) => { e.preventDefault(); saveServer(form); });

  // Where the index connection is the whole form (serve-only process: no
  // monitor capability), or the entry being edited carries index fields,
  // the "advanced" block must start expanded — and in serve-only mode
  // there is nothing to collapse it back to, so the toggle hides. Note a
  // monitor-first save also ends with index fields (the derived index DSN
  // round-trips as host/dbname in the DTO), so "collapsed by default"
  // means a fresh add; editing any saved entry shows its index.
  const adv = $("#server-advanced", form);
  const hasIndexFields = !!(prefill && (prefill.host || prefill.dbname || prefill.baseline_dir || prefill.baseline_s3 || prefill.no_archive));
  adv.open = !capsCache.monitor || hasIndexFields;
  $(".form-adv-summary", adv).hidden = !capsCache.monitor;

  if (prefill) {
    form.elements.id.value = prefill.id || "";
    ["name", "host", "port", "user", "dbname", "baseline_dir", "baseline_s3", "archive_s3", "source_host", "source_port", "source_user", "schemas"].forEach((k) => {
      if (form.elements[k] && prefill[k] != null) form.elements[k].value = prefill[k];
    });
    if (form.elements.no_archive) form.elements.no_archive.checked = !!prefill.no_archive;
    form.elements.password.placeholder = prefill.has_password ? "(unchanged — leave blank to keep)" : "(none)";
    form.elements.source_password.placeholder = prefill.has_source_password ? "(unchanged — leave blank to keep)" : "";
  }
  form.elements.name.focus();
}
function hideServerForm() {
  document.getElementById("server-form-mount").replaceChildren();
  const addWrap = document.getElementById("server-add-wrap");
  if (addWrap) addWrap.hidden = false;
}

function formMsg(text, isError) {
  const m = document.getElementById("server-form-msg");
  if (!m) return;
  m.className = "form-msg " + (isError ? "err" : "ok");
  m.textContent = text;
}

// keep-password semantics: omit password fields when blank (= keep stored).
function serverFormBody(form) {
  const f = form.elements;
  const body = {
    name: f.name.value.trim(),
    host: f.host.value.trim(), port: f.port.value.trim(), user: f.user.value.trim(), dbname: f.dbname.value.trim(),
    baseline_dir: f.baseline_dir.value.trim(), baseline_s3: f.baseline_s3.value.trim(),
    no_archive: !!f.no_archive.checked,
    archive_s3: f.archive_s3.value.trim(),
    source_host: f.source_host.value.trim(), source_port: f.source_port.value.trim(),
    source_user: f.source_user.value.trim(), schemas: f.schemas.value.trim(),
  };
  if (f.password.value !== "") body.password = f.password.value;
  if (f.source_password.value !== "") body.source_password = f.source_password.value;
  return body;
}

async function editServer(id) {
  // Catch only the fetch: a render throw from showServerForm must surface
  // as an uncaught error, not masquerade as "failed to load server".
  let s;
  try { s = await api("/api/servers/" + encodeURIComponent(id)); }
  catch (err) { toast("failed to load server: " + ((err && err.message) || err)); return false; }
  showServerForm(s);
  return true;
}

async function saveServer(form) {
  const id = form.elements.id.value;
  const body = serverFormBody(form);
  let saved;
  try {
    saved = await api(id ? "/api/servers/" + encodeURIComponent(id) : "/api/servers", { method: id ? "PUT" : "POST", body });
  } catch (err) { formMsg((err && err.message) || String(err), true); return; }

  // Zero-terminal auto-start: a monitor-capable process with a source DSN starts
  // streaming on save (after preflight). Doctor warnings keep the form open.
  if (capsCache.monitor && saved.has_source && !isLiveMonitorState(saved.monitor_state)) {
    formMsg("running preflight checks…", false);
    const res = await startMonitor(saved.id);
    await refreshServersList();
    if (res && res.started && !doctorWarnings(res.doctor)) { hideServerForm(); toast("Monitoring started — events will appear within a minute"); }
    else if (res && res.started) { renderDoctor(res.doctor); formMsg("monitoring started — review the warnings below", false); }
    else if (res) { renderDoctor(res.doctor); formMsg("preflight failed — fix the items below and Save again", true); }
    else { formMsg("could not start monitoring — see the error toast and try again", true); } // startMonitor returned null (transport error)
    return;
  }
  hideServerForm();
  await refreshServersList();
  toast(id ? "Server updated" : "Server added");
}

async function deleteServer(s) {
  if (!window.confirm('Remove server "' + s.name + '"? This only removes the saved connection — nothing happens to the server itself.')) return;
  try { await api("/api/servers/" + encodeURIComponent(s.id), { method: "DELETE" }); }
  catch (err) { toast("delete failed: " + ((err && err.message) || err)); return; }
  if (currentServer === s.id) { await switchServer(""); const sel = document.getElementById("server-select"); if (sel) sel.value = defaultServerId; }
  await refreshServersList();
  toast("Server removed");
}

// test / doctor / monitor -----------------------------------------------------

function testResultText(res) {
  if (!res.ok) return "✗ " + (res.error || "unreachable");
  let s = "✓ ok · " + res.latency_ms + " ms";
  if (res.server_version) s += " · MySQL " + res.server_version;
  // has_index/schema_current are tri-state: absent = the metadata lookup itself
  // failed (unknown) — never render that as the confident negative.
  if (res.has_index === false) s += " · no binlog_events table (not a dbtrail index?)";
  else if (res.has_index === undefined || res.schema_current === undefined) s += " · index metadata unavailable";
  else if (res.schema_current === false) s += " · index schema outdated (run bintrail index/stream once)";
  return s;
}

async function testServerForm(form) {
  const id = form.elements.id.value;
  const body = serverFormBody(form);
  formMsg("testing…", false);
  try {
    const res = await api(id ? "/api/servers/" + encodeURIComponent(id) + "/test" : "/api/servers/test", { method: "POST", body });
    formMsg(testResultText(res), !res.ok);
  } catch (err) { formMsg((err && err.message) || String(err), true); }
}

async function testServerRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) { slot.className = "srv-status"; slot.textContent = "testing…"; }
  try {
    const res = await api("/api/servers/" + encodeURIComponent(id) + "/test", { method: "POST", body: {} });
    if (slot) { slot.className = "srv-status " + (res.ok ? "ok" : "err"); slot.textContent = testResultText(res); }
  } catch (err) { if (slot) { slot.className = "srv-status err"; slot.textContent = "✗ " + ((err && err.message) || err); } }
}

function doctorWarnings(report) { return !!(report && report.warnings > 0); }

function renderDoctor(report) {
  const box = document.getElementById("doctor-cards");
  if (!box) return;
  clear(box);
  if (!report || !report.checks) return;
  report.checks.forEach((chk) => {
    const status = ["pass", "fail", "warn"].includes(chk.status) ? chk.status : "skip";
    const mark = { pass: "✓", fail: "✗", warn: "!", skip: "–" }[status];
    const card = el("div", { class: "doctor-card " + status });
    card.append(el("span", { class: "dc-mark", text: mark }));
    const bodyEl = el("div", { class: "dc-body" }, el("div", { class: "dc-name", text: chk.name + (chk.detail ? " — " + chk.detail : "") }));
    if (chk.remediation) bodyEl.append(el("pre", { class: "dc-rem", text: chk.remediation }));
    card.append(bodyEl);
    box.append(card);
  });
}

async function startMonitor(id) {
  try { return await api("/api/servers/" + encodeURIComponent(id) + "/monitor/start", { method: "POST", body: {} }); }
  catch (err) { toast("start failed: " + ((err && err.message) || err)); return null; }
}

async function startMonitorRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) { slot.className = "srv-status"; slot.textContent = "running checks…"; }
  const res = await startMonitor(id);
  await refreshServersList();
  if (!res) return;
  if (res.started && !doctorWarnings(res.doctor)) { toast("Monitoring started"); return; }
  const opened = await editServer(id);
  if (opened) {
    renderDoctor(res.doctor);
    formMsg(res.started ? "monitoring started — review the warnings below" : "preflight failed — fix the items below, Save, and Start again", !res.started);
  } else { toast(res.started ? "Monitoring started — with warnings" : "Preflight failed"); }
}

async function stopMonitorRow(id) {
  try { await api("/api/servers/" + encodeURIComponent(id) + "/monitor/stop", { method: "POST", body: {} }); toast("Monitoring stopped"); }
  catch (err) { toast("stop failed: " + ((err && err.message) || err)); }
  await refreshServersList();
}

// ── command palette (⌘K) ──────────────────────────────────────────────────

let cmdkSel = 0, cmdkItems = [];

function cmdkCommands() {
  const cmds = [
    { group: "Navigate", label: "Overview", run: () => navigate("overview") },
    { group: "Navigate", label: "Events", run: () => navigate("events") },
    { group: "Navigate", label: "Recover", run: () => navigate("recover") },
    { group: "Navigate", label: "Status", run: () => navigate("status") },
  ];
  if (capsCache.reconstruct) cmds.push({ group: "Navigate", label: "Time-travel", run: () => navigate("timetravel") });
  if (capsCache.monitor) cmds.push({ group: "Navigate", label: "Storage", run: () => navigate("storage") });
  cmds.push({ group: "Actions", label: "Manage servers", run: () => { closeCmdk(); openServersModal(); } });
  if (capsCache.monitor) cmds.push({ group: "Actions", label: "Configure rotation…", run: () => { closeCmdk(); showRotationDialog(); } });
  if (capsCache.auth) {
    cmds.push({
      group: "Actions",
      label: capsCache.auth.password_set ? "Change console password…" : "Set console password…",
      run: () => { closeCmdk(); showPasswordDialog(); },
    });
    if (capsCache.auth.auth_kind === "session") {
      cmds.push({ group: "Actions", label: "Log out", run: () => { closeCmdk(); doLogout(); } });
    }
  }
  cmds.push({ group: "Actions", label: "Search events for…", hint: "type then ↵", search: true });
  return cmds;
}

function openCmdk() {
  if (loginGateRaised) return; // the sign-in gate owns the screen
  const mount = document.getElementById("cmdk-mount");
  clear(mount);
  const scrim = el("div", { class: "cmdk-scrim open" });
  const panel = el("div", { class: "cmdk", role: "dialog", "aria-label": "Commands" });
  const top = el("div", { class: "cmdk-top" });
  top.append(icon("search"));
  const q = el("input", { class: "cmdk-q", id: "cmdk-q", placeholder: "Search commands, or type to find events…", autocomplete: "off", spellcheck: "false" });
  top.append(q);
  top.append(el("span", { class: "cmdk-escpill", text: "esc" }));
  panel.append(top);
  panel.append(el("div", { class: "cmdk-list", id: "cmdk-list" }));
  scrim.append(panel);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeCmdk(); });
  mount.append(scrim);
  q.addEventListener("input", () => renderCmdk(q.value));
  q.addEventListener("keydown", cmdkKeydown);
  renderCmdk("");
  q.focus();
}
function closeCmdk() { document.getElementById("cmdk-mount").replaceChildren(); }

function renderCmdk(query) {
  const list = document.getElementById("cmdk-list");
  if (!list) return;
  const q = (query || "").toLowerCase().trim();
  let cmds = cmdkCommands();
  if (q) cmds = cmds.filter((c) => c.search || c.label.toLowerCase().includes(q));
  cmdkItems = cmds; cmdkSel = 0;
  clear(list);
  if (!cmds.length) { list.append(el("div", { class: "cmdk-empty", text: "No commands match." })); return; }
  let group = null;
  cmds.forEach((c, i) => {
    if (c.group !== group) { group = c.group; list.append(el("div", { class: "cmdk-group", text: group })); }
    const item = el("button", { class: "cmdk-item" + (i === cmdkSel ? " sel" : ""), type: "button",
      "data-idx": i, onclick: () => runCmdk(c, query) });
    item.append(icon("search", "cmdk-ic"));
    item.append(el("span", { class: "cmdk-label", text: c.search && q ? 'Search events: "' + query.trim() + '"' : c.label }));
    if (c.hint) item.append(el("span", { class: "cmdk-hint", text: c.hint }));
    list.append(item);
  });
}
function runCmdk(c, query) {
  if (c.search) { closeCmdk(); navigate("events", query && query.trim() ? { q: query.trim() } : null); return; }
  closeCmdk(); c.run();
}
function cmdkKeydown(e) {
  const list = document.getElementById("cmdk-list");
  if (e.key === "Escape") { closeCmdk(); return; }
  if (e.key === "ArrowDown") { e.preventDefault(); cmdkSel = Math.min(cmdkItems.length - 1, cmdkSel + 1); }
  else if (e.key === "ArrowUp") { e.preventDefault(); cmdkSel = Math.max(0, cmdkSel - 1); }
  else if (e.key === "Enter") { e.preventDefault(); const c = cmdkItems[cmdkSel]; if (c) runCmdk(c, e.target.value); return; }
  else return;
  $all(".cmdk-item", list).forEach((n) => n.classList.toggle("sel", Number(n.dataset.idx) === cmdkSel));
  const sel = $(".cmdk-item.sel", list); if (sel) sel.scrollIntoView({ block: "nearest" });
}

// ── global keyboard ──────────────────────────────────────────────────────────

function globalKeydown(e) {
  // The sign-in gate is modal: no shortcuts reach the workspace behind it.
  if (loginGateRaised) return;
  // ⌘K / Ctrl+K opens the palette anywhere.
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openCmdk(); return; }
  // j/k/↵/u row nav — only on Events, only when not typing in a field.
  const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement && document.activeElement.tagName);
  if (typing || routeFromLocation() !== "events") return;
  const rows = $all(".ev-row", VIEW());
  if (e.key === "j") { e.preventDefault(); moveCursor(1); }
  else if (e.key === "k") { e.preventDefault(); moveCursor(-1); }
  else if (e.key === "Enter") { if (cursorIdx >= 0 && rows[cursorIdx]) { e.preventDefault(); rows[cursorIdx].click(); } }
  else if (e.key === "u") { if (cursorIdx >= 0 && lastEvents[cursorIdx]) { e.preventDefault(); undoEvent(lastEvents[cursorIdx]); } }
}

// ── init ─────────────────────────────────────────────────────────────────────

// bootSequence is the load-bearing startup order: servers (reconcile
// selection) → caps → route. ONE definition, called from both the normal boot
// and the post-login path — two inline copies of an order-sensitive sequence
// is how wrong-shape-UI bugs come back. Returns the server list (null when
// the boot aborted on a dead credential — the sign-in gate is already up).
async function bootSequence() {
  let servers = [];
  // renderRoute() below clears #view, so an in-view error here would be wiped;
  // toast it instead. If the backend is down, the chosen view surfaces its own
  // error when its fetch fails.
  try { servers = await loadServers(); } catch (err) {
    if (err && err.status === 401) return null;
    toast("couldn't load servers: " + ((err && err.message) || err));
  }
  try { await gateCapabilities(); } catch (err) {
    if (err && err.status === 401) return null;
    throw err;
  }
  renderRoute();
  return servers;
}

async function init() {
  document.getElementById("server-select").addEventListener("change", (e) => switchServer(e.target.value));
  document.getElementById("manage-servers").addEventListener("click", openServersModal);
  document.getElementById("open-cmdk").addEventListener("click", openCmdk);
  document.getElementById("logout-btn").addEventListener("click", doLogout);
  document.addEventListener("keydown", globalKeydown);

  // Sidebar nav (real hrefs upgraded to in-place swaps). A manual nav starts
  // fresh — clear any carried "Undo" context so the sidebar's Recover link
  // never shows a stale banner (the undoEvent bridge sets it and navigates
  // directly, bypassing this handler, so its context survives).
  $all(".nav-item").forEach((a) => a.addEventListener("click", (e) => {
    e.preventDefault();
    // Route-less nav items are modal triggers (e.g. #nav-rotation) with their
    // own bindings — navigate(undefined) would coerce to /overview and repaint
    // the view behind the modal.
    if (!a.dataset.route) return;
    pendingRecover = null;
    navigate(a.dataset.route);
  }));
  document.getElementById("nav-rotation").addEventListener("click", showRotationDialog);
  window.addEventListener("popstate", renderRoute);

  // Pre-auth gate: ask the (unauthenticated) probe how this console
  // authenticates BEFORE firing data fetches that are guaranteed 401s. First
  // run with no credential → create-password screen; password configured →
  // sign-in form; token mode → the printed-link hint.
  if (!TOKEN) {
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) { /* server down — fall through, the view will surface it */ }
    if (auth.setup) { showLoginOverlay({ setup: true }); return; }
    if (auth.password_login) { showLoginOverlay({ passwordLogin: true }); return; }
    toast("No token in URL — open the link printed by `bintrail-console`.");
  }

  const servers = await bootSequence();
  if (servers === null) return; // dead credential: the sign-in gate is up

  // First-run onboarding: a monitor-capable process with no source yet opens the
  // servers modal once per tab so the operator can add one without a terminal.
  try {
    if (capsCache.monitor && servers.every((s) => !s.has_source) && !sessionStorage.getItem(ONBOARD_KEY)) {
      sessionStorage.setItem(ONBOARD_KEY, "1");
      openServersModal();
    }
  } catch (_) {}
}

if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
else init();
