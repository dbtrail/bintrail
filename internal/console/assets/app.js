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
//     Logins ALSO set an HttpOnly session cookie server-side (#1370) so a new
//     tab is already signed in; when sessionStorage is empty this file PROBES
//     a cheap authenticated endpoint and, on 200, runs cookie-only (no
//     Authorization header — the middleware accepts the cookie). Every
//     non-GET request must then send Content-Type: application/json: that is
//     the server's cookie-CSRF marker, and api() sets it unconditionally.
//  2. The X-Bintrail-Server header is captured INSIDE api() at dispatch time, so
//     an in-flight request keeps the server it was fired for.
//  3. Every async render captures `serverGen` before its await and bails if a
//     server switch happened mid-flight (no cross-server repaint).
//  4. Capability gating toggles the `.cap-on` class on [data-capability] nodes.
//  5. CSV export (EVENT_CSV_COLUMNS) and the JSON view stay in lockstep: both
//     mirror whatever eventDTO serializes server-side (including connection_id,
//     per epic #701 D1) — CSV is not a separate, narrower boundary to maintain.
//  6. The DOM is built with el()/textContent only — no innerHTML anywhere. The
//     one string→DOM path is svgEl(), which DOMParses STATIC icon constants
//     (never data) into SVG nodes. No value from the API touches markup.
"use strict";

// ── constants ──────────────────────────────────────────────────────────────

const TOKEN_KEY = "bintrail_console_token";
const SERVER_KEY = "bintrail_console_server";
const ONBOARD_KEY = "bintrail_console_onboarded";

// Export columns. connection_id is INCLUDED (epic #701 D1 — no longer a
// gated field on the events API; CSV mirrors the JSON view exactly).
const EVENT_CSV_COLUMNS = [
  "event_id", "event_timestamp", "schema_name", "table_name", "event_type",
  "pk_values", "changed_columns", "gtid", "connection_id", "binlog_file",
  "start_pos", "end_pos", "row_before", "row_after",
];

// event_type → badge modifier class.
// Ceiling for an Events export (#1297). Must track eventsMaxLimit in
// internal/console/api.go — the server clamps to it regardless, so a larger
// number here would only make the UI promise more than it delivers.
const EVENT_EXPORT_MAX = 1000;
const BADGE_CLASS = { UPDATE: "b-update", INSERT: "b-insert", DELETE: "b-delete" };
function badgeClass(t) { return BADGE_CLASS[t] || "b-baseline"; }

const ROUTES = ["overview", "events", "timetravel", "recover", "sql", "status", "storage", "connect",
  // Protect (#1384): baselines and verification used to be two of three panels
  // on Settings > Storage. They are operations that produce and validate
  // recovery artifacts, not settings, and the snapshot list is unbounded in
  // practice — it pushed verification, the panel that answers "are my backups
  // restorable", roughly two screens below the fold.
  "baselines", "verification"];

const MON_STATE_TITLES = {
  failed: "connection is failing and retrying automatically; press Start for details",
  stalled: "connected, but hasn't made progress for several minutes",
  lost_position: "some old changes were deleted before dbtrail could capture them; those are permanently lost, but current changes are still being captured",
};

// Static decorative SVGs (module constants — parsed by svgEl via DOMParser).
const ICONS = {
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"><circle cx="11" cy="11" r="7"></circle><path d="M21 21l-4.3-4.3"></path></svg>`,
  caret: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"></path></svg>`,
  file: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"></path><path d="M14 3v5h5"></path></svg>`,
  warn: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/></svg>`,
  calendar: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"></rect><path d="M8 3v4M16 3v4M3 10h18"></path></svg>`,
  ext: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>`,
  refresh: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg>`,
};

// ── module state ─────────────────────────────────────────────────────────────

let TOKEN = "";
let currentServer = "";       // X-Bintrail-Server target ("" = backend default)
let defaultServerId = "";
let serverGen = 0;            // bumped on every server switch (staleness guard)
let viewGen = 0;              // bumped on every route render (staleness guard)
let capsCache = {};           // last /api/capabilities for the selected server
let extViews = [];            // extension views advertised for the selected server (embedding builds)
let extSettings = [];         // extension settings panels advertised for this SESSION (permission-gated, not per-server)
let lastSQL = "";             // last generated undo SQL (for copy/download)
let lastEvents = [];          // last rendered (filtered, capped) event page
// Events keyset paging (#1297). evPages[k] is the `before` cursor that opens
// page k (page 0 opens with none) plus how many events precede it, so the
// header can say "showing 201–300" without an OFFSET. Cursors are echoed back
// from the server, never built here.
let evPages = [{ before: null, offset: 0 }];
let evPageIdx = 0;
// The last Events request (API params + client-side refine terms), captured so
// Export can re-run the SAME search across every page instead of dumping only
// the page on screen. See exportEvents.
let evLastQuery = null;
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

// prefersReducedMotion reports the OS-level setting for motion started from
// JavaScript, which no media query in style.css can reach.
//
// Read at CALL time rather than cached once: the setting can change while the
// console is open — both macOS and Windows apply it live — and this console
// stays open for hours during an incident.
function prefersReducedMotion() {
  return !!(window.matchMedia && window.matchMedia("(prefers-reduced-motion: reduce)").matches);
}

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
// XML parsing applies no default namespace, so a constant without an explicit
// xmlns yields a namespace-less <svg>: it takes up its CSS box and paints
// NOTHING. Every icon here was in that state — the extension nav item, for one,
// has been rendering label-only. Inject the namespace rather than requiring
// each constant to remember it, so the next icon added cannot reintroduce this.
const SVG_NS = `xmlns="http://www.w3.org/2000/svg"`;
function svgEl(s) {
  if (!s.includes("xmlns=")) s = s.replace("<svg", "<svg " + SVG_NS);
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
  // No Authorization header without a token: a cookie-bootstrapped tab (fresh
  // tab, HttpOnly session cookie, empty sessionStorage) authenticates via the
  // cookie the browser attaches on its own (same-origin fetch sends cookies
  // by default).
  const headers = TOKEN ? { Authorization: "Bearer " + TOKEN } : {};
  if (currentServer) headers["X-Bintrail-Server"] = currentServer; // captured at dispatch
  // Content-Type on EVERY state-changing request, body or not (a body-less
  // POST like logout included): the server's cookie-auth CSRF belt requires
  // the application/json marker on non-GET methods, and sending it under
  // Bearer too keeps the request shape uniform.
  const method = (opts.method || "GET").toUpperCase();
  if (opts.body || (method !== "GET" && method !== "HEAD")) headers["Content-Type"] = "application/json";
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
    // AbortController support (#1363): callers that show a cancelable busy
    // affordance pass a signal; an abort rejects with err.name "AbortError".
    signal: opts.signal,
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

// apiText is api() for a text/plain endpoint. Same auth, same server header,
// same central 401 handling — only the parsing differs, because views.sql is a
// SQL file and api()'s JSON.parse would reject it as malformed.
async function apiText(path) {
  const headers = TOKEN ? { Authorization: "Bearer " + TOKEN } : {};
  if (currentServer) headers["X-Bintrail-Server"] = currentServer;
  const res = await fetch(path, { headers });
  const text = await res.text();
  if (!res.ok) {
    if (res.status === 401) handleUnauthorized();
    // An error body here is the API's JSON {error}; fall back to the raw text
    // so a proxy's HTML error page still says something useful.
    let msg = text || "HTTP " + res.status;
    try { const j = JSON.parse(text); if (j && j.error) msg = j.error; } catch (_) { /* raw text it is */ }
    throw apiError(res.status, msg);
  }
  return text;
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

// probeCookieSession: a fresh tab has no sessionStorage token (it is per-tab),
// but a login in another tab left the HttpOnly session cookie — try one cheap
// authenticated GET before raising the sign-in gate. /api/servers is the
// lightest authenticated read (registry only, touches no index DB), the
// browser attaches the cookie on its own, and a 200 means the middleware
// accepted it: the tab then runs cookie-only with TOKEN empty. Raw fetch on
// purpose — a 401 here is the NORMAL signed-out case and must not recurse
// into handleUnauthorized's "session expired" messaging.
async function probeCookieSession() {
  try {
    const res = await fetch("/api/servers");
    return res.ok;
  } catch (_) { return false; }
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
  if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
  // Token mode has no session to expire and no form to "sign in" to — say what
  // actually happened (the stored token is no longer accepted). An advertised
  // external provider also mints real sessions, so with one present the
  // session-expired copy is the accurate one (SSO-only deployments never had
  // an access token at all).
  const msg = auth.password_login || auth.sso_start ? "Session expired; sign in again." : "This access token is no longer valid.";
  showLoginOverlay({ passwordLogin: !!auth.password_login, message: msg, ssoName: auth.sso_name, ssoStart: auth.sso_start });
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
  // .login-gate marks THIS scrim as the full-screen brand canvas (#1371).
  // showPasswordDialog() mounts its panel in the same slot but is a task modal
  // over an authenticated workspace, so it keeps the ordinary translucent scrim.
  const scrim = el("div", { class: "modal-scrim show login-gate" });
  const panel = el("div", { class: "modal login-panel", role: "dialog", "aria-label": opts.setup ? "Set up console" : "Sign in" });
  panel.append(el("h2", { class: "modal-title", text: "dbtrail console" }));

  if (opts.setup) {
    panel.append(el("p", { class: "modal-desc", text: "First run: create a username and password for this console." }));
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
    appendSSOEntry(panel, opts);
    scrim.append(panel);
    mount.replaceChildren(scrim);
    form.elements.password.focus();
    return;
  }

  if (!opts.passwordLogin) {
    if (opts.message) panel.append(el("p", { class: "modal-desc", text: opts.message }));
    // The printed-?token=-link hint only fits token mode. When the probe
    // advertises an external provider, that provider may be the SOLE
    // credential (no token, no password — the server allows it), so pointing
    // at a nonexistent token link would be false guidance; the SSO entry
    // below is the sign-in path.
    if (opts.ssoStart) {
      panel.append(el("p", { class: "modal-desc", text: "Sign in with the provider below to continue." }));
    } else {
      panel.append(el("p", { class: "modal-desc", text: "Open the link that bintrail-console printed when it started. It carries the access token this page needs." }));
    }
    appendSSOEntry(panel, opts);
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
  appendSSOEntry(panel, opts);
  scrim.append(panel);
  mount.replaceChildren(scrim);
  form.elements.password.focus();
}

// appendSSOEntry adds the external-login entry ("Continue with <name>") under
// whatever the gate mode rendered, when the /api/auth probe advertised a
// provider (sso_start/sso_name → opts.ssoStart/opts.ssoName). A plain <a> on
// purpose — full navigation, never fetch: the provider owns the whole flow
// and lands back on /?token=<session>, reusing the existing token bootstrap.
function appendSSOEntry(panel, opts) {
  if (!opts.ssoStart) return;
  panel.append(el("div", { class: "login-divider", text: "or" }));
  panel.append(el("a", { class: "btn login-sso", href: opts.ssoStart, text: "Continue with " + (opts.ssoName || "single sign-on") }));
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
  } catch (_) { loginMsg(msg, "Network error. Is the console still running?"); return; }
  if (res.status === 403) {
    // Setup closed under us (a concurrent `user set-password`, another tab, or
    // a CLI set it first). Unlike login, the setup endpoint self-disables —
    // re-probe and switch the gate to the sign-in form instead of leaving the
    // operator stuck re-posting to a now-closed endpoint.
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) {}
    showLoginOverlay({ passwordLogin: !!auth.password_login, message: "A password was already created. Sign in.", ssoName: auth.sso_name, ssoStart: auth.sso_start });
    return;
  }
  if (!res.ok) {
    let m = "Could not set the password.";
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts; wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
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
  } catch (_) { loginMsg(msg, "Network error. Is the console still running?"); return; }
  if (res.status === 429) {
    const retry = res.headers.get("Retry-After");
    loginMsg(msg, "Too many attempts; wait " + (retry ? retry + "s" : "a minute") + " and retry.");
    return;
  }
  if (!res.ok) {
    let m = "Invalid username or password.";
    if (res.status !== 401) { try { m = (await res.json()).error || m; } catch (_) {} }
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
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
  // Session auth no longer implies password login: an external provider mints
  // normal sessions too, so re-probe the gate mode like handleUnauthorized
  // does — an SSO-only deployment must get its SSO entry back (not a password
  // form every submit 401s), and after a `user remove` the console can be in
  // first-run setup again. Probe failure falls back to the password form.
  let auth = null;
  try { auth = await fetchAuthInfo(); } catch (_) {}
  if (!auth) { showLoginOverlay({ passwordLogin: true, message: "Signed out." }); return; }
  if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
  showLoginOverlay({ passwordLogin: !!auth.password_login, message: "Signed out.", ssoName: auth.sso_name, ssoStart: auth.sso_start });
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
    ? "Lets you sign in with a username and password instead of just the access token."
    : "Changing your password signs you out of every other open session." }));

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
    // Cookie-bootstrapped tabs have no TOKEN — the session cookie carries the
    // credential, and the JSON Content-Type doubles as the CSRF marker.
    const headers = TOKEN
      ? { Authorization: "Bearer " + TOKEN, "Content-Type": "application/json" }
      : { "Content-Type": "application/json" };
    res = await fetch("/api/auth/password", {
      method: "POST",
      headers,
      body: JSON.stringify(body),
    });
  } catch (_) { loginMsg(msg, "Network error. Is the console still running?"); return; }
  if (!res.ok) {
    let m = "HTTP " + res.status;
    try { m = (await res.json()).error || m; } catch (_) {}
    if (res.status === 429) m = "Too many attempts; wait " + (res.headers.get("Retry-After") || "60") + "s.";
    loginMsg(msg, m);
    return;
  }
  let data;
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server; try again."); return; }
  // Every other session just died; this tab continues on the fresh one.
  TOKEN = data.token || TOKEN;
  try { sessionStorage.setItem(TOKEN_KEY, TOKEN); } catch (_) {}
  closeLoginOverlay();
  toast("Password " + (firstSet ? "set" : "updated"));
  // password_set (and possibly auth_kind) changed server-side.
  try { await gateCapabilities(); } catch (_) {}
}

// ── toast / errors / warnings ─────────────────────────────────────────────────

// toast shows a transient notice. Use it for things that went RIGHT, or for
// neutral progress. Failures must not fade — see toastError.
//
// It writes to its OWN node, never the error node. An earlier attempt shared
// one element and had toast() yield to a visible error, which silently dropped
// messages this function also carries: "rotate your MCP token" after a display
// interruption (the token is already unrecoverable by then), "capture did NOT
// restart onto the new snapshot", export-truncation warnings. Each is reported
// nowhere else, and one undismissed error would have suppressed them all for
// the rest of the session. Two nodes, no contention.
function toast(msg) {
  const t = document.getElementById("toast");
  if (!t) return;
  clearTimeout(toast._t);
  t.textContent = msg;
  t.hidden = false;
  toast._t = setTimeout(() => { t.hidden = true; }, 2200);
}

// toastError shows a failure that stays until the operator dismisses it.
//
// A failure that disappears on its own is a failure nobody saw. The baseline
// privilege refusal is ~550 characters of remediation — which privilege to
// grant, the exact GRANT statement, and the alternative modes — and at the
// 2.2s auto-hide it was not merely easy to miss, it was unreadable. The
// operator was left with a button that did nothing and no way to recover the
// reason. Nothing here starts a timer.
function toastError(msg) {
  const t = document.getElementById("toast-error");
  if (!t) return;
  // A second failure STACKS rather than replaces. These never auto-hide, so a
  // silent overwrite would destroy an unread failure with nothing to hint one
  // existed — two servers' baselines refusing together is the ordinary case.
  //
  // A REPEAT of a message already showing gets a count, not a dropped call.
  // Dropping it was wrong: several of these messages carry no server name
  // ("Startup checks failed", "Copy failed."), so failing on server B after
  // server A produced NO change on screen — indistinguishable from success.
  const prior = t.hidden ? [] : $all(".toast-msg", t).map((n) => ({
    text: n.dataset.msg || n.textContent,
    n: Number(n.dataset.count || "1"),
  }));
  const dupe = prior.find((p) => p.text === msg);
  if (dupe) dupe.n += 1;
  else prior.push({ text: msg, n: 1 });
  // role=alert so a screen reader announces it; the visual persistence is
  // useless to someone who cannot see it fade. The node is unhidden BEFORE the
  // text lands: a live region that appears with its content already in place
  // is the shape screen readers routinely fail to announce.
  t.setAttribute("role", "alert");
  t.replaceChildren();
  t.hidden = false;
  const body = el("div", { class: "toast-body" });
  for (const m of prior) {
    const span = el("span", { class: "toast-msg", text: m.n > 1 ? m.text + "  (\u00d7" + m.n + ")" : m.text });
    span.dataset.msg = m.text;
    span.dataset.count = String(m.n);
    body.append(span);
  }
  t.append(body);
  const close = el("button", { class: "toast-close", type: "button", "aria-label": "Dismiss all" });
  close.textContent = "\u2715";
  close.addEventListener("click", () => dismissToast());
  t.append(close);
}

function dismissToast() {
  const t = document.getElementById("toast-error");
  if (!t) return;
  t.hidden = true;
  t.removeAttribute("role");
  t.textContent = "";
}

// ESC dismisses a persistent error, matching every other dismissible surface
// in this console — but only as the LAST of them.
//
// cmdkKeydown closes the ⌘K palette WITHOUT stopping propagation
// (deliberately, see globalKeydown), so the same Escape reaches the document
// and would also destroy an unread error — the failure this whole change
// exists to prevent, arriving through a different door.
//
// Hence the CAPTURE phase (registered with `true` in init). Deciding on the
// bubble phase cannot work: by then globalKeydown has already emptied #modal
// and cmdkKeydown has already emptied #cmdk-mount, so the very state this
// guard reads to yield the key has been erased by the handlers it is yielding
// to, and it would dismiss the notice anyway. Verified in a browser: on the
// bubble phase one Escape closes a modal AND wipes the notice behind it.
//
// It never calls preventDefault or stopPropagation, so yielding is all it does
// — the dialog handlers still run normally on the same event.
function toastEscape(e) {
  if (e.key !== "Escape") return;
  if (loginGateRaised) return;
  const cmdk = document.getElementById("cmdk-mount");
  if (cmdk && cmdk.firstChild) return;
  const modalMount = document.getElementById("modal");
  if (modalMount && modalMount.firstChild) return;
  // Every surface that consumes Escape must be listed here, including ones
  // that are NOT inside #modal. The date picker renders into document.body
  // (see toggleDatePicker) and closes itself on Escape without stopping
  // propagation, so closing a calendar popover used to destroy the notice —
  // the same defect as the ⌘K palette, through a third door. A fourth surface
  // will not announce itself; if you add one that handles Escape, add it here.
  if (document.querySelector(ESCAPE_OWNING_POPOVERS)) return;
  const t = document.getElementById("toast-error");
  if (t && !t.hidden) dismissToast();
}

// Popovers that live outside #modal and consume Escape themselves.
const ESCAPE_OWNING_POPOVERS = ".dt-pop";

function renderError(container, err) {
  if (!container) return;
  clear(container);
  const msg = String((err && err.message) || err);
  // A server whose index database doesn't exist yet (MySQL 1049) is the normal
  // pre-monitoring state, not a fault — and the #1 source of confusion: the
  // index DB lives on the INDEX server and is created when monitoring starts;
  // it is NEVER expected on the source. Render an actionable empty state, not
  // a raw red error wall.
  const m = msg.match(/Unknown database '([^']+)'/);
  if (m) {
    const box = el("div", { class: "empty" });
    box.append(el("h3", { text: "This server isn't indexing yet" }));
    box.append(el("p", { text:
      "Its index database \"" + m[1] + "\" doesn't exist on the index server yet. " +
      "It's created automatically when monitoring starts for this source; it never lives on the source MySQL itself. " +
      "Start monitoring from Manage servers, or switch to a server that's already indexing." }));
    box.append(el("button", { class: "btn btn-sm", type: "button", text: "Manage servers",
      onclick: () => openServersModal() }));
    container.append(box);
    return;
  }
  container.append(el("div", { class: "error-box", text: msg }));
}

function renderWarnings(node, warnings) {
  if (!node) return;
  clear(node);
  (warnings || []).forEach((w) => node.append(
    el("div", { class: "warn-item" }, icon("warn"), el("span", { text: w }))
  ));
}

// renderNotes is renderWarnings' quiet sibling (#1365): the response `notes`
// list carries benign informational audit facts (the archive-elision record,
// #1353) whose job is auditability, not attention. Muted ink, no icon, no
// amber — rendering these through the alert component is exactly the bug
// #1365 fixed (a note saying "nothing is missing" read as an incident).
function renderNotes(node, notes) {
  if (!node) return;
  clear(node);
  (notes || []).forEach((n) => node.append(el("div", { class: "note-item", text: n })));
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

// isKnownRoute accepts the built-in ROUTES plus any live extension-view route
// ("ext-<id>" advertised in the current server's capabilities). An "ext-" route
// for a view the selected server does not expose is treated as unknown (the
// caller redirects to overview), the same gating Time-travel/Storage get.
function isKnownRoute(route) {
  if (ROUTES.includes(route)) return true;
  // "extset-<id>" is checked first: it does NOT start with "ext-" (the fourth
  // character is "s", not "-"), so the two families never claim each other's
  // routes, and a panel the session may not reach is unknown here exactly like
  // an unadvertised view.
  if (route.startsWith("extset-")) return extSettings.some((p) => "extset-" + p.id === route);
  return route.startsWith("ext-") && extViews.some((v) => "ext-" + v.id === route);
}

function routeFromLocation() {
  const path = location.pathname.replace(/^\//, "").split("/")[0] || "overview";
  return isKnownRoute(path) ? path : "overview";
}

function navigate(route, params, push = true) {
  if (!isKnownRoute(route)) route = "overview";
  // Time-travel merged into Restore (#1298). The route stays known so old
  // bookmarks and Back entries land somewhere useful instead of on Overview.
  if (route === "timetravel") route = "recover";
  // Storage is a watch-daemon surface (rotation + archiving live there).
  if (route === "storage" && !capsCache.monitor) route = "overview";
  // Protect shares that gate: both routes read watch-daemon state (the
  // snapshot listing and the verification runner). Same treatment, so a
  // bookmark to either lands on Overview rather than an empty page.
  if ((route === "baselines" || route === "verification") && !capsCache.monitor) route = "overview";
  // The SQL panel is opt-in (BINTRAIL_CONSOLE_SQL_PANEL) and per-server gated.
  if (route === "sql" && !capsCache.sql) route = "overview";
  const qs = params && Object.keys(params).length
    ? "?" + new URLSearchParams(params).toString() : "";
  if (push) history.pushState({ route }, "", "/" + route + qs);
  renderRoute();
}

function renderRoute() {
  // The date-picker popover lives in document.body (position:fixed), outside
  // the #view subtree route changes normally clear — there's no per-view
  // teardown hook in this codebase to hang that cleanup on otherwise. An
  // in-flight SQL panel query is abandoned here for the same reason: leaving
  // the page must release the daemon's single-query latch.
  closeDatePicker();
  abortSQLRun();
  // Route-level staleness: the full-view async renderers (overview / status /
  // storage) fetch before painting, so navigating away while their fetches are
  // in flight would let the OLD view's completion clear and repaint over the
  // new one (nav highlighting one route, content showing another). serverGen
  // only covers server switches; this covers same-server navigation.
  viewGen++;
  const route = routeFromLocation();
  setActiveNav(route);
  cursorIdx = -1;
  const params = Object.fromEntries(new URLSearchParams(location.search));
  // Extension views (embedding builds): "ext-<id>" dispatches to the provider's
  // frontend module. routeFromLocation already redirected an unknown/ungated
  // ext route to overview, so a match here is always a live view.
  if (route.startsWith("extset-")) {
    const panel = extSettings.find((p) => "extset-" + p.id === route);
    return panel ? renderExtensionSettings(panel) : renderOverview();
  }
  if (route.startsWith("ext-")) {
    const view = extViews.find((v) => "ext-" + v.id === route);
    return view ? renderExtensionView(view) : renderOverview();
  }
  switch (route) {
    case "overview": return renderOverview();
    case "events": return renderEvents(params);
    case "timetravel": return renderTimetravel(params);
    case "recover": return renderRecover(params);
    case "sql": return renderSQL();
    case "status": return renderStatus();
    case "storage": return renderStorage();
    case "baselines": return renderBaselines();
    case "verification": return renderVerification();
    case "connect": return renderConnect();
    default: return renderOverview();
  }
}

function setActiveNav(route) {
  $all(".nav-item").forEach((a) => a.classList.toggle("active", a.dataset.route === route));
}

// ── Timezone discipline (#1354) ──────────────────────────────────────────────
// Every time this console renders is UTC — the zone the wire speaks
// (consoleTSFormat / status.TSFmt) and the zone the Since/Until/At filters and
// `--since`/`--until`/`AS OF` parse. So the DISPLAYED text of a data timestamp
// is the exact wire string (copy-pasteable into those inputs unchanged), and
// the zone is DECLARED next to it instead of suffixed onto every row: a
// section-level chip or "(UTC)" label, plus a hover tooltip carrying the
// viewer's local equivalent. Freshness/metadata stamps ("as of …", "created …")
// are not paste targets, so those carry an inline " UTC" label directly.
// Never render an unlabeled browser-local time.

// utcLocalTitle builds the hover tooltip for a UTC stamp: it names the zone of
// the displayed value and gives the viewer's local equivalent. "" for
// non-stamps ("—", empty), so callers can set title unconditionally.
function utcLocalTitle(stamp) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(stamp || ""));
  if (!m) return "";
  const t = new Date(m[1] + "T" + m[2] + "Z");
  if (isNaN(t)) return "";
  return "UTC; in your local time: " + t.toLocaleString();
}

// tsSpan renders one data timestamp: exact wire text, local-time tooltip.
function tsSpan(cls, stamp) {
  return el("span", { class: cls, text: stamp, title: utcLocalTitle(stamp) || null });
}

// utcLabel normalizes a wire timestamp (RFC3339 "…T…Z" or the bare
// "YYYY-MM-DD HH:MM:SS" UTC form) into the labeled display shape
// "YYYY-MM-DD HH:MM:SS UTC", for prose and freshness lines that are read, not
// copy-pasted. Non-stamps pass through unlabeled — never label a value as UTC
// unless it parses as one.
function utcLabel(stamp) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})(?:\.\d+)?(?:Z)?$/.exec(String(stamp || ""));
  return m ? m[1] + " " + m[2] + " UTC" : String(stamp || "");
}

// tzChip is the section-level zone declaration: a small "UTC" chip for the
// head of a card/panel whose body renders bare timestamps.
function tzChip() {
  return el("span", { class: "tz-chip", text: "UTC",
    title: "All times in this section are UTC, ready to paste into the Since/Until/At filters. Hover a timestamp for your local time." });
}

// nowClock is the freshness clock ("as of …"). UTC and labeled, NOT
// toLocaleTimeString: it sits beside data timestamps that are all UTC, and an
// unlabeled browser-local clock made the same instant appear hours apart on
// one page (#1354).
function nowClock() { return new Date().toISOString().slice(11, 19) + " UTC"; }

// ── Overview ─────────────────────────────────────────────────────────────────

// covLast is the payload the visible coverage card was built from, so a FAILED
// refresh can re-render the same numbers instead of blanking them — and label
// them stale, which is the part that keeps that honest. One Overview is on
// screen at a time, so one slot is enough.
let covLast = null; // { data, at } — `at` is the UTC clock time of the fetch

// covRefresh builds the card's refresh control: a fetch-time stamp plus the
// button. `stamp` is {at, error} — a refresh that failed keeps the previous
// numbers on screen and says when they are from, rather than replacing real
// values with an "unavailable" card or, worse, leaving them looking current.
function covRefresh(stamp) {
  const wrap = el("div", { class: "cov-refresh" });
  if (stamp && stamp.error) {
    wrap.append(el("span", { class: "cov-asof bad", text: "refresh failed · showing " + stamp.at }));
  } else if (stamp && stamp.at) {
    wrap.append(el("span", { class: "cov-asof", text: "as of " + stamp.at }));
  }
  const btn = el("button", {
    class: "cov-refresh-btn", type: "button",
    title: "Re-read capture lag and continuity",
    "aria-label": "Refresh restore coverage",
    onclick: (e) => refreshCovCard(e.currentTarget),
  });
  btn.append(icon("refresh", "cov-refresh-ico"));
  wrap.append(btn);
  return wrap;
}

// refreshCovCard re-reads /api/coverage and rebuilds ONLY this card. The values
// on screen stay put until the response lands, so there is no flash of empty
// chips, and the rebuild goes back through covCard so the freshness-driven
// tones (#1227) and the never-green rules apply to refreshed values exactly as
// they do on first paint.
async function refreshCovCard(btn) {
  const card = btn.closest(".cov-card");
  if (!card || btn.disabled) return;
  const gen = serverGen, vgen = viewGen;
  btn.disabled = true;
  btn.classList.add("spin");
  let next = null, failed = false;
  try {
    next = await api("/api/coverage");
  } catch (err) {
    console.error("coverage refresh failed", err);
    failed = true;
  }
  // Switched server or route mid-flight: whatever came back describes a view
  // the operator is no longer looking at.
  if (gen !== serverGen || vgen !== viewGen || !card.isConnected) return;
  if (failed) {
    const prev = covLast || { data: { continuity: "unavailable" }, at: "" };
    card.replaceWith(covCard(prev.data, { at: prev.at, error: true }));
    return;
  }
  covLast = { data: next, at: nowClock() };
  card.replaceWith(covCard(next, { at: covLast.at }));
}

// covCard renders the live RPO statement (#1194). The window's upper edge is
// the last INDEXED event on purpose — "restorable up to now" with a dead
// stream would be false assurance; the lag chip is what says "now". Degrades
// loudly: gap_lost/unavailable red, unknown amber, empty index explicit.
function covCard(c, stamp) {
  const card = el("section", { class: "ov-panel cov-card" });
  card.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Restore coverage" }),
    tzChip(),
    covRefresh(stamp)));
  const cont = c.continuity || "unknown";
  const bad = cont === "gap_lost" || cont === "unavailable";
  const warn = cont === "unknown";
  if (bad && !c.delta_to) {
    // Unreachable/broken backend: say nothing about the window — "no events
    // yet" would be a positive factual claim about an index we couldn't read.
  } else if (!c.delta_to) {
    card.append(el("p", { class: "cov-line warn", text: "No indexed events yet, so there is nothing to restore from." }));
  } else if (!c.delta_from) {
    // Unknown floor: don't assert a bounded window whose start we don't know.
    card.append(el("p", { class: "cov-line" },
      "Restorable up to ", el("b", { text: c.delta_to, title: utcLocalTitle(c.delta_to) || null }), "; the window start could not be determined."));
  } else {
    card.append(el("p", { class: "cov-line" },
      "Any point between ", el("b", { text: c.delta_from, title: utcLocalTitle(c.delta_from) || null }),
      " and ", el("b", { text: c.delta_to, title: utcLocalTitle(c.delta_to) || null }), " is restorable."));
  }
  const chips = el("div", { class: "cov-chips" });
  // Freshness (#1227) is what makes the lag number readable, so it decides the
  // lag chip's colour instead of a bare threshold. The same "3600s" means a
  // DEAD DAEMON under "stalled" and a source nobody wrote to under "idle", and
  // those need opposite responses — the old unconditional amber said neither.
  const fresh = c.freshness || "unknown";
  if (typeof c.lag_seconds === "number") {
    const lagTone = fresh === "stalled" ? " bad" : fresh === "current" ? " ok" : " warn";
    chips.append(el("span", { class: "cov-chip" + lagTone, text: "capture lag " + c.lag_seconds + "s" }));
  }
  // "none" (file-mode: no capture ran) and "unknown"/"unavailable" stay
  // NEUTRAL/amber — never green, which would paint a non-claim as assurance.
  const freshTone = fresh === "stalled" || fresh === "unavailable" ? " bad"
    : fresh === "current" ? " ok"
    : fresh === "none" ? "" : " warn";
  chips.append(el("span", { class: "cov-chip" + freshTone, text: "capture " + fresh }));
  // "none" (file-mode: no capture ran) stays NEUTRAL — green would paint a
  // non-claim as assurance.
  chips.append(el("span", { class: "cov-chip" + (bad ? " bad" : warn ? " warn" : cont === "ok" ? " ok" : ""), text: "continuity " + cont }));
  card.append(chips);
  if (cont === "gap_lost") {
    card.append(el("p", { class: "cov-line bad", text: "Events were lost for good: the window has a hole, and points past it need a fresh baseline." }));
  } else if (cont === "unavailable") {
    card.append(el("p", { class: "cov-line bad", text: "Continuity could not be read. Treat the window as unverified." }));
  } else if (warn) {
    card.append(el("p", { class: "cov-line warn", text: "This index cannot report continuity, so the window may have undetected holes." }));
  }
  // The freshness explanation lines. "stalled" is the only one that is an
  // error state: the checkpoint ticker runs even with no traffic, so a stale
  // checkpoint is the daemon, never the workload.
  if (fresh === "stalled") {
    const age = typeof c.checkpoint_age_seconds === "number" ? " for " + Math.round(c.checkpoint_age_seconds / 60) + "m" : "";
    card.append(el("p", { class: "cov-line bad", text:
      "Capture is STALLED: the daemon has not checkpointed" + age + ". The window's upper edge is frozen: changes since then are NOT recoverable. Check that the stream is running." }));
  } else if (fresh === "idle") {
    card.append(el("p", { class: "cov-line warn", text:
      "Capture is checkpointing but has indexed nothing recent. From the index alone a quiet source and a capture falling behind look identical; the daemon's bintrail_stream_index_commit_latency_seconds metric tells them apart." }));
  } else if (fresh === "unavailable") {
    card.append(el("p", { class: "cov-line bad", text: "Capture liveness could not be read. Treat the window's upper edge as unverified." }));
  }
  if (c.baseline_configured) {
    if (c.full_table_status === "unknown") {
      // An error must never render like "nothing broken" — the broken-table
      // warning would silently vanish behind a failed listing.
      card.append(el("p", { class: "cov-line warn", text: "Full-table coverage could not be checked. See the daemon log." }));
    }
    if (c.full_table_from) {
      card.append(el("p", { class: "cov-line" },
        "Full-table restore for tables with a baseline: any point from ",
        el("b", { text: c.full_table_from, title: utcLocalTitle(c.full_table_from) || null }), " onwards."));
    }
    if (c.broken_tables && c.broken_tables.length) {
      card.append(el("p", { class: "cov-line bad", text: "Not fully restorable (newest baseline predates coverage): " + c.broken_tables.join(", ") + ". Take a fresh baseline." }));
    }
  }
  return card;
}

// ── Overview: progressive render (#1352) ─────────────────────────────────────
// The page frame and per-card skeletons paint SYNCHRONOUSLY; each card fills as
// ITS fetch lands. The pre-#1352 Promise.all gated first paint on the slowest
// of four endpoints, so an archive-heavy source sat on a bare "Loading…" for
// tens of seconds while /api/events had answered in under one. buildOverview()
// composes the same per-card fills from already-fetched payloads — the seam the
// e2e fixture drives — so the progressive path and the fixture path render
// through identical code.

// ovSkelLines: shimmer placeholder block for a still-loading card body.
function ovSkelLines(n) {
  const box = el("div", { class: "skel-box" });
  for (let i = 0; i < n; i++) box.append(el("div", { class: "skel-line" }));
  return box;
}

// ovPendingCard: a card's loading state — its REAL title plus shimmer lines and
// a label naming what is still being computed, so a slow aggregate reads as
// work in progress instead of a hang (and the page layout shows immediately).
function ovPendingCard(title, waiting, cls) {
  const card = el("section", { class: "ov-panel" + (cls ? " " + cls : "") });
  card.append(el("div", { class: "ov-panel-head" }, el("h2", { class: "ov-panel-title", text: title })));
  card.append(ovSkelLines(3));
  card.append(el("div", { class: "skel-note", text: waiting }));
  return card;
}

// ovStatPending: one tile in its loading state — key and scope already legible,
// value shimmering.
function ovStatPending(key, scope) {
  return el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v" }, el("div", { class: "skel-line skel-stat" })),
    el("div", { class: "ov-stat-k", text: key }),
    el("div", { class: "ov-stat-scope", text: scope || "" }));
}

// ovFrame builds the whole page skeleton synchronously and returns handles to
// every slot a fetch will fill. Nothing here waits on the network.
function ovFrame() {
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "What changed recently, and where: your starting point. Each figure below states the window it covers.");
  v.append(pageHead("Overview", sub));

  const f = {};
  f.covSlot = el("div");
  f.covSlot.append(ovPendingCard("Restore coverage", "computing restore coverage…", "cov-card"));
  v.append(f.covSlot);

  const stats = el("div", { class: "ov-stats" });
  f.statTotal = ovStatPending("changes indexed", "all time · estimate");
  f.statDeletes = ovStatPending("deletes", "");
  f.statTables = ovStatPending("tables touched", "");
  f.statLatest = ovStatPending("most recent change", "point in time (UTC)");
  stats.append(f.statTotal, f.statDeletes, f.statTables, f.statLatest);
  v.append(stats);

  // Whatever the aggregate could not account for lands here, at the point of
  // use — between the tiles and the panels, where the old layout put it.
  f.warnSlot = el("div");
  v.append(f.warnSlot);

  const grid = el("div", { class: "ov-grid" });

  // The two panels carry the home's tint layer (#1421): violet and sun, the
  // structure tints. The pill is the eyebrow — the title stays an h2 for the
  // document outline; the pill is presentation, not the heading.
  f.recentPanel = el("section", { class: "ov-panel tcard-violet" });
  f.recentPanel.append(el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Recent changes" })),
    tzChip(),
    el("a", { class: "btn btn-sm btn-ghost", href: "/events",
      onclick: (e) => { e.preventDefault(); navigate("events"); }, text: "Browse all events ›" })));
  f.recentBody = el("div", { class: "ov-evlist" });
  f.recentBody.append(ovSkelLines(4), el("div", { class: "skel-note", text: "loading recent changes…" }));
  f.recentPanel.append(f.recentBody);
  grid.append(f.recentPanel);

  f.tablesPanel = el("section", { class: "ov-panel tcard-sun" });
  f.tablesHead = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Activity by table" })));
  f.tablesPanel.append(f.tablesHead);
  f.tablesBody = el("div", { class: "ov-tables" });
  f.tablesBody.append(ovSkelLines(4), el("div", { class: "skel-note", text: "computing window activity…" }));
  f.tablesPanel.append(f.tablesBody);
  f.tablesFoot = el("div", { class: "ov-coverage" });
  f.tablesPanel.append(f.tablesFoot);
  grid.append(f.tablesPanel);

  v.append(grid);
  viewEnter();
  return f;
}

// fillOvCoverage — the live RPO statement (#1194). Best-effort: a null payload
// removes the pending card and renders nothing, never a fabricated window (the
// fetch path substitutes {continuity:"unavailable"} on failure, which renders
// the red card).
function fillOvCoverage(f, coverage) {
  clear(f.covSlot);
  if (!coverage) return;
  covLast = { data: coverage, at: nowClock() };
  f.covSlot.append(covCard(coverage, { at: covLast.at }));
}

// fillOvStatus fills the all-time tile. "changes indexed" is
// status.total_events_estimate — information_schema TABLE_ROWS, an InnoDB
// ESTIMATE. Say so on the tile: presenting a sampled number in the same type
// as three exact ones is its own quiet lie.
function fillOvStatus(f, status) {
  if (status) updateSideMeta(status);
  const cov = (status && status.coverage) || {};
  const total = status ? (status.total_events_estimate || cov.total_events || "—") : "—";
  f.statTotal.replaceWith(f.statTotal = ovStat(String(total), "changes indexed", "", "all time · estimate"));
}

// fillOvEvents fills the Recent-changes panel and the most-recent-change tile.
// A failed fetch renders a red error box in the panel — never a swallowed
// blank list.
function fillOvEvents(f, eventsData, err) {
  const events = (eventsData && eventsData.events) || [];
  const latest = (events[0] && events[0].event_timestamp) || "—";
  const wide = el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v small", text: latest, title: utcLocalTitle(latest) || null }),
    el("div", { class: "ov-stat-k", text: "most recent change" }),
    el("div", { class: "ov-stat-scope", text: "point in time (UTC)" }));
  f.statLatest.replaceWith(f.statLatest = wide);
  clear(f.recentBody);
  if (err) {
    f.recentBody.append(el("div", { class: "error-box", text: "Recent changes unavailable: " + (err.message || err) }));
    return;
  }
  if (!events.length) {
    f.recentBody.append(el("div", { class: "ev-empty", text: "No changes indexed yet." }));
  } else {
    events.slice(0, 8).forEach((e) => f.recentBody.append(ovEventRow(e)));
  }
}

// fillOvActivity fills every window-scoped surface from the /api/activity
// materialization (#1352): the deletes/tables tiles, the warning items, the
// Activity-by-table panel, and the window footer. The aggregate is precomputed
// server-side, so its refreshed_at is rendered wherever its numbers appear
// ("as of …") — a stale count must be visibly stale, never silently so. The
// window is the LIVE RETENTION, derived server-side from the oldest live
// partition; the label travels in the payload so the tile and the measurement
// can never disagree.
function fillOvActivity(f, activity) {
  const deletes = activity ? activity.deletes : null;
  const tableCount = activity ? activity.tables : null;
  const refreshed = (activity && activity.refreshed_at) || "";
  // Tiles carry the compact time-of-day stamp; the footer carries the full
  // one. Both are labeled: the freshness clock beside them (nowClock) says
  // "UTC", and an unlabeled sibling would read as a different zone (#1354).
  const asofShort = refreshed ? " · as of " + (refreshed.length > 11 ? refreshed.slice(11) : refreshed) + " UTC" : "";
  // The window scope printed on every window-scoped tile. "partial" is not
  // decoration: the server sets complete=false when the counts are knowably a
  // floor, and a narrower number under a wider label is the bug this page is
  // fixing.
  const winScope = activity ? (activity.label + (activity.complete ? "" : " · partial") + asofShort) : "unavailable";
  f.statDeletes.replaceWith(f.statDeletes =
    ovStat(deletes === null ? "—" : String(deletes), "deletes", deletes ? "danger" : "", winScope));
  f.statTables.replaceWith(f.statTables =
    ovStat(tableCount === null ? "—" : String(tableCount), "tables touched", "", winScope));

  clear(f.warnSlot);
  if (!activity) {
    f.warnSlot.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "The window counts could not be loaded, so the deletes and tables tiles show no number rather than a zero." })));
  }
  (activity && activity.notes || []).forEach((n) => {
    f.warnSlot.append(el("div", { class: "warn-item" }, icon("warn"), el("span", { text: n })));
  });

  if (refreshed) {
    f.tablesHead.append(el("span", { class: "cov-asof", text: "as of " + utcLabel(refreshed) }));
  }
  clear(f.tablesBody);
  const tables = (activity && activity.top_tables || []).map((t) => ({
    key: t.schema + "." + t.table, insert: t.insert, update: t.update, delete: t.delete, total: t.total,
  }));
  if (!tables.length) {
    f.tablesBody.append(el("div", { class: "ev-empty", text: activity ? "No changes in this window." : "Window activity unavailable." }));
  }
  tables.forEach((s) => f.tablesBody.append(ovTableRow(s, activity && activity.since)));
  // The footer states the AGGREGATE's own bounds. It must never fall back to
  // status.coverage.oldest (#679/#684/#686): that is the index's whole history,
  // and printing it under "window" would attribute counts from one span to a
  // far wider one — the same class of mismatch as the tiles' (#1300).
  clear(f.tablesFoot);
  f.tablesFoot.append(
    el("span", { text: "window (UTC)" }), " ",
    el("b", { text: activity ? activity.since : "—", title: activity ? utcLocalTitle(activity.since) || null : null }),
    " → ",
    el("b", { text: activity ? activity.until : "—", title: activity ? utcLocalTitle(activity.until) || null : null }),
    el("span", { text: activity ? " · " + winScope : "" }));
}

function renderOverview() {
  const gen = serverGen, vgen = viewGen;
  const f = ovFrame();
  const live = () => gen === serverGen && vgen === viewGen;
  // Four independent fetches; each fills its card as it lands. No Promise.all:
  // the slowest aggregate must not hold the frame or its siblings hostage —
  // the #1352 target (p95 < 5 s to a useful first paint) is about the PAINT,
  // not the backend. Every fill re-checks the generation guards so a server
  // switch or navigation mid-flight drops the late payload instead of
  // painting over the new view.
  api("/api/status").catch(() => null)
    .then((status) => { if (live()) fillOvStatus(f, status); });
  // Only the Recent-changes list needs event ROWS, and it renders 8 of them.
  // The tiles' counts come from /api/activity (#1300), so this page never
  // pulls row images to derive four integers.
  api("/api/events?limit=8&order=DESC").then(
    (d) => { if (live()) fillOvEvents(f, d, null); },
    (err) => { console.error("events fetch failed", err); if (live()) fillOvEvents(f, null, err); });
  // A failed fetch must render the same red "unavailable" card the nil-db
  // path gets — a swallowed null would make a broken endpoint
  // indistinguishable from a console without the feature.
  api("/api/coverage").catch((err) => { console.error("coverage fetch failed", err); return { continuity: "unavailable" }; })
    .then((coverage) => { if (live()) fillOvCoverage(f, coverage); });
  // null on failure, never {} — the fill renders "—" for a missing aggregate.
  // A zero-filled fallback would print "0 deletes", an assurance nobody
  // measured. No period parameter: the server derives the window from the
  // live retention (#1352) and names it in the payload's label.
  api("/api/activity").catch((err) => { console.error("activity fetch failed", err); return null; })
    .then((activity) => { if (live()) fillOvActivity(f, activity); });
}

// buildOverview renders the dashboard from already-fetched payloads — the
// composition seam the e2e fixture drives directly, sharing every fill with
// the progressive path above. status and activity may each be null (their
// fetches are best-effort); when one is, the tiles it feeds read "—". Every
// tile carries its OWN scope line, because these numbers get screenshotted
// into incident channels without the page around them (#1300): "N deletes"
// beside "N changes indexed" invites reading the first as a share of the
// second, and before this they were different denominators.
function buildOverview(status, eventsData, coverage, activity) {
  const f = ovFrame();
  fillOvStatus(f, status);
  fillOvEvents(f, eventsData, null);
  fillOvCoverage(f, coverage);
  fillOvActivity(f, activity);
}

// ovStat renders one tile. scope is REQUIRED for any tile carrying a number:
// the tiles are visually identical, so without it a reader cannot tell an
// all-time total from a window count — which is precisely how "53 deletes" got
// read as a share of "3121 changes indexed" (#1300).
function ovStat(value, key, mod, scope) {
  // Tiles animate their ARRIVAL (a rise, in style.css), never their VALUE. A
  // count-up was tried and removed: it writes intermediate numbers into the
  // DOM, so for the length of the animation the tile states something untrue —
  // the console-e2e read "0" from the deletes tile mid-flight. A forensics
  // surface that briefly reports zero deletions is a small lie, and the
  // entrance animation already supplies the sense of arrival without one.
  // The brand gradient (#1385) is opt-in per tile, and this is the only place
  // that grants it. `mod` already names the exact set that must not have it,
  // so the gate needs no list of its own: "danger" paints the deletes count in
  // the semantic --delete, and the `small` variant is 19px at weight 500 —
  // below WCAG's large-text bar, which is the only bar the gradient's stops
  // clear. (`small` is built inline elsewhere and has never reached ovStat;
  // gating on `mod` rather than on the name covers it if it ever does.)
  const brand = mod ? " " + mod : " ov-stat-num";
  return el("div", { class: "ov-stat" },
    el("div", { class: "ov-stat-v" + brand, text: value }),
    el("div", { class: "ov-stat-k", text: key }),
    el("div", { class: "ov-stat-scope", text: scope || "" }));
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
  row.append(tsSpan("ov-ev-time", e.event_timestamp));
  row.append(badge(e.event_type));
  const tbl = el("span", { class: "ov-ev-tbl", text: e.schema_name + "." + e.table_name + " " });
  tbl.append(el("span", { class: "ov-ev-pk", text: "#" + e.pk_values }));
  row.append(tbl);
  row.append(el("span", { class: "ov-ev-cols" }, ...colsSummary(e.changed_columns, false)));
  const undo = el("button", { class: "btn btn-sm ov-ev-undo", type: "button", text: "Undo",
    onclick: (ev) => { ev.stopPropagation(); undoEvent(e); } });
  row.append(undo);
  return row;
}

function ovTableRow(s, winSince) {
  // The click carries the widget's OWN window (#1414): the count was computed
  // over the live retention, so the search it opens states that bound — as a
  // visible since: token, not a hidden field — and the server's window proof
  // (windowSatisfiedLive) can then skip the archive scan that structurally
  // cannot contribute. Spelled RFC3339 (T...Z) because the smart-search
  // tokenizer splits on spaces.
  const q = s.key + (winSince ? " since:" + winSince.replace(" ", "T") + "Z" : "");
  const row = el("a", { class: "ov-tablerow",
    onclick: () => navigate("events", { q }) });
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

  // Connection-id availability note (#595): PostgreSQL logical replication
  // (pgoutput) carries no backend connection id, so the connection_id column
  // stays empty for PG sources — unlike MySQL, it cannot be recovered upstream
  // at all, so say so here on the Events page rather than leave the empty
  // column an unexplained gap. capsCache.source is resolved before this paints.
  if (capsCache.source === "postgresql") {
    v.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "Postgres's replication stream does not say who made each change, so that column stays empty for PostgreSQL sources." })));
  }

  const form = el("form", { id: "ev-form" });
  // search bar
  const searchwrap = el("div", { class: "ev-searchwrap" });
  searchwrap.append(icon("search", "ev-search-ic"));
  const search = el("input", { class: "ev-search", id: "ev-search", name: "q",
    autocomplete: "off", spellcheck: "false",
    placeholder: 'Search changes. Try "orders", "type:delete", "pk:1006", "col:email"' });
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
  adv.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  adv.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
  adv.append(fieldInput("Limit", "limit", "sm", "100"));
  form.append(adv);
  v.append(form);

  // result bar
  const bar = el("div", { class: "result-bar" });
  bar.append(el("span", { class: "result-count" }, el("b", { id: "ev-count", text: "…" }), el("span", { id: "ev-count-note", text: " event(s)" })));
  bar.append(el("span", { class: "spacer" }));
  bar.append(el("span", { class: "kbd-hint" },
    el("b", { text: "j" }), "/", el("b", { text: "k" }), " move · ",
    el("b", { text: "↵" }), " expand · ", el("b", { text: "u" }), " undo"));
  // Paging controls (#1297). Disabled rather than hidden so the affordance is
  // discoverable on page 1 — the bug this fixes was an operator having no way
  // to know event 101 was reachable at all.
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", id: "ev-prev", text: "‹ Newer",
    disabled: "", onclick: () => eventsGoPage(-1) }));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", id: "ev-next", text: "Older ›",
    disabled: "", onclick: () => eventsGoPage(1) }));
  // Labels name their SCOPE: these export every match of the current search
  // (up to the server's cap), not the page on screen. See exportEvents.
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Export JSON",
    title: "Export all matches of this search, not just this page (max 1000 events)",
    onclick: (e) => exportEvents("json", e.target) }));
  bar.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Export CSV",
    title: "Export all matches of this search, not just this page (max 1000 events)",
    onclick: (e) => exportEvents("csv", e.target) }));
  v.append(bar);

  // Scope/coverage notices for this result set (#1311). The response has
  // carried a `warnings` array all along and this view dropped it, which meant
  // the default browse -- the exact case a profiled session reads live-index
  // only with no time filter -- computed the right sentence server-side and
  // threw it away at the browser. Above the list on purpose: a caveat about
  // what a result does NOT include is worthless below the result.
  v.append(el("div", { id: "ev-warnings", class: "warnings" }));
  // Info notes (#1365) under the result-count line, BELOW the warnings:
  // alerts render first in both views (Recover has the same order). The muted
  // register for the response `notes` list (benign audit facts like the
  // archive-elision record) — never the alert component.
  v.append(el("div", { id: "ev-notes", class: "notes" }));

  // events list
  const list = el("div", { class: "events", id: "events-list" });
  const head = el("div", { class: "ev-head" });
  ["time (UTC)", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
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

function fieldLabel(label, required) {
  const lbl = el("label", { class: "field-label" }, label);
  if (required) lbl.append(el("span", { class: "field-required", title: "Required", text: " *" }));
  return lbl;
}
function fieldInput(label, name, size, placeholder, required) {
  return el("div", { class: "field field--" + size },
    fieldLabel(label, required),
    el("input", { class: "input", name, placeholder: placeholder || "" }));
}
function fieldSelect(label, name, size, isSchema, isTable, options, anyLabel, required) {
  const sel = el("select", { class: "select" + (isSchema ? " schema-select" : "") + (isTable ? " table-select" : ""), name });
  if (options) options.forEach((o) => sel.append(opt(o, o === "" ? (anyLabel || "any") : o)));
  else sel.append(opt("", "any"));
  return el("div", { class: "field field--" + size },
    fieldLabel(label, required), sel);
}

// fieldTableCombo is an input + datalist combobox for a table name (#1364):
// suggestions come from the selected schema's listing (loadTables fills the
// datalist on schema change), but a hand-typed name still submits — recover
// can legitimately target a dropped table whose events are indexed, and a
// closed <select> would make that recovery impossible from the UI. The
// combo-hint span is the brief busy note while a listing loads; field--combo
// anchors it OUT of flow (#1369) so the empty hint reserves no height — under
// .filters' align-items:flex-end it pushed the label+input above the row.
let tableComboSeq = 0;
function fieldTableCombo(label, name, size, placeholder) {
  const listId = "table-combo-list-" + (++tableComboSeq);
  return el("div", { class: "field field--combo field--" + size },
    fieldLabel(label),
    el("input", { class: "input table-combo", name, placeholder: placeholder || "",
      list: listId, autocomplete: "off", spellcheck: "false" }),
    el("datalist", { id: listId }),
    el("span", { class: "combo-hint", "aria-live": "polite" }));
}

// ── date/time picker (calendar + clock) for Since/Until/At filter fields ────
// event_timestamp round-trips through consoleTSFormat under the UTC location
// config.Connect forces on every index-DB connection (internal/config), so
// "today"/"now" here mean UTC today/now — using local Date getters would
// silently offset every filter the picker writes relative to the UTC values
// the events list shows.
const DT_DOW = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"];
const DT_MON = ["January", "February", "March", "April", "May", "June", "July",
  "August", "September", "October", "November", "December"];

function pad2(n) { return String(n).padStart(2, "0"); }
function fmtDT(y, mo, d, h, mi) { return `${y}-${pad2(mo + 1)}-${pad2(d)} ${pad2(h)}:${pad2(mi)}:00`; }
function daysInMonth(y, mo) { return new Date(Date.UTC(y, mo + 1, 0)).getUTCDate(); }

// Seeds the picker from whatever's already typed. Matches the shape of the
// formats cliutil.ParseTime accepts (MySQL datetime, RFC3339, date-only) —
// not full semantic equivalence: a non-UTC RFC3339 offset is ignored, and
// out-of-range components are rejected here even though a trailing offset
// would shift them back in range on the Go side. Anything that doesn't match
// or fails range validation (or an empty field) leaves the typed text alone
// and just opens on today's UTC date — Date.UTC() silently rolls over
// invalid values instead of erroring, so an unchecked "2026-02-30" would
// otherwise render a header/grid for a different month than the day count
// implies.
function parseDTValue(s) {
  const m = /^(\d{4})-(\d{2})-(\d{2})(?:[ T](\d{2}):(\d{2}))?/.exec((s || "").trim());
  if (!m) return null;
  const y = +m[1], mo = +m[2] - 1, d = +m[3];
  const h = m[4] ? +m[4] : 0, mi = m[5] ? +m[5] : 0;
  if (mo < 0 || mo > 11 || h > 23 || mi > 59) return null;
  if (d < 1 || d > daysInMonth(y, mo)) return null;
  return { y, mo, d, h, mi };
}

let openDT = null; // { pop, trigger, cleanup() } — one date picker open at a time

function closeDatePicker() {
  if (!openDT) return;
  openDT.cleanup();
  openDT.pop.remove();
  openDT = null;
}

// Setting .value programmatically doesn't fire native input/change events.
// The Events view's debounced auto-search relies on them (form listeners at
// runEventsQuery's call site); Recover/Time-travel are submit-only
// and ignore them, so the dispatch is a harmless no-op there — kept for
// consistency rather than because every caller needs it.
function applyDTValue(input, y, mo, d, h, mi) {
  input.value = fmtDT(y, mo, d, h, mi);
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function clearDTValue(input) {
  input.value = "";
  input.dispatchEvent(new Event("input", { bubbles: true }));
  input.dispatchEvent(new Event("change", { bubbles: true }));
}

function toggleDatePicker(input, trigger) {
  if (openDT && openDT.trigger === trigger) { closeDatePicker(); return; }
  closeDatePicker();

  const now = new Date();
  const seed = parseDTValue(input.value) || {
    y: now.getUTCFullYear(), mo: now.getUTCMonth(), d: now.getUTCDate(), h: 0, mi: 0,
  };
  const state = { view: { y: seed.y, mo: seed.mo }, sel: { y: seed.y, mo: seed.mo, d: seed.d }, h: seed.h, mi: seed.mi };

  // Rendered into document.body, not in-flow — see the .dt-pop rule in
  // style.css for why (an ancestor overflow:hidden would clip it otherwise).
  const pop = el("div", { class: "dt-pop" });
  document.body.append(pop);
  renderDTPop(pop, state, input);

  const rect = trigger.getBoundingClientRect();
  const pw = pop.getBoundingClientRect().width;
  let left = rect.left + window.scrollX;
  const maxLeft = window.scrollX + window.innerWidth - pw - 8;
  if (left > maxLeft) left = Math.max(window.scrollX + 8, maxLeft);
  pop.style.top = (rect.bottom + window.scrollY + 6) + "px";
  pop.style.left = left + "px";

  // Self-cleaning: if the view was rebuilt out from under us (route change
  // via renderRoute already calls closeDatePicker, but this is the backstop)
  // the listener removes itself instead of acting on stale state.
  const onDocClick = (e) => {
    if (!pop.isConnected) { document.removeEventListener("click", onDocClick, true); return; }
    if (pop.contains(e.target) || e.target === trigger || trigger.contains(e.target)) return;
    closeDatePicker();
  };
  const onKey = (e) => {
    if (!pop.isConnected) { document.removeEventListener("keydown", onKey, true); return; }
    if (e.key === "Escape") closeDatePicker();
  };
  const onDismiss = () => { if (pop.isConnected) closeDatePicker(); };
  document.addEventListener("click", onDocClick, true);
  document.addEventListener("keydown", onKey, true);
  window.addEventListener("scroll", onDismiss, true);
  window.addEventListener("resize", onDismiss);

  openDT = {
    pop, trigger,
    cleanup() {
      document.removeEventListener("click", onDocClick, true);
      document.removeEventListener("keydown", onKey, true);
      window.removeEventListener("scroll", onDismiss, true);
      window.removeEventListener("resize", onDismiss);
    },
  };
}

function renderDTPop(pop, state, input) {
  clear(pop);

  const head = el("div", { class: "dt-head" });
  head.append(
    el("button", { class: "dt-nav dt-nav-prev", type: "button", "aria-label": "Previous month", onclick: () => {
      state.view.mo--; if (state.view.mo < 0) { state.view.mo = 11; state.view.y--; }
      renderDTPop(pop, state, input);
    } }, icon("caret", "dt-nav-ic")),
    el("span", { class: "dt-month", text: `${DT_MON[state.view.mo]} ${state.view.y}` }),
    el("button", { class: "dt-nav", type: "button", "aria-label": "Next month", onclick: () => {
      state.view.mo++; if (state.view.mo > 11) { state.view.mo = 0; state.view.y++; }
      renderDTPop(pop, state, input);
    } }, icon("caret", "dt-nav-ic")));
  pop.append(head);

  const grid = el("div", { class: "dt-grid" });
  DT_DOW.forEach((d) => grid.append(el("span", { class: "dt-dow", text: d })));

  const firstDow = new Date(Date.UTC(state.view.y, state.view.mo, 1)).getUTCDay();
  const nDays = daysInMonth(state.view.y, state.view.mo);
  const prevY = state.view.mo === 0 ? state.view.y - 1 : state.view.y;
  const prevMo = state.view.mo === 0 ? 11 : state.view.mo - 1;
  const prevNDays = daysInMonth(prevY, prevMo);
  const nextY = state.view.mo === 11 ? state.view.y + 1 : state.view.y;
  const nextMo = state.view.mo === 11 ? 0 : state.view.mo + 1;
  const today = new Date();

  const dayCell = (y, mo, d, muted) => {
    const isToday = y === today.getUTCFullYear() && mo === today.getUTCMonth() && d === today.getUTCDate();
    const isSel = y === state.sel.y && mo === state.sel.mo && d === state.sel.d;
    return el("button", {
      class: "dt-day" + (muted ? " is-muted" : "") + (isToday ? " is-today" : "") + (isSel ? " is-sel" : ""),
      type: "button", text: String(d),
      onclick: () => { state.sel = { y, mo, d }; state.view = { y, mo }; renderDTPop(pop, state, input); },
    });
  };
  for (let i = firstDow - 1; i >= 0; i--) grid.append(dayCell(prevY, prevMo, prevNDays - i, true));
  for (let d = 1; d <= nDays; d++) grid.append(dayCell(state.view.y, state.view.mo, d, false));
  const trailing = (7 - ((firstDow + nDays) % 7)) % 7;
  for (let d = 1; d <= trailing; d++) grid.append(dayCell(nextY, nextMo, d, true));
  pop.append(grid);

  const timeRow = el("div", { class: "dt-time" });
  const hSel = el("select", { class: "select dt-time-select", "aria-label": "Hour" });
  for (let h = 0; h < 24; h++) hSel.append(opt(String(h), pad2(h)));
  hSel.value = String(state.h);
  hSel.addEventListener("change", () => { state.h = +hSel.value; });
  const miSel = el("select", { class: "select dt-time-select", "aria-label": "Minute" });
  for (let mi = 0; mi < 60; mi++) miSel.append(opt(String(mi), pad2(mi)));
  miSel.value = String(state.mi);
  miSel.addEventListener("change", () => { state.mi = +miSel.value; });
  timeRow.append(hSel, el("span", { class: "dt-time-sep", text: ":" }), miSel,
    el("span", { class: "dt-tz", text: "UTC" }));
  pop.append(timeRow);

  const foot = el("div", { class: "dt-foot" });
  foot.append(
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Now", onclick: () => {
      const n = new Date();
      applyDTValue(input, n.getUTCFullYear(), n.getUTCMonth(), n.getUTCDate(), n.getUTCHours(), n.getUTCMinutes());
      closeDatePicker();
    } }),
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Clear", onclick: () => {
      clearDTValue(input);
      closeDatePicker();
    } }),
    el("button", { class: "btn btn-sm btn-primary", type: "button", text: "Apply", onclick: () => {
      applyDTValue(input, state.sel.y, state.sel.mo, state.sel.d, state.h, state.mi);
      closeDatePicker();
    } }));
  pop.append(foot);
}

// fieldDateInput builds the same field chrome as fieldInput but wires a
// calendar+clock popover to a trailing button — manual typing still works,
// the picker is a progressive-enhancement affordance over the same input.
function fieldDateInput(label, name, size, placeholder, required) {
  const input = el("input", { class: "input dt-input", name, placeholder: placeholder || "" });
  const trigger = el("button", { class: "dt-trigger", type: "button", "aria-label": "Open calendar" },
    icon("calendar", "dt-trigger-ic"));
  trigger.addEventListener("click", (e) => { e.preventDefault(); toggleDatePicker(input, trigger); });
  const wrap = el("div", { class: "dt-wrap" }, input, trigger);
  return el("div", { class: "field field--" + size }, fieldLabel(label, required), wrap);
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

// keepPage is set ONLY by the Prev/Next buttons. Every other caller — the
// debounced `input` handler, `change`, `submit` — is a filter edit, and a
// filter edit must drop the cursor: a cursor from the previous search names a
// row that need not exist in the new one, so keeping it would serve a page
// from the middle of a search the operator never ran.
async function runEventsQuery(form, keepPage) {
  if (!keepPage) { evPages = [{ before: null, offset: 0 }]; evPageIdx = 0; }
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

  // Client-side refine terms are computed before the fetch so export can reuse
  // the exact same pair (server filters + refine) the page was built from.
  const refine = [];
  if (!hasScope && merged.pk) refine.push(merged.pk.toLowerCase());
  if (!hasScope && merged.changed_column) refine.push(merged.changed_column.toLowerCase());
  refine.push(...parsed.terms);
  evLastQuery = { apiParams, refine };

  const pageParams = Object.assign({ order: "DESC" }, apiParams);
  const before = evPages[evPageIdx] && evPages[evPageIdx].before;
  if (before) pageParams.before = before;

  // Loading state (#1353): skeleton rows from the moment the fetch starts — a
  // blank list must never be the "busy" rendering (it reads as "no events" or
  // "broken", on the page an operator opens mid-incident). Past ~2s the
  // skeleton names what is happening, so a slow archive read looks like work
  // instead of a hang. The skeleton lives inside #ev-rows, so the success path
  // (buildEventRows) and the error path (renderError) both sweep it away with
  // their own clear() — a failed fetch can never leave a stuck skeleton. The
  // isConnected guard keeps a superseded search's timer from touching the
  // newer render.
  const skel = renderEventsLoading(rowsEl);
  const slowT = setTimeout(() => { if (skel.isConnected) skel.classList.add("slow"); }, 2000);
  if (countEl) countEl.textContent = "…";

  // Progressive read (#1414): the first page of a browse fetches the LIVE
  // index only (scope=live) and paints immediately — rows already sitting in
  // binlog_events must not wait behind an S3 scan — then the full merged read
  // completes it in the background. Cursor pages keep the single full fetch:
  // a deep page is usually IN the archives, so there is no fast half to show
  // first.
  const progressive = !before;
  const myQuery = evLastQuery;

  // paint renders one response. It runs twice on a progressive read — the
  // live phase, then the merged phase — so everything it derives (cursors,
  // counts, advisory lists) is REPLACED by phase 2 wholesale rather than
  // merged client-side: the phase-2 response is the same authoritative shape
  // a plain fetch returns, which is what makes reordering impossible even
  // when plan.ArchivesBelowLive is false (#1037 backfills). Row identity for
  // the keyboard cursor and expanded diffs survives the repaint keyed on
  // eventDTO.anchor (#1411).
  const paint = (data, partialPending) => {
    let warnings = data.warnings || [];
    if (partialPending) {
      // The server's PARTIAL warning states the fact; this line states the
      // in-progress half — it exists only while phase 2 is in flight and is
      // swept by phase 2's own paint (or replaced by the failure line).
      warnings = warnings.concat("Reading archived history in the background; the list below will complete itself.");
    }
    renderWarnings($("#ev-warnings", VIEW()), warnings);
    renderNotes($("#ev-notes", VIEW()), data.notes);

    // Client-side refine: unscoped pk/col + free terms.
    const events = refineEvents(data.events || [], refine);

    // Remember where the NEXT page starts before rendering this one. The cursor
    // comes from the server (data.next_before), which derives it from the last
    // row it actually served — not from `events`, which the refine above may
    // have dropped the boundary row from. Deriving it here would skip whatever
    // the refine hid.
    if (data.has_more && data.next_before) {
      evPages[evPageIdx + 1] = { before: data.next_before, offset: evPages[evPageIdx].offset + data.count };
    } else {
      evPages.length = evPageIdx + 1;
    }

    // Carry the operator's place across the phase-2 repaint: the focused row
    // and any expanded diffs, keyed on anchor — event_id alone is per-index,
    // which suffices here (both phases read one index), but anchor is the
    // identity Undo already relies on.
    const openAnchors = {}, focusedAnchor = { v: null };
    // The j/k cursor is a THIRD identity channel (review pass 1): cursorIdx
    // is module state that survives the repaint while its .cursor class does
    // not — `u` would then fire undoEvent(lastEvents[cursorIdx]) against a
    // reindexed list with no highlight anywhere on screen. Captured by
    // anchor with the rest and re-seated below; unresolvable → -1, the same
    // reset eventsGoPage does for the same reason.
    const cursorAnchor = (cursorIdx >= 0 && lastEvents[cursorIdx] && lastEvents[cursorIdx].anchor) || null;
    if (lastEvents.length) {
      rowsEl.querySelectorAll(".ev-row").forEach((r) => {
        const ev = lastEvents[Number(r.dataset.ev)];
        if (!ev || !ev.anchor) return;
        if (r.classList.contains("open")) openAnchors[ev.anchor] = true;
        if (r === document.activeElement) focusedAnchor.v = ev.anchor;
      });
    }

    lastEvents = events;
    // Honest scope (#966 + #1297): free terms / unscoped pk are refined
    // client-side over ONE fetched page, so a refined count is page-local. The
    // window note no longer restates the limit back at the reader ("100 events in
    // the newest 100" answered nothing about whether 101 or ten million sat
    // behind it) — has_more, one probe row on the server, says which.
    const refining = refine.length > 0;
    const from = evPages[evPageIdx].offset + 1;
    const to = evPages[evPageIdx].offset + data.count;
    let scopeNote = "";
    if (data.has_more) {
      scopeNote = " · showing " + from + "–" + to + " of more; page older for the rest";
    } else if (evPageIdx > 0) {
      // Paged to the end, so the total IS known exactly: it is what we walked.
      scopeNote = " · showing " + from + "–" + to + " of " + to + " (end)";
    }
    if (refining && data.count !== events.length) {
      // The refine ran over this page only; say so rather than let the number
      // read as an index-wide match count.
      scopeNote += " · refined within this page";
    }
    if (countEl) countEl.textContent = String(events.length);
    const noteEl = $("#ev-count-note", VIEW());
    if (noteEl) noteEl.textContent = (refining ? " match(es)" : " event(s)") + scopeNote;
    const prevBtn = $("#ev-prev", VIEW());
    const nextBtn = $("#ev-next", VIEW());
    if (prevBtn) prevBtn.disabled = evPageIdx === 0;
    if (nextBtn) nextBtn.disabled = !data.has_more;
    buildEventRows(rowsEl, events, scopeNote);

    cursorIdx = -1;
    if (focusedAnchor.v || cursorAnchor || Object.keys(openAnchors).length) {
      rowsEl.querySelectorAll(".ev-row").forEach((r) => {
        const ev = events[Number(r.dataset.ev)];
        if (!ev || !ev.anchor) return;
        if (openAnchors[ev.anchor] && !r.classList.contains("open")) r.click();
        if (ev.anchor === focusedAnchor.v) r.focus();
        if (ev.anchor === cursorAnchor) { cursorIdx = Number(r.dataset.ev); r.classList.add("cursor"); }
      });
    }
  };

  let data;
  try {
    const p1 = progressive ? Object.assign({}, pageParams, { scope: "live" }) : pageParams;
    data = await api("/api/events?" + new URLSearchParams(p1).toString());
  } catch (err) {
    if (gen !== serverGen) return;
    clear(rowsEl); renderError(rowsEl, err);
    // Clear stale advisories along with the rows: a lingering "nothing is
    // missing here" (or an old warning) beside an error is misleading (#1365).
    renderWarnings($("#ev-warnings", VIEW()), []);
    renderNotes($("#ev-notes", VIEW()), []);
    if (countEl) countEl.textContent = "0";
    return;
  } finally {
    clearTimeout(slowT);
  }
  if (gen !== serverGen) return;
  const pending = progressive && !!data.archives_pending;
  paint(data, pending);
  if (!pending) return;

  // Phase 2: the same search, full scope. Applied only if this search is
  // still the one on screen — a filter edit or page step replaces
  // evLastQuery, a server switch bumps serverGen, and either bails this
  // stale completion out. On failure the partial marker STAYS UP and names
  // the failure: a live-only list must never quietly present as complete.
  api("/api/events?" + new URLSearchParams(pageParams).toString()).then((full) => {
    if (gen !== serverGen || evLastQuery !== myQuery || !rowsEl.isConnected) return;
    paint(full, false);
  }, (err) => {
    if (gen !== serverGen || evLastQuery !== myQuery || !rowsEl.isConnected) return;
    renderWarnings($("#ev-warnings", VIEW()), (data.warnings || []).concat(
      "The archive read FAILED: this list remains live-only and may be missing archived history: " +
      ((err && err.message) || err)));
  });
}

// renderEventsLoading paints the Events list's busy state (#1353): skeleton
// rows in the shape of the real ones, plus a what's-happening note that stays
// hidden until the caller's slow-fetch timer flips the wrapper to "slow".
// Returns the wrapper so that timer can check it is still on screen
// (isConnected) before speaking — a superseded search repaints its own
// skeleton, and the stale timer must not touch it.
function renderEventsLoading(container) {
  clear(container);
  const wrap = el("div", { class: "ev-loading", role: "status", "aria-label": "Loading events" });
  for (let i = 0; i < 8; i++) {
    const r = el("div", { class: "ev-skel-row" });
    ["time", "table", "type", "pk", "cols"].forEach((c) => r.append(el("span", { class: "ev-skel-bar ev-skel-" + c })));
    wrap.append(r);
  }
  wrap.append(el("div", { class: "ev-skel-note",
    text: "Still working: reading history. Archived hours can take a while…" }));
  container.append(wrap);
  return wrap;
}

// refineEvents applies the client-side free-text / unscoped-pk refine. Shared
// by the rendered page and by export so the two can never diverge on what
// "matches this search" means.
function refineEvents(events, refine) {
  if (!refine.length) return events;
  return events.filter((e) => {
    const hay = (e.schema_name + "." + e.table_name + " " + e.event_type + " " + e.pk_values + " " +
      (e.changed_columns || []).join(" ") + " " +
      valueToString(e.row_before) + " " + valueToString(e.row_after)).toLowerCase();
    return refine.every((t) => hay.includes(t));
  });
}

// eventsGoPage steps the Events view one keyset page in `dir` (+1 = older).
function eventsGoPage(dir) {
  const next = evPageIdx + dir;
  if (next < 0 || next >= evPages.length) return;
  evPageIdx = next;
  cursorIdx = -1; // the keyboard cursor indexes into the old page's rows
  runEventsQuery($("#ev-form", VIEW()), true);
}

// exportEvents downloads EVERY match of the current search, not just the page
// on screen.
//
// The decision, and why: once the view pages, "export what is rendered" means
// an operator who filtered to one table, walked four pages of an incident and
// hit Export silently gets a quarter of their evidence. That is worse than the
// un-paged behavior it replaces. Exporting the whole filtered set costs one
// cursor-less request at the limit the endpoint ALREADY sanctions
// (eventsMaxLimit, 1000) — it grants no capability the caps did not already
// permit, and deliberately does NOT page the export loop, which is exactly how
// a download button would become the way to pull an index into a browser.
// The cap is not silent: a result that comes back at the ceiling says so.
async function exportEvents(kind, btn) {
  if (!evLastQuery) return;
  const label = btn ? btn.textContent : "";
  if (btn) { btn.disabled = true; btn.textContent = "…"; }
  try {
    const params = Object.assign({}, evLastQuery.apiParams, { order: "DESC", limit: String(EVENT_EXPORT_MAX) });
    const data = await api("/api/events?" + new URLSearchParams(params).toString());
    const rows = refineEvents(data.events || [], evLastQuery.refine);
    if (kind === "csv") downloadEventsCSV(rows); else downloadEventsJSON(rows);
    if (data.has_more) {
      // rows.length, not data.count: the client-side refine runs after the
      // fetch, so naming the server's count would report a number that is not
      // the row count of the file just downloaded — the same species of
      // misleading figure this whole change set out to remove.
      toast("Exported the newest " + rows.length + " matches; the search has more. Add a time range to export the rest.");
    }
  } catch (err) {
    toastError("Export failed: " + (err && err.message ? err.message : String(err)));
  } finally {
    if (btn) { btn.disabled = false; btn.textContent = label; }
  }
}

function buildEventRows(container, events, scopeNote) {
  clear(container);
  if (!events.length) {
    const empty = el("div", { class: "ev-empty" },
      scopeNote ? "No changes match your search" + scopeNote + ", or " : "No changes match your search. ",
      el("b", { text: scopeNote ? "clear it" : "Clear it", style: "cursor:pointer",
        onclick: () => { const s = $("#ev-search", VIEW()); if (s) { s.value = ""; runEventsQuery($("#ev-form", VIEW())); } } }),
      " to see everything.");
    container.append(empty);
    return;
  }
  events.forEach((e, i) => {
    const row = el("div", { class: "ev-row", "data-ev": i, tabindex: "0", role: "button", "aria-expanded": "false" });
    row.append(icon("caret", "ev-caret"));
    row.append(tsSpan("ev-time", e.event_timestamp));
    row.append(el("span", { class: "ev-table", text: e.schema_name + "." + e.table_name }));
    row.append(el("span", {}, badge(e.event_type)));
    row.append(el("span", { class: "ev-pk", text: e.pk_values }));
    row.append(el("span", { class: "ev-cols" }, ...colsSummary(e.changed_columns, true)));
    const wrap = el("div", { class: "diff-wrap", id: "diff-" + i });
    let loaded = false;
    row.addEventListener("click", () => {
      const open = row.classList.toggle("open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
      if (open && !loaded) { clear(wrap); wrap.append(renderDiff(e)); loaded = true; }
    });
    // Keyboard activation (#968): the row is a focusable expando, so Enter and
    // Space must toggle it like a real button would.
    row.addEventListener("keydown", (ke) => {
      if (ke.key === "Enter" || ke.key === " ") { ke.preventDefault(); row.click(); }
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
  foot.append(el("span", { class: "diff-foot-note", text: "Generates undo SQL. Nothing runs on its own." }));
  const label = ev.event_type === "DELETE" ? "Restore this row" : ev.event_type === "INSERT" ? "Undo this insert" : "Undo this change";
  foot.append(el("button", { class: "btn btn-sm btn-primary", type: "button", text: label, onclick: () => undoEvent(ev) }));

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

// ── exports (client-side, over the redacted rows on screen) ──────────────────────────────────────────────

function downloadBlob(filename, content, mime) {
  try {
    const url = URL.createObjectURL(new Blob([content], { type: mime }));
    const a = el("a", { href: url, download: filename });
    document.body.append(a); a.click(); a.remove();
    URL.revokeObjectURL(url);
  } catch (err) { toastError("download failed: " + ((err && err.message) || err)); }
}
function csvCell(v) {
  if (v === null || v === undefined) return "";
  let s = typeof v === "object" ? JSON.stringify(v) : String(v);
  // Formula-injection guard (OWASP): a leading =, +, -, @, tab or CR would be
  // interpreted as a formula by Excel/Sheets — prefix a quote to neutralize.
  if (/^[=+\-@\t\r]/.test(s)) s = "'" + s;
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
    "See a row as it was at any moment, and get the SQL that puts it back. ",
    el("b", { text: "Nothing is ever executed" }),
    "; copy or download the script and apply it yourself after review.");
  v.append(pageHead("Restore", sub));

  // Context banner when arriving via an event "Undo" (pendingRecover).
  const ctx = pendingRecover;
  if (ctx) {
    const banner = el("div", { class: "ctx-banner", id: "undo-ctx-banner" });
    banner.append(el("span", { class: "badge " + badgeClass(ctx.type), text: ctx.type }));
    banner.append(el("div", { class: "ctx-main" },
      // Scope, and since #1411 the scope is an EVENT, not a window of time.
      // The prefill sets form.elements.event from ctx.anchor — the server's own
      // `<RFC3339Nano>|<event_id>` token for the clicked row — and the engine
      // filters on that identity, so exactly one event is reversed and it is
      // the one pointed at.
      //
      // What this block used to have to explain, kept because it is the reason
      // the anchor exists rather than history for its own sake: with only
      // `until` + latest-per-row = 1, the ceiling was the END OF A SECOND and
      // the cap kept the last event inside it. On a row INSERTed and DELETEd
      // within one second that is the DELETE whichever row was clicked, and
      // reversing a DELETE re-creates the row — so Undo on the INSERT put the
      // row BACK while the badge read INSERT. The banner disclosed it in
      // words; disclosure was the right immediate move and a poor permanent
      // one.
      //
      // `until` stays prefilled and is still worth stating: it is how the
      // operator reads WHEN, and it is what remains as the scope if they press
      // Clear. It no longer decides WHICH.
      //
      // Worth knowing about the guard: assets_recover_banner_test.go reads
      // string literals only, so no test can see this prose. The same-second
      // caveat it used to pin as REQUIRED text is now false and was removed in
      // the same commit that made it false — dropping one without the other
      // either fails the build or ships a lie.
      el("span", { class: "ctx-eyebrow", text: "Undoing one change" }),
      el("span", { class: "ctx-title", text: ctx.schema + "." + ctx.table + " \u00b7 pk " + ctx.pk }),
      el("span", { class: "ctx-detail", text: "reverses exactly this " + ctx.type
        + ", the one you clicked (" + ctx.time + " UTC), not the rest of the row's history, and not other changes sharing that second" }),
      el("span", { class: "ctx-detail", text: "Clear to search this row freely instead; the time you clicked stays as the upper bound." })));
    banner.append(el("span", { class: "spacer" }));
    // Clear retires the SELECTION, not the form.
    //
    // It used to `navigate("recover")`, which re-renders the route and builds a
    // fresh EMPTY form — so the one control labelled Clear wiped the target and
    // the upper bound, while the sentence above it promised the opposite
    // ("search this row freely… the time you clicked stays as the upper
    // bound"). The mechanism that sentence describes already existed, as
    // clearUndoAnchor; it just had no name in the UI. Calling it here is what
    // makes the copy true.
    banner.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Clear",
      onclick: () => clearUndoAnchor(form) }));
    v.append(banner);
  }

  // Manual filter form
  const form = el("form", { class: "filters", id: "recover-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  // Input + datalist, not a closed select (#1364): suggestions come from the
  // selected schema, but recover legitimately targets dropped tables whose
  // events are still indexed — a typed name must always submit.
  form.append(fieldTableCombo("Table", "table", "md", "orders"));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7"));
  // The undo anchor rides in the form rather than in a module variable so the
  // two request builders pick it up the same way every other filter does (both
  // read the form through FormData). That is what keeps "Preview rows" and
  // "Generate undo SQL" showing the same events — the promise previewRecover
  // makes. Hidden because it is an identity, not a filter an operator can
  // usefully type: it is set by Undo and removed by the banner's Clear.
  form.append(el("input", { type: "hidden", name: "event" }));
  // Latest-per-row sits beside PK because it is meaningless without one (the
  // server refuses the pair). It caps a row's HISTORY — it does not name an
  // event: this comment used to claim it was "the only filter that can
  // separate events sharing a timestamp", which was true of the time filters
  // and false as a general claim, and Undo relied on it (#1411). The hidden
  // `event` field above is what separates events sharing a second.
  form.append(fieldInput("Latest per row", "limit_per_pk", "sm", "all"));
  form.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  form.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Preview rows",
    onclick: () => previewRecover(form) }));
  actions.append(el("button", { class: "btn btn-primary", type: "submit", text: "Generate undo SQL" }));
  form.append(actions);
  v.append(form);

  v.append(stateSection(form));
  v.append(el("div", { id: "recover-warnings", class: "warnings" }));
  v.append(el("div", { id: "recover-notes", class: "notes" }));
  v.append(el("div", { id: "recover-preview" }));
  v.append(el("div", { id: "recover-out" }));

  form.addEventListener("submit", (e) => { e.preventDefault(); generateUndo(form); });
  // Editing the target retires the anchor, and the banner with it.
  //
  // The anchor names one event of one row. Retype the PK and it still names
  // the OLD row's event, so the request comes back empty — a 200 with no
  // statements, which reads as "this row has no history to undo". It fails
  // closed rather than producing wrong SQL, but the narrowing moved from a
  // VISIBLE field the banner named ("Latest per row is set to 1 — clear it
  // to…") into a hidden one nothing names, so an operator has nothing to
  // notice. Retiring it on a target edit is what keeps the form's visible
  // state and its actual scope the same thing.
  //
  // Bound to `change`, not `input`: a keystroke-by-keystroke clear would drop
  // the anchor while the operator is still typing over a value they mean to
  // restore.
  for (const name of ["schema", "table", "pk"]) {
    const field = form.elements[name];
    if (field) field.addEventListener("change", () => clearUndoAnchor(form));
  }
  wireSchemaCascade(form);
  populateSchemas(form);

  // Prefill from context and auto-generate.
  if (ctx) {
    setSelectWhenReady(form, "schema", ctx.schema, () => {
      form.elements.table.value = ctx.table;
      form.elements.pk.value = ctx.pk;
      if (ctx.time) form.elements.until.value = ctx.time;
      // Undo means "undo THIS change", and it now says which one. #1404 got
      // the scope down to a single event with `until` + latest-per-row = 1,
      // which is one event but not necessarily the CLICKED one: both filters
      // are second-granular between them, so a row INSERTed and DELETEd inside
      // one second resolved to the DELETE whichever of the two was clicked —
      // and reversing a DELETE re-creates the row, inverting the outcome
      // (#1411). The anchor names the event itself, so no cap is needed and
      // none is set: two mechanisms narrowing the same scope is how they drift.
      //
      // `until` is left prefilled even though the anchor makes it redundant for
      // membership. It is what the operator reads to know WHEN, it bounds the
      // engine's partition scan, and clearing the anchor (banner → Clear) must
      // leave a sane window behind rather than the whole index.
      if (ctx.anchor) form.elements.event.value = ctx.anchor;
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

// ── busy modal for long actions (#1363, generalized in #1375) ────────────────
//
// Originally Restore-only, hence the recover-flavored default note. Verify's
// Explain reuses it (#1375) because it has the identical shape: one click
// starts minutes of work with nothing to show, and the failure mode is an
// operator concluding the button is dead. Callers that are not a form pass
// opts.disable (the elements to hold disabled) instead of a form element.
//
// Generate-undo (and Preview, which sits on the same latency) used to give no
// feedback until the fetch returned — seconds, when the window reaches
// archived hours — so a user who suspected the click didn't register clicked
// again and queued a second generation. The modal opens the moment the button
// is clicked: it states what is being generated (the same facts as the blue
// context banner), animates with pure-CSS keyframes on the brand accent
// (never the semantic insert/update/delete colors — those are data, not
// decoration), collapses to a static note under prefers-reduced-motion (the
// #1353 skeleton rule), blocks re-entry while open, and Cancel/ESC abort the
// in-flight fetch via AbortController. Errors render IN the modal with the
// server's own actionable text — never a silently-vanished overlay.

let busyModalOpen = false; // one click = one generation (re-entry guard)

// busyModalActive is the re-entry guard READ, and it self-heals: the shared
// #modal slot has other occupants (Manage servers, the rotation dialog) that
// replace its children without our teardown, so a set flag with no
// .busy-modal actually in the slot is stale — honoring it would wedge every
// future Generate/Preview click silently. The isConnected guards in
// openBusyModal tear down on the next keystroke; this covers the click
// path that arrives first.
function busyModalActive() {
  if (!busyModalOpen) return false;
  if (document.querySelector("#modal .busy-modal")) return true;
  busyModalOpen = false;
  return false;
}

// recoverBusyFacts mirrors the context banner's facts: target, pk, window.
// parseLatestPerRow normalises the "Latest per row" field. Returns null for
// anything that is not a whole number >= 0 — the caller reports that rather
// than sending it, because a silently dropped filter here means reversing MORE
// events than the operator asked for. Blank is 0 (all).
function parseLatestPerRow(raw) {
  const s = (raw || "").trim();
  if (s === "") return 0;
  const n = Number(s);
  if (!Number.isInteger(n) || n < 0) return null;
  return n;
}

function recoverBusyFacts(f) {
  const facts = [];
  facts.push(["target", f.schema ? f.schema + (f.table ? "." + f.table : "") : "(all schemas)"]);
  if (f.pk) facts.push(["pk", f.pk]);
  if (f.since) facts.push(["since", f.since + " UTC"]);
  if (f.until) facts.push(["until", f.until + " UTC"]);
  // Both call sites pass their own request object — generateUndo a number,
  // previewRecover a string — so stringify rather than assume either.
  if (f.limit_per_pk) facts.push(["latest per row", String(f.limit_per_pk)]);
  // The anchor is the filter that most determines the scope and the only one
  // with no visible field, so the busy modal is where an operator can see it
  // at all. Shown as the event id rather than the raw token: the timestamp is
  // already on the `until` line above, and the id is what the Events view
  // shows beside the row they clicked.
  if (f.event) facts.push(["single event", String(f.event).split("|").pop()]);
  return facts;
}

// openBusyModal opens the busy dialog over a long-running request. form, when
// given, supplies the action row to disable; a caller with no form (Explain)
// passes null and names its own elements in opts.disable. opts.note adds a
// line under the title explaining what the wait is. Returns {close(refocus),
// showError(err)}; Cancel and ESC run opts.onCancel (the fetch abort) and
// restore focus to the element that had it. Accessibility: role="dialog",
// aria-busy while working, focus trapped inside, focus restored on
// cancel/dismiss.
function openBusyModal(form, opts) {
  busyModalOpen = true;
  const mount = document.getElementById("modal");
  const trigger = document.activeElement;
  // A form caller disables its action row; a non-form caller (Explain) names
  // the elements itself. Either may be empty — the scrim is what actually
  // blocks input; this is the visible confirmation on top of it.
  const actions = opts.disable || (form ? $all(".filter-actions button", form) : []);
  actions.forEach((b) => { b.disabled = true; });

  const scrim = el("div", { class: "modal-scrim show" });
  const panel = el("div", { class: "modal busy-modal", role: "dialog", "aria-label": opts.title, "aria-busy": "true" });
  panel.append(el("div", { class: "modal-head" }, el("h2", { class: "modal-title", text: opts.title })));
  const body = el("div", { class: "modal-body" });
  body.append(el("div", { class: "busy-anim", "aria-hidden": "true" }, el("span"), el("span"), el("span")));
  // The reduced-motion arm: CSS swaps the animation for this static note.
  body.append(el("p", { class: "busy-static", text: "Working. This closes when the result is ready." }));
  const facts = el("div", { class: "busy-facts" });
  (opts.facts || []).forEach(([k, v]) => facts.append(el("div", { class: "busy-fact" },
    el("span", { class: "bf-k", text: k }), el("span", { class: "bf-v", text: v }))));
  body.append(facts);
  body.append(el("p", { class: "busy-note", text: opts.note ||
    "Reading indexed changes: a window that reaches archived hours can take a few seconds." }));
  const foot = el("div", { class: "modal-foot" });
  const cancelBtn = el("button", { class: "btn", type: "button", text: "Cancel", onclick: () => cancel() });
  foot.append(cancelBtn);
  body.append(foot);
  panel.append(body);
  scrim.append(panel);
  clear(mount);
  mount.append(scrim);
  cancelBtn.focus();

  let closed = false;
  // Capture phase, so this runs BEFORE globalKeydown's generic #modal-emptying
  // Escape handler — closing that way would leave the fetch in flight and the
  // form's buttons disabled. Tab is trapped inside the dialog.
  const onKey = (e) => {
    if (closed) return;
    // The ⌘K palette stacks ABOVE this dialog in its own mount and handles
    // its own keys (its Escape handler sits on the palette input, which this
    // capture-phase listener would otherwise beat to the event) — while it is
    // open, bail entirely: same check globalKeydown does.
    const cmdk = document.getElementById("cmdk-mount");
    if (cmdk && cmdk.firstChild) return;
    // Another dialog clobbered the shared #modal slot (openServersModal /
    // showRotationDialog replace its children without our teardown): the trap
    // must dissolve, not keep intercepting Tab over a detached dialog and
    // holding the form's buttons disabled.
    if (!scrim.isConnected) { teardown(); return; }
    if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); cancel(); return; }
    if (e.key !== "Tab") return;
    const foci = $all("button, input, select, textarea, a[href]", panel).filter((n) => !n.disabled);
    if (!foci.length) { e.preventDefault(); return; }
    const first = foci[0], last = foci[foci.length - 1];
    if (!panel.contains(document.activeElement)) { e.preventDefault(); first.focus(); }
    else if (e.shiftKey && document.activeElement === first) { e.preventDefault(); last.focus(); }
    else if (!e.shiftKey && document.activeElement === last) { e.preventDefault(); first.focus(); }
  };
  document.addEventListener("keydown", onKey, true);

  const teardown = () => {
    if (closed) return false;
    closed = true;
    document.removeEventListener("keydown", onKey, true);
    scrim.remove();
    actions.forEach((b) => { b.disabled = false; });
    busyModalOpen = false;
    return true;
  };
  const restoreFocus = () => {
    if (trigger && document.contains(trigger) && typeof trigger.focus === "function") trigger.focus();
  };
  const cancel = () => {
    if (!teardown()) return;
    if (opts.onCancel) opts.onCancel();
    restoreFocus();
  };
  return {
    close(refocus) { if (teardown() && refocus) restoreFocus(); },
    showError(err) {
      if (closed) return;
      const msg = String((err && err.message) || err);
      if (!scrim.isConnected) {
        // Another dialog clobbered the shared #modal slot mid-flight:
        // rendering into the detached panel would be exactly the silently
        // vanished failure this modal exists to prevent. Tear down and
        // surface the error where it can be seen.
        teardown();
        toastError(opts.errTitle + ": " + msg);
        return;
      }
      panel.setAttribute("aria-busy", "false");
      panel.classList.add("busy-failed");
      body.insertBefore(el("div", { class: "busy-error", role: "alert" },
        el("p", { class: "busy-error-title", text: opts.errTitle }),
        el("p", { class: "busy-error-msg", text: msg })), foot);
      cancelBtn.textContent = "Dismiss";
      cancelBtn.focus();
    },
  };
}

async function previewRecover(form) {
  if (busyModalActive()) return; // shares the one-in-flight guard with Generate (#1363)
  const gen = serverGen;
  const container = $("#recover-preview", VIEW());
  const warns = $("#recover-warnings", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const params = {};
  ["schema", "table", "pk", "since", "until", "event"].forEach((k) => { if (f[k] && f[k].trim()) params[k] = f[k].trim(); });
  // Part of mirroring recover's effective window (see below): without this the
  // preview would list events the generated script will not touch. The anchor
  // above is here for the same reason and matters more — unmirrored, the
  // preview would list every event in the clicked second while the script
  // reverses exactly one of them.
  const plpp = parseLatestPerRow(f.limit_per_pk);
  if (plpp === null) { renderError(container, "Latest per row must be a whole number, 0 or more."); return; }
  if (plpp > 0) params.limit_per_pk = String(plpp);
  // Mirror /api/recover's EFFECTIVE fetch window (#967) so the preview shows
  // the same events the undo script will actually reverse: newest-first, same
  // limit as recoverDefaultLimit in internal/console/api.go. Hardcoded here —
  // there is no Go-to-JS constant-sharing mechanism in this codebase.
  params.limit = "1000";
  params.order = "desc";
  const ctrl = new AbortController();
  const busy = openBusyModal(form, {
    title: "Previewing affected rows",
    errTitle: "Couldn't preview the rows",
    facts: recoverBusyFacts(params),
    onCancel: () => ctrl.abort(),
  });
  try {
    const data = await api("/api/events?" + new URLSearchParams(params).toString(), { signal: ctrl.signal });
    if (gen !== serverGen) { busy.close(false); return; }
    if (!container || !container.isConnected) {
      // The view was rebuilt mid-flight (palette navigation): don't render
      // into detached nodes — close and say what happened.
      busy.close(false);
      toast("The page changed while previewing. Run Preview rows again.");
      return;
    }
    clear(container);
    container.append(el("div", { class: "meta-line" }, el("b", { text: String(data.count) }), " affected event(s) · limit " + data.limit));
    const list = el("div", { class: "events" });
    const head = el("div", { class: "ev-head" });
    ["time (UTC)", "table", "type", "pk", "changed columns"].forEach((h) => head.append(el("span", { text: h })));
    list.append(head);
    const rows = el("div");
    buildEventRows(rows, data.events || []);
    list.append(rows);
    container.append(list);
    // Truncation warning (#967): more matches than the preview's limit means
    // the actual undo script (same limit, applied server-side) may cover more
    // events than are shown here.
    // Preview and Generate-undo share #recover-warnings, so this must render
    // the server's OWN warnings (the archive-exclusion notice among them)
    // alongside its truncation note -- not clear the box. Clearing it made the
    // caveat vanish the moment an operator adjusted a filter and re-previewed,
    // which is exactly when it is most load-bearing.
    const warnList = (data.warnings || []).slice();
    if (data.count >= data.limit) {
      warnList.push("Only the newest " + data.limit + " events are shown. The actual undo script may include more if you increase the limit.");
    }
    renderWarnings(warns, warnList);
    // The info half (#1365): /api/events carries `notes` (the archive-elision
    // record among them) — muted register, same container the undo response
    // fills, so a re-preview updates rather than duplicates it. Rendered on
    // the same success path as the warnings (past the isConnected guard), and
    // like #recover-warnings it is deliberately NOT cleared on a failed
    // preview (#1311: server notices must survive a re-preview).
    renderNotes($("#recover-notes", VIEW()), data.notes);
    busy.close(true); // success: back to the button that started the preview
  } catch (err) {
    // Cancel/ESC = a WITHDRAWN request: the modal is already closed and the
    // page keeps its pre-click state, previous preview included.
    if (err && err.name === "AbortError") return;
    if (gen !== serverGen) { busy.close(false); return; }
    // A FAILED preview must not leave the previous run's rows on screen as
    // if they answered the current filters; the error renders in the modal.
    // #recover-warnings is deliberately NOT cleared (#1311: server notices
    // must survive a re-preview).
    clear(container);
    busy.showError(err);
  }
}

// formatGeneratedIn renders the server's generation time as a trailing meta
// clause. Absence and zero are DIFFERENT answers and must not collapse: a
// server that predates generated_in_ms sends nothing (render nothing), while a
// recover served entirely from live partitions can legitimately round to 0 ms
// (render "<0.1s" — fast, measured). Hence the typeof check rather than a
// falsy one, which would misreport the fast case as unreported.
function formatGeneratedIn(ms) {
  if (typeof ms !== "number" || !isFinite(ms) || ms < 0) return "";
  // Coarse on purpose: an orientation signal, not a benchmark. Sub-100ms is
  // reported as a floor rather than a spuriously precise 0.04s.
  return " · generated in " + (ms < 100 ? "<0.1s" : (ms / 1000).toFixed(1) + "s");
}

async function generateUndo(form) {
  if (busyModalActive()) return; // one click = one generation (#1363)
  const gen = serverGen;
  const warns = $("#recover-warnings", VIEW());
  const out = $("#recover-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const body = {};
  ["schema", "table", "pk", "since", "until", "event"].forEach((k) => { if (f[k] && f[k].trim()) body[k] = f[k].trim(); });
  if (!body.schema) { renderError(out, "Choose at least a schema to search."); return; }
  // Sent as a NUMBER: the field is an int on the wire, and a string would be
  // rejected by the decoder rather than silently ignored. Only when > 0 —
  // blank and 0 both mean "all", so neither needs to travel.
  const lpp = parseLatestPerRow(f.limit_per_pk);
  if (lpp === null) { renderError(out, "Latest per row must be a whole number, 0 or more."); return; }
  if (lpp > 0) body.limit_per_pk = lpp;
  const ctrl = new AbortController();
  const busy = openBusyModal(form, {
    title: "Generating undo SQL",
    errTitle: "Couldn't generate the undo SQL",
    facts: recoverBusyFacts(body),
    onCancel: () => ctrl.abort(),
  });
  try {
    const data = await api("/api/recover", { method: "POST", body, signal: ctrl.signal });
    if (gen !== serverGen) { busy.close(false); return; }
    if (!out || !out.isConnected) {
      // The view was rebuilt mid-flight (palette navigation): don't render
      // into detached nodes — close and say what happened.
      busy.close(false);
      toast("The page changed while generating. Run Generate undo SQL again.");
      return;
    }
    renderWarnings(warns, data.warnings);
    renderNotes($("#recover-notes", VIEW()), data.notes);
    lastSQL = data.sql || "";
    clear(out);
    // When the target is auto-detected as a foreign-key parent, the script also
    // repairs the child rows InnoDB changed below the binlog: rows a delete
    // cascade removed, references it cleared, and references an ON UPDATE
    // cascade re-pointed. Surface all three so the larger script isn't a
    // surprise — and so a script that is ENTIRELY reference repairs never shows
    // a bare "0 child row(s)" (coverage caveats, if any, are in the warnings
    // above).
    //
    // cascade_detected implies at least one repair: the server falls back to the
    // plain script/response when the synthesis produced nothing (an UPDATE undo
    // that turned out not to have moved a referenced key), so `parts` is never
    // empty here and there is no "nothing was repaired" branch to render.
    if (data.cascade_detected) {
      const victims = data.victim_count || 0;
      const setNulls = data.set_null_count || 0;
      const keyRestores = data.key_restore_count || 0;
      const parts = [];
      if (victims) parts.push("restores " + victims + " related row(s) that MySQL deleted automatically");
      if (setNulls) parts.push("fixes " + setNulls + " reference(s) that were cleared automatically");
      if (keyRestores) parts.push("fixes " + keyRestores + " reference(s) that MySQL re-pointed automatically");
      out.append(el("div", { class: "ctx-banner" },
        el("span", { class: "badge b-baseline", text: "CASCADE" }),
        el("div", { class: "ctx-main" },
          el("span", { class: "ctx-eyebrow", text: "Also repairing rows MySQL changed automatically along with this one" }),
          el("span", { class: "ctx-detail", text: "this script also " + parts.join(", ") + "." }))));
    }
    const meta = (data.cascade_detected
      ? data.statement_count + " statement(s) · " + (data.victim_count || 0) + " cascade child row(s) · " +
        (data.set_null_count || 0) + " SET NULL restore(s) · " + (data.key_restore_count || 0) + " FK restore(s)"
      : data.statement_count + " statement(s) from " + data.row_count + " event(s)")
      + formatGeneratedIn(data.generated_in_ms);
    out.append(codePanel(lastSQL, meta));
    // Success: close the busy dialog and land keyboard focus on the result —
    // the reversal.sql panel header (#1363).
    busy.close(false);
    const head = $("#sql-panel .code-head", out);
    if (head) head.focus();
  } catch (err) {
    // Cancel/ESC = a WITHDRAWN request: the modal is already closed and the
    // page keeps its pre-click state, previous result included.
    if (err && err.name === "AbortError") return;
    if (gen !== serverGen) { busy.close(false); return; }
    // A FAILED generation is different: the previous run's script must not
    // stay on screen with Copy/Download live — those bytes answer a filter
    // nobody named. Clear the result and the download buffer, then render
    // the error IN the modal (with a Dismiss), never as a silently vanished
    // overlay.
    clear(warns);
    // The info notes are cleared with the warnings (#1365): a lingering
    // "nothing is missing here" would caption a script that no longer exists.
    clear($("#recover-notes", VIEW()));
    clear(out);
    lastSQL = "";
    busy.showError(err);
  }
}

function codePanel(sql, metaLabel) {
  const panel = el("div", { class: "codepanel", id: "sql-panel" });
  // tabindex -1: programmatic focus target — the busy modal moves keyboard
  // focus here when a generation succeeds (#1363).
  const head = el("div", { class: "code-head", tabindex: "-1" });
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
  navigator.clipboard.writeText(lastSQL).then(() => toast("SQL copied to clipboard"), () => toastError("Copy failed."));
}
function downloadSQL() { downloadBlob("dbtrail-undo.sql", lastSQL, "application/sql"); }

// Bridge: an event → Recover, scoped to that row up to the event's timestamp.
function undoEvent(e) {
  pendingRecover = {
    schema: e.schema_name, table: e.table_name, pk: e.pk_values,
    type: e.event_type, time: e.event_timestamp,
    // The server's own identity token for this row, echoed back verbatim
    // (eventDTO.Anchor). Never rebuilt from `time`: that one is second-
    // granular and offset-less, which is exactly the ambiguity the anchor
    // exists to remove (#1411).
    anchor: e.anchor,
  };
  navigate("recover");
}

// ── Row state, inside Restore (#1298) ────────────────────────────────────────

// stateSection is the folded-in Time-travel half: the SAME target the undo
// form already carries (schema/table/pk), plus an instant. Two views that used
// to be two destinations with identical forms — the operator read a timestamp
// off Events, retyped the target into one, looked, then retyped it into the
// other. Here the target is entered once.
//
// Gated twice, for two different reasons. capsCache.reconstruct is a per-server
// capability (no baseline configured, nothing to reconstruct from), and the
// panel says so rather than vanishing — an operator who cannot find it has no
// way to learn that a baseline is what is missing. The permission gate mirrors
// the nav's [data-perm]: the server's 403 is the real boundary, this only
// spares a scoped session a control that would always error.
function stateSection(form) {
  const wrap = el("section", { class: "state-section", "data-perm": "reconstruct:execute" });
  wrap.append(el("h2", { class: "state-title", text: "Row at a point in time" }));

  if (!capsCache.reconstruct) {
    wrap.append(el("p", { class: "state-note", text:
      "Configure a baseline for this server to see a row's earlier state here. Undo SQL below works without one; it reverses recorded changes, so it cannot show a row nothing has touched." }));
    return wrap;
  }

  wrap.append(el("p", { class: "state-note", text:
    "Uses the schema, table and PK above. Shows the row as of an instant: your latest snapshot plus every change since." }));

  const bar = el("div", { class: "state-bar" });
  const at = fieldDateInput("At (UTC)", "state_at", "md", "now");
  bar.append(at);
  bar.append(el("button", { class: "btn btn-ghost", type: "button", text: "Show state",
    onclick: () => runState(form, false) }));
  bar.append(el("button", { class: "btn btn-ghost", type: "button", text: "Show history",
    onclick: () => runState(form, true) }));
  wrap.append(bar);
  wrap.append(el("div", { id: "state-warnings", class: "warnings" }));
  wrap.append(el("div", { id: "state-out" }));
  return wrap;
}

// runState reads the target from the undo form — one target, two questions.
async function runState(form, history) {
  const gen = serverGen;
  const out = $("#state-out", VIEW());
  const warns = $("#state-warnings", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const atField = $('[name="state_at"]', VIEW());
  if (!f.schema || !f.table || !f.pk) {
    clear(warns);
    renderError(out, "Schema, table and PK are all required; fill them in above.");
    return;
  }
  const params = { schema: f.schema, table: f.table, pk: f.pk };
  const atVal = atField && atField.value.trim();
  if (atVal) params.at = atVal;
  if (history) params.history = "true";
  clear(out);
  clear(warns);
  try {
    const data = await api("/api/reconstruct?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    // Into a modal, not between the filter form and the reversal panel
    // (#1405). The output is unbounded — a busy row's history is a long table
    // — and it is consulted on the way to the script, so rendering it inline
    // pushed the artifact the page exists to produce off screen.
    //
    // The WARNINGS come with it. They used to render into a sibling container
    // that would now sit behind the scrim, and a reconstruct can return
    // stale_baseline or a capture-gap caveat: an operator reading a row state
    // with the caveat hidden behind the dialog is worse off than before this
    // change, not better.
    const dlg = openModal({
      class: "state-modal",
      label: history ? "Row history" : "Row at a point in time",
      title: f.schema + "." + f.table + " · pk " + f.pk,
      desc: [history
        ? "Every recorded change to this row, newest last."
        : "The row as of " + (atVal || "now") + " UTC: your latest snapshot plus every change since."],
    });
    const mwarns = el("div", { class: "warnings" });
    dlg.body.append(mwarns);
    renderWarnings(mwarns, data.warnings);
    const mnotes = el("div", { class: "notes" });
    dlg.body.append(mnotes);
    renderNotes(mnotes, data.notes);
    if (history) renderTimeline(dlg.body, data, dlg.close);
    else {
      renderStateAt(dlg.body, data);
      // The action retargets the form UNDERNEATH this dialog, so the dialog
      // has to get out of the way — leaving it open hides the change it just
      // made, which is the same complaint that moved this output in here.
      const action = restoreToStateAction(form, data, dlg.close);
      // Empty on a not-found/deleted row, and an empty footer is a stray
      // divider with padding under it, so only mount one that has a button.
      if (action.firstChild) dlg.panel.append(action);
    }
  } catch (err) {
    if (gen !== serverGen) return;
    // The fetch failed rather than the form being wrong, so the answer belongs
    // where the answer would have been. A 422 gap refusal is a result about
    // the request, not a hint about the fields.
    const dlg = openModal({
      class: "state-modal",
      label: history ? "Row history" : "Row at a point in time",
      title: f.schema + "." + f.table + " · pk " + f.pk,
    });
    renderError(dlg.body, err);
  }
}

// clearUndoAnchor retires the single-event selection and the banner that
// describes it, leaving the visible filters untouched.
//
// Shared by the target-edit listeners and available to anything else that
// widens the scope. It does NOT touch `until`: that is the scope the operator
// is left with, and blanking it here would silently widen a retargeted search
// to the whole index.
function clearUndoAnchor(form) {
  if (!form.elements.event || !form.elements.event.value) return;
  form.elements.event.value = "";
  pendingRecover = null;
  const banner = document.getElementById("undo-ctx-banner");
  if (banner) banner.remove();
}

// aimUndoAtInstant points the undo window at the state as of `at`: reverse
// everything AFTER that instant, so the row lands exactly on what was shown.
//
// since = at + 1s is exact, not approximate. reconstruct applies events
// timestamped <= at; recover reverses events timestamped >= since and leaves
// the row before the earliest of them. event_timestamp is DATETIME(0), so no
// event can hide between at and at+1s — while passing `at` itself would be off
// by every event sharing that second, and these indexes routinely carry dozens
// from a single write burst.
//
// Clearing `until` matters just as much: a leftover upper bound (the Undo
// bridge from Events sets one) would drop the newest damage out of the window
// and quietly restore to the wrong place.
function aimUndoAtInstant(form, at) {
  const since = shiftSeconds(at, 1);
  if (!since) { toastError("Could not read the selected instant."); return; }
  form.elements.since.value = since;
  form.elements.until.value = "";
  // Cleared for the same reason `until` is, and it became necessary with the
  // same change that made `until` matter here: since #1404 the Undo bridge
  // prefills limit_per_pk = 1, so an operator who used Undo first would arrive
  // with a leftover cap. This action reverses EVERY change after `at` — that
  // is what makes the row land on the state shown — and a cap of 1 would
  // reverse only the newest of them and land it somewhere else entirely,
  // silently, with the button still naming the state it did not produce.
  form.elements.limit_per_pk.value = "";
  // The undo anchor goes for the strongest version of the same reason: it
  // names ONE event, and this action reverses every change after `at`. Left
  // set, the generated script would reverse exactly the event the operator
  // clicked minutes ago in Events and nothing else, while the button that
  // produced it named a completely different outcome.
  form.elements.event.value = "";
  // …and retire the banner that describes the scope just replaced. It asserts
  // that exactly the clicked event is reversed, and the lines above make that
  // false: this action reverses EVERYTHING after `at`. (Before #1411 the same
  // contradiction ran through the cap — the banner then read "Latest per row is
  // set to 1", which the clear above falsified.) The stale-label half of the
  // same bug, one level up from the fields.
  //
  // By ID, not by `.ctx-banner`: generateUndo appends a second node with that
  // class into #recover-out for the cascade notice, and an unscoped remove
  // would take whichever came first.
  pendingRecover = null;
  const staleBanner = document.getElementById("undo-ctx-banner");
  if (staleBanner) staleBanner.remove();
  previewRecover(form);
  form.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "start" });
  toast("Undo window set to everything after " + at + " UTC");
}

// restoreToStateAction is the bridge that makes the two halves one errand:
// the state on screen becomes the undo window that produces it.
//
// The arithmetic is exact, not approximate. reconstruct applies events
// timestamped <= at; recover reverses events timestamped >= since and leaves
// the row before the earliest of them. Setting since = at + 1s therefore
// reverses precisely the events AFTER `at`, landing the row on the state shown
// — event_timestamp is DATETIME(0), so no event can hide between at and at+1s.
//
// Passing `at` itself would be off by every event sharing that second, and
// these indexes routinely carry dozens (a single write burst stamps them all
// alike). Until is cleared for the same reason it is set by the Undo bridge:
// a leftover upper bound would silently drop the newest damage from the window.
function restoreToStateAction(form, data, onDone) {
  const row = el("div", { class: "state-actions" });
  if (!data.found) return row;
  row.append(el("button", {
    class: "btn btn-primary", type: "button", text: "Restore to this state",
    title: "Reverse every change after " + data.at + " UTC, leaving the row exactly as shown",
    onclick: () => {
      // Close BEFORE retargeting: aimUndoAtInstant scrolls the form into view
      // and previews the rows, and both are invisible behind a scrim.
      if (onDone) onDone();
      aimUndoAtInstant(form, data.at);
    },
  }));
  row.append(el("span", { class: "state-actions-note", text:
    "Sets the undo window to every change after this instant. Review the rows, then generate the SQL." }));
  return row;
}

// useTimelineInstant fills the At field from a timeline node and re-runs the
// state view. This is what replaces typing a timestamp: the operator points at
// the change that broke things instead of transcribing its time from Events.
function useTimelineInstant(at) {
  const field = $('[name="state_at"]', VIEW());
  const form = document.getElementById("recover-form");
  if (!field || !form) return;
  field.value = at;
  runState(form, false);
  field.scrollIntoView({ behavior: prefersReducedMotion() ? "auto" : "smooth", block: "center" });
}

// shiftSeconds adds n seconds to a "YYYY-MM-DD HH:MM:SS" UTC stamp, returning
// the same shape. Parsed as UTC explicitly: bare "YYYY-MM-DD HH:MM:SS" is
// LOCAL time to Date(), which would shift the window by the browser's offset.
function shiftSeconds(stamp, n) {
  const m = /^(\d{4}-\d{2}-\d{2})[ T](\d{2}:\d{2}:\d{2})/.exec(String(stamp || ""));
  if (!m) return "";
  const t = Date.parse(m[1] + "T" + m[2] + "Z");
  if (Number.isNaN(t)) return "";
  return new Date(t + n * 1000).toISOString().slice(0, 19).replace("T", " ");
}

// ── Time-travel (reconstruct) ─────────────────────────────────────────────────

function renderTimetravel(params) {
  // Merged into Restore (#1298). Rewrite the URL then re-dispatch: calling
  // navigate(push=false) here would leave the URL on /timetravel and re-resolve
  // straight back into this guard (infinite recursion).
  history.replaceState({}, "", "/recover");
  renderRoute();
  return;
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "See what a row looked like at any moment in the past: your latest full snapshot plus every change since. Pick a row and a time to see its value then, or see its entire history.");
  v.append(pageHead("Time-travel", sub));

  const form = el("form", { class: "filters", id: "tt-form" });
  form.append(fieldSelect("Schema", "schema", "md", true, false, null, "— select —"));
  form.append(fieldSelect("Table", "table", "md", false, true, null, null, true));
  form.append(fieldInput("PK", "pk", "sm", "42 or 42|7", true));
  form.append(fieldDateInput("As of (UTC)", "at", "md", "YYYY-MM-DD HH:MM:SS (default: now)"));
  const gapsField = el("div", { class: "field", style: "justify-content:flex-end" },
    el("label", { class: "check" }, el("input", { type: "checkbox", name: "allow_gaps" }), el("span", { text: "Continue even if some history is missing" })));
  form.append(gapsField);
  const actions = el("div", { class: "filter-actions" });
  actions.append(el("button", { class: "btn btn-ghost", type: "button", text: "Full history", onclick: () => runReconstruct(form, true) }));
  actions.append(el("button", { class: "btn btn-primary", type: "submit", text: "Value at that time" }));
  form.append(actions);
  v.append(form);

  v.append(el("div", { id: "tt-warnings", class: "warnings" }));
  v.append(el("div", { id: "tt-notes", class: "notes" }));
  const out = el("div", { id: "tt-out" });
  out.append(el("div", { class: "tt-meta", text: "Fill in a row above and pick a button to see what it looked like." }));
  v.append(out);

  form.addEventListener("submit", (e) => { e.preventDefault(); runReconstruct(form, false); });
  wireSchemaCascade(form);
  populateSchemas(form);
  viewEnter();
}

// runReconstruct is the standalone Time-travel view, NOT the Restore view's
// embedded panel. It renders inline and keeps doing so: #1405 moved runState
// into a dialog because its output was wedged between a filter form and the
// reversal panel it was pushing off screen. Here the reconstructed state IS
// the page, with nothing below it to displace, so a dialog would add a scrim
// and buy nothing.
async function runReconstruct(form, history) {
  const gen = serverGen;
  const warns = $("#tt-warnings", VIEW());
  const ttNotes = $("#tt-notes", VIEW());
  const out = $("#tt-out", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  if (!f.schema || !f.table || !f.pk) { clear(warns); renderNotes(ttNotes, []); renderError(out, "Schema, table, and PK are all required."); return; }
  const params = { schema: f.schema, table: f.table, pk: f.pk };
  if (f.at && f.at.trim()) params.at = f.at.trim();
  if (form.elements.allow_gaps && form.elements.allow_gaps.checked) params.allow_gaps = "true";
  if (history) params.history = "true";
  try {
    const data = await api("/api/reconstruct?" + new URLSearchParams(params).toString());
    if (gen !== serverGen) return;
    renderWarnings(warns, data.warnings);
    renderNotes(ttNotes, data.notes);
    clear(out);
    if (history) renderTimeline(out, data);
    else renderStateAt(out, data);
  } catch (err) {
    if (gen !== serverGen) return;
    // Sweep BOTH registers (#1365, same rule the events catch states): a
    // lingering "nothing is missing here" elision note beside an error
    // belongs to a different query and reads as reassurance about this one.
    clear(warns); renderNotes(ttNotes, []); renderError(out, err);
  }
}

function reconstructMeta(data, label) {
  return el("div", { class: "meta-line" },
    el("b", { text: data.schema + "." + data.table + " pk=" + data.pk }),
    " · " + label + " · baseline " + data.baseline_time + " · " + data.event_count + " event(s)",
    tzChip());
}

function renderStateAt(container, data) {
  container.append(reconstructMeta(data, "as of " + data.at));
  if (!data.found) { container.append(el("div", { class: "deleted-note", text: "No row with this primary key existed at or before the selected time." })); return; }
  if (data.deleted) { container.append(el("div", { class: "deleted-note", text: "Row was deleted as of " + data.at + " UTC." })); return; }
  container.append(stateTable(data.state || {}));
}

function stateTable(state) {
  const table = el("table", { class: "statetable" });
  Object.keys(state).forEach((k) => {
    table.append(el("tr", {}, el("th", { text: k }), el("td", { text: valueToString(state[k]) })));
  });
  return table;
}

// onDone, when given, is the caller's dismissal — the timeline is rendered
// into a dialog from runState and inline from runReconstruct, and only the
// first has anything to close. Each node carries its OWN restore button, so
// unlike the state panel there is no single footer to pin outside the scroll;
// the button travels with the node it names, which is where it belongs.
function renderTimeline(container, data, onDone) {
  const entries = data.history || [];
  container.append(reconstructMeta(data, "history through " + data.at + " · " + entries.length + " state(s)"));
  if (!entries.length) { container.append(el("div", { class: "deleted-note", text: "No history for this primary key in the time range that's been indexed." })); return; }

  const tl = el("div", { class: "timeline", id: "timeline" });
  let prev = null;
  entries.forEach((e) => {
    const node = el("div", { class: "tl-node" });
    const kind = e.source === "baseline" ? "baseline" : e.source.toLowerCase();
    node.append(el("span", { class: "tl-dot " + kind }));
    const head = el("div", { class: "tl-head" });
    head.append(el("span", { class: "badge " + (e.source === "baseline" ? "b-baseline" : badgeClass(e.source)), text: e.source }));
    head.append(tsSpan("tl-time", e.time));
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

    // Both actions are "pick this moment" — the timeline is how an operator
    // chooses an instant without leaving to read one off Events and retype it.
    //
    // The restore button used to route through undoEvent, which sets `until`:
    // that reverses everything UP TO this point and lands the row before its
    // whole recorded history — the opposite end from the state the button is
    // pointing at. It shares the state panel's exact bridge now, so the label
    // and the SQL finally agree.
    const acts = el("div", { class: "tl-actions" });
    acts.append(el("button", {
      class: "btn btn-sm tl-use", type: "button", text: "Use this moment",
      title: "Set the At field above to " + e.time + " UTC",
      onclick: () => useTimelineInstant(e.time),
    }));
    if (e.source !== "baseline") {
      acts.append(el("button", {
        class: "btn btn-sm tl-restore", type: "button", text: "Restore to this state",
        title: "Reverse every change after " + e.time + " UTC, leaving the row as shown here",
        onclick: () => {
          // Same reason the state panel's action closes first: aimUndoAtInstant
          // scrolls the form into view and previews the rows, and both are
          // invisible behind a scrim. This one used to be carried by accident —
          // previewRecover's busy dialog happens to replace the mount — which
          // held right up until previewRecover took one of its two early
          // returns and left the error rendering behind the scrim.
          if (onDone) onDone();
          const form = document.getElementById("recover-form");
          if (form) aimUndoAtInstant(form, e.time);
        },
      }));
    }
    node.append(acts);
    tl.append(node);
  });
  container.append(tl);

  // "draws itself" — progressive-enhancement reveal (decorative).
  //
  // The stagger is gated here and not only in CSS, because the CSS guard
  // removes the SMOOTHNESS while this loop still schedules the position
  // change: with the transition gone, a 50-row timeline snapped one node at a
  // time across ~2.8s of content shifting under the reader — a jumpier version
  // of the motion the preference asked to remove. Under reduce every node
  // arrives at once and nothing moves.
  requestAnimationFrame(() => {
    tl.classList.add("drawn");
    const nodes = $all(".tl-node", tl);
    if (prefersReducedMotion()) { nodes.forEach((n) => n.classList.add("in")); return; }
    nodes.forEach((n, i) => setTimeout(() => n.classList.add("in"), 60 + i * 55));
  });
}

// ── Status ─────────────────────────────────────────────────────────────────

async function renderStatus() {
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  let data;
  try { data = await api("/api/status"); }
  catch (err) { if (gen !== serverGen || vgen !== viewGen) return; const v = VIEW(); clear(v); v.append(pageHead("Status", null)); renderError(v, err); return; }
  if (gen !== serverGen || vgen !== viewGen) return;
  updateSideMeta(data);

  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub", text: "A quick health check: what was captured, how far back it goes, and where live capture stands now." });
  v.append(pageHead("Status", sub));

  const cards = el("div", { class: "cards" });
  const cov = data.coverage || {};
  const stream = data.stream || null;
  const arch = data.archives || null;
  // Source-aware presentation: a PostgreSQL stream's cursor is an LSN, written
  // across binlog_file (the "X/Y" string form, the one shown here) and
  // binlog_position (the same value as a uint64, for resume); mode='gtid' is an
  // internal detail. MySQL vocabulary would mislabel all of it. capsCache.source
  // is resolved before this paints (bootSequence/switchServer await
  // gateCapabilities → renderRoute).
  const pg = capsCache.source === "postgresql";

  cards.append(statusCard("Summary", [
    ["total events (est.)", data.total_events_estimate, true],
    ["indexed files", (data.files || []).length],
    ["partitions", (data.partitions || []).length],
  ]));
  cards.append(statusCard("Coverage", [
    ["earliest event (UTC)", cov.earliest_event],
    ["latest event (UTC)", cov.latest_event],
    ["total events", cov.total_events],
    ["schema changes", cov.schema_changes],
  ]));
  if (stream) cards.append(statusCard(pg ? "Stream · PostgreSQL" : "Stream", pg ? [
    ["source", "PostgreSQL · logical replication"],
    ["LSN", stream.binlog_file],
    ["events indexed", stream.events_indexed],
  ] : [
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
  // Replication-health panel (#599): the streaming daemon polls the PostgreSQL source
  // (slot wal_status/lag + REPLICA IDENTITY coverage) and persists a snapshot to the
  // index; this renders it. Gated on source==postgresql AND a snapshot existing.
  if (pg && stream && stream.source_health) cards.append(pgHealthCard(stream.source_health));
  // Stream-continuity surface (see continuityBox) — appended above the cards so a
  // permanent-loss alarm, or the affirmative all-clear, is the first thing read.
  const continuity = continuityBox(stream, pg);
  if (continuity) v.append(continuity);
  // Capture-health surface (#1034): the continuity box's sibling for in-stream
  // discards — events the daemon read and chose to drop. Only the degraded
  // state renders (below the continuity box, above the cards); "ok" adds no
  // second green box.
  const captureHealth = captureHealthBox(stream);
  if (captureHealth) v.append(captureHealth);
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

// continuityBox renders the stream-continuity surface, or null when there is
// nothing to assert. The red error-box is the durable permanent-loss record: an
// unfillable binlog gap (MySQL) or an invalidated/lost replication slot
// (PostgreSQL, #532); the index is valid only up to that point and capture must
// be re-baselined to resume. It keys on gap_lost, emitted independently of
// continuity — so a legacy backend that omits continuity still shows it on a lost
// stream. The green ok-box is the affirmative counterpart and keys on
// continuity.status === "ok" (newer backends only); the two are mutually
// exclusive (gap_lost takes precedence). The green box asserts only
// gap-CONTIGUITY of the captured range — NOT that the stream is live or caught
// up; "unknown" (legacy index) and a missing continuity field return null
// (neither box). Pure and fixture-drivable, mirroring pgHealthCard — the
// console-e2e harness pins the ok/gap_lost/neither states.
function continuityBox(stream, pg) {
  if (!stream) return null;
  if (stream.gap_lost) {
    const lost = el("div", { class: "error-box" });
    lost.append(el("b", { text: "⚠ Events permanently lost" }));
    lost.append(el("div", { text: stream.gap_lost.detail ||
      (pg ? "The replication slot PostgreSQL was using got invalidated. To keep capturing changes, create a new baseline and start over."
          : "A gap in the binlog can't be filled; some history is permanently missing. To keep capturing changes, create a new baseline and start over.") }));
    lost.append(el("div", { text: "Detected: " + utcLabel(stream.gap_lost.at) }));
    return lost;
  }
  if (stream.continuity && stream.continuity.status === "ok") {
    const ok = el("div", { class: "ok-box" });
    ok.append(el("b", { text: "✓ No gaps in captured stream" }));
    ok.append(el("div", { text: "No gaps in what was captured so far. That does not mean the stream is running or caught up right now." }));
    return ok;
  }
  return null;
}

// captureHealthBox renders the capture-health surface (#1034), or null when
// there is nothing to warn about. It keys on stream.capture_health.status ===
// "degraded" (newer backends only): the daemon READ these events off the
// stream and chose to DROP them — e.g. the column-count guard rejecting every
// row against a stale schema snapshot — so the stream can look "active" (fresh
// checkpoint, green continuity) while indexing nothing. "ok", "unknown"
// (missing field) and no stream all render nothing; the continuity box remains
// the affirmative/loss surface. Pure and fixture-drivable like continuityBox.
// Two things this renders that the tally alone cannot say (#1312):
//
//   - HISTORIC vs ACTIVE. The tally is monotonic, so a successful re-snapshot
//     leaves it byte-identical and the button this box offers had no visible
//     effect at all — an operator clicked it, reloaded, and saw the same orange
//     alarm. `skips_predate_snapshot` (computed by the backend against the
//     schema snapshot's own timestamp) separates "still dropping rows" from "a
//     record of something that stopped". Historic goes quiet; it does NOT go
//     away, because those events are permanently missing and a dismissed box
//     would trade an annoyance for lost evidence.
//   - The long form is one click away instead of five paragraphs down. ~250
//     words of cause/remedy/scope in an alarm box is text nobody reads.
//
// An old daemon sends neither field: no anchor, so the box stays loud and open
// — the pre-#1312 rendering, which is the safe direction.
function captureHealthBox(stream) {
  if (!stream || !stream.capture_health || stream.capture_health.status !== "degraded") return null;
  const h = stream.capture_health;
  const acked = h.acknowledged === true;
  const historic = h.skips_predate_snapshot === true;
  const box = el("div", { class: acked ? "muted-box" : (historic ? "ok-box" : "warn-box") });
  box.append(el("b", { text: acked
    ? "Capture gap on record (acknowledged)"
    : historic
      ? "Capture gap on record: nothing skipped since the current snapshot"
      : "⚠ Capture incomplete: some changes were not indexed" }));
  const reasons = Object.keys(h.skipped || {}).sort().join(", ");
  box.append(el("div", { text: h.total_skipped + " event(s) were read from the stream but not indexed" +
    (reasons ? " (" + reasons + ")" : "") + (h.last_skip_at ? "; last " + utcLabel(h.last_skip_at) : "") +
    ". Those changes are missing from the index for good." +
    (acked && h.acknowledged_at ? " Acknowledged " + utcLabel(h.acknowledged_at) + "; anything skipped after that count raises this again." : "") +
    (!acked && historic && h.snapshot_at ? " The schema snapshot in force since " + utcLabel(h.snapshot_at) + " has recorded none." : "") }));
  // Cause, remedy and scope come from the backend (status.ExplainCaptureSkips),
  // the same strings `bintrail status` prints — the console must not re-author
  // this advice in JavaScript, because the half that drifts is always the half
  // saying what a remedy does NOT recover. The fallback covers a daemon too old
  // to send the field, and deliberately promises nothing on its behalf.
  const lines = (Array.isArray(h.explanation) && h.explanation.length) ? h.explanation
    : ["Changes in those events are missing from the index. This daemon is too old to say why; run `bintrail status` against this index for the reason and the fix."];
  const details = el("details", { class: "warn-details" }, el("summary", { text: "Why this happened, and what fixes it" }));
  lines.forEach((t) => details.append(el("div", { class: "warn-line", text: t })));
  const action = schemaSnapshotButton();
  // The action sits inside the disclosure once the skips are historic: it has
  // already been pressed, and leaving it under the headline invites pressing it
  // again forever against a tally that will never move.
  if (action) (historic || acked ? details : box).append(action);
  // Mark-as-read (#1314) is the only action on an already-acknowledged record,
  // so it is absent once acked — and it stays OUTSIDE the disclosure while
  // unacknowledged, because it is the one thing an operator looking at a
  // permanent record actually wants and it must not be five paragraphs down.
  //
  // seen_total is the count THIS render displayed: the endpoint refuses the
  // acknowledgement if the live tally has since gone higher, so a tab left
  // open cannot retire skips that happened while nobody was looking.
  if (!acked) {
    // A class of its own, NOT .warn-actions: the e2e pins where the
    // schema-snapshot action sits by querying ":scope > .warn-actions", and a
    // second wrapper sharing that class would make the assertion pass no
    // matter where the snapshot button went.
    const wrap = el("div", { class: "ack-actions" });
    const ackBtn = el("button", { class: "btn btn-sm", type: "button", text: "Mark as read" });
    ackBtn.onclick = () => acknowledgeCaptureSkips(h.total_skipped, ackBtn);
    wrap.append(ackBtn);
    box.append(wrap);
  }
  box.append(details);
  return box;
}

// acknowledgeCaptureSkips records that the operator has seen this tally, so the
// box stops being an alarm (#1314). It does not clear the tally and does not
// pretend the events came back: the record stays, quietly, and a later skip
// pushes the count above what was acknowledged and turns the alarm back on by
// itself. That is why this is safe to offer as a plain button — it can retire a
// record, it cannot suppress the next incident.
async function acknowledgeCaptureSkips(total, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Marking…"; }
  try {
    await api("/api/capture-skips/ack", { method: "POST", body: { seen_total: total } });
  } catch (err) {
    // The 409 here is the stale-tab refusal, and its message already says to
    // reload — surfacing the server's own text keeps the two in one voice.
    toastError("Could not mark as read: " + ((err && err.message) || err));
    if (btn) { btn.disabled = false; btn.textContent = "Mark as read"; }
    return;
  }
  toast("Marked as read. The record stays; if anything gets skipped from now on, the warning comes back.");
  renderStatus();
}

// schemaSnapshotButton renders the Refresh-schema-snapshot action for the
// selected server, or null when this console cannot perform it (#1296). It
// exists because the old banner named a remedy with no button anywhere in the
// UI, leaving the CLI — in a different container — as the only route.
//
// Gated on a real REGISTRY server: the reserved "default" entry is the daemon's
// own command-line stream, which the control plane does not supervise and the
// endpoint refuses with 409. The label never says just "snapshot": the button
// next to it creates a BASELINE (a full copy of the data), and the two artifacts
// were already being confused.
function schemaSnapshotButton() {
  const id = currentServer || defaultServerId;
  if (!capsCache.schema_snapshot_trigger || !id || id === "default") return null;
  const wrap = el("div", { class: "warn-actions" });
  const btn = el("button", { class: "btn btn-sm", type: "button", text: "Refresh schema snapshot" });
  btn.onclick = () => refreshSchemaSnapshot(id, btn);
  wrap.append(btn);
  return wrap;
}

// refreshSchemaSnapshot re-reads the source's column layout and restarts that
// server's capture stream onto it, then reports what actually happened. The
// three outcomes are reported separately on purpose: a failed snapshot, a
// snapshot whose stream did NOT reload (capture is still on the old layout —
// nothing is fixed yet), and tables validation excluded (those stay uncaptured
// no matter how often this runs).
async function refreshSchemaSnapshot(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Refreshing…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = "Refresh schema snapshot"; } };
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/schema-snapshot", { method: "POST", body: {} });
  } catch (err) {
    toastError("Schema snapshot failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  toast("Reading the source's table layout…");
  const done = await pollSchemaSnapshot(id);
  restore();
  if (!done) { toast("Schema snapshot is still running. Check back shortly."); return; }
  if (done.state !== "succeeded") {
    toastError("Schema snapshot failed: " + (done.last_error || "unknown error"));
    return;
  }
  let msg = "Schema snapshot updated: " + (done.tables || 0) + " table(s).";
  // Never assert WHICH state capture ended in when the reload failed: the
  // reload can fail with the old stream still running (registry gone) or with
  // it stopped (it did not shut down in time). Print the daemon's own account
  // instead of guessing — "still using the previous snapshot" would be a lie
  // for the stopped case.
  msg += done.stream_reloaded
    ? " Capture restarted on it."
    : " Capture did NOT restart onto it: " + (done.reload_error || "restart this server's capture to pick it up") + ".";
  if ((done.excluded_tables || []).length) {
    msg += " Still not captured (no primary key / not InnoDB): " + done.excluded_tables.join(", ") + ".";
  }
  // The banner is driven by a monotonic tally, so it stays up after a working
  // fix. Say so here or the operator concludes the button did nothing.
  msg += " Events already skipped stay missing, and this warning stays up: it counts skips that happened; use \u201cMark as read\u201d once you have seen the count.";
  toast(msg);
  renderStatus();
}

// pollSchemaSnapshot polls until the job leaves "running" (or a ~2-minute cap:
// a snapshot is an information_schema read plus a stream restart, not a dump).
// Returns the terminal status, or null if it never settled. Transient poll
// errors are retried — the stream restart briefly disturbs nothing else, but a
// blip must not be reported as a failed snapshot.
async function pollSchemaSnapshot(id) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (let i = 0; i < 60; i++) {
    await sleep(2000);
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/schema-snapshot")).schema_snapshot;
    } catch (_) {
      continue;
    }
    if (st && st.state !== "running") return st;
  }
  return null;
}

// PG_HEALTH_STALE_SEC: a source_health snapshot older than this reads as STALE. The
// daemon polls every 30s, so 90s ≈ 3 missed polls → likely stopped. Load-bearing, not
// decoration: an index-only console cannot tell a frozen "reserved" from a live one, so
// a stale snapshot must never render as healthy green (#599).
const PG_HEALTH_STALE_SEC = 90;

// pgHealthCard renders the persisted PostgreSQL replication-health snapshot. h is the
// parsed stream.source_health object {exists,active,wal_status,retained_bytes,
// safe_wal_size,restart_lsn,confirmed_flush_lsn,replica_identity_not_full,checked_at,
// probe_error}. probe_error is the failed-probe discriminator: when set, this snapshot
// records a probe failure (the slot fields are absent) and the card shows "probe failing".
function pgHealthCard(h) {
  const card = el("div", { class: "card" });
  card.append(el("div", { class: "card-title", text: "Replication health" }));

  // Staleness drives everything: an unparseable/old checked_at mutes the whole card and
  // labels it, so a dead poller never reads healthy.
  const checked = h.checked_at ? Date.parse(h.checked_at) : NaN;
  const ageSec = isNaN(checked) ? Infinity : Math.max(0, (Date.now() - checked) / 1000);
  const stale = ageSec >= PG_HEALTH_STALE_SEC;
  if (stale) card.classList.add("card-stale");

  // probe_error: the daemon could not read source health (e.g. a standby source, or a
  // query-DSN failure). Show it explicitly — a recorded failure, never a blank panel or
  // a misleading "all FULL". The slot/RI sections are skipped (their fields are absent).
  if (h.probe_error) {
    card.append(healthKV("status", el("span", { class: "hstat hstat-err", text: "probe failing" })));
    card.append(el("div", { class: "hlist", text: h.probe_error }));
  } else {
    if (!h.exists) {
      card.append(healthKV("slot", el("span", { class: "hstat hstat-muted", text: "not found yet" })));
    } else {
      card.append(healthKV("WAL status", el("span", { class: "hstat " + walStatusClass(h.wal_status), text: h.wal_status || "—" })));
      card.append(healthKV("retained WAL", el("span", { class: "kv-v", text: humanBytes(h.retained_bytes) })));
      card.append(healthKV("safe margin", el("span", { class: "kv-v", text: h.safe_wal_size == null ? "unlimited" : humanBytes(h.safe_wal_size) })));
      card.append(healthKV("consumer", el("span", { class: "kv-v", text: h.active ? "connected" : "—" })));
    }

    const nf = h.replica_identity_not_full || [];
    if (nf.length === 0) {
      card.append(healthKV("replica identity", el("span", { class: "hstat hstat-ok", text: "all FULL ✓" })));
    } else {
      card.append(healthKV("replica identity", el("span", { class: "hstat hstat-warn", text: "⚠ " + nf.length + " not FULL" })));
      const list = el("div", { class: "hlist" });
      nf.forEach((t) => list.append(el("div", { text: t })));
      card.append(list);
    }
  }

  const foot = el("div", { class: "hstale" + (stale ? " hstale-warn" : "") });
  foot.append(stale
    ? "stale; last checked " + agoText(ageSec) + " (daemon may be stopped)"
    : "checked " + agoText(ageSec));
  card.append(foot);
  return card;
}

function healthKV(k, valNode) {
  return el("div", { class: "kv" }, el("span", { class: "kv-k", text: k }), valNode);
}

function walStatusClass(s) {
  switch (s) {
    case "reserved": return "hstat-ok";
    case "extended": return "hstat-warn";
    case "unreserved": return "hstat-warn hstat-strong";
    case "lost": return "hstat-err";
    default: return "hstat-muted";
  }
}

function humanBytes(n) {
  n = Number(n) || 0;
  if (n < 1024) return n + " B";
  const u = ["KB", "MB", "GB", "TB"];
  let i = -1;
  do { n /= 1024; i++; } while (n >= 1024 && i < u.length - 1);
  return n.toFixed(1) + " " + u[i];
}

function agoText(sec) {
  if (!isFinite(sec)) return "unknown";
  if (sec < 60) return Math.round(sec) + "s ago";
  if (sec < 3600) return Math.round(sec / 60) + "m ago";
  return Math.round(sec / 3600) + "h ago";
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
      // PG stores its LSN cursor in binlog_file; "gtid" mode is an internal detail.
      const pg = capsCache.source === "postgresql";
      streamEl.append(el("b", { text: pg ? "PostgreSQL" : status.stream.mode }));
      if (status.stream.binlog_file) streamEl.append((pg ? " · LSN " : " · ") + status.stream.binlog_file);
    } else {
      streamEl.append(el("b", { text: "—" }));
    }
  }
}

// ── Storage (rotation · S3 archiving · credentials · telemetry) ──────────────

async function renderStorage() {
  // Gated like Time-travel: a direct URL / Back with the capability off must
  // REWRITE the URL (replaceState) before re-dispatching — see renderTimetravel.
  if (!capsCache.monitor) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  // Each fetch degrades independently: a panel renders its own failure note
  // instead of one error wiping the whole page. (A 401 inside api() raises the
  // sign-in gate and bumps serverGen, so the stale-render guard below bails.)
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  const [serversRes, rotation, storage, baselines, telemetry] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/rotation").catch(asErr),
    api("/api/storage").catch(asErr),
    api("/api/baselines").catch(asErr),
    api("/api/telemetry").catch(asErr),
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  // Same guard as renderOverview: a throw inside the build must show an
  // error, never leave the "Loading…" skeleton up forever.
  try {
    buildStorage(serversRes, rotation, storage, baselines, telemetry);
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Storage", null)); renderError(v, err);
  }
}

function buildStorage(serversRes, rotation, storage, baselines, telemetry) {
  // serversRes is the raw /api/servers payload or {error} — archivingPanel
  // must be able to tell "failed to load" from "genuinely no sources", or a
  // transient 500 would render the affirmative "No monitored sources yet" lie.
  const servers = (serversRes && serversRes.servers) || [];
  const serversErr = serversRes && serversRes.error;
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Where old data goes, how long it's kept, and the snapshots Time-travel uses. ",
    el("b", { text: "No credentials are stored here." }));
  v.append(pageHead("Storage", sub));

  const cards = el("div", { class: "cards" });
  const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
  cards.append(rotationCard(rotation));
  cards.append(credentialsCard(storage));
  cards.append(telemetryCard(telemetry));
  if (capsCache.views) cards.append(duckdbCard());
  cards.append(baselineSummaryCard(baselines, cur, { linkOnward: true }));
  v.append(cards);

  // Storage keeps storage POLICY. Baselines and verification moved to their
  // own routes (#1384): the snapshot list is unbounded, so it used to push
  // verification off the fold inside a grid built for two panels (the
  // verification builder is verifyRegions since #1419). baselineSummaryCard
  // stays as the pointer — the count and age belong on a storage overview,
  // the full list does not.
  const grid = el("div", { class: "ov-grid", style: "margin-top:18px" });
  grid.append(archivingPanel(servers, serversErr));
  v.append(grid);
  viewEnter();
}

// ── Protect: baselines and verification (#1384) ──
//
// Both gate on capsCache.monitor and both use the replaceState + re-dispatch
// pattern renderStorage/renderTimetravel share: a direct URL or Back with the
// capability off must REWRITE the URL before re-rendering, or the address bar
// keeps pointing at a view the session cannot show.

async function renderBaselines() {
  if (!capsCache.monitor) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  // Independent degradation, as on Storage: a panel renders its own failure
  // note rather than one error blanking the page.
  const asErr = (err) => ({ error: (err && err.message) || String(err) });
  const [serversRes, baselines] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/baselines").catch(asErr),
  ]);
  if (gen !== serverGen || vgen !== viewGen) return;
  try {
    const servers = (serversRes && serversRes.servers) || [];
    // A failed /api/servers must be REPORTED, not absorbed into an empty list:
    // with servers=[] the Create baseline button silently disappears and the
    // empty state advises "Add a server first" — on the page that owns the
    // button. buildStorage keeps serversErr for the same reason.
    const serversErr = serversRes && serversRes.error;
    const v = VIEW(); clear(v);
    v.append(pageHead("Baselines", el("p", { class: "page-sub" },
      "Full-table snapshots Time-travel and full restores are built from. ",
      el("b", { text: "Nothing is ever executed" }), " against your source by viewing this page.")));
    if (serversErr) v.append(el("div", { class: "error-box", text: "Could not load servers: " + serversErr }));
    // #1415: a context strip and a full-width list, not two half-width cards
    // sharing only a left edge. The strip carries the facts that are ABOUT the
    // collection (source, count, freshness, tables-per-snapshot when uniform)
    // so the rows below can carry only what varies between snapshots.
    const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
    v.append(baselineContextStrip(baselines, cur));
    v.append(baselinesPanel(baselines, servers, { serversErr: serversErr }));
    viewEnter();
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Baselines", null)); renderError(v, err);
  }
}

async function renderVerification() {
  if (!capsCache.monitor) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const gen = serverGen, vgen = viewGen;
  viewLoading();
  const serversRes = await api("/api/servers").catch((err) => ({ error: (err && err.message) || String(err) }));
  if (gen !== serverGen || vgen !== viewGen) return;
  try {
    const servers = (serversRes && serversRes.servers) || [];
    // Same reason as renderBaselines: swallowed, a transient 500 renders
    // "Select a server to run verification" while a server IS selected.
    const serversErr = serversRes && serversRes.error;
    const v = VIEW(); clear(v);
    // The old subtitle described one of the three modes and was wrong about a
    // second (#1418): "prove a snapshot still reconstructs" — the
    // recovery-inputs check uses no snapshot at all.
    v.append(pageHead("Verification", el("p", { class: "page-sub" },
      "Prove your safety net works before you need it: three checks, from the index's own consistency to a full comparison against your live data.")));
    if (serversErr) v.append(el("div", { class: "error-box", text: "Could not load servers: " + serversErr }));
    // Three regions with visible separation (#1419): what you can run, what is
    // running or just ran, what ran before. One undifferentiated card made
    // "No verification run yet" sit directly above five past runs.
    verifyRegions(servers, { serversErr: serversErr }).forEach((region) => v.append(region));
    viewEnter();
  } catch (err) {
    const v = VIEW(); clear(v); v.append(pageHead("Verification", null)); renderError(v, err);
  }
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
  if (!rot.enabled) card.append(el("p", { class: "form-hint", text: "Rotation is turned off. Changes you save here won't take effect until the daemon restarts." }));
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
  let summary = "No credentials set directly; dbtrail relies on your AWS environment (for example, an EC2 instance role) to provide them automatically.";
  if (aws.access_key_env) summary = "Using access keys set in an environment variable.";
  else if (aws.container_creds) summary = "Using an IAM role (found an ECS task role).";
  else if (aws.web_identity) summary = "Using an IAM role (found an EKS service-account role).";
  else if (aws.shared_config || aws.profile) summary = "Using credentials from a shared ~/.aws config file.";
  card.append(el("p", { class: "stg-hint", text: summary }));
  const adv = el("details", { class: "form-advanced" },
    el("summary", { class: "form-adv-summary", text: "Raw signals" }));
  kvRow(adv, "access keys (env)", aws.access_key_env ? "set" : "not set");
  kvRow(adv, "profile (env)", aws.profile || "—");
  kvRow(adv, "region (env)", aws.region_env || "—");
  kvRow(adv, "~/.aws config", aws.shared_config ? "present" : "absent");
  if (aws.container_creds) kvRow(adv, "ECS task role", "detected");
  if (aws.web_identity) kvRow(adv, "EKS IRSA", "detected");
  card.append(adv);
  card.append(el("p", { class: "form-hint", text: "Note: an IAM role can still be active even if none of the signals above show as set." }));
  return card;
}

// telemetryCard shows the machine-wide usage-telemetry state and an opt-out
// toggle. Turning it off here stops THIS running daemon's beacons immediately
// (the daemon wired its live client) and persists the choice for every bintrail
// process on the machine.
// duckdbCard offers the generated DuckDB schema for this server's Parquet
// layout. The console does not run the SQL and gains no query engine: the file
// is executed by the operator's own DuckDB, on their machine, which is why
// "unrestricted SQL over your lake" needs no sandbox, timeout or result cap here.
function duckdbCard() {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Query in DuckDB" }));
  card.append(el("p", { class: "form-hint", text:
    "Download a ready-made schema over your archived Parquet: an events view across every archive source, " +
    "plus one view per table in the newest baseline snapshot. Run it in your own DuckDB; nothing runs here." }));
  card.append(el("p", { class: "form-hint", text:
    "No credentials are in the file: S3 access uses your AWS credential chain, so it is safe to share." }));
  const btn = el("button", { class: "btn btn-sm", type: "button", text: "Open in DuckDB…" });
  btn.onclick = async () => {
    btn.disabled = true;
    try {
      const sql = await apiText("/api/views.sql");
      downloadBlob("views.sql", sql, "text/plain");
      toast("views.sql downloaded. Run it with: duckdb lake.db < views.sql");
    } catch (err) {
      toastError("could not generate views: " + ((err && err.message) || err));
    } finally {
      btn.disabled = false;
    }
  };
  card.append(el("div", { class: "stg-cardfoot" }, btn));
  return card;
}

// ── SQL panel (#1177) ────────────────────────────────────────────────────────
//
// Free-form DuckDB SELECTs over this server's archived Parquet, executed
// server-side in a locked-down sandbox (see internal/console/sqlpanel.go). The
// panel is opt-in and per-server gated; navigate() already redirected here only
// when capsCache.sql is true. The Cancel button IS the cancellation mechanism:
// it aborts the fetch, which kills the request context, which interrupts the
// DuckDB query — there is no server-side cancel endpoint to call.

let sqlRunController = null; // AbortController for the in-flight query, if any

// abortSQLRun cancels an in-flight SQL panel query. Called on navigation away
// and on server switch so a long query never keeps holding the daemon's
// single-in-flight latch after the operator has left the page (they would come
// back to a 429 with no running query to cancel). Safe to call when idle.
function abortSQLRun() { if (sqlRunController) sqlRunController.abort(); }

async function renderSQL() {
  if (!capsCache.sql) { history.replaceState({}, "", "/overview"); renderRoute(); return; }
  const v = VIEW();
  clear(v);
  v.append(pageHead("SQL", null));

  const card = el("div", { class: "card" });
  card.append(el("p", { class: "form-hint", text:
    "Run a read-only SQL query over this server's archived Parquet: an \"events\" view across every archive source, " +
    "plus one \"state_<schema>_<table>\" view per table in the newest baseline. Only SELECT runs; results are capped." }));

  const input = el("textarea", {
    class: "sql-input", id: "sql-input", spellcheck: "false", rows: "6",
    placeholder: "SELECT event_type, schema_name, table_name FROM events ORDER BY commit_time DESC LIMIT 100",
  });
  card.append(input);

  const runBtn = el("button", { class: "btn btn-primary btn-sm", type: "button", text: "Run" });
  const cancelBtn = el("button", { class: "btn btn-sm", type: "button", text: "Cancel", hidden: true });
  const statusLine = el("span", { class: "sql-status", id: "sql-status" });
  card.append(el("div", { class: "sql-actions" }, runBtn, cancelBtn, statusLine));
  v.append(card);

  const results = el("div", { id: "sql-results" });
  v.append(results);
  viewEnter();

  const run = () => runSQL(input.value, { runBtn, cancelBtn, statusLine, results });
  runBtn.onclick = run;
  cancelBtn.onclick = () => { if (sqlRunController) sqlRunController.abort(); };
  // Cmd/Ctrl+Enter submits from the textarea — the expected shortcut for a
  // query box, and it keeps Enter free for newlines in a multi-line statement.
  input.addEventListener("keydown", (e) => {
    if ((e.metaKey || e.ctrlKey) && e.key === "Enter") { e.preventDefault(); run(); }
  });
  input.focus();
}

async function runSQL(sql, ui) {
  const { runBtn, cancelBtn, statusLine, results } = ui;
  if (!sql || !sql.trim()) { statusLine.textContent = "enter a SELECT statement"; return; }
  const gen = serverGen;
  sqlRunController = new AbortController();
  runBtn.disabled = true;
  cancelBtn.hidden = false;
  statusLine.textContent = "running…";
  clear(results);
  try {
    // Content-Type doubles as the cookie-auth CSRF marker; no Authorization
    // header in a cookie-bootstrapped tab (TOKEN empty), same as api().
    const headers = { "Content-Type": "application/json" };
    if (TOKEN) headers.Authorization = "Bearer " + TOKEN;
    if (currentServer) headers["X-Bintrail-Server"] = currentServer;
    const res = await fetch("/api/sql", {
      method: "POST", headers, body: JSON.stringify({ sql }), signal: sqlRunController.signal,
    });
    const text = await res.text();
    if (gen !== serverGen) return; // a server switch abandoned this query
    if (!res.ok) {
      if (res.status === 401) { handleUnauthorized(); return; }
      let msg = text || "HTTP " + res.status;
      try { const j = JSON.parse(text); if (j && j.error) msg = j.error; } catch (_) {}
      statusLine.textContent = "";
      renderError(results, new Error(msg));
      return;
    }
    const data = JSON.parse(text);
    statusLine.textContent = data.row_count + " row" + (data.row_count === 1 ? "" : "s") +
      " in " + data.elapsed_ms + " ms" + (data.truncated ? " (truncated)" : "");
    renderSQLResult(results, data);
  } catch (err) {
    if (err && err.name === "AbortError") { statusLine.textContent = "canceled"; return; }
    if (gen !== serverGen) return;
    statusLine.textContent = "";
    renderError(results, err);
  } finally {
    sqlRunController = null;
    runBtn.disabled = false;
    cancelBtn.hidden = true;
  }
}

function renderSQLResult(mount, data) {
  clear(mount);
  const cols = data.columns || [];
  const rows = data.rows || [];
  if (!rows.length) { mount.append(el("div", { class: "empty" }, el("p", { text: "No rows." }))); return; }
  const wrap = el("div", { class: "sql-table-wrap" });
  const table = el("table", { class: "statetable sql-table" });
  table.append(el("thead", {}, el("tr", {}, ...cols.map((c) => el("th", { text: c })))));
  const tbody = el("tbody");
  for (const r of rows) {
    tbody.append(el("tr", {}, ...r.map((cell) => el("td", { text: valueToString(cell) }))));
  }
  table.append(tbody);
  wrap.append(table);
  mount.append(wrap);
}

function telemetryCard(t) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Usage telemetry" }));
  if (!t || t.error) {
    card.append(el("p", { class: "form-hint", text: "Could not read telemetry state" + (t && t.error ? ": " + t.error : ".") }));
    return card;
  }
  if (!t.endpoint_set) {
    card.append(el("p", { class: "stg-hint", text: "This build sends no telemetry; no endpoint is compiled in." }));
    return card;
  }
  card.append(el("p", { class: "stg-hint", text: t.reporting
    ? "Sending metadata-only usage stats (command names, version, OS/arch, a bounded error class) to help prioritize the roadmap. Never your data, schemas, tables, DSNs, IPs, or any identifier."
    : "Not sending any usage telemetry." }));
  kvRow(card, "status", (t.reporting ? "On" : "Off") + (t.ci_detected ? " (suppressed: CI detected)" : ""));
  if (t.overridden) {
    const by = t.decided_by === "DO_NOT_TRACK" ? "the DO_NOT_TRACK environment variable"
      : t.decided_by === "BINTRAIL_TELEMETRY" ? "the BINTRAIL_TELEMETRY environment variable"
      : "the --telemetry flag";
    card.append(el("p", { class: "form-hint", text: "Set by " + by + " on the daemon, which overrides this toggle. Change it there." }));
    return card;
  }
  card.append(el("div", { class: "stg-cardfoot" },
    el("button", { class: "btn btn-sm", type: "button",
      text: t.consent ? "Turn telemetry off" : "Turn telemetry on",
      onclick: () => setTelemetry(!t.consent) })));
  return card;
}

async function setTelemetry(enabled) {
  try {
    await api("/api/telemetry", { method: "POST", body: JSON.stringify({ enabled: enabled }) });
  } catch (e) {
    toastError("Could not change telemetry: " + ((e && e.message) || e));
    return;
  }
  toast(enabled ? "Telemetry turned on." : "Telemetry turned off. This daemon stops sending now.");
  renderStorage();
}

// On Storage this card is the POINTER to Protect > Baselines, so it carries a
// link there — docs/console.md says it "links onward" and, before this, it did
// not. On the Baselines page itself the link would point at the current page,
// so the caller omits it.
function baselineSummaryCard(b, cur, opts) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Baselines" }));
  // Appended at every exit, so the link is the card's FOOT rather than sitting
  // above an error message. Every branch returns through here.
  const done = () => {
    if (opts && opts.linkOnward) {
      card.append(el("div", { class: "stg-cardfoot" },
        el("button", { class: "btn btn-sm", type: "button", text: "All snapshots \u2192",
          onclick: () => navigate("baselines") })));
    }
    return card;
  };
  if (!b || b.error) {
    card.append(el("p", { class: "form-hint", text: "Could not list baselines: " + ((b && b.error) || "unavailable") }));
    return done();
  }
  if (!b.configured) {
    kvRow(card, "source", "not configured");
    kvRow(card, "time-travel", "off");
    return done();
  }
  const snaps = b.snapshots || [];
  kvRow(card, "source", b.source);
  kvRow(card, "snapshots", String(snaps.length) + (b.truncated ? "+" : ""));
  kvRow(card, "latest (UTC)", snaps.length ? snaps[0].time : "none yet");
  if (snaps.length) kvRow(card, "age", formatAge(snaps[0].age_hours));
  kvRow(card, "time-travel", b.reconstruct ? "enabled" : "off (archives disabled)");
  return done();
}

// baselineConfigHint: the boot (cli) entry is not editable from the UI — its
// baseline comes only from --baseline-dir/--baseline-s3 (or the BINTRAIL_
// CONSOLE_BASELINE_DIR/_S3 env; BASELINE_DIR in the compose stack) — so the
// "edit the server" instruction would point it at a dead end.
function baselineConfigHint(cur, serversErr) {
  // A failed server list is not an empty one. "Add a server first" told an
  // operator who has servers to create another, and hid the real cause.
  if (serversErr) return "The server list could not be loaded (" + serversErr + "), so this cannot be checked.";
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
    el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Manage servers ›", onclick: openServersModal })));
  const list = el("div", { class: "stg-list" });
  const sources = servers.filter((s) => s.has_source);
  if (serversErr) {
    list.append(el("div", { class: "ev-empty", text: "Could not load servers: " + serversErr }));
  } else if (!sources.length) {
    list.append(el("div", { class: "ev-empty", text: "No monitored sources yet. Add one under Manage servers." }));
  } else {
    sources.forEach((s) => {
      const row = el("div", { class: "stg-row" });
      row.append(el("span", { class: "stg-name", text: s.name }));
      row.append(el("span", { class: "stg-dest" + (s.archive_s3 ? "" : " muted"), text: s.archive_s3 || "not archived: old data gets deleted, not saved" }));
      if (s.monitor_state) row.append(el("span", { class: "chip chip-mon", text: s.monitor_state.replace("_", " ").toUpperCase(), title: MON_STATE_TITLES[s.monitor_state] || ("monitoring " + s.monitor_state) }));
      row.append(el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Configure",
        onclick: () => { openServersModal(); editServer(s.id); } }));
      list.append(row);
    });
  }
  panel.append(list);
  panel.append(el("p", { class: "form-hint stg-foot", text:
    "Old data is saved to S3 before it's deleted locally, so your history isn't lost." }));
  return panel;
}

// baselineRefreshNote renders the last automatic refresh for the selected
// server.
function baselineRefreshNote(rf) {
  // finished_at/since are RFC3339 UTC on the wire; utcLabel renders them in
  // the console's labeled shape ("YYYY-MM-DD HH:MM:SS UTC", #1354).
  const when = utcLabel(rf.finished_at || rf.since || "");
  let text;
  switch (rf.state) {
    case "running":
      text = "Automatic refresh running" + (rf.since ? " since " + utcLabel(rf.since) : "") + "…";
      break;
    case "succeeded":
      text = "Automatic refresh: " + (rf.tables || 0) + " table(s) refreshed" + (when ? " at " + when : "") + ".";
      break;
    case "failed":
      text = "Automatic refresh published nothing" + (when ? " at " + when : "") +
        (rf.refused ? "; " + rf.refused + " table(s) refused" : "") +
        (rf.last_error ? ": " + rf.last_error : "") +
        " Nothing was overwritten; the next run retries.";
      break;
    default:
      return el("p", { class: "form-hint", text: "Automatic refresh is enabled; it has not run yet." });
  }
  return el("p", { class: "form-hint", text: text });
}

// snapshotTablesUniform: the per-snapshot table count, when EVERY snapshot
// has the same one — else null. A value identical in all 22 rows was the
// list's widest column saying nothing (#1415); uniform, it is a fact about
// the collection and belongs in the context strip.
function snapshotTablesUniform(snaps, truncated) {
  // A TRUNCATED listing cannot decide either way: promoting the visible
  // rows' count to "N per snapshot" claims snapshots the API never showed,
  // and suppressing the per-row column does the same in the other
  // direction — under truncation the rows keep their column.
  if (truncated || !snaps || !snaps.length) return null;
  const n = (snaps[0].tables || []).length;
  return snaps.every((sn) => (sn.tables || []).length === n) ? n : null;
}

// baselineContextStrip (#1415): one horizontal band of facts about where the
// snapshots come from and how fresh they are — a context strip, not a card —
// plus the page's one primary action. Replaces the half-width summary card
// whose eyebrow duplicated the H1 and whose 405px forced the source path to
// wrap mid-word.
function baselineContextStrip(b, cur) {
  const strip = el("section", { class: "tcard ctx-strip" });
  const item = (label, val, cls) => el("div", { class: "ctx-item" + (cls ? " " + cls : "") },
    el("span", { class: "ctx-label", text: label }),
    typeof val === "string" ? el("span", { class: "ctx-value", text: val }) : val);
  if (!b || b.error) {
    strip.append(item("BASELINES", "could not load: " + ((b && b.error) || "unavailable")));
    return strip;
  }
  if (!b.configured) {
    strip.append(item("SOURCE", "not configured"));
    strip.append(item("TIME-TRAVEL", "off"));
    return strip;
  }
  const snaps = b.snapshots || [];
  // The source path is code, and it gets the width to render as one line —
  // the dark code-ink treatment the recipe reserves for SQL/DSNs/paths.
  strip.append(item("SOURCE", el("code", { class: "code-ink ctx-source", text: b.source }), "ctx-grow"));
  strip.append(item("SNAPSHOTS", String(snaps.length) + (b.truncated ? "+" : "")));
  if (snaps.length) {
    // One fact, one place: absolute and relative side by side, instead of the
    // same freshness spelled two ways 300px apart.
    strip.append(item("LATEST", snaps[0].time + " UTC · " + formatAge(snaps[0].age_hours) + " ago"));
  }
  const uniform = snapshotTablesUniform(snaps, b.truncated);
  if (uniform !== null) strip.append(item("TABLES", uniform + " per snapshot"));
  strip.append(item("TIME-TRAVEL", b.reconstruct ? "enabled" : "off (archives disabled)"));
  // The page's primary action, at page level — not a list-header costume.
  if (capsCache.baseline_trigger && cur && cur.id && b.configured) {
    const btn = el("button", { class: "btn ctx-action", type: "button", text: "Create baseline" });
    btn.onclick = () => createBaseline(cur.id, btn);
    strip.append(btn);
  }
  return strip;
}

// opts.serversErr: when /api/servers failed, `servers` is empty for the WRONG
// reason. Without it this panel derives affirmative claims from a failure —
// the empty state advises adding a server that exists, and the `owner`
// fallback below attributes the snapshots to the daemon when the selected
// server may have its own baseline_s3.
function baselinesPanel(b, servers, opts) {
  // Full-width (#1415): this list is the page. The Create-baseline action
  // moved to the context strip — at page level it is a page action; inside
  // this header it read as scoped to the list.
  const panel = el("section", { class: "ov-panel" });
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  let owner = cur ? serverLabel(cur) : "";
  if (!owner && b && !b.error && b.configured && !(opts && opts.serversErr)) {
    owner = "daemon (--baseline-dir / --baseline-s3)";
  }
  const head = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Baseline snapshots" + (owner ? " · " + owner : "") }),
    tzChip());
  panel.append(head);
  // The daemon's periodic refresh (#1171). Shown next to the list because the
  // question it answers — "is this list going to keep moving on its own?" — is
  // only meaningful here. A failed refresh is reported plainly: it usually means
  // the fold refused (a capture gap, a schema change), which is the fail-closed
  // contract working, not a broken daemon.
  //
  // Gated on the PAYLOAD, never on capsCache.baseline_trigger: the refresh and
  // the mydumper dump are independently opt-in, so a refresh-only daemon reports
  // here with baseline_trigger false, and a capability gate would render nothing.
  if (b && !b.error && b.refresh) panel.append(baselineRefreshNote(b.refresh));
  const list = el("div", { class: "stg-list" });
  if (!b || b.error) {
    list.append(el("div", { class: "ev-empty", text: "Could not list baselines: " + ((b && b.error) || "unavailable") }));
  } else if (!b.configured) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "No baselines configured." }),
      el("p", { class: "stg-empty-sub", text: "A baseline is a full copy of your table at one point in time. With one, Time-travel can show complete rows, not just the ones that changed lately." }),
      el("p", { class: "stg-empty-sub", text: "1. Create snapshots:" }),
      el("code", { class: "stg-code", text: "docker compose --profile baseline run --rm baseline" }),
      el("p", { class: "stg-empty-sub", text: "2. " + baselineConfigHint(cur, opts && opts.serversErr) })));
  } else if (!(b.snapshots || []).length) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "Source configured, no snapshots found." }),
      el("code", { class: "stg-code", text: b.source }),
      el("p", { class: "stg-empty-sub", text: "Run bintrail dump and bintrail baseline to create your first snapshot. The path must point at the folder that contains the snapshots, not a specific file (<timestamp>/<schema>/<table>.parquet)." })));
  } else {
    // Panel headline: the newest-per-table rollup. Older snapshots being past
    // coverage is routine (superseded) — only the headline and the newest
    // row's verdict are actionable, so only those get a chip.
    if (b.staleness && b.staleness !== "ok") {
      list.append(el("div", { class: "vfy-summary" },
        el("span", { class: "chip chip-mon", text: b.staleness === "broken"
          ? "⚠ BASELINE STALE: full-table restore broken; take a fresh baseline"
          : "BASELINE " + b.staleness.toUpperCase() })));
    }
    // Row hierarchy (#1415): the newest snapshot is what Time-travel and a
    // restore actually use — it gets the treatment; the rest are history and
    // read denser. Relative age sits NEXT TO the absolute time (two facts
    // about the same instant, formerly ~650px apart), and the table count
    // appears per-row only when it VARIES — a value identical in every row
    // is a fact about the collection and lives in the context strip.
    const uniformTables = snapshotTablesUniform(b.snapshots, b.truncated);
    b.snapshots.forEach((sn, idx) => {
      const row = el("div", { class: "stg-row" + (idx === 0 ? " stg-row-latest" : "") });
      if (idx === 0) row.append(el("span", { class: "tag-pill", text: "Newest" }));
      row.append(tsSpan("stg-name mono", sn.time));
      row.append(el("span", { class: "stg-rel", text: formatAge(sn.age_hours) + " ago" }));
      row.append(el("span", { class: "stg-dest", text:
        (uniformTables === null ? (sn.tables || []).length + " table(s)" + (sn.binlog_file ? " · " : "") : "") +
        (sn.binlog_file ? sn.binlog_file + ":" + sn.binlog_pos : "") }));
      if (idx === 0 && sn.staleness && sn.staleness !== "ok") {
        row.append(el("span", { class: "chip chip-mon", text:
          sn.staleness === "broken" ? "⚠ STALE: restore broken" : sn.staleness.toUpperCase() }));
      }
      list.append(row);
    });
    if (b.truncated) list.append(el("div", { class: "ev-empty", text: "…older snapshots not shown." }));
  }
  panel.append(list);
  return panel;
}

// createBaseline triggers an in-process baseline (dump→convert→upload) on the
// daemon for the selected server, then polls until it finishes and refreshes the
// Storage view so the new snapshot appears. The button is disabled while in flight.
async function createBaseline(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Creating…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = "Create baseline"; } };
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/baseline", { method: "POST", body: {} });
  } catch (err) {
    toastError("Baseline failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  toast("Baseline started: copying your data and uploading it…");
  const done = await pollBaseline(id);
  restore();
  if (done && done.state === "succeeded") {
    toast("Baseline complete: " + (done.tables || 0) + " table(s)" +
      (done.uploaded ? ", " + done.uploaded + " file(s) uploaded" : ""));
  } else if (done) {
    toastError("Baseline failed: " + (done.last_error || "unknown error"));
  } else {
    toast("The baseline is still running. Check back shortly.");
  }
  // Only /baselines needs the refresh: the button lives in
  // baselineContextStrip (#1415 moved it out of baselinesPanel), and both the
  // strip and the snapshot list render only on this page — a /storage arm
  // here would be unreachable.
  if (location.pathname === "/baselines") renderBaselines();
}

// pollBaseline polls the per-server baseline status until it leaves "running"
// (or a ~20-minute cap). Returns the terminal status, or null if it never
// settled within the cap. Transient poll errors are ignored and retried.
async function pollBaseline(id) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (let i = 0; i < 600; i++) {
    await sleep(2000);
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/baseline")).baseline;
    } catch (_) {
      continue; // a blip mid-dump shouldn't abort the wait
    }
    if (st && st.state !== "running") return st;
  }
  return null;
}

// verifyRegions (#677, restructured #1419): trigger/poll/explain the
// recovery-chain verification engine (`bintrail verify`) for the selected
// server. Its own capability gate (verify_trigger, process-global, like
// baseline_trigger) plus a per-server precondition (verify: a baseline is
// configured; verify_live_source: a source DSN is also configured), both
// re-enforced server-side so this gating is UX only.
//
// The three regions of /verification — control,
// current run, history — as separate surfaces. The gating branches (feature
// off, no server) collapse to a single explanatory card, since with nothing
// runnable the other two regions have nothing to hold.
function verifyRegions(servers, opts) {
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));

  if (!capsCache.verify_trigger) {
    const card = el("section", { class: "tcard vfy-region" },
      el("div", { class: "stg-empty" },
        el("p", { class: "stg-empty-lead", text: "Verification from the console is turned off." }),
        el("p", { class: "stg-empty-sub", text:
          "Ask whoever manages this server to turn it on (set BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 and restart). Already on the default setup? Re-download docker-compose.yml, because \"docker compose pull\" alone does not add new settings to a file you already have." })));
    return [card];
  }
  if (!cur || !cur.id) {
    // A failed /api/servers arrives here as an empty list, and "Select a
    // server" is then an instruction to do something the operator already did.
    const card = el("section", { class: "tcard vfy-region" },
      el("div", { class: "ev-empty", text: (opts && opts.serversErr)
        ? "Could not load the server list, so verification cannot target one: " + opts.serversErr
        : "Select a server to run verification." }));
    return [card];
  }

  // ── Region 1: what you can run ──
  const control = el("section", { class: "tcard tcard-violet vfy-region vfy-control" });
  control.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Run a check" }))));
  const modeSel = el("select", { class: "select vfy-mode" },
    el("option", { value: "baseline-anchored", text: "Compare two saved snapshots (recommended)" }));
  if (capsCache.verify_live_source) {
    modeSel.append(el("option", { value: "live-source", text: "Compare against your live database (slower)" }));
  }
  modeSel.append(el("option", { value: "recover-inputs", text: "Check recovery inputs (no snapshot needed)" }));

  const results = el("div", { class: "vfy-results" });
  const btn = el("button", { class: "btn vfy-run", type: "button", text: "Run verification" });
  const configured = !!capsCache.verify;
  // The snapshot-comparison modes need a baseline location; the
  // recover-inputs check reads only the index, so it stays runnable on a
  // server with no baseline configured.
  const help = el("p", { class: "form-hint vfy-modehelp" });
  const updateMode = () => {
    btn.disabled = !configured && modeSel.value !== "recover-inputs";
    help.textContent = VFY_MODE_HELP[modeSel.value] || "";
  };
  modeSel.onchange = updateMode;
  updateMode();
  btn.onclick = () => createVerify(cur.id, modeSel.value, btn, results);
  control.append(el("div", { class: "vfy-actions" }, modeSel, btn));
  control.append(help);
  if (!configured) {
    control.append(el("p", { class: "form-hint", text:
      "No baseline set up for this server yet. The two snapshot modes need one (Manage servers → Edit → Advanced, then create at least two snapshots). \"Check recovery inputs\" works without one: it only reads the index." }));
  }

  // ── Region 2: what is running or just ran ──
  const current = el("section", { class: "tcard vfy-region vfy-current" });
  current.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "Current run" }))));
  renderVerifyResults(results, null, cur.id);
  current.append(results);
  // The per-row nouns are precise AND internal (#1419 §5) — the glossary is
  // the affordance that keeps them from requiring a source dive.
  current.append(el("details", { class: "form-advanced vfy-glossary" },
    el("summary", { class: "form-adv-summary", text: "What these words mean" }),
    el("p", { class: "form-hint", text: "Row history: every recorded change to one row, oldest to newest. The check walks each row's history in order." }),
    el("p", { class: "form-hint", text: "Before-image: each update or delete stores what the row looked like just before it. The check compares that against what the previous change left. Undo scripts are built from these images." }),
    el("p", { class: "form-hint", text: "No known earlier state: the check saw a change but held nothing older to compare it against. A longer window may reach the history it needs (CLI: verify --check recover --lookback)." }),
    el("p", { class: "form-hint", text: "Nothing to check: the table did not change, or only gained new rows. Zero comparisons is the expected result there, not a finding." })));

  // ── Region 3: what ran before ──
  const historyCard = el("section", { class: "tcard vfy-region vfy-histcard" });
  historyCard.append(el("div", { class: "vfy-region-head" },
    el("h2", { class: "ov-panel-title" }, el("span", { class: "tag-pill", text: "History" })),
    tzChip()));
  const history = el("div", { class: "vfy-history" });
  historyCard.append(history);
  loadVerifyHistory(cur.id, history);

  return [control, current, historyCard];
}

// VFY_MODE_HELP (#1418): what each mode proves, what it needs, what it costs
// — one compressed sentence set per mode, swapped under the select as the
// operator browses. Source of truth for the long form is the issue; keep
// these three claims per entry: proof, prerequisite, cost.
const VFY_MODE_HELP = {
  "baseline-anchored": "Takes your two newest snapshots, replays the recorded changes from the older one forward, and checks the result matches the newer one. Strong evidence your backup chain is sound. Needs two snapshots. Never touches your database.",
  "live-source": "Rebuilds each table from a snapshot plus the recorded changes, then compares it row by row against the real table. The strongest content check, and the only one that reads your database: it takes time, adds load, and needs a quiet table, because writes that land during the scan show up as mismatches. Run it outside busy hours.",
  "recover-inputs": "Reads the index's own record of each change and checks that every row's history holds together from one change to the next. This is the data an undo script is built from. Needs no snapshot and never touches your database.",
};

// createVerify triggers an in-process verify run on the daemon for the
// selected server, then polls until it finishes, updating resultsEl live
// after every poll tick so results appear "as they land" (#677) — the engine
// itself has no progress callback; the console's own poll loop is the only
// source of incremental updates.
async function createVerify(id, mode, btn, resultsEl) {
  if (btn) { btn.disabled = true; btn.textContent = "Running…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = "Run verification"; } };
  let status;
  try {
    status = (await api("/api/servers/" + encodeURIComponent(id) + "/verify", { method: "POST", body: { mode } })).verify;
  } catch (err) {
    toastError("Verify failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  renderVerifyResults(resultsEl, status, id);
  toast("Verification started…");
  const done = await pollVerify(id, (st) => renderVerifyResults(resultsEl, st, id));
  restore();
  // justFinished: the running→done transition gets a one-shot highlight so
  // completion is perceptible off-chip (#1420); the toast below is the other
  // half for an operator who looked away.
  if (done) renderVerifyResults(resultsEl, done, id, { justFinished: done.state === "succeeded" });
  // The finished run is now in the persisted history too — refresh the list.
  const histBox = document.querySelector(".vfy-history");
  if (histBox) loadVerifyHistory(id, histBox);
  if (done && done.state === "succeeded") {
    const s = done.summary || {};
    toast(done.note || ("Verification complete: " + vfySummaryText(s)));
  } else if (done) {
    toastError("Verification failed: " + (done.last_error || "unknown error"));
  } else {
    toast("Verification is still running. Check back shortly.");
  }
}

// pollVerify polls the per-server verify status until it leaves "running" (or
// a ~20-minute cap), invoking onTick after every poll so the caller can
// re-render mid-run progress. Returns the terminal status, or null if it
// never settled within the cap. Transient poll errors are ignored and retried.
async function pollVerify(id, onTick) {
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  for (let i = 0; i < 600; i++) {
    await sleep(2000);
    let st;
    try {
      st = (await api("/api/servers/" + encodeURIComponent(id) + "/verify")).verify;
    } catch (err) {
      // 403/404 are durable (the feature got disabled, an RBAC profile came
      // on, or the server was deleted mid-run) — the run this poll is
      // watching is gone or unreachable, so retrying for the full ~20-minute
      // cap would just leave the button stuck on "Running…". Everything else
      // (network blip, 5xx) is presumed transient and retried. 401 is already
      // handled centrally by api()'s sign-in gate.
      if (err && (err.status === 403 || err.status === 404)) {
        return { state: "failed", last_error: (err && err.message) || String(err) };
      }
      continue;
    }
    if (st && onTick) onTick(st);
    if (st && st.state !== "running") return st;
  }
  return null;
}

const VFY_STATUS_CLASS = { match: "pass", mismatch: "fail", error: "fail", inconclusive: "warn" };

// The benign inconclusive kinds (#1416): quiet or append-only tables, where
// zero assertions is the expected and PERMANENT outcome — not a finding. They
// render neutral, never amber: 20 warning-coloured rows on a healthy server is
// how operators learn to stop reading this page. The mapping is by KIND, not by
// status — an inconclusive with no kind (content modes, older runs) stays
// amber, because defaulting the unknown to benign is the direction a verify
// surface must never round.
const VFY_BENIGN_KINDS = { "no-activity": true, "nothing-to-assert": true };
function vfyCardClass(r) {
  if (r.status === "inconclusive" && VFY_BENIGN_KINDS[r.inconclusive_kind]) return "note";
  return VFY_STATUS_CLASS[r.status] || "fail";
}
const VFY_STATUS_MARK = { pass: "✓", fail: "✗", warn: "!", note: "–" };
// vfySummaryText renders the summary counts, splitting the inconclusive
// bucket when the split exists (#1416): "20 inconclusive" was unreadable when
// 18 were quiet or append-only tables. The attention-worthy number is the
// REMAINDER, so an unclassified inconclusive lands on the attention side.
function vfySummaryText(s) {
  let inc = s.inconclusive + " inconclusive";
  if (s.inconclusive_nothing_to_check > 0) {
    inc = s.inconclusive + " inconclusive (" + s.inconclusive_nothing_to_check
      + " nothing to check · " + (s.inconclusive - s.inconclusive_nothing_to_check) + " unproven)";
  }
  return s.match + " match · " + s.mismatch + " mismatch · " + inc + " · " + s.error + " error";
}

// vfyVerdictSentence answers "is my restore sound?" in words. It never claims
// more than was proven: benign inconclusives are named as not-applicable, and
// any unproven remainder is called out rather than absorbed.
function vfyVerdictSentence(s) {
  const unproven = s.inconclusive - (s.inconclusive_nothing_to_check || 0);
  if (s.mismatch > 0) return s.mismatch + " table(s) failed. Read those tables first.";
  if (s.error > 0) return "Errors stopped the check on " + s.error + " table(s).";
  const parts = [];
  if (s.match > 0) parts.push(s.match + " table(s) checked out clean");
  if (s.inconclusive_nothing_to_check > 0) parts.push(s.inconclusive_nothing_to_check + " had nothing to check (no changes in the window, or only new rows; that is normal)");
  if (unproven > 0) parts.push("the check could not prove " + unproven + " table(s); worth a look");
  if (!parts.length) return "Nothing was verified.";
  return parts.join("; ") + ".";
}

const VFY_MODE_LABEL = { "baseline-anchored": "compared two saved snapshots", "live-source": "compared against the live database", "recover-inputs": "checked recovery inputs in the index" };

// loadVerifyHistory renders the persisted run history into box: a "last
// verified" headline plus the most recent runs (newest first; the server
// stores up to 20 per server, this list shows up to 8). Manual runs,
// scheduled runs and scheduled skips all appear — the daemon's
// --verify-interval loop writes the same store. On a fetch error (including
// the 403 feature-off case) the box keeps whatever it already shows; the
// trigger UI above explains how to enable verification.
async function loadVerifyHistory(id, box) {
  let recs;
  try {
    recs = (await api("/api/servers/" + encodeURIComponent(id) + "/verify/history")).history || [];
  } catch (err) {
    return;
  }
  clear(box);
  if (!recs.length) {
    box.append(el("div", { class: "ev-empty", text: "No past runs yet." }));
    return;
  }
  const latest = recs.find((r) => r.state === "succeeded" || r.state === "failed");
  if (latest && latest.finished_at) {
    const sec = (Date.now() - Date.parse(latest.finished_at)) / 1000;
    const s = latest.summary || {};
    // chip-age, NOT chip-mon and NOT the live treatment: this is a staleness
    // age, and it used to wear the same amber as RUNNING (#1420) — a live
    // state and an old fact were indistinguishable at a glance.
    box.append(el("div", { class: "vfy-summary" },
      el("span", { class: "chip chip-age", text: "LAST VERIFIED " + agoText(sec) }),
      el("span", { class: "stg-age", text: latest.state === "failed"
        ? "failed: " + (latest.last_error || "unknown error")
        : ((s.mismatch || s.error)
          ? s.match + " match · " + s.mismatch + " mismatch · " + s.error + " error"
          : s.match + "/" + s.total + " match") })));
  }
  recs.slice(0, 8).forEach((r, i) => {
    const s = r.summary || {};
    let outcome;
    if (r.state === "skipped") outcome = "skipped: " + (r.skip_reason || "");
    else if (r.state === "failed") outcome = "failed: " + (r.last_error || "unknown error");
    else outcome = vfySummaryText(s);
    const when = utcLabel(r.finished_at || r.since || "");
    // Expandable (#1417): the per-table detail is ALREADY in this record —
    // VerifyRunRecord embeds VerifyStatus, results included — the old
    // renderer just dropped it on the floor. Disclosure, not navigation:
    // the history is short and comparing runs side by side is the point.
    const detailID = "vfy-hist-" + i;
    const row = el("button", { class: "stg-row vfy-histrow", type: "button", "aria-expanded": "false", "aria-controls": detailID });
    row.append(
      icon("caret", "ev-caret"),
      el("span", { class: "stg-name mono", text: when }),
      el("span", { text: (VFY_MODE_LABEL[r.mode] || r.mode || "") + (r.trigger === "scheduled" ? " (scheduled)" : "") }),
      el("span", { class: "stg-age", text: outcome }));
    const detail = el("div", { class: "vfy-histdetail", id: detailID, hidden: "" });
    let rendered = false;
    row.onclick = () => {
      const open = row.classList.toggle("open");
      row.setAttribute("aria-expanded", open ? "true" : "false");
      detail.hidden = !open;
      if (open && !rendered) {
        rendered = true;
        if ((r.results || []).length) {
          renderVerifyResults(detail, r, id, { history: true });
        } else {
          detail.append(el("div", { class: "ev-empty", text: "This run recorded no per-table detail" +
            (r.state === "skipped" ? "; it was skipped before any table was checked." : ".") }));
        }
      }
    };
    box.append(row, detail);
  });
}

// vfySortResults: worst verdict first (#1419 §3) — a mismatch must not sit
// at alphabetical position 31 below the fold, visually identical to the 46
// clean rows around it. Within a band the incoming order is kept
// (stable sort) — alphabetical in practice, because both result enumerators
// sort; the ORDER BY is theirs, not this function's. Display-only: the wire order is the
// engine's completion order and the summary counts are order-free.
const VFY_SORT_BAND = { warn: 2, note: 3, pass: 4 }; // fail band ranks 0/1 inline (mismatch first)
function vfySortResults(results) {
  return results.map((r, i) => [r, i]).sort((a, b) => {
    const ba = vfyCardClass(a[0]), bb = vfyCardClass(b[0]);
    // error shares the "fail" class with mismatch; keep mismatch first.
    const rank = (r, band) => band === "fail" ? (r.status === "mismatch" ? 0 : 1) : VFY_SORT_BAND[band];
    const d = rank(a[0], ba) - rank(b[0], bb);
    return d !== 0 ? d : a[1] - b[1];
  }).map((p) => p[0]);
}

// vfyCountsText: the per-table counters as a compact fixed column (#1419 §2).
// The wire carries them only for recover-inputs rows (toWireResult copies the
// walk's counters; the content modes never set them) — review caught the
// first cut reading keys the DTO did not carry at all, rendering the column
// permanently blank.
function vfyCountsText(r) {
  if (r.events_checked === undefined && r.chains_checked === undefined) return "";
  const n = (v) => Number(v || 0).toLocaleString("en-US");
  return n(r.events_checked) + " changes · " + n(r.chains_checked) + " rows";
}

// renderVerifyResults draws one run's summary + per-table rows into
// container. Used by the live poll loop (results appear as they land) AND by
// an expanded history record (#1417) — a VerifyRunRecord embeds VerifyStatus,
// so the shapes are compatible by construction.
//
// opts.history: rendering a PAST run — no Explain buttons (a new verify run
// discards the previous run's drill-down artifacts, so the button would 404
// or answer about a different run), and no "no run yet" placeholder. Also
// DERIVED from the record itself: every persisted VerifyRunRecord carries
// `trigger` ("manual"/"scheduled", no omitempty) and the live VerifyStatus
// never does, so a call site that forgets the option cannot resurrect the
// dead buttons — deriving beats threading, and the fixture cannot red-check
// the threaded half (recover-inputs rows are never explainable).
// opts.justFinished: the completion transition (#1420) — a one-shot highlight
// on the summary row so the running→done change is perceptible to someone
// not staring at the chip.
function renderVerifyResults(container, status, id, opts) {
  clear(container);
  const history = (opts && opts.history) || (status && status.trigger !== undefined);
  if (!status || status.state === "idle") {
    if (!history) {
      container.append(el("div", { class: "ev-empty", text: "No run yet. Results appear here, table by table, once you start one. Past runs sit under History." }));
    }
    return;
  }
  const running = status.state === "running";
  // RUNNING is a live state and gets a live treatment — animated, distinct
  // from every age/staleness chip on the page (#1420): the old amber
  // chip-mon was also the LAST VERIFIED treatment, so a glance could not
  // tell "in flight" from "13h old".
  const chipCls = { running: "chip chip-live", succeeded: "chip chip-done", failed: "chip chip-fail" }[status.state] || "chip chip-mon";
  const stateLabel = { running: "RUNNING", succeeded: "DONE", failed: "FAILED" }[status.state] || status.state.toUpperCase();
  const summaryRow = el("div", { class: "vfy-summary" + ((opts && opts.justFinished) ? " vfy-flash" : "") },
    el("span", { class: chipCls, text: stateLabel }));
  if (status.mode) summaryRow.append(el("span", { class: "stg-age", text: VFY_MODE_LABEL[status.mode] || status.mode }));
  const s = status.summary || {};
  const done = (status.results || []).length;
  if (running) {
    // Progress, not a tally (#1420): the engine has no planned-total, so the
    // honest number is tables completed so far — framed as progress, because
    // a partial count read as final says "20 inconclusive" about a run that
    // has not finished (sharpest for inconclusive, #1416).
    summaryRow.append(el("span", { class: "stg-age", text: done + " table(s) checked so far" }));
    if (done) summaryRow.append(el("span", { class: "stg-age vfy-sofar", text: vfySummaryText(s) + " (so far)" }));
  } else if (done) {
    summaryRow.append(el("span", { class: "stg-age", text: vfySummaryText(s) }));
  }
  container.append(summaryRow);
  if (running) {
    // Motion (#1420): the page must look like a page doing work. The strip is
    // CSS-animated behind prefers-reduced-motion, like every other motion here.
    container.append(el("div", { class: "vfy-progress" }, el("span", { class: "vfy-progress-bar" })));
  }
  // The verdict sentence (#1416): the answer to the operator's question in
  // words, so it does not have to be derived from 28 rows. Only on a FINISHED
  // run — a partial tally must not be read as a verdict (#1420).
  if (status.state === "succeeded") {
    container.append(el("p", { class: "form-hint vfy-verdict-sentence", text: vfyVerdictSentence(s) }));
  }
  if (status.note) container.append(el("p", { class: "form-hint", text: status.note }));
  if (status.last_error) container.append(el("p", { class: "form-msg err", text: status.last_error }));

  // Structured rows (#1419 §2), worst first (§3): table, verdict and counts
  // are columns the eye can scan; the detail sentence is the LAST, flexible
  // column, ellipsized with the full text a click (or hover title) away.
  const rows = el("div", { class: "vfy-rows" });
  vfySortResults(status.results || []).forEach((r) => {
    // Statuses are normalized server-side (verify.NormalizeStatus), so only
    // the four keys above can arrive; if that ever breaks, fail — never reassure.
    const cls = vfyCardClass(r);
    const row = el("div", { class: "vfy-row " + cls });
    row.append(el("span", { class: "vfy-mark", text: VFY_STATUS_MARK[cls] || "?" }));
    row.append(el("span", { class: "vfy-tbl", text: r.schema + "." + r.table }));
    const verdict = r.status === "inconclusive" && VFY_BENIGN_KINDS[r.inconclusive_kind]
      ? "nothing to check" : r.status;
    row.append(el("span", { class: "vfy-verdict", text: verdict }));
    row.append(el("span", { class: "vfy-counts", text: vfyCountsText(r) }));
    const reason = el("span", { class: "vfy-reason", text: r.reason || "", title: r.reason || "" });
    reason.onclick = () => reason.classList.toggle("wrap");
    row.append(reason);
    if (r.explainable && !history) {
      const explainBtn = el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Explain" });
      explainBtn.onclick = () => openVerifyExplain(id, r.schema, r.table, explainBtn);
      row.append(explainBtn);
    }
    rows.append(row);
  });
  container.append(rows);
}

// openVerifyExplain shows the row-level drill-down for one mismatched table,
// re-using the modal chrome showRotationDialog established. The server
// computes it in the background (#1375), so most of this is the wait: a busy
// dialog with Cancel, a ~20-minute poll, and the rules for which failures are
// worth retrying.
async function openVerifyExplain(id, schema, table, btn) {
  if (busyModalActive()) return; // one drill-down at a time (#1375)
  const gen = serverGen;
  const ctrl = new AbortController();
  const busy = openBusyModal(null, {
    title: "Working out what differs",
    errTitle: "Couldn't explain this mismatch",
    facts: [["table", schema + "." + table]],
    note: "Rebuilding this table from the older snapshot and its change log to diff it row by row: minutes on a large table. " +
      "The work continues on the server; closing this only stops the waiting. " +
      "A new verify run discards drill-downs from the previous one; Explain is unavailable until a baseline-anchored run reports this table as a mismatch again.",
    disable: btn ? [btn] : [],
    onCancel: () => ctrl.abort(),
  });

  // The server answers 202 while the reconstruction runs and 200 with the
  // drill-down once it lands (#1375), so this polls instead of holding one
  // long request open — a synchronous answer could not outlive a fronting
  // proxy's read timeout, which is what made this button look dead.
  const url = "/api/servers/" + encodeURIComponent(id) + "/verify/explain" +
    "?schema=" + encodeURIComponent(schema) + "&table=" + encodeURIComponent(table);
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  let ex;
  // Consecutive transport failures tolerated before giving up. A dropped
  // poll does not affect the daemon's job, and the whole reason this endpoint
  // went async is that long waits sit behind proxies that hiccup.
  let misses = 0;
  // ~20 minutes at 2s, the same cap and cadence pollVerify uses.
  for (let i = 0; i < 600; i++) {
    let res;
    try {
      res = await api(url, { signal: ctrl.signal });
    } catch (err) {
      if (err && err.name === "AbortError") return; // Cancel/ESC already tore the modal down
      // A 401 already raised the sign-in gate inside api() and bumped
      // serverGen. Bail on that here, not only in the success branch below:
      // a dead session never reaches the success branch, and retrying would
      // end in a red "Couldn't explain this mismatch: session expired" that
      // blames the drill-down and pulls focus off the password field.
      if (gen !== serverGen) { busy.close(); return; }
      // Only a MISSING response or a gateway status is the proxy hiccup this
      // loop tolerates. Any other status is an answer the console actually
      // received, and retrying it is worse than useless: the 500 that carries
      // the drill-down's own failure was CONSUMED by the read that produced
      // it, so the next poll starts a whole new reconstruction and answers
      // 202 — which resets the miss count. A failing drill-down would loop
      // for the full 20 minutes and never show the operator the error.
      // Durable 403/404 are terminal for the same reason pollVerify treats
      // them so. Re-clicking Explain is the retry.
      const gateway = err && (err.status === 502 || err.status === 503 || err.status === 504);
      if (err && err.status && !gateway) { busy.showError(err); return; }
      if (++misses > 5) { busy.showError(err); return; }
      await sleep(2000);
      continue;
    }
    misses = 0;
    if (gen !== serverGen) { busy.close(); return; }
    if (res && res.explain) { ex = res.explain; break; }
    // api() does not throw on a 202: it returns the {state:"running"} body,
    // which has no .explain, so the loop falls through to the sleep.
    await sleep(2000);
  }
  if (!ex) {
    // NOT showError: this loop cannot distinguish a STUCK reconstruction from
    // a merely slow one, and it may have spent its last ticks on tolerated
    // poll failures rather than 202s (misses resets on any success), so it
    // cannot even assert the daemon answered recently. Either way the red
    // "Couldn't explain this mismatch" treatment would claim a failure it has
    // not seen. Mirrors pollBaseline's neutral "check back" toast. The
    // wording promises nothing about reopening: a scheduled run may have
    // discarded the result, and the daemon log is where a repeat belongs.
    busy.close();
    toast("Still waiting after 20 minutes; the work continues on the server. Reopen Explain to try again, and check the daemon log if this repeats.");
    return;
  }
  busy.close();
  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal vfy-explain-modal", role: "dialog", "aria-label": "Verify mismatch drill-down" });
  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: ex.schema + "." + ex.table + " doesn't match" }));
  head.append(el("p", { class: "modal-desc", text:
    (ex.total === 1 ? "1 row differs" : ex.total + " rows differ") +
    "; checked against binlog position " + ex.anchor + "." }));
  head.append(el("p", { class: "modal-desc", text:
    "Recovered = what replaying the change log on top of the older snapshot produced. Baseline (real) = the actual values from the newer, trusted snapshot." }));
  head.append(el("button", { class: "modal-x", type: "button", text: "✕", onclick: closeVerifyExplain }));
  modal.append(head);

  const body = el("div", { class: "vfy-explain-body" });
  if (!ex.diffs || !ex.diffs.length) {
    body.append(el("p", { class: "form-hint", text:
      "The row count differs, but no per-row content difference was found; see the raw output below." }));
  } else {
    ex.diffs.forEach((d) => body.append(verifyDiffCard(d)));
    if (ex.total > ex.diffs.length) {
      body.append(el("p", { class: "form-hint", text:
        "…and " + (ex.total - ex.diffs.length) + " more differing row(s), not shown here." }));
    }
  }
  modal.append(body);

  const raw = el("details", { class: "form-advanced vfy-explain-raw" },
    el("summary", { class: "form-adv-summary", text: "Raw output" }));
  raw.append(el("pre", { class: "dc-rem vfy-explain-pre", text: ex.rendered }));
  modal.append(raw);

  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Close", onclick: closeVerifyExplain }));
  modal.append(foot);
  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeVerifyExplain(); });
  mount.replaceChildren(scrim);
  focusModal(scrim);
}

function closeVerifyExplain() { document.getElementById("modal").replaceChildren(); }

// openModal builds the scrim/panel/head boilerplate the console's dialogs all
// repeat, and returns the body to fill plus the close it wired.
//
// Extracted rather than copied a seventh time (#1405), and it has ONE caller
// today — which is worth stating plainly rather than dressing up as a shared
// abstraction.
//
// The six existing dialogs are not migrated, and the reason is structural, not
// caution: they append their body, footer and extras as SIBLINGS of the panel,
// while this returns a body div nested inside it. Adopting it would move their
// content one level deeper and put their .modal-foot and body rules on
// different ancestors — a DOM change to working recovery-adjacent UI, for
// tidiness, with no browser coverage of most of them to catch a layout break.
//
// What it does own is the part that is genuinely identical everywhere: the
// mount, the scrim, scrim-click dismissal, the ✕, and the focus handoff.
// Escape still comes from globalKeydown keyed off the shared #modal slot, so
// nothing here re-implements it.
function openModal(opts) {
  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal" + (opts.class ? " " + opts.class : ""),
    role: "dialog", "aria-label": opts.label });
  const close = () => {
    if (mount) mount.replaceChildren();
    if (opts.onClose) opts.onClose();
  };
  const head = el("div", { class: "modal-head" });
  if (opts.title) head.append(el("h2", { class: "modal-title", text: opts.title }));
  for (const d of opts.desc || []) head.append(el("p", { class: "modal-desc", text: d }));
  head.append(el("button", { class: "modal-x", type: "button", text: "✕", onclick: close }));
  modal.append(head);
  const body = el("div", { class: "modal-body" });
  modal.append(body);
  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) close(); });
  if (mount) mount.replaceChildren(scrim);
  focusModal(scrim);
  // The PANEL comes back alongside the body because a footer must not live
  // inside a scrolling body: a wide row's state table is taller than the
  // viewport, and an action rendered after it scrolls out of reach. Callers
  // append their own .modal-foot-ish row as a sibling, which is also how the
  // dialogs this did not absorb are built.
  return { body, panel: modal, close };
}

// focusModal moves keyboard focus into a freshly-opened dialog (#968): the
// first form field when there is one, else the first button (usually the ✕
// close). Escape-to-close lives in globalKeydown, keyed off the shared #modal
// slot — no per-dialog wiring needed.
function focusModal(scrim) {
  const f = scrim.querySelector("input, select, textarea") || scrim.querySelector("button");
  if (f) f.focus();
}

const VFY_KIND_LABEL = {
  changed: "Value differs",
  missing: "Missing from recovery",
  extra: "Unexpected in recovery",
};
const VFY_KIND_CLASS = { changed: "warn", missing: "fail", extra: "fail" };
const VFY_KIND_NOTE = {
  missing: "This row exists in the real baseline, but replaying the change log never reproduced it.",
  extra: "Replaying the change log produced this row, but it isn't in the real baseline.",
};

// verifyDiffCard renders one RowDiff, reusing the doctor-preflight card
// styling already established for verify's own match/mismatch/inconclusive
// results — same "named check + status + detail" shape.
function verifyDiffCard(d) {
  const cls = VFY_KIND_CLASS[d.kind] || "warn";
  const card = el("div", { class: "doctor-card " + cls });
  card.append(el("span", { class: "dc-mark", text: cls === "fail" ? "✗" : "!" }));
  const bodyEl = el("div", { class: "dc-body" },
    el("div", { class: "dc-name", text: (VFY_KIND_LABEL[d.kind] || d.kind) + " · " + d.pk }));
  if (VFY_KIND_NOTE[d.kind]) {
    bodyEl.append(el("p", { class: "form-hint", text: VFY_KIND_NOTE[d.kind] }));
  } else if (d.cells && d.cells.length) {
    bodyEl.append(verifyDiffCellsTable(d.cells));
  }
  card.append(bodyEl);
  return card;
}

function verifyDiffCellsTable(cells) {
  const table = el("table", { class: "vfy-diff-table" });
  table.append(el("thead", {}, el("tr", {},
    el("th", { text: "Column" }), el("th", { text: "Recovered" }), el("th", { text: "Baseline (real)" }))));
  const tbody = el("tbody");
  cells.forEach((c) => tbody.append(el("tr", {},
    el("td", { text: c.column }),
    el("td", {}, verifyDiffValue(c.recovery)),
    el("td", {}, verifyDiffValue(c.baseline)))));
  table.append(tbody);
  return table;
}

// verifyDiffValue styles a NULL cell distinctly from the literal text "NULL"
// a real value could (rarely) contain — a display-only best effort, not a
// data distinction: the underlying comparison that flagged this diff already
// happened server-side against the real NULL-vs-empty-aware bytes.
function verifyDiffValue(v) {
  if (v === "NULL") return el("span", { class: "vfy-null", text: "NULL" });
  return document.createTextNode(v);
}

// ── schemas / tables cascade ──────────────────────────────────────────────────

async function loadSchemas() {
  if (schemaCache) return schemaCache;
  const gen = serverGen;
  const data = await api("/api/schemas");
  // snapshot_only marks schemas listed via the schema snapshot with no live
  // events observed; snapshot_unavailable means the snapshot half was skipped
  // because the resolver failed to load (#1071). Both are provenance for the
  // pickers — `schemas` stays the full selectable union.
  const result = {
    schemas: data.schemas || [],
    snapshotOnly: data.snapshot_only || [],
    snapshotUnavailable: !!data.snapshot_unavailable,
  };
  // Guard the cache WRITE, not just the render: a response in flight when the
  // operator switches servers must not poison the freshly-cleared cache with
  // the previous server's schemas.
  if (gen === serverGen) schemaCache = result;
  return result;
}

async function populateSchemas(root) {
  const gen = serverGen;
  const selects = $all(".schema-select", root || document);
  if (!selects.length) return;
  let data;
  try { data = await loadSchemas(); }
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
    data.schemas.forEach((s) => {
      const snapOnly = data.snapshotOnly.includes(s);
      const o = opt(s, snapOnly ? s + " (snapshot only)" : s);
      if (snapOnly) o.title = "Listed by the schema snapshot only: no live events indexed; queries may return nothing.";
      sel.append(o);
    });
    if (data.snapshotUnavailable) {
      // Otherwise an empty (or truncated) picker is indistinguishable from a
      // healthy index with no schemas — the very ambiguity #1065 fixed.
      const note = opt("", "(schema snapshot unreadable; archive-only schemas may be missing; see server log)");
      note.disabled = true;
      sel.append(note);
    }
    if (keep) sel.value = keep;
  });
}

async function loadTables(form) {
  const gen = serverGen;
  const sel = form.querySelector(".schema-select");
  const tsel = form.querySelector(".table-select");
  // A form carries either a closed table <select> (query-style filters, where
  // "any" is meaningful) or a table-combo input+datalist (#1364, Restore —
  // where a hand-typed dropped table must still submit).
  const combo = form.querySelector(".table-combo");
  if (!tsel && !combo) return;
  const schema = sel ? sel.value : "";
  if (tsel) {
    clear(tsel);
    tsel.append(opt("", "— any —"));
  }
  const dl = combo ? document.getElementById(combo.getAttribute("list")) : null;
  const hint = combo ? combo.parentElement.querySelector(".combo-hint") : null;
  // The schema the current combo value was entered under — read BEFORE the
  // fetch so a slow listing still knows whether this is a schema SWITCH.
  // The marker only advances on a REAL selection: letting "— select —"
  // overwrite it would launder A → "— select —" → B into a first selection
  // and carry A's table silently into B.
  const prevSchema = combo ? (combo.dataset.schema || "") : "";
  if (combo && schema) combo.dataset.schema = schema;
  if (dl) clear(dl);
  if (hint) hint.textContent = "";
  if (!schema) return;
  let tables = tablesCache.get(schema);
  try {
    if (!tables) {
      if (hint) hint.textContent = "loading tables…";
      if (combo) combo.setAttribute("aria-busy", "true");
      const data = await api("/api/schemas?schema=" + encodeURIComponent(schema));
      tables = data.tables || [];
      if (gen === serverGen) tablesCache.set(schema, tables); // don't cache under a server we've since switched away from
    }
  } catch (err) {
    if (combo) combo.removeAttribute("aria-busy");
    // Persistent, announced (aria-live) failure note — the toast alone lasts
    // 2.2s. Set BEFORE the serverGen bail so no path strands "loading…".
    if (hint) hint.textContent = "couldn't load suggestions; type the table name";
    if (gen !== serverGen) return;
    if (tsel) tsel.append(opt("", "(error loading tables)"));
    // A failed listing leaves the combo as usable free text with its value
    // intact — we cannot know whether the value belongs to the new schema,
    // and a dead or emptied field would be worse than a stale suggestion.
    toastError("failed to load tables: " + ((err && err.message) || err));
    return;
  }
  if (combo) combo.removeAttribute("aria-busy");
  if (gen !== serverGen) return;
  if (hint) hint.textContent = "";
  // A newer selection may have superseded this fetch (rapid schema switching):
  // the last resolver must not repopulate under the wrong schema.
  if ((sel ? sel.value : "") !== schema) return;
  if (tsel) tables.forEach((t) => tsel.append(opt(t, t)));
  if (dl) tables.forEach((t) => dl.append(opt(t, t)));
  // Switching schema clears a stale table value that doesn't belong to the new
  // schema (#1364) — but only on a SWITCH (previous schema non-empty and
  // different): a name typed before the FIRST schema selection is the
  // dropped-table flow and must survive. A value present in the new schema's
  // own listing belongs there and is kept.
  if (combo && prevSchema && prevSchema !== schema && combo.value && !tables.includes(combo.value)) {
    combo.value = "";
  }
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

// ── Connect AI client (#1041, managed token #1052) ───────────────────────────
//
// Settings view for wiring an MCP client (Claude Desktop, claude.ai custom
// connectors, any Streamable-HTTP client) to this console's /mcp endpoint.
// Availability comes from capabilities.mcp — a static or UI-managed token is
// configured. Token VALUES are never rendered here, with one deliberate
// exception: the just-minted plaintext (mcpMintedOnce), displayed exactly
// once right after generation and never re-displayable.

// mcpMintedOnce holds a just-generated token for exactly one render of the
// Connect AI view (#1052) — consumed and cleared by renderConnect so a later
// navigation back to the view can never re-display it.
let mcpMintedOnce = null;

async function renderConnect() {
  const gen = serverGen;
  // Consume the one-time plaintext FIRST — before any await or early return —
  // so a server-switch mid-load can never leave it parked in the module
  // global to be re-displayed (stale) on a later visit.
  const minted = mcpMintedOnce;
  mcpMintedOnce = null;
  viewLoading();
  // The server list only picks between /mcp and /mcp/{id-or-name}; a failure
  // (or the registry-only 404 on an empty console) degrades to the bare
  // default-server URL instead of blanking the page.
  let servers = [];
  try { servers = (await api("/api/servers")).servers || []; } catch (_) {}
  // Token status (#1052): presence/provenance only, never a value. null on
  // failure — the card degrades to a reload hint instead of blanking the page.
  let tokStatus = null;
  try { tokStatus = await api("/api/mcp-token"); } catch (_) {}
  if (gen !== serverGen) {
    // The consumed plaintext cannot be re-shown; say so instead of losing it
    // silently (the user must rotate to get a usable value).
    if (minted) toastError("Token display interrupted; the plain token is gone. Rotate to get a fresh one");
    return;
  }
  try {
    buildConnect(servers, tokStatus, minted);
  } catch (err) {
    if (minted) toastError("Token display interrupted; the plain token is gone. Rotate to get a fresh one");
    const v = VIEW(); clear(v); v.append(pageHead("Connect AI", null)); renderError(v, err);
  }
}

// mcpSelector maps a server entry to its /mcp/{id-or-name} path selector: the
// registry display name when set (readable), the id otherwise, and the
// reserved "default" selector for the ephemeral boot (cli) entry.
function mcpSelector(entry) {
  if (!entry) return "";
  if (entry.kind === "ephemeral") return "default";
  return entry.name || entry.id || "";
}

// mcpURL builds the ready-to-copy endpoint URL from the BROWSER's origin, so
// it is correct behind a reverse proxy for the common case. With one listed
// server the bare /mcp (the console's default server) is enough; with several,
// the per-server form pins the server currently selected in the sidebar.
function mcpURL(servers) {
  let path = "/mcp";
  if ((servers || []).length > 1) {
    const cur = servers.find((s) => s.id === (currentServer || defaultServerId));
    const sel = mcpSelector(cur);
    if (sel) path += "/" + encodeURIComponent(sel);
  }
  return location.origin + path;
}

function copyText(text, what) {
  navigator.clipboard.writeText(text).then(() => toast(what + " copied to clipboard"), () => toastError("Copy failed."));
}

function buildConnect(servers, tokStatus, minted) {
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Let an AI assistant query this console: the same read-only tools and result caps as this UI. ",
    el("b", { text: "A token is only ever shown once, at the moment you generate it." }));
  v.append(pageHead("Connect AI", sub));

  const cards = el("div", { class: "cards" });
  cards.append(mcpTokenCard(tokStatus, minted));
  cards.append(mcpEndpointCard(servers));
  cards.append(bundleCard());
  v.append(cards);
  if (capsCache.mcp) v.append(otherClientsPanel(servers));
  viewEnter();
}

// mintMCPToken generates (or rotates) the managed token, refreshes the
// capability gate (the endpoint may have just become usable), and re-renders
// the view with the plaintext displayed once.
async function mintMCPToken(rotate) {
  if (rotate && !window.confirm("Rotate the MCP token? The current value stops working immediately and every connected AI client will need the new one.")) return;
  // Only the mutation itself may report failure — once the POST succeeded the
  // token EXISTS (and on rotate the old one is already dead), so a failing
  // follow-up refresh must never toast "generation failed".
  let res;
  try {
    res = await api("/api/mcp-token", { method: "POST" });
  } catch (err) {
    toastError("Token generation failed: " + (err.message || err));
    return;
  }
  mcpMintedOnce = (res && res.token) || null;
  try { await gateCapabilities(); } catch (_) {} // 401 already raised the sign-in gate
  renderConnect();
}

async function revokeMCPToken() {
  if (!window.confirm("Revoke the managed MCP token? Connected AI clients stop working immediately.")) return;
  try {
    await api("/api/mcp-token", { method: "DELETE" });
  } catch (err) {
    toastError("Revoke failed: " + (err.message || err));
    return;
  }
  toast("Managed MCP token revoked");
  try { await gateCapabilities(); } catch (_) {}
  renderConnect();
}

// mcpTokenCard (#1052): generate, rotate, and revoke the managed MCP token
// without leaving the UI. The token value renders exactly once — right after
// generation — and is otherwise represented only by its creation date.
function mcpTokenCard(tok, minted) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Access token" }));
  // The one-time plaintext renders UNCONDITIONALLY — a failed status fetch
  // must never swallow a token that was just minted (after a rotate, it is
  // the only valid credential and cannot be re-displayed).
  if (minted) {
    card.append(el("p", { class: "stg-hint", text: "Token generated. Copy it now; it is not stored and will never show again:" }));
    card.append(el("div", { class: "cn-urlrow" },
      el("code", { class: "stg-code cn-url", text: minted }),
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(minted, "MCP token") })));
  }
  if (!tok) {
    card.append(el("p", { class: "stg-hint", text: "Token status unavailable. Reload the page to retry." }));
    return card;
  }
  if (tok.managed) {
    if (!minted) {
      card.append(el("p", { class: "stg-hint", text:
        "A managed token is active" + (tok.created_at ? " (created " + utcLabel(tok.created_at) + ")" : "") +
        ". Its value is not stored and cannot be re-displayed; rotate to get a fresh one." }));
    }
    if (tok.read_only) {
      card.append(el("p", { class: "form-hint", text:
        "The token file was written by a newer bintrail; the token works, but rotate/revoke are unavailable from this build." }));
    } else {
      card.append(el("div", { class: "cn-links" },
        el("button", { class: "btn btn-sm", type: "button", text: "Rotate token", onclick: () => mintMCPToken(true) }),
        el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Revoke", onclick: revokeMCPToken })));
    }
  } else if (!minted) {
    card.append(el("p", { class: "stg-hint", text:
      "AI clients authenticate with a token (password login is a browser credential and cannot be used by them). Generate one here: no flags, no environment variables, no restart. The token only grants the read-only MCP tools; it cannot manage this console." }));
    card.append(el("div", { class: "cn-links" },
      el("button", { class: "btn btn-sm", type: "button", text: "Generate token", onclick: () => mintMCPToken(false) })));
  }
  if (tok.static) {
    card.append(el("p", { class: "form-hint", text:
      "A static token from --token / BINTRAIL_CONSOLE_TOKEN is also configured. It keeps working, but it is environment-owned and cannot be managed here." }));
  }
  return card;
}

// mcpEndpointCard: the ready-to-copy URL when the endpoint is usable, or the
// how-to-enable explanation when no token is configured (capabilities.mcp) —
// never a URL presented as ready that would only ever answer 403.
function mcpEndpointCard(servers) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "MCP endpoint" }));
  if (!capsCache.mcp) {
    card.append(el("p", { class: "stg-hint", text:
      "MCP is not available yet: this console has no access token configured. Generate one in the Access token card above and this card becomes a ready-to-copy URL." }));
    return card;
  }
  const url = mcpURL(servers);
  card.append(el("div", { class: "cn-urlrow" },
    el("code", { class: "stg-code cn-url", text: url }),
    el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(url, "MCP URL") })));
  if ((servers || []).length > 1) {
    card.append(el("p", { class: "form-hint", text:
      "This URL targets the server selected in the sidebar; the bare /mcp targets the console's default server. Switch servers to get each one's URL." }));
  }
  card.append(el("p", { class: "form-hint", text:
    "The credential is the console access token from the card above (or --token / BINTRAIL_CONSOLE_TOKEN), sent as an Authorization: Bearer header." }));
  return card;
}

// bundleCard links the .mcpb bundle (one-click Claude Desktop install) for the
// RUNNING version. Best-effort: a release that predates the bundle artifact
// 404s the direct link, so a releases-page path is always offered too.
function bundleCard() {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Claude Desktop bundle" }));
  const ver = String(capsCache.version || "").replace(/^v/, "");
  const released = /^\d+\.\d+\.\d+$/.test(ver);
  card.append(el("p", { class: "stg-hint", text:
    "Install the bundle in Claude Desktop (double-click), then paste the URL above and your console token: no config files to edit." }));
  if (released) {
    const asset = "dbtrail-" + guessPlatform() + ".mcpb";
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases/download/v" + ver + "/" + asset, text: "Download " + asset }),
      el("a", { class: "btn btn-sm btn-ghost", href: "https://github.com/dbtrail/dbtrail/releases/tag/v" + ver, target: "_blank", rel: "noopener", text: "All downloads for v" + ver })));
    card.append(el("p", { class: "form-hint", text:
      "The link matches this console's version (v" + ver + "). Older releases don't carry the bundle; if the download 404s, pick a newer release from the releases page." }));
  } else {
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases", target: "_blank", rel: "noopener", text: "Open the releases page" })));
    card.append(el("p", { class: "form-hint", text:
      "This console is an unversioned build, so no exact bundle link can be derived; pick the bundle matching your platform from the latest release." }));
  }
  return card;
}

// guessPlatform maps the browser's environment to a release-artifact os-arch
// pair. Best-effort presentation only (Apple Silicon is assumed on macOS); the
// all-downloads link covers every other combination.
function guessPlatform() {
  const ua = navigator.userAgent || "";
  if (/Windows/i.test(ua)) return "windows-amd64";
  if (/Mac/i.test(ua)) return "darwin-arm64";
  return "linux-amd64";
}

// otherClientsPanel: collapsed raw-config fallback for MCP clients that don't
// install .mcpb bundles. The snippet carries a PLACEHOLDER for the token — the
// real value is never rendered.
function otherClientsPanel(servers) {
  const url = mcpURL(servers);
  const snippet = JSON.stringify({
    mcpServers: {
      dbtrail: { command: "bintrail-mcp", args: ["--connect", url, "--token", "YOUR_CONSOLE_TOKEN"] },
    },
  }, null, 2);
  const panel = el("section", { class: "ov-panel cn-other", style: "margin-top:18px" });
  const adv = el("details", { class: "form-advanced", style: "margin-top:0" },
    el("summary", { class: "form-adv-summary", text: "Other clients (raw config)" }));
  adv.append(el("p", { class: "form-hint", text:
    "For claude_desktop_config.json, or any client that launches stdio MCP servers: bintrail-mcp bridges stdio to this console. Replace the placeholder with your console token:" }));
  adv.append(el("pre", { class: "stg-code cn-snippet", text: snippet }));
  adv.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy snippet", onclick: () => copyText(snippet, "Config snippet") }));
  adv.append(el("p", { class: "form-hint", text:
    "If this console is reachable over public HTTPS, the same URL also works directly as a claude.ai custom connector; no bridge needed." }));
  panel.append(adv);
  return panel;
}

// ── capabilities gating ────────────────────────────────────────────────────

async function gateCapabilities() {
  const gen = serverGen;
  let caps = {};
  // capsOK distinguishes "the server reported no version" from "we could not
  // ask" — the version row must never claim a build we failed to read (#1221).
  // It tracks a payload we actually READ, not merely a call that did not throw:
  // api() returns null for an empty body (a legitimate 204 elsewhere), which
  // would otherwise land here as a successful read of nothing.
  let capsOK = false;
  // Degrading to {} hides capability-gated UI (Time-travel tab, the source
  // section of the server form) — warn so a wrongly-shaped UI is diagnosable.
  // A 401 is NOT capability loss: rethrow so session expiry surfaces as the
  // sign-in gate (api() already raised it), never as silently vanished tabs.
  try { caps = await api("/api/capabilities"); capsOK = !!caps; } catch (err) {
    if (err && err.status === 401) throw err;
    console.warn("capabilities check failed; UI degrades to no-capability gating", err);
    caps = {};
  }
  if (gen !== serverGen) return;
  capsCache = caps || {};
  // Extension views advertised for this server (embedding builds; empty in the
  // stock binary and under any active profile — the backend omits them there).
  // Rebuild the nav before the route renders so a deep-linked ext route resolves.
  extViews = Array.isArray(capsCache.extension_views) ? capsCache.extension_views : [];
  extSettings = Array.isArray(capsCache.extension_settings) ? capsCache.extension_settings : [];
  syncExtNav();
  syncExtSettingsNav();
  $all("[data-capability]").forEach((node) => node.classList.toggle("cap-on", !!capsCache[node.dataset.capability]));
  gatePermissions();
  applyAuthGate();
  updateSrvNote(); // capsCache.monitor may have just changed
  updateSideVersion(capsOK);
}

// updateSideVersion paints the running build into the sidebar footer (#1221),
// reading the capabilities payload already fetched above — no extra request.
// Two producers, both of which must survive without emitting "vundefined" or
// "vdev": a plain `go build` sends the LITERAL "dev" (cmd/bintrail-console
// defaults Version to it and consoleapp.Main passes it through unexamined),
// while `version` is also `omitempty`, so a Config with an empty Version — an
// embedder handing consoleapp.Main "" — sends no key at all.
// `known` is false when the capabilities fetch itself failed — the row keeps
// its "—" placeholder there instead of reporting "dev" for a build whose real
// version we never read (a wrong version in a bug report is worse than none).
// The leading-"v" strip is CLASSIFICATION, not formatting: it has no effect on
// the rendered string (a "v1.2.3" that skips the released branch is echoed
// verbatim by the fallback and reads the same), but it keeps a tag-shaped
// -ldflags value on the same side of the released/unreleased split that
// bundleCard() puts it on. That sibling anchors its regex and this one does
// not, deliberately: bundleCard derives a download URL that has to resolve, so
// it rejects "1.2.3-rc1"; this row only echoes what the server reported.
function updateSideVersion(known) {
  const b = $("#meta-version b");
  if (!b) return;
  const ver = String(capsCache.version || "").replace(/^v/, "");
  if (!known) b.textContent = "—";
  else b.textContent = /^\d+\.\d+\.\d+/.test(ver) ? "v" + ver : (ver || "dev");
}

// gatePermissions hides a [data-perm] surface when the session's policy denies
// that permission (per-session RBAC, #1074). Default is VISIBLE: a policy-less
// session — the static token, the password login, every OSS session — reports
// every permission true, so nothing is hidden and the UI is unchanged. A missing
// permissions map (a degraded {} capabilities response) also leaves everything
// visible: only an explicit `false` hides. The server's 403 is the real gate;
// this just spares a scoped user a tab that would only error.
function gatePermissions() {
  const perms = capsCache.permissions || {};
  $all("[data-perm]").forEach((node) => node.classList.toggle("perm-off", perms[node.dataset.perm] === false));
}

// syncExtNav rebuilds the extension-view nav group from extViews. Idempotent
// across server switches: it drops the previously-injected group first, so a
// server that exposes fewer (or no) extension views cannot leave stale items
// behind. The group is anchored right after the built-in Monitor group (the one
// holding Status). el()/textContent only — a view label is never markup.
function syncExtNav() {
  const prev = document.getElementById("ext-nav-group");
  if (prev) prev.remove();
  if (!extViews.length) return;
  const nav = document.getElementById("nav");
  if (!nav) return;
  const group = el("div", { class: "nav-group", id: "ext-nav-group", "data-ext-nav": "1" });
  group.append(el("div", { class: "nav-label", text: "Extensions" }));
  for (const v of extViews) {
    const route = "ext-" + v.id;
    const item = el("a", {
      class: "nav-item",
      "data-ext-nav": "1",
      "data-route": route,
      href: "/" + route,
      onclick: (e) => { e.preventDefault(); pendingRecover = null; navigate(route); },
    }, icon("ext", "ni-icon"), el("span", { text: v.label }));
    group.append(item);
  }
  const statusItem = $('.nav-item[data-route="status"]');
  const anchor = statusItem ? statusItem.closest(".nav-group") : null;
  if (anchor && anchor.parentNode) anchor.parentNode.insertBefore(group, anchor.nextSibling);
  else nav.append(group);
}

// renderExtensionView loads a provider's ES module and hands it a mount node and
// a small context: apiBase ("/api/ext/<id>/") and the console's own authed fetch
// primitive (api), so the module reads its data routes with the operator's
// bearer credential and selected-server header already applied. serverGen is
// captured before the dynamic import and re-checked after: a server switch
// mid-import abandons the render (no cross-server paint), matching every other
// async view here.
// syncExtSettingsNav rebuilds the extension settings-panel nav items inside the
// existing Settings group (not a group of their own: a panel administers the
// console, so it belongs where Connect AI and Storage already are). Idempotent —
// the previously injected items are removed first, so a capabilities refresh
// after a server switch or a re-login never accumulates duplicates.
function syncExtSettingsNav() {
  $all("[data-extset-nav]").forEach((n) => n.remove());
  if (!extSettings.length) return;
  const anchor = $('.nav-item[data-route="connect"]');
  const group = anchor ? anchor.closest(".nav-group") : null;
  if (!group) return;
  for (const p of extSettings) {
    const route = "extset-" + p.id;
    group.append(el("a", {
      class: "nav-item",
      "data-extset-nav": "1",
      "data-route": route,
      // settings:read is the visibility floor server-side, so the item hides
      // under a session that lacks it — the same data-perm contract every
      // built-in nav item uses. Without this the link stays lit for a scoped
      // operator and the panel's first request answers 403. syncExtSettingsNav
      // runs immediately before gatePermissions(), so these nodes are in the
      // DOM when the sweep reads [data-perm].
      "data-perm": "settings:read",
      href: "/" + route,
      onclick: (e) => { e.preventDefault(); pendingRecover = null; navigate(route); },
    }, icon("ext", "ni-icon"), el("span", { text: p.label })));
  }
}

// extContract is what an extension module receives. Both extension surfaces —
// the settings panel and the full view — build it here, so the two cannot
// drift into different contracts for the same kind of consumer.
//
// `apiBase` and `api` are the data plane. `ui` is the small set of console
// widgets an extension should NOT be reimplementing: a second copy drifts, and
// the operator ends up looking at two different date pickers in one console.
//
// Handed over explicitly rather than left to be discovered. app.js is a classic
// script, so its function declarations are reachable as window globals and an
// extension COULD simply call one — which is exactly the coupling to avoid.
// The two sides are built in different repos on different release cadences and
// never compile together, so a rename here would break the extension with no
// error at all: the widget would just stop appearing. What is in `ui` is a
// promise the console keeps; what is not is an internal that may move.
//
// dateField is fieldDateInput itself rather than a wrapper, so a rename cannot
// leave the two spellings pointing at different builders.
function extContract(apiBase) {
  return { apiBase, api, ui: { dateField: fieldDateInput } };
}


// renderExtensionSettings mounts a settings panel's ES module. Same contract as
// renderExtensionView, with the panel's own data prefix — the two surfaces are
// authorized differently server-side (settings:read/write vs extview:read), so
// handing a panel the view's apiBase would send its requests through the wrong
// gate and 403.
async function renderExtensionSettings(panel) {
  const gen = serverGen;
  const v = VIEW();
  clear(v);
  v.append(pageHead(panel.label, null));
  const mount = el("div", { class: "ext-view-mount" });
  v.append(mount);
  try {
    const mod = await import(panel.script);
    if (gen !== serverGen) return;
    if (!mod || typeof mod.render !== "function") {
      renderError(mount, "This settings panel did not export a render() function.");
      return;
    }
    mod.render(mount, extContract("/api/ext-settings/" + panel.id + "/"));
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(mount, err);
  }
}

async function renderExtensionView(view) {
  const gen = serverGen;
  const v = VIEW();
  clear(v);
  v.append(pageHead(view.label, null));
  const mount = el("div", { class: "ext-view-mount" });
  v.append(mount);
  try {
    const mod = await import(view.script);
    if (gen !== serverGen) return;
    if (!mod || typeof mod.render !== "function") {
      renderError(mount, "This extension view did not export a render() function.");
      return;
    }
    mod.render(mount, extContract("/api/ext/" + view.id + "/"));
  } catch (err) {
    if (gen !== serverGen) return;
    renderError(mount, err);
  }
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
        const o = opt(s.id, serverLabel(s) + (s.flavor && s.flavor !== "mysql" ? " · " + (s.flavor === "postgres" ? "PG" : "MariaDB") : ""));
        if (s.kind === "ephemeral") o.title = "The daemon's own index database (set with --index-dsn on the command line)";
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
  // can never be auto-applied against a different server's index, and abort any
  // in-flight panel query (it targeted the old server, and holds its latch).
  pendingRecover = null;
  lastSQL = "";
  abortSQLRun();
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
    "The servers you're monitoring with dbtrail, saved in a file on this machine. ");
  desc.append(el("span", { "data-capability": "monitor" },
    "This process can also ", el("b", { text: "monitor" }),
    " a new MySQL database for you: add one below and dbtrail checks it's ready, sets up its index, and starts capturing changes; no terminal needed."));
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
  focusModal(mount);
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
  catch (err) { toastError("Could not load rotation settings: " + ((err && err.message) || err)); return; }

  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal", role: "dialog", "aria-label": "Rotation" });

  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: "Rotation" }));
  head.append(el("p", { class: "modal-desc", text:
    "On a regular schedule, the daemon deletes indexed data older than the retention period below, and gets ready ahead of time for new data coming in. One schedule applies to every server being monitored; changes take effect on the next run." }));
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
  if (!cur.enabled) note.textContent = "Rotation is turned off. Your changes will be saved but won't take effect until the daemon restarts.";
  else if (cur.source === "default") note.textContent = "Currently using the daemon's built-in defaults. Saving here creates a custom setting that takes effect immediately.";
  else note.textContent = "A custom setting is active and takes effect immediately.";
  form.append(note);

  const msg = el("div", { class: "form-msg" });
  const foot = el("div", { class: "modal-foot" });
  foot.append(el("button", { class: "btn btn-primary", type: "submit", text: "Save" }));
  foot.append(el("button", { class: "btn btn-ghost", type: "button", text: "Cancel", onclick: closeRotationDialog }));
  form.append(foot);
  form.append(msg);
  form.addEventListener("submit", (e) => { e.preventDefault(); submitRotation(form, msg, cur); });
  modal.append(form);

  scrim.append(modal);
  scrim.addEventListener("click", (e) => { if (e.target === scrim) closeRotationDialog(); });
  mount.replaceChildren(scrim);
  focusModal(scrim);
}

function closeRotationDialog() { document.getElementById("modal").replaceChildren(); }

async function submitRotation(form, msg, cur) {
  // Future partitions (#969): blank keeps the current value — the field came
  // prefilled, so an accidental clear must not silently save 0. Non-integer
  // input is rejected inline (mirroring the server's retain/interval 400s);
  // an explicit "0" stays valid (external partition management).
  const rawFuture = form.elements.add_future.value.trim();
  let addFuture;
  if (rawFuture === "") {
    addFuture = cur.add_future != null ? cur.add_future : 0;
  } else if (/^\d+$/.test(rawFuture)) {
    addFuture = parseInt(rawFuture, 10);
  } else {
    msg.textContent = "Future partitions must be a whole number (e.g. 3).";
    msg.className = "form-msg err";
    return;
  }
  const body = {
    retain: form.elements.retain.value.trim(),
    interval: form.elements.interval.value.trim(),
    add_future: addFuture,
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
  toast(cur.enabled ? "Rotation settings saved" : "Saved. Rotation is off, so this takes effect when the daemon restarts");
}

async function refreshServersList() {
  const list = document.getElementById("servers-list");
  if (!list) return;
  let servers;
  try { servers = await loadServers(); }
  catch (err) { renderError(list, err); return; }
  clear(list);
  if (!servers.length) { list.append(el("div", { class: "ev-empty", text: "No servers yet. Add your first connection." })); return; }
  servers.forEach((s) => list.append(serverRow(s)));
}

function isLiveMonitorState(st) { return st === "running" || st === "pending" || st === "stalled" || st === "lost_position"; }

function serverRow(s) {
  const item = el("div", { class: "srv-item" });
  const nm = el("span", { class: "nm" },
    el("span", { class: "health-dot" + (s.connected ? " ok" : ""), title: s.connected ? "connected" : "not connected yet" }),
    serverLabel(s));
  item.append(nm);
  if (s.kind === "ephemeral") item.append(el("span", { class: "chip chip-cli", text: "CLI", title: "Set from the command line with --index-dsn" }));
  if (s.reconstruct) item.append(el("span", { class: "chip chip-tt", text: "TT", title: "Baseline configured: Time-travel available" }));
  if (s.monitor_state) item.append(el("span", { class: "chip chip-mon", text: s.monitor_state.replace("_", " ").toUpperCase(), title: MON_STATE_TITLES[s.monitor_state] || ("monitoring " + s.monitor_state) }));
  if (s.flavor && s.flavor !== "mysql") item.append(el("span", { class: "chip", text: s.flavor === "postgres" ? "PG" : s.flavor.toUpperCase(), title: "Source type: " + s.flavor }));

  let desc;
  if (s.has_source && s.source_host) desc = "watching " + s.source_user + "@" + s.source_host + ":" + (s.source_port || (s.flavor === "postgres" ? "5432" : "3306")) + (s.source_database ? "/" + s.source_database : "") + (s.schemas ? " [" + s.schemas + "]" : "");
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

// tagFlavor marks a form node visible only for the given space-separated source
// families (e.g. "postgres" or "mysql mariadb"); applyFlavor toggles .flavor-on.
function tagFlavor(node, families) { node.setAttribute("data-flavor", families); return node; }

// applyFlavor reveals the [data-flavor] nodes matching the selected source
// family, mirroring the capability cap-on gating EXACTLY — a class toggle over a
// CSS :not() default-hide, never a [hidden]/style="display:none" toggle (the
// documented display-bug class this codebase already hit).
function applyFlavor(form) {
  const f = (form.elements.flavor && form.elements.flavor.value) || "mysql";
  $all("[data-flavor]", form).forEach((n) =>
    n.classList.toggle("flavor-on", n.dataset.flavor.split(" ").includes(f)));
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
  mon.append(el("legend", { class: "form-legend", text: "Monitor a source database" }));
  mon.append(el("p", { class: "form-hint", text: "Paste the server you want to watch. dbtrail checks that it is ready, creates an index database for it, and starts capturing changes. Nothing else to fill in beyond a name." }));
  const monGrid = el("div", { class: "form-grid" });
  // Source family selector — reveals the PostgreSQL-only fields below.
  monGrid.append(el("label", { class: "field" },
    el("span", { class: "field-label", text: "Source type" }),
    el("select", { class: "input", name: "flavor" },
      opt("mysql", "MySQL"), opt("postgres", "PostgreSQL"), opt("mariadb", "MariaDB"))));
  monGrid.append(srvField("Source host", "source_host", { placeholder: "db.example.com" }));
  monGrid.append(srvField("Source port", "source_port", { placeholder: "3306" }));
  monGrid.append(srvField("Source user", "source_user", { placeholder: "repl" }));
  monGrid.append(srvField("Source password", "source_password", { type: "password", autocomplete: "new-password" }));
  // PostgreSQL-only: a logical-replication connection is per-database, and the
  // slot/publication are operator-created (validate-don't-create).
  monGrid.append(tagFlavor(srvField("Database", "source_database", { placeholder: "appdb" }), "postgres"));
  monGrid.append(tagFlavor(srvField("Replication slot", "source_slot", { placeholder: "bintrail_slot" }), "postgres"));
  monGrid.append(tagFlavor(srvField("Publication", "source_publication", { placeholder: "bintrail_pub" }), "postgres"));
  monGrid.append(srvField("Schemas", "schemas", { placeholder: "(optional) shop,billing" }));
  monGrid.append(srvField("Archive to S3", "archive_s3", { placeholder: "(optional) s3://bucket/prefix/" }));
  mon.append(monGrid);
  // The source user is the #1 friction point — spell out the grant inline,
  // never behind a <details>. REPLICATION SLAVE/CLIENT drive the stream;
  // SELECT covers the information_schema snapshot of columns/PKs/FKs.
  //
  // LOCK TABLES is listed here too, commented, because omitting it is a
  // DELAYED failure: capture starts clean and only Create baseline refuses,
  // hours or days later. The form is the last place anyone reads a grant list
  // before pasting it, so the line has to exist here even though the stream
  // does not need it.
  const grantHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "mysql mariadb");
  grantHint.append("Source user needs ");
  grantHint.append(el("code", { text: "REPLICATION SLAVE, REPLICATION CLIENT, SELECT" }));
  grantHint.append(" to capture, plus ");
  grantHint.append(el("code", { text: "LOCK TABLES" }));
  grantHint.append(" if you want baselines. Create one on the source MySQL; copy and run:");
  mon.append(grantHint);
  mon.append(tagFlavor(el("pre", { class: "form-code", text:
    "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';\n" +
    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';\n" +
    "-- Baselines only (point-consistent by default). On RDS/Aurora also set\n" +
    "-- BINTRAIL_CONSOLE_BASELINE_LOCK_MODE=lock-all.\n" +
    "GRANT LOCK TABLES ON *.* TO 'dbtrail'@'%';" }), "mysql mariadb"));
  // PostgreSQL prerequisites — the console reads them, it never runs CREATE
  // PUBLICATION / ALTER SYSTEM (validate-don't-create; capture is pgoutput-only).
  const pgHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "postgres");
  pgHint.append("PostgreSQL source needs ");
  pgHint.append(el("code", { text: "wal_level=logical" }));
  pgHint.append(", a role with the ");
  pgHint.append(el("code", { text: "REPLICATION" }));
  pgHint.append(" attribute, a publication you create, and ");
  pgHint.append(el("code", { text: "REPLICA IDENTITY FULL" }));
  pgHint.append(" on replicated tables; copy and run on the source:");
  mon.append(pgHint);
  mon.append(tagFlavor(el("pre", { class: "form-code", text:
    "CREATE PUBLICATION bintrail_pub FOR ALL TABLES;\n" +
    "ALTER TABLE your_table REPLICA IDENTITY FULL;" }), "postgres"));
  mon.append(el("p", { class: "form-hint", style: "margin-top:10px", text: "Archive to S3: old data is uploaded here before it's deleted locally, so your history is kept and can still be searched. Needs AWS credentials set up on the daemon (environment variables or an IAM role)." }));
  form.append(mon);

  // BYO index is the advanced path — collapsed behind a <details> so the
  // monitor-first form stays one field + source. Open/close rules live in
  // showServerForm.
  const adv = el("details", { class: "form-advanced", id: "server-advanced" });
  adv.append(el("summary", { class: "form-adv-summary", text: "Advanced: bring your own index (optional)" }));
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
    el("input", { type: "checkbox", name: "no_archive" }), el("span", { text: "Don't automatically include archived data in queries" })));
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
  form.elements.flavor.addEventListener("change", () => applyFlavor(form));

  // Where the index connection is the whole form (serve-only process: no
  // monitor capability), or the entry being edited carries index fields,
  // the "advanced" block must start expanded — and in serve-only mode
  // there is nothing to collapse it back to, so the toggle hides. Note a
  // monitor-first save also ends with index fields (the derived index DSN
  // round-trips as host/dbname in the DTO), so "collapsed by default"
  // means a fresh add; editing any saved entry shows its index.
  const adv = $("#server-advanced", form);
  const hasIndexFields = !!(prefill && (prefill.host || prefill.dbname || prefill.baseline_dir || prefill.baseline_s3 || prefill.no_archive));
  // Keep "bring your own index (optional)" COLLAPSED for a monitored source:
  // its index DSN is auto-derived and round-trips as host/dbname, so expanding
  // it — e.g. when a failed-preflight error card opens the form — would show
  // the operator a per-source index they never typed. Open it only for a pure
  // BYO-index entry (no source) or a serve-only process where the index is the
  // whole form.
  const byoIndex = hasIndexFields && !(prefill && prefill.has_source);
  adv.open = !capsCache.monitor || byoIndex;
  $(".form-adv-summary", adv).hidden = !capsCache.monitor;

  if (prefill) {
    form.elements.id.value = prefill.id || "";
    ["name", "host", "port", "user", "dbname", "baseline_dir", "baseline_s3", "archive_s3", "source_host", "source_port", "source_user", "schemas", "source_database", "source_slot", "source_publication"].forEach((k) => {
      if (form.elements[k] && prefill[k] != null) form.elements[k].value = prefill[k];
    });
    if (form.elements.no_archive) form.elements.no_archive.checked = !!prefill.no_archive;
    form.elements.password.placeholder = prefill.has_password ? "(unchanged; leave blank to keep)" : "(none)";
    form.elements.source_password.placeholder = prefill.has_source_password ? "(unchanged; leave blank to keep)" : "";
  }
  // Flavor init runs for both add and edit; it's immutable after create (the
  // backend rejects a change on PUT), so disable the selector when editing.
  form.elements.flavor.value = (prefill && prefill.flavor) || "mysql";
  if (prefill && prefill.id) form.elements.flavor.disabled = true;
  applyFlavor(form);
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
    flavor: f.flavor.value,
    host: f.host.value.trim(), port: f.port.value.trim(), user: f.user.value.trim(), dbname: f.dbname.value.trim(),
    baseline_dir: f.baseline_dir.value.trim(), baseline_s3: f.baseline_s3.value.trim(),
    no_archive: !!f.no_archive.checked,
    archive_s3: f.archive_s3.value.trim(),
    source_host: f.source_host.value.trim(), source_port: f.source_port.value.trim(),
    source_user: f.source_user.value.trim(), schemas: f.schemas.value.trim(),
    source_database: f.source_database.value.trim(),
    source_slot: f.source_slot.value.trim(), source_publication: f.source_publication.value.trim(),
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
  catch (err) { toastError("Could not load server: " + ((err && err.message) || err)); return false; }
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
    formMsg("Running startup checks…", false);
    const res = await startMonitor(saved.id);
    await refreshServersList();
    if (res && res.started && !doctorWarnings(res.doctor)) { hideServerForm(); toast("Monitoring started. Events will appear within a minute"); }
    else if (res && res.started) { renderDoctor(res.doctor); formMsg("Monitoring started; review the warnings below", false); }
    else if (res) { renderDoctor(res.doctor); formMsg("Startup checks failed: fix the items below and save again", true); }
    else { formMsg("Could not start monitoring; check the notification for details and try again", true); } // startMonitor returned null (transport error)
    return;
  }
  hideServerForm();
  await refreshServersList();
  toast(id ? "Server updated" : "Server added");
}

async function deleteServer(s) {
  if (!window.confirm('Remove server "' + s.name + '"? This only removes the saved connection; nothing happens to the server itself.')) return;
  try { await api("/api/servers/" + encodeURIComponent(s.id), { method: "DELETE" }); }
  catch (err) { toastError("Could not remove server: " + ((err && err.message) || err)); return; }
  if (currentServer === s.id) { await switchServer(""); const sel = document.getElementById("server-select"); if (sel) sel.value = defaultServerId; }
  await refreshServersList();
  toast("Server removed");
}

// test / doctor / monitor -----------------------------------------------------

function testResultText(res) {
  // provision_pending: a monitored source whose per-source index isn't created
  // yet (Start creates it). Reachable server, normal pre-Start state — render
  // it as a neutral hint, not a red failure.
  if (res.provision_pending) return "○ " + (res.error || "index not created yet; click Start");
  if (!res.ok) return "✗ " + (res.error || "unreachable");
  let s = "✓ ok · " + res.latency_ms + " ms";
  if (res.server_version) s += " · MySQL " + res.server_version;
  // has_index/schema_current are tri-state: absent = the metadata lookup itself
  // failed (unknown) — never render that as the confident negative.
  if (res.has_index === false) s += " · doesn't look like a dbtrail index (missing the binlog_events table)";
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
    formMsg(testResultText(res), !res.ok && !res.provision_pending);
  } catch (err) { formMsg((err && err.message) || String(err), true); }
}

async function testServerRow(id) {
  const slot = document.getElementById("srv-status-" + id);
  if (slot) { slot.className = "srv-status"; slot.textContent = "testing…"; }
  try {
    const res = await api("/api/servers/" + encodeURIComponent(id) + "/test", { method: "POST", body: {} });
    if (slot) { slot.className = "srv-status " + (res.provision_pending ? "pending" : (res.ok ? "ok" : "err")); slot.textContent = testResultText(res); }
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
    const bodyEl = el("div", { class: "dc-body" }, el("div", { class: "dc-name", text: chk.name + (chk.detail ? ": " + chk.detail : "") }));
    if (chk.remediation) bodyEl.append(el("pre", { class: "dc-rem", text: chk.remediation }));
    card.append(bodyEl);
    box.append(card);
  });
}

async function startMonitor(id) {
  try { return await api("/api/servers/" + encodeURIComponent(id) + "/monitor/start", { method: "POST", body: {} }); }
  catch (err) { toastError("Could not start: " + ((err && err.message) || err)); return null; }
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
    formMsg(res.started ? "Monitoring started; review the warnings below" : "Startup checks failed: fix the items below, save, and start again", !res.started);
  } else if (res.started) { toast("Monitoring started, with warnings"); }
  else { toastError("Startup checks failed"); }
}

async function stopMonitorRow(id) {
  try { await api("/api/servers/" + encodeURIComponent(id) + "/monitor/stop", { method: "POST", body: {} }); toast("Monitoring stopped"); }
  catch (err) { toastError("Could not stop: " + ((err && err.message) || err)); }
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
  if (capsCache.sql) cmds.push({ group: "Navigate", label: "SQL", run: () => navigate("sql") });
  if (capsCache.monitor) cmds.push({ group: "Navigate", label: "Baselines", run: () => navigate("baselines") });
  if (capsCache.monitor) cmds.push({ group: "Navigate", label: "Verification", run: () => navigate("verification") });
  if (capsCache.monitor) cmds.push({ group: "Navigate", label: "Storage", run: () => navigate("storage") });
  cmds.push({ group: "Navigate", label: "Connect AI", run: () => navigate("connect") });
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
  // Escape closes whatever dialog occupies the shared #modal slot (#968). The
  // ⌘K palette lives in its own mount and closes itself; the sign-in gate is
  // unreachable here (guard above), so it stays un-dismissable by design.
  if (e.key === "Escape") {
    const cmdk = document.getElementById("cmdk-mount");
    if (cmdk && cmdk.firstChild) return;
    const modalMount = document.getElementById("modal");
    // The recover busy dialog owns its Escape (a capture-phase handler that
    // also aborts the in-flight fetch, #1363) — never generically empty the
    // mount over it. This branch is still reachable with that dialog open:
    // cmdkKeydown closes the palette without stopping propagation, so the
    // SAME Escape that closed the palette lands here with the cmdk check
    // above already passing.
    if (modalMount && modalMount.querySelector(".busy-modal")) return;
    if (modalMount && modalMount.firstChild) { e.preventDefault(); modalMount.replaceChildren(); }
    return;
  }
  // ⌘K / Ctrl+K opens the palette anywhere.
  if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") { e.preventDefault(); openCmdk(); return; }
  // j/k/↵/u row nav — only on Events, only when not typing in a field.
  const typing = /^(INPUT|TEXTAREA|SELECT)$/.test(document.activeElement && document.activeElement.tagName);
  if (typing || routeFromLocation() !== "events") return;
  const rows = $all(".ev-row", VIEW());
  if (e.key === "j") { e.preventDefault(); moveCursor(1); }
  else if (e.key === "k") { e.preventDefault(); moveCursor(-1); }
  else if (e.key === "Enter") {
    // A focused interactive element handles its own Enter (buttons, links,
    // expandable rows) — only drive the j/k cursor row otherwise (#968).
    const a = document.activeElement;
    if (a && (a.classList.contains("ev-row") || /^(BUTTON|A|SUMMARY)$/.test(a.tagName))) return;
    if (cursorIdx >= 0 && rows[cursorIdx]) { e.preventDefault(); rows[cursorIdx].click(); }
  }
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
    toastError("Could not load servers: " + ((err && err.message) || err));
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
  // Capture phase on purpose — see toastEscape. An Escape that closes a dialog
  // must not also dismiss an error notice behind it, and only the capture phase
  // still sees that the dialog was open.
  document.addEventListener("keydown", toastEscape, true);

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

  // Pre-auth gate: with no stored token, first try the HttpOnly session
  // cookie a login in another tab may have left (#1370) — on success the tab
  // proceeds signed-in with TOKEN empty (api() omits the Authorization header
  // and the cookie authenticates every call). Otherwise ask the
  // (unauthenticated) probe how this console authenticates BEFORE firing data
  // fetches that are guaranteed 401s. First run with no credential →
  // create-password screen; password configured → sign-in form; token mode →
  // the printed-link hint.
  if (!TOKEN && !(await probeCookieSession())) {
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) { /* server down — fall through, the view will surface it */ }
    if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    if (auth.password_login) { showLoginOverlay({ passwordLogin: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    // An external provider can be the sole credential (no token, no
    // password): raise the gate with the SSO entry instead of toasting about
    // a printed token link that never existed in that deployment.
    if (auth.sso_start) { showLoginOverlay({ passwordLogin: false, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    toastError("No token in the URL. Open the link that bintrail-console printed.");
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
