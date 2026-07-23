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
const BADGE_CLASS = { UPDATE: "b-update", INSERT: "b-insert", DELETE: "b-delete" };
function badgeClass(t) { return BADGE_CLASS[t] || "b-baseline"; }

const ROUTES = ["overview", "events", "timetravel", "recover", "status", "storage", "connect"];

const MON_STATE_TITLES = {
  failed: "connection is failing and retrying automatically; press Start for details",
  stalled: "connected, but hasn't made progress for several minutes",
  lost_position: "some old changes were deleted before dbtrail could capture them — those are permanently lost, but current changes are still being captured",
};

// Static decorative SVGs (module constants — parsed by svgEl via DOMParser).
const ICONS = {
  search: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round"><circle cx="11" cy="11" r="7"></circle><path d="M21 21l-4.3-4.3"></path></svg>`,
  caret: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.2" stroke-linecap="round" stroke-linejoin="round"><path d="M9 6l6 6-6 6"></path></svg>`,
  file: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round" style="width:16px;height:16px"><path d="M14 3H7a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V8z"></path><path d="M14 3v5h5"></path></svg>`,
  warn: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round" style="width:15px;height:15px"><path d="M10.3 3.9 1.8 18a2 2 0 0 0 1.7 3h17a2 2 0 0 0 1.7-3L13.7 3.9a2 2 0 0 0-3.4 0z"/><path d="M12 9v4M12 17h.01"/></svg>`,
  calendar: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"></rect><path d="M8 3v4M16 3v4M3 10h18"></path></svg>`,
  ext: `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.9" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/></svg>`,
};

// ── module state ─────────────────────────────────────────────────────────────

let TOKEN = "";
let currentServer = "";       // X-Bintrail-Server target ("" = backend default)
let defaultServerId = "";
let serverGen = 0;            // bumped on every server switch (staleness guard)
let capsCache = {};           // last /api/capabilities for the selected server
let extViews = [];            // extension views advertised for the selected server (embedding builds)
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
  if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
  // Token mode has no session to expire and no form to "sign in" to — say what
  // actually happened (the stored token is no longer accepted). An advertised
  // external provider also mints real sessions, so with one present the
  // session-expired copy is the accurate one (SSO-only deployments never had
  // an access token at all).
  const msg = auth.password_login || auth.sso_start ? "Session expired — sign in again." : "This access token is no longer valid.";
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
      panel.append(el("p", { class: "modal-desc", text: "Open the link printed when bintrail-console started — it has the access token this page needs." }));
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
  } catch (_) { loginMsg(msg, "Network error — is the console still running?"); return; }
  if (res.status === 403) {
    // Setup closed under us (a concurrent `user set-password`, another tab, or
    // a CLI set it first). Unlike login, the setup endpoint self-disables —
    // re-probe and switch the gate to the sign-in form instead of leaving the
    // operator stuck re-posting to a now-closed endpoint.
    let auth = {};
    try { auth = await fetchAuthInfo(); } catch (_) {}
    showLoginOverlay({ passwordLogin: !!auth.password_login, message: "A password was already created — sign in.", ssoName: auth.sso_name, ssoStart: auth.sso_start });
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
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server — try again."); return; }
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
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server — try again."); return; }
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
  try { data = await res.json(); } catch (_) { loginMsg(msg, "Unexpected response from the server — try again."); return; }
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
      "It's created automatically when monitoring starts for this source — it never lives on the source MySQL itself. " +
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
  return route.startsWith("ext-") && extViews.some((v) => "ext-" + v.id === route);
}

function routeFromLocation() {
  const path = location.pathname.replace(/^\//, "").split("/")[0] || "overview";
  return isKnownRoute(path) ? path : "overview";
}

function navigate(route, params, push = true) {
  if (!isKnownRoute(route)) route = "overview";
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
  // The date-picker popover lives in document.body (position:fixed), outside
  // the #view subtree route changes normally clear — there's no per-view
  // teardown hook in this codebase to hang that cleanup on otherwise.
  closeDatePicker();
  const route = routeFromLocation();
  setActiveNav(route);
  cursorIdx = -1;
  const params = Object.fromEntries(new URLSearchParams(location.search));
  // Extension views (embedding builds): "ext-<id>" dispatches to the provider's
  // frontend module. routeFromLocation already redirected an unknown/ungated
  // ext route to overview, so a match here is always a live view.
  if (route.startsWith("ext-")) {
    const view = extViews.find((v) => "ext-" + v.id === route);
    return view ? renderExtensionView(view) : renderOverview();
  }
  switch (route) {
    case "overview": return renderOverview();
    case "events": return renderEvents(params);
    case "timetravel": return renderTimetravel(params);
    case "recover": return renderRecover(params);
    case "status": return renderStatus();
    case "storage": return renderStorage();
    case "connect": return renderConnect();
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
  const latest = (events[0] && events[0].event_timestamp) || "—";
  const earliest = events.length ? events[events.length - 1].event_timestamp : "—";

  const v = VIEW(); clear(v);

  const sub = el("p", { class: "page-sub" },
    "What changed recently, and where — your starting point. ",
    el("b", { text: deletes + " delete(s)" }),
    " in the last " + events.length + " event(s): the ones worth a look first.");
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
    el("span", { text: "window" }), " ",
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

  // Connection-id availability note (#595): PostgreSQL logical replication
  // (pgoutput) carries no backend connection id, so the connection_id column
  // stays empty for PG sources — unlike MySQL, it cannot be recovered upstream
  // at all, so say so here on the Events page rather than leave the empty
  // column an unexplained gap. capsCache.source is resolved before this paints.
  if (capsCache.source === "postgresql") {
    v.append(el("div", { class: "warn-item" }, icon("warn"),
      el("span", { text: "Who made each change isn't available for PostgreSQL sources — Postgres's replication stream doesn't include that information." })));
  }

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
  adv.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  adv.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
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

// ── exports (client-side, over the redacted rows on screen) ──────────────────────────────────────────────

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
  form.append(fieldDateInput("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS"));
  form.append(fieldDateInput("Until (UTC)", "until", "md", "YYYY-MM-DD HH:MM:SS"));
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
  const warns = $("#recover-warnings", VIEW());
  const f = Object.fromEntries(new FormData(form).entries());
  const params = {};
  ["schema", "table", "pk", "since", "until"].forEach((k) => { if (f[k] && f[k].trim()) params[k] = f[k].trim(); });
  // Mirror /api/recover's EFFECTIVE fetch window (#967) so the preview shows
  // the same events the undo script will actually reverse: newest-first, same
  // limit as recoverDefaultLimit in internal/console/api.go. Hardcoded here —
  // there is no Go-to-JS constant-sharing mechanism in this codebase.
  params.limit = "1000";
  params.order = "desc";
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
    // Truncation warning (#967): more matches than the preview's limit means
    // the actual undo script (same limit, applied server-side) may cover more
    // events than are shown here.
    if (data.count >= data.limit) {
      renderWarnings(warns, [
        "Only the newest " + data.limit + " events are shown. The actual undo script may include more if you increase the limit."
      ]);
    } else {
      clear(warns);
    }
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
  if (!body.schema) { renderError(out, "Choose at least a schema to search."); return; }
  try {
    const data = await api("/api/recover", { method: "POST", body });
    if (gen !== serverGen) return;
    renderWarnings(warns, data.warnings);
    lastSQL = data.sql || "";
    clear(out);
    // When the target is auto-detected as a foreign-key parent, the script also
    // re-creates the child rows InnoDB cascade-deleted below the binlog — surface
    // it so the larger script isn't a surprise (coverage caveats, if any, are in
    // the warnings above).
    if (data.cascade_detected) {
      out.append(el("div", { class: "ctx-banner" },
        el("span", { class: "badge b-baseline", text: "CASCADE" }),
        el("div", { class: "ctx-main" },
          el("span", { class: "ctx-eyebrow", text: "Also restoring rows that were deleted automatically along with this one" }),
          el("span", { class: "ctx-detail", text:
            "this script also restores " + (data.victim_count || 0) + " related row(s) that MySQL deleted automatically" +
            (data.set_null_count ? " and fixes " + data.set_null_count + " reference(s) that were cleared automatically" : "") }))));
    }
    const meta = data.cascade_detected
      ? data.statement_count + " statement(s) · " + (data.victim_count || 0) + " cascade child row(s) · " + (data.set_null_count || 0) + " SET NULL restore(s)"
      : data.statement_count + " statement(s) from " + data.row_count + " event(s)";
    out.append(codePanel(lastSQL, meta));
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
  navigator.clipboard.writeText(lastSQL).then(() => toast("SQL copied to clipboard"), () => toast("Copy failed."));
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
    "See what a row looked like at any moment in the past — your latest full snapshot plus every change since. Pick a row and a time to see its value then, or see its entire history.");
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
  const out = el("div", { id: "tt-out" });
  out.append(el("div", { class: "tt-meta", text: "Fill in a row above and pick a button to see what it looked like." }));
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
  if (!f.schema || !f.table || !f.pk) { clear(warns); renderError(out, "Schema, table, and PK are all required."); return; }
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
  if (!entries.length) { container.append(el("div", { class: "deleted-note", text: "No history for this primary key in the time range that's been indexed." })); return; }

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
  const sub = el("p", { class: "page-sub", text: "A quick health check — what's been captured, how far back it goes, and where live capture stands right now." });
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
    ["earliest event", cov.earliest_event],
    ["latest event", cov.latest_event],
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
          : "A gap in the binlog can't be filled — some history is permanently missing. To keep capturing changes, create a new baseline and start over.") }));
    lost.append(el("div", { text: "Detected: " + stream.gap_lost.at }));
    return lost;
  }
  if (stream.continuity && stream.continuity.status === "ok") {
    const ok = el("div", { class: "ok-box" });
    ok.append(el("b", { text: "✓ No gaps in captured stream" }));
    ok.append(el("div", { text: "No gaps in what's been captured so far — this doesn't mean the stream is currently running or caught up." }));
    return ok;
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
    ? "stale — last checked " + agoText(ageSec) + " (daemon may be stopped)"
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
  const [serversRes, rotation, storage, baselines, telemetry] = await Promise.all([
    api("/api/servers").catch(asErr),
    api("/api/rotation").catch(asErr),
    api("/api/storage").catch(asErr),
    api("/api/baselines").catch(asErr),
    api("/api/telemetry").catch(asErr),
  ]);
  if (gen !== serverGen) return;
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
  cards.append(baselineSummaryCard(baselines, cur));
  v.append(cards);

  const grid = el("div", { class: "ov-grid", style: "margin-top:18px" });
  grid.append(archivingPanel(servers, serversErr));
  grid.append(baselinesPanel(baselines, servers));
  grid.append(verifyPanel(servers));
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
  let summary = "No credentials set directly — relying on your AWS environment (for example, an EC2 instance role) to provide them automatically.";
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
function telemetryCard(t) {
  const card = el("div", { class: "card" }, el("div", { class: "card-title", text: "Usage telemetry" }));
  if (!t || t.error) {
    card.append(el("p", { class: "form-hint", text: "Could not read telemetry state" + (t && t.error ? ": " + t.error : ".") }));
    return card;
  }
  if (!t.endpoint_set) {
    card.append(el("p", { class: "stg-hint", text: "This build sends no telemetry — no endpoint is compiled in." }));
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
    card.append(el("p", { class: "form-hint", text: "Set by " + by + " on the daemon, which overrides this toggle — change it there." }));
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
    toast("Could not change telemetry: " + ((e && e.message) || e));
    return;
  }
  toast(enabled ? "Telemetry turned on." : "Telemetry turned off — this daemon stops beaconing now.");
  renderStorage();
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
      row.append(el("span", { class: "stg-dest" + (s.archive_s3 ? "" : " muted"), text: s.archive_s3 || "not archived — old data is deleted, not saved" }));
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

function baselinesPanel(b, servers) {
  const panel = el("section", { class: "ov-panel" });
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  let owner = cur ? serverLabel(cur) : "";
  if (!owner && b && !b.error && b.configured) owner = "daemon (--baseline-dir / --baseline-s3)";
  const head = el("div", { class: "ov-panel-head" },
    el("h2", { class: "ov-panel-title", text: "Baseline snapshots" + (owner ? " — " + owner : "") }));
  // Create-baseline action: only when the daemon opted in (capsCache.baseline_trigger),
  // a real server is selected, and it has a baseline destination configured (else the
  // endpoint 400s). The endpoint still re-validates the source DSN server-side.
  if (capsCache.baseline_trigger && cur && cur.id && b && !b.error && b.configured) {
    const btn = el("button", { class: "btn btn-sm", type: "button", text: "Create baseline" });
    btn.onclick = () => createBaseline(cur.id, btn);
    head.append(btn);
  }
  panel.append(head);
  const list = el("div", { class: "stg-list" });
  if (!b || b.error) {
    list.append(el("div", { class: "ev-empty", text: "Could not list baselines: " + ((b && b.error) || "unavailable") }));
  } else if (!b.configured) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "No baselines configured." }),
      el("p", { class: "stg-empty-sub", text: "A baseline is a full copy of your table at one point in time. With one, Time-travel can show complete rows — not just the ones that changed recently." }),
      el("p", { class: "stg-empty-sub", text: "1. Create snapshots:" }),
      el("code", { class: "stg-code", text: "docker compose --profile baseline run --rm baseline" }),
      el("p", { class: "stg-empty-sub", text: "2. " + baselineConfigHint(cur) })));
  } else if (!(b.snapshots || []).length) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "Source configured, no snapshots found." }),
      el("code", { class: "stg-code", text: b.source }),
      el("p", { class: "stg-empty-sub", text: "Run bintrail dump and bintrail baseline to create your first snapshot. The path must point at the folder that contains the snapshots, not a specific file (<timestamp>/<schema>/<table>.parquet)." })));
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

// createBaseline triggers an in-process baseline (dump→convert→upload) on the
// daemon for the selected server, then polls until it finishes and refreshes the
// Storage view so the new snapshot appears. The button is disabled while in flight.
async function createBaseline(id, btn) {
  if (btn) { btn.disabled = true; btn.textContent = "Creating…"; }
  const restore = () => { if (btn) { btn.disabled = false; btn.textContent = "Create baseline"; } };
  try {
    await api("/api/servers/" + encodeURIComponent(id) + "/baseline", { method: "POST", body: {} });
  } catch (err) {
    toast("Baseline failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  toast("Baseline started — copying your data and uploading it…");
  const done = await pollBaseline(id);
  restore();
  if (done && done.state === "succeeded") {
    toast("Baseline complete: " + (done.tables || 0) + " table(s)" +
      (done.uploaded ? ", " + done.uploaded + " file(s) uploaded" : ""));
  } else if (done) {
    toast("Baseline failed: " + (done.last_error || "unknown error"));
  } else {
    toast("Baseline still running — check back shortly.");
  }
  if (location.pathname === "/storage") renderStorage();
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

// verifyPanel (#677): trigger/poll/explain the recovery-chain verification
// engine (`bintrail verify`) for the selected server. Mirrors baselinesPanel's
// structure — its own capability gate (verify_trigger, process-global, like
// baseline_trigger) plus a per-server precondition (verify: a baseline is
// configured; verify_live_source: a source DSN is also configured), both
// re-enforced server-side so this gating is UX only.
function verifyPanel(servers) {
  // Full-width: ov-grid is a 2-column grid (archiving | baselines) with
  // align-items:start, and Baseline snapshots routinely runs much taller
  // than S3 archiving (one row per snapshot) — a 3rd item left to the grid's
  // normal auto-placement lands in row 2 col 1, floating under the SHORT
  // sibling with a large dead gap to its right where the tall one still
  // extends. Spanning both columns puts it in its own row instead.
  const panel = el("section", { class: "ov-panel vfy-panel-full" });
  const cur = (servers || []).find((s) => s.id === (currentServer || defaultServerId));
  const head = el("div", { class: "ov-panel-head" }, el("h2", { class: "ov-panel-title", text: "Verification" }));
  const list = el("div", { class: "stg-list vfy-list" });

  if (!capsCache.verify_trigger) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "Verification from the console is turned off." }),
      el("p", { class: "stg-empty-sub", text:
        "Ask whoever manages this server to turn it on (set BINTRAIL_CONSOLE_VERIFY_TRIGGER=1 and restart). Already on the default setup? Re-download docker-compose.yml — running \"docker compose pull\" alone doesn't add new settings to a file you already have." })));
    panel.append(head, list);
    return panel;
  }
  if (!cur || !cur.id) {
    list.append(el("div", { class: "ev-empty", text: "Select a server to run verification." }));
    panel.append(head, list);
    return panel;
  }

  const modeSel = el("select", { class: "select vfy-mode" },
    el("option", { value: "baseline-anchored", text: "Compare two saved snapshots (recommended)" }));
  if (capsCache.verify_live_source) {
    modeSel.append(el("option", { value: "live-source", text: "Compare against your live database (slower)" }));
  }
  const warn = el("p", { class: "form-hint vfy-livewarn", hidden: true, text:
    "This reads your entire live table — it can take a while and adds load on your database. Best run outside busy hours." });
  modeSel.onchange = () => { warn.hidden = modeSel.value !== "live-source"; };

  const results = el("div", { class: "vfy-results" });
  const btn = el("button", { class: "btn btn-sm", type: "button", text: "Run verification" });
  const configured = !!capsCache.verify;
  btn.disabled = !configured;
  btn.onclick = () => createVerify(cur.id, modeSel.value, btn, results);
  head.append(el("div", { class: "vfy-actions" }, modeSel, btn));
  panel.append(head);

  if (!configured) {
    list.append(el("div", { class: "stg-empty" },
      el("p", { class: "stg-empty-lead", text: "No baseline set up for this server yet." }),
      el("p", { class: "stg-empty-sub", text:
        "This checks your two most recent snapshots against each other. Set a baseline location (Manage servers → Edit → Advanced) and create at least two snapshots to use it." })));
  } else {
    list.append(warn);
    renderVerifyResults(results, null, cur.id);
    list.append(results);
  }
  panel.append(list);
  return panel;
}

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
    toast("Verify failed: " + ((err && err.message) || err));
    restore();
    return;
  }
  renderVerifyResults(resultsEl, status, id);
  toast("Verification started…");
  const done = await pollVerify(id, (st) => renderVerifyResults(resultsEl, st, id));
  restore();
  if (done) renderVerifyResults(resultsEl, done, id);
  if (done && done.state === "succeeded") {
    const s = done.summary || {};
    toast(done.note || ("Verification complete: " + s.match + " match, " + s.mismatch + " mismatch, " +
      s.inconclusive + " inconclusive, " + s.error + " error"));
  } else if (done) {
    toast("Verification failed: " + (done.last_error || "unknown error"));
  } else {
    toast("Verification still running — check back shortly.");
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
const VFY_STATUS_MARK = { pass: "✓", fail: "✗", warn: "!" };
const VFY_MODE_LABEL = { "baseline-anchored": "compared two saved snapshots", "live-source": "compared against the live database" };

// renderVerifyResults draws the current run's summary + per-table cards into
// container, reusing the doctor preflight card styling (pass/fail/warn) since
// both are "a list of named checks, each with a status and free-text detail".
function renderVerifyResults(container, status, id) {
  clear(container);
  if (!status || status.state === "idle") {
    container.append(el("div", { class: "ev-empty", text: "No verification run yet." }));
    return;
  }
  const stateLabel = { running: "RUNNING", succeeded: "DONE", failed: "FAILED" }[status.state] || status.state.toUpperCase();
  const summaryRow = el("div", { class: "vfy-summary" },
    el("span", { class: "chip chip-mon", text: stateLabel }));
  if (status.mode) summaryRow.append(el("span", { class: "stg-age", text: VFY_MODE_LABEL[status.mode] || status.mode }));
  const s = status.summary || {};
  if (status.results && status.results.length) {
    summaryRow.append(el("span", { class: "stg-age", text:
      s.match + " match · " + s.mismatch + " mismatch · " + s.inconclusive + " inconclusive · " + s.error + " error" }));
  }
  container.append(summaryRow);
  if (status.note) container.append(el("p", { class: "form-hint", text: status.note }));
  if (status.last_error) container.append(el("p", { class: "form-msg err", text: status.last_error }));

  const cards = el("div", { class: "doctor-cards" });
  (status.results || []).forEach((r) => {
    const cls = VFY_STATUS_CLASS[r.status] || "warn";
    const card = el("div", { class: "doctor-card " + cls });
    card.append(el("span", { class: "dc-mark", text: VFY_STATUS_MARK[cls] || "?" }));
    const body = el("div", { class: "dc-body" },
      el("div", { class: "dc-name", text: r.schema + "." + r.table + " — " + r.status + (r.detail ? " — " + r.detail : "") }));
    if (r.explainable) {
      const explainBtn = el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Explain" });
      explainBtn.onclick = () => openVerifyExplain(id, r.schema, r.table);
      body.append(explainBtn);
    }
    card.append(body);
    cards.append(card);
  });
  container.append(cards);
}

// openVerifyExplain fetches and shows the row-level drill-down for one
// mismatched table, re-using the modal chrome showRotationDialog established.
async function openVerifyExplain(id, schema, table) {
  let ex;
  try {
    ex = (await api("/api/servers/" + encodeURIComponent(id) + "/verify/explain" +
      "?schema=" + encodeURIComponent(schema) + "&table=" + encodeURIComponent(table))).explain;
  } catch (err) {
    toast("Explain failed: " + ((err && err.message) || err));
    return;
  }
  const mount = document.getElementById("modal");
  const scrim = el("div", { class: "modal-scrim show" });
  const modal = el("div", { class: "modal vfy-explain-modal", role: "dialog", "aria-label": "Verify mismatch drill-down" });
  const head = el("div", { class: "modal-head" });
  head.append(el("h2", { class: "modal-title", text: ex.schema + "." + ex.table + " doesn't match" }));
  head.append(el("p", { class: "modal-desc", text:
    (ex.total === 1 ? "1 row differs" : ex.total + " rows differ") +
    " — checked against binlog position " + ex.anchor + "." }));
  head.append(el("p", { class: "modal-desc", text:
    "Recovered = what replaying the change log on top of the older snapshot produced. Baseline (real) = the actual values from the newer, trusted snapshot." }));
  head.append(el("button", { class: "modal-x", type: "button", text: "✕", onclick: closeVerifyExplain }));
  modal.append(head);

  const body = el("div", { class: "vfy-explain-body" });
  if (!ex.diffs || !ex.diffs.length) {
    body.append(el("p", { class: "form-hint", text:
      "The row count differs, but no per-row content difference was found — see raw output below." }));
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
}

function closeVerifyExplain() { document.getElementById("modal").replaceChildren(); }

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
    el("div", { class: "dc-name", text: (VFY_KIND_LABEL[d.kind] || d.kind) + " — " + d.pk }));
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
    if (minted) toast("Token display interrupted — rotate to get a fresh value");
    return;
  }
  try {
    buildConnect(servers, tokStatus, minted);
  } catch (err) {
    if (minted) toast("Token display interrupted — rotate to get a fresh value");
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
  navigator.clipboard.writeText(text).then(() => toast(what + " copied to clipboard"), () => toast("Copy failed."));
}

function buildConnect(servers, tokStatus, minted) {
  const v = VIEW(); clear(v);
  const sub = el("p", { class: "page-sub" },
    "Let an AI assistant query this console — the same read-only tools and result caps as this UI. ",
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
    toast("Token generation failed: " + (err.message || err));
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
    toast("Revoke failed: " + (err.message || err));
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
    card.append(el("p", { class: "stg-hint", text: "Token generated. Copy it now — it is not stored and will never be shown again:" }));
    card.append(el("div", { class: "cn-urlrow" },
      el("code", { class: "stg-code cn-url", text: minted }),
      el("button", { class: "btn btn-sm", type: "button", text: "Copy", onclick: () => copyText(minted, "MCP token") })));
  }
  if (!tok) {
    card.append(el("p", { class: "stg-hint", text: "Token status unavailable — reload the page to retry." }));
    return card;
  }
  if (tok.managed) {
    if (!minted) {
      card.append(el("p", { class: "stg-hint", text:
        "A managed token is active" + (tok.created_at ? " (created " + tok.created_at + ")" : "") +
        ". Its value is not stored and cannot be re-displayed — rotate to get a fresh one." }));
    }
    if (tok.read_only) {
      card.append(el("p", { class: "form-hint", text:
        "The token file was written by a newer bintrail — the token works, but rotate/revoke are unavailable from this build." }));
    } else {
      card.append(el("div", { class: "cn-links" },
        el("button", { class: "btn btn-sm", type: "button", text: "Rotate token", onclick: () => mintMCPToken(true) }),
        el("button", { class: "btn btn-sm btn-ghost", type: "button", text: "Revoke", onclick: revokeMCPToken })));
    }
  } else if (!minted) {
    card.append(el("p", { class: "stg-hint", text:
      "AI clients authenticate with a token (password login is a browser credential and cannot be used by them). Generate one here — no flags, no environment variables, no restart. The token only grants the read-only MCP tools; it cannot manage this console." }));
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
    "Install the bundle in Claude Desktop (double-click), then paste the URL above and your console token — no config files to edit." }));
  if (released) {
    const asset = "dbtrail-" + guessPlatform() + ".mcpb";
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases/download/v" + ver + "/" + asset, text: "Download " + asset }),
      el("a", { class: "btn btn-sm btn-ghost", href: "https://github.com/dbtrail/dbtrail/releases/tag/v" + ver, target: "_blank", rel: "noopener", text: "All downloads for v" + ver })));
    card.append(el("p", { class: "form-hint", text:
      "The link matches this console's version (v" + ver + "). Older releases don't carry the bundle — if the download 404s, pick a newer release from the releases page." }));
  } else {
    card.append(el("div", { class: "cn-links" },
      el("a", { class: "btn btn-sm", href: "https://github.com/dbtrail/dbtrail/releases", target: "_blank", rel: "noopener", text: "Open the releases page" })));
    card.append(el("p", { class: "form-hint", text:
      "This console is an unversioned build, so no exact bundle link can be derived — pick the bundle matching your platform from the latest release." }));
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
    "For claude_desktop_config.json — or any client that launches stdio MCP servers — bintrail-mcp bridges stdio to this console. Replace the placeholder with your console token:" }));
  adv.append(el("pre", { class: "stg-code cn-snippet", text: snippet }));
  adv.append(el("button", { class: "btn btn-sm", type: "button", text: "Copy snippet", onclick: () => copyText(snippet, "Config snippet") }));
  adv.append(el("p", { class: "form-hint", text:
    "If this console is reachable over public HTTPS, the same URL also works directly as a claude.ai custom connector — no bridge needed." }));
  panel.append(adv);
  return panel;
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
  // Extension views advertised for this server (embedding builds; empty in the
  // stock binary and under any active profile — the backend omits them there).
  // Rebuild the nav before the route renders so a deep-linked ext route resolves.
  extViews = Array.isArray(capsCache.extension_views) ? capsCache.extension_views : [];
  syncExtNav();
  $all("[data-capability]").forEach((node) => node.classList.toggle("cap-on", !!capsCache[node.dataset.capability]));
  gatePermissions();
  applyAuthGate();
  updateSrvNote(); // capsCache.monitor may have just changed
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
    mod.render(mount, { apiBase: "/api/ext/" + view.id + "/", api });
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
    "The servers you're monitoring with dbtrail, saved in a file on this machine. ");
  desc.append(el("span", { "data-capability": "monitor" },
    "This process can also ", el("b", { text: "monitor" }),
    " a new MySQL database for you: add one below and dbtrail checks it's ready, sets up its index, and starts capturing changes — no terminal needed."));
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
  catch (err) { toast("Could not load rotation settings: " + ((err && err.message) || err)); return; }

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
  toast(wasEnabled ? "Rotation settings saved" : "Saved — rotation is off, so this won't take effect until the daemon restarts");
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
  mon.append(el("p", { class: "form-hint", text: "Paste the server you want to watch — dbtrail checks that it's ready, creates an index database for it automatically, and starts capturing changes. Nothing else to fill in beyond a name." }));
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
  const grantHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "mysql mariadb");
  grantHint.append("Source user needs ");
  grantHint.append(el("code", { text: "REPLICATION SLAVE, REPLICATION CLIENT, SELECT" }));
  grantHint.append(". Create one on the source MySQL — copy and run:");
  mon.append(grantHint);
  mon.append(tagFlavor(el("pre", { class: "form-code", text:
    "CREATE USER 'dbtrail'@'%' IDENTIFIED BY 'strong-password';\n" +
    "GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'dbtrail'@'%';" }), "mysql mariadb"));
  // PostgreSQL prerequisites — the console reads them, it never runs CREATE
  // PUBLICATION / ALTER SYSTEM (validate-don't-create; capture is pgoutput-only).
  const pgHint = tagFlavor(el("p", { class: "form-hint", style: "margin-top:10px" }), "postgres");
  pgHint.append("PostgreSQL source needs ");
  pgHint.append(el("code", { text: "wal_level=logical" }));
  pgHint.append(", a role with the ");
  pgHint.append(el("code", { text: "REPLICATION" }));
  pgHint.append(" attribute, a publication you create, and ");
  pgHint.append(el("code", { text: "REPLICA IDENTITY FULL" }));
  pgHint.append(" on replicated tables — copy and run on the source:");
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
    form.elements.password.placeholder = prefill.has_password ? "(unchanged — leave blank to keep)" : "(none)";
    form.elements.source_password.placeholder = prefill.has_source_password ? "(unchanged — leave blank to keep)" : "";
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
  catch (err) { toast("Could not load server: " + ((err && err.message) || err)); return false; }
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
    if (res && res.started && !doctorWarnings(res.doctor)) { hideServerForm(); toast("Monitoring started — events will appear within a minute"); }
    else if (res && res.started) { renderDoctor(res.doctor); formMsg("Monitoring started — review the warnings below", false); }
    else if (res) { renderDoctor(res.doctor); formMsg("Startup checks failed — fix the items below and save again", true); }
    else { formMsg("Could not start monitoring — check the notification for details and try again", true); } // startMonitor returned null (transport error)
    return;
  }
  hideServerForm();
  await refreshServersList();
  toast(id ? "Server updated" : "Server added");
}

async function deleteServer(s) {
  if (!window.confirm('Remove server "' + s.name + '"? This only removes the saved connection — nothing happens to the server itself.')) return;
  try { await api("/api/servers/" + encodeURIComponent(s.id), { method: "DELETE" }); }
  catch (err) { toast("Could not remove server: " + ((err && err.message) || err)); return; }
  if (currentServer === s.id) { await switchServer(""); const sel = document.getElementById("server-select"); if (sel) sel.value = defaultServerId; }
  await refreshServersList();
  toast("Server removed");
}

// test / doctor / monitor -----------------------------------------------------

function testResultText(res) {
  // provision_pending: a monitored source whose per-source index isn't created
  // yet (Start creates it). Reachable server, normal pre-Start state — render
  // it as a neutral hint, not a red failure.
  if (res.provision_pending) return "○ " + (res.error || "index not created yet — click Start");
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
    const bodyEl = el("div", { class: "dc-body" }, el("div", { class: "dc-name", text: chk.name + (chk.detail ? " — " + chk.detail : "") }));
    if (chk.remediation) bodyEl.append(el("pre", { class: "dc-rem", text: chk.remediation }));
    card.append(bodyEl);
    box.append(card);
  });
}

async function startMonitor(id) {
  try { return await api("/api/servers/" + encodeURIComponent(id) + "/monitor/start", { method: "POST", body: {} }); }
  catch (err) { toast("Could not start: " + ((err && err.message) || err)); return null; }
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
    formMsg(res.started ? "Monitoring started — review the warnings below" : "Startup checks failed — fix the items below, save, and start again", !res.started);
  } else { toast(res.started ? "Monitoring started — with warnings" : "Startup checks failed"); }
}

async function stopMonitorRow(id) {
  try { await api("/api/servers/" + encodeURIComponent(id) + "/monitor/stop", { method: "POST", body: {} }); toast("Monitoring stopped"); }
  catch (err) { toast("Could not stop: " + ((err && err.message) || err)); }
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
    toast("Could not load servers: " + ((err && err.message) || err));
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
    if (auth.setup) { showLoginOverlay({ setup: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    if (auth.password_login) { showLoginOverlay({ passwordLogin: true, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
    // An external provider can be the sole credential (no token, no
    // password): raise the gate with the SSO entry instead of toasting about
    // a printed token link that never existed in that deployment.
    if (auth.sso_start) { showLoginOverlay({ passwordLogin: false, ssoName: auth.sso_name, ssoStart: auth.sso_start }); return; }
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
