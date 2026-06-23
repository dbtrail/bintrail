// Headless-Chrome E2E regression guard for the bintrail-console frontend.
//
// Why this exists: `go test` never renders the embedded SPA (assets/*.js,
// *.css), so a whole class of bugs is invisible to the Go suite — CSS cascade
// (an invisible button), DOM/state logic (a form that auto-expands), and the
// way the UI presents a backend state (a raw 1049 error wall, the control
// plane vanishing when the selected server's index is missing). Each scenario
// below pins a bug that shipped in 0.13.3 and reached a user.
//
// It drives an ALREADY-RUNNING console (run.sh starts the daemon + seeds a
// monitored source whose per-source index is NOT provisioned — the exact
// lifecycle state that broke everything). Env: CONSOLE_URL, CONSOLE_TOKEN,
// optional PW_CHANNEL (e.g. "chrome" to use system Chrome instead of the
// playwright-managed chromium).
import { chromium } from "playwright";

const URL = process.env.CONSOLE_URL || "http://127.0.0.1:8090";
const TOKEN = process.env.CONSOLE_TOKEN || "";
const CHANNEL = process.env.PW_CHANNEL || undefined; // undefined → bundled chromium
const ART = process.env.E2E_ARTIFACT_DIR || "/tmp";

const results = [];
const ok = (name) => results.push({ name, pass: true });
const bad = (name, detail) => results.push({ name, pass: false, detail });

const browser = await chromium.launch({ headless: true, channel: CHANNEL });
const page = await browser.newPage({ viewport: { width: 1300, height: 1000 } });
const jsErrors = [];
page.on("pageerror", (e) => jsErrors.push(String(e)));

try {
  await page.goto(`${URL}/?token=${encodeURIComponent(TOKEN)}`, { waitUntil: "networkidle" });
  await page.waitForFunction(
    () => typeof openServersModal === "function" && typeof renderError === "function",
    { timeout: 10000 },
  );

  // Scenario 0 — the page loads and the monitor capability is reported. A
  // regression here (caps fetch failing for the default server — even a
  // broken, unprovisioned one — and degrading to {}) would itself hide the
  // control plane. The fetch is async, so wait for it rather than racing it.
  // capsCache is a module-scoped `let` (not window.capsCache): reachable by
  // bare name in evaluate's global scope, never as a window property.
  let monitor = false;
  try {
    await page.waitForFunction(() => typeof capsCache !== "undefined" && capsCache.monitor === true, { timeout: 8000 });
    monitor = true;
  } catch (_) {
    monitor = await page.evaluate(() => typeof capsCache !== "undefined" && !!capsCache.monitor);
  }
  monitor ? ok("boot: monitor capability reported") : bad("boot: monitor capability reported", "capsCache.monitor !== true after caps load");

  await page.evaluate(() => openServersModal());
  await page.waitForSelector("#servers-list", { timeout: 5000 });
  await page.waitForTimeout(400);

  // Scenario 1 — control plane survives a broken (unprovisioned) selected
  // server: Start button + the "monitor" copy must render. Guards the
  // /api/capabilities 502 cascade (a per-source index that doesn't exist must
  // not zero out the process-level Monitor capability).
  const cp = await page.evaluate(() => {
    const btns = Array.from(document.querySelectorAll("#servers-list button"));
    const start = btns.find((b) => b.textContent.trim() === "Start");
    const copy = document.querySelector('.modal-desc [data-capability="monitor"]');
    return { start: !!start, copy: copy ? getComputedStyle(copy).display !== "none" : false };
  });
  cp.start ? ok("control plane: Start button present") : bad("control plane: Start button present", "missing — caps cascade?");
  cp.copy ? ok("control plane: monitor copy visible") : bad("control plane: monitor copy visible", "hidden — caps cascade?");

  // Scenario 2 — primary button stays visible on hover (gradient survives;
  // .btn:hover must not win the background and leave white text on light gray).
  await page.hover("#server-add");
  await page.waitForTimeout(150);
  const hov = await page.evaluate(() => {
    const b = document.getElementById("server-add");
    const cs = getComputedStyle(b);
    return { bgImage: cs.backgroundImage, bgColor: cs.backgroundColor, color: cs.color };
  });
  // Visibility is contrast: the gradient must survive AND the text must stay
  // white. Checking only the background would miss a light-text regression.
  /gradient/.test(hov.bgImage)
    ? ok("button: primary keeps gradient on hover")
    : bad("button: primary keeps gradient on hover", `bgImage=${hov.bgImage} bgColor=${hov.bgColor}`);
  hov.color === "rgb(255, 255, 255)"
    ? ok("button: primary text stays white on hover")
    : bad("button: primary text stays white on hover", `color=${hov.color}`);

  // Scenario 3 — editing a monitored source keeps the optional "bring your own
  // index" section COLLAPSED (its index DSN is auto-derived; expanding it shows
  // a per-source index the operator never typed).
  await page.evaluate(() => {
    const edit = Array.from(document.querySelectorAll("#servers-list button")).find((b) => b.textContent.trim() === "Edit");
    edit.click();
  });
  await page.waitForSelector("#server-advanced", { timeout: 5000 });
  await page.waitForTimeout(200);
  const form = await page.evaluate(() => {
    const adv = document.getElementById("server-advanced");
    const src = document.querySelector('input[name="source_user"]');
    return { advOpen: adv.open, srcVisible: src ? src.offsetParent !== null : false };
  });
  !form.advOpen ? ok("form: advanced section collapsed for a source entry") : bad("form: advanced section collapsed for a source entry", "auto-expanded");
  form.srcVisible ? ok("form: source fields visible") : bad("form: source fields visible", "hidden");

  // Scenario 4 — the REAL missing-index path (not a fabricated string): query
  // a data endpoint against the default (unprovisioned wp) server, take the
  // ACTUAL backend error, and feed it to renderError. This proves the backend
  // 1049 reaches the frontend in the shape the empty-state needs — i.e. that
  // scrubDSNError preserves the index db name and the 1049 text survives.
  await page.evaluate(() => closeServersModal());
  const es = await page.evaluate(async () => {
    let errMsg = null;
    try { await api("/api/status"); } catch (e) { errMsg = (e && e.message) || String(e); }
    if (!errMsg) return { errMsg: null };
    const v = document.getElementById("view") || document.querySelector(".view") || document.body;
    renderError(v, new Error(errMsg));
    const empty = document.querySelector(".empty");
    return { errMsg, empty: empty ? empty.textContent : null };
  });
  if (!es.errMsg) {
    bad("error: real 1049 reaches the frontend", "/api/status did not error for the unprovisioned default server");
  } else if (!/Unknown database '(bintrail_idx_[^']+)'/.test(es.errMsg)) {
    bad("error: real 1049 reaches the frontend", `backend error not the expected 1049 shape: ${es.errMsg}`);
  } else {
    ok("error: real 1049 reaches the frontend");
    const t = es.empty || "";
    !es.empty ? bad("error: 1049 renders friendly empty state", "no .empty element produced") : ok("error: 1049 renders friendly empty state");
    /indexing yet/.test(t) ? ok("error: 1049 empty state has friendly title") : bad("error: 1049 empty state has friendly title", t);
    /never lives on the source/.test(t) ? ok("error: 1049 empty state clarifies source-vs-index") : bad("error: 1049 empty state clarifies source-vs-index", t);
    /bintrail_idx_/.test(t) ? ok("error: 1049 empty state names the index db") : bad("error: 1049 empty state names the index db", t);
  }

  // Scenario 5 — a pure BYO-index entry (no source) must auto-EXPAND the
  // advanced section: the index IS the whole form. This is the other arm of
  // byoIndex (scenario 3 covers the collapse-for-source arm). Its dbname points
  // at the daemon's own (existing) boot index, so it resolves cleanly.
  const byoId = await page.evaluate(async () => {
    const res = await api("/api/servers", { method: "POST", body: {
      name: "byo-idx", host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: "bintrail_e2e_idx",
    } });
    return res.id;
  });
  await page.evaluate(() => openServersModal());
  await page.waitForSelector("#servers-list", { timeout: 5000 });
  await page.waitForTimeout(300);
  await page.evaluate((id) => editServer(id), byoId);
  await page.waitForSelector("#server-advanced", { timeout: 5000 });
  await page.waitForTimeout(200);
  const byoOpen = await page.evaluate(() => document.getElementById("server-advanced").open);
  byoOpen
    ? ok("form: advanced section expanded for a BYO-index entry")
    : bad("form: advanced section expanded for a BYO-index entry", "collapsed — the byoIndex open-arm regressed");

  // Scenario 6 — Cascade recovery tab (#580). The class/[data-capability] gating
  // bug class is invisible to Go tests, so pin it here: the nav item + ⌘K entry
  // appear when recover_cascade is on (free tier, true here — no RBAC profile),
  // the tab renders its parent form, and — critically — both the nav item HIDES
  // and the route REDIRECTS to overview when the capability is off.
  await page.evaluate(() => closeServersModal());
  const casc = await page.evaluate(() => {
    const navItem = document.querySelector('.nav-item[data-route="cascade"]');
    const cmds = (typeof cmdkCommands === "function") ? cmdkCommands().map((c) => c.label) : [];
    return {
      capOn: typeof capsCache !== "undefined" && !!capsCache.recover_cascade,
      navPresent: !!navItem,
      navVisible: navItem ? getComputedStyle(navItem).display !== "none" : false,
      inPalette: cmds.includes("Cascade recovery"),
    };
  });
  casc.capOn ? ok("cascade: recover_cascade capability reported") : bad("cascade: recover_cascade capability reported", "false — expected true (no RBAC profile)");
  casc.navPresent ? ok("cascade: nav item present") : bad("cascade: nav item present", "missing from DOM");
  casc.navVisible ? ok("cascade: nav item visible when capability on") : bad("cascade: nav item visible when capability on", "hidden despite cap-on");
  casc.inPalette ? ok("cascade: command-palette entry present") : bad("cascade: command-palette entry present", "missing");

  await page.evaluate(() => navigate("cascade"));
  await page.waitForSelector("#cascade-form", { timeout: 5000 });
  const cform = await page.evaluate(() => {
    const f = document.getElementById("cascade-form");
    if (!f) return { form: false };
    const btn = Array.from(f.querySelectorAll("button")).find((b) => /Generate cascade recovery SQL/.test(b.textContent));
    return {
      form: true,
      onRoute: location.pathname === "/cascade",
      schema: !!f.querySelector('[name="schema"]'),
      table: !!f.querySelector('[name="table"]'),
      pk: !!f.querySelector('[name="pk"]'),
      allowInc: !!f.querySelector('[name="allow_incomplete"]'),
      genBtn: !!btn,
      warnings: !!document.getElementById("cascade-warnings"),
    };
  });
  cform.form ? ok("cascade: form renders on the tab") : bad("cascade: form renders on the tab", "#cascade-form missing");
  cform.onRoute ? ok("cascade: navigates to /cascade") : bad("cascade: navigates to /cascade", "wrong route");
  (cform.schema && cform.table && cform.pk) ? ok("cascade: parent schema/table/pk fields present") : bad("cascade: parent schema/table/pk fields present", JSON.stringify(cform));
  cform.allowInc ? ok("cascade: allow-incomplete checkbox present") : bad("cascade: allow-incomplete checkbox present", "missing");
  cform.genBtn ? ok("cascade: generate action present") : bad("cascade: generate action present", "button missing");
  cform.warnings ? ok("cascade: warnings container present") : bad("cascade: warnings container present", "missing");

  const off = await page.evaluate(() => {
    capsCache.recover_cascade = false;
    // Re-apply the same cap-on toggle gateCapabilities() runs (without refetching,
    // which would reset the flag) — this is the exact class gating under test.
    document.querySelectorAll("[data-capability]").forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
    const navItem = document.querySelector('.nav-item[data-route="cascade"]');
    const navHidden = navItem ? getComputedStyle(navItem).display === "none" : false;
    // (a) navigate() guard: navigating to a gated route bounces to overview.
    navigate("cascade");
    const navGuard = location.pathname === "/overview";
    // (b) renderCascade's OWN replaceState self-guard. A direct dispatch — a
    // bookmarked/typed /cascade URL, Back/popstate, or boot — reaches renderRoute
    // WITHOUT navigate()'s guard, so the in-render guard is the sole defense there.
    // Force /cascade and call renderCascade directly: it must redirect AND render
    // no form (deleting that guard would leave this the only failing assertion).
    history.replaceState({}, "", "/cascade");
    renderCascade({});
    const renderGuard = location.pathname === "/overview" && !document.getElementById("cascade-form");
    capsCache.recover_cascade = true; // restore for any later scenarios
    document.querySelectorAll("[data-capability]").forEach((n) => n.classList.toggle("cap-on", !!capsCache[n.dataset.capability]));
    return { navHidden, navGuard, renderGuard };
  });
  off.navHidden ? ok("cascade: nav item hides when capability off") : bad("cascade: nav item hides when capability off", "still visible — gating regression");
  off.navGuard ? ok("cascade: navigate() guard redirects to overview when off") : bad("cascade: navigate() guard redirects to overview when off", "did not redirect");
  off.renderGuard ? ok("cascade: renderCascade self-guard redirects direct/popstate entry when off") : bad("cascade: renderCascade self-guard redirects direct/popstate entry when off", "form rendered or no redirect — the in-render guard gap");

  // Scenario 7 — PostgreSQL replication-health panel (#599). pgHealthCard is the
  // load-bearing anti-silent-failure surface: a frozen snapshot over a stopped daemon
  // must degrade to muted/warn (never render healthy-green), a missing/unparseable
  // checked_at must read as stale (fail-safe), a recorded probe failure must show as
  // "probe failing" (not a blank panel), and an absent slot reads "not found yet".
  // Driven directly with fixtures — the seeded console is MySQL-sourced, so this
  // unit-drives the render function rather than needing a live PG source.
  const ph = await page.evaluate(() => {
    const iso = (msAgo) => new Date(Date.now() - msAgo).toISOString();
    const base = { exists: true, active: true, wal_status: "reserved", retained_bytes: 16384,
      safe_wal_size: 1073741824, replica_identity_not_full: [] };
    const fresh = pgHealthCard({ ...base, checked_at: iso(3000) });
    const stale = pgHealthCard({ ...base, safe_wal_size: null, checked_at: iso(300000) });
    const noTs = pgHealthCard({ ...base }); // missing checked_at
    const lost = pgHealthCard({ ...base, wal_status: "lost", safe_wal_size: null, checked_at: iso(3000) });
    const perr = pgHealthCard({ exists: false, replica_identity_not_full: [], checked_at: iso(2000), probe_error: "recovery is in progress" });
    const nofound = pgHealthCard({ exists: false, replica_identity_not_full: [], checked_at: iso(2000) });
    return {
      freshGreen: !fresh.classList.contains("card-stale") && /checked \d+s ago/.test(fresh.textContent) && !!fresh.querySelector(".hstat-ok"),
      staleMuted: stale.classList.contains("card-stale") && /daemon may be stopped/.test(stale.textContent),
      missingTsStale: noTs.classList.contains("card-stale"),
      lostRed: !!lost.querySelector(".hstat-err") && /lost/.test(lost.textContent),
      probeErrVisible: /probe failing/i.test(perr.textContent) && /recovery is in progress/.test(perr.textContent) && !!perr.querySelector(".hstat-err"),
      notFoundShown: /not found yet/.test(nofound.textContent),
    };
  });
  ph.freshGreen ? ok("pg-health: fresh snapshot renders healthy + 'checked Ns ago'") : bad("pg-health: fresh snapshot renders healthy + 'checked Ns ago'", "missing fresh/ok state");
  ph.staleMuted ? ok("pg-health: stale snapshot degrades to muted/warn (never silent-green)") : bad("pg-health: stale snapshot degrades to muted/warn (never silent-green)", "no card-stale / warn footer");
  ph.missingTsStale ? ok("pg-health: missing checked_at reads as stale (fail-safe)") : bad("pg-health: missing checked_at reads as stale (fail-safe)", "rendered fresh");
  ph.lostRed ? ok("pg-health: a lost slot renders with the red error chip (critical state never benign)") : bad("pg-health: a lost slot renders with the red error chip (critical state never benign)", "wal_status=lost not .hstat-err");
  ph.probeErrVisible ? ok("pg-health: probe failure shows 'probe failing' (not a blank panel)") : bad("pg-health: probe failure shows 'probe failing' (not a blank panel)", "probe_error not surfaced");
  ph.notFoundShown ? ok("pg-health: absent slot shows 'not found yet'") : bad("pg-health: absent slot shows 'not found yet'", "missing not-found state");

  // No uncaught JS errors over the whole run.
  jsErrors.length === 0 ? ok("no uncaught JS errors") : bad("no uncaught JS errors", JSON.stringify(jsErrors));
} catch (err) {
  bad("driver", String((err && err.stack) || err));
  try { await page.screenshot({ path: `${ART}/console-e2e-failure.png`, fullPage: true }); } catch (_) {}
}

const failed = results.filter((r) => !r.pass);
for (const r of results) console.log(`${r.pass ? "PASS" : "FAIL"}  ${r.name}${r.pass ? "" : "  — " + r.detail}`);
console.log(`\n${results.length - failed.length}/${results.length} passed`);

if (failed.length) {
  try { await page.screenshot({ path: `${ART}/console-e2e-failure.png`, fullPage: true }); } catch (_) {}
}
await browser.close();
process.exit(failed.length ? 1 : 0);
