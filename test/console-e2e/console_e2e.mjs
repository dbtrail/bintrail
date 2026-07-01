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

  // Scenario 6 — Recover/Cascade merge. Cascade recovery is no longer a separate
  // tab: it is auto-detected inside the single Recover flow (the backend routes by
  // detection and folds the invisible children into one script when the target is
  // an FK parent). Pin the REMOVAL here — a stray cascade nav item, route, palette
  // entry, or render function would be a merge regression — and confirm Recover is
  // the sole Resolve tab and still renders its form.
  await page.evaluate(() => closeServersModal());
  const merged = await page.evaluate(() => {
    const cmds = (typeof cmdkCommands === "function") ? cmdkCommands().map((c) => c.label) : [];
    return {
      cascadeNavGone: !document.querySelector('.nav-item[data-route="cascade"]'),
      cascadeNotInPalette: !cmds.includes("Cascade recovery"),
      cascadeRouteGone: typeof ROUTES !== "undefined" ? !ROUTES.includes("cascade") : true,
      recoverNavPresent: !!document.querySelector('.nav-item[data-route="recover"]'),
      renderCascadeGone: typeof renderCascade === "undefined",
    };
  });
  merged.cascadeNavGone ? ok("merge: cascade nav item removed") : bad("merge: cascade nav item removed", "still present — merge regression");
  merged.cascadeNotInPalette ? ok("merge: cascade command-palette entry removed") : bad("merge: cascade command-palette entry removed", "still present");
  merged.cascadeRouteGone ? ok("merge: /cascade route removed from ROUTES") : bad("merge: /cascade route removed from ROUTES", "still routable");
  merged.recoverNavPresent ? ok("merge: Recover is the sole Resolve tab") : bad("merge: Recover is the sole Resolve tab", "recover nav missing");
  merged.renderCascadeGone ? ok("merge: renderCascade function removed") : bad("merge: renderCascade function removed", "still defined");

  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 5000 });
  const rform = await page.evaluate(() => {
    const f = document.getElementById("recover-form");
    if (!f) return { form: false };
    return {
      form: true,
      onRoute: location.pathname === "/recover",
      schema: !!f.querySelector('[name="schema"]'),
      table: !!f.querySelector('[name="table"]'),
      pk: !!f.querySelector('[name="pk"]'),
      noCascadeForm: !document.getElementById("cascade-form"),
    };
  });
  rform.form ? ok("merge: recover form renders") : bad("merge: recover form renders", "#recover-form missing");
  rform.onRoute ? ok("merge: navigates to /recover") : bad("merge: navigates to /recover", "wrong route");
  (rform.schema && rform.table && rform.pk) ? ok("merge: recover schema/table/pk fields present") : bad("merge: recover schema/table/pk fields present", JSON.stringify(rform));
  rform.noCascadeForm ? ok("merge: no standalone cascade form remains") : bad("merge: no standalone cascade form remains", "#cascade-form still present");

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

  // Scenario 8 — stream-continuity surface (#645). Fixture-drives continuityBox
  // (pure, like pgHealthCard): the green "no gaps" affirmation must RENDER as the
  // green ok-box (color actually applied, not just the class present), a stamped
  // gap must render the red error-box and take precedence over a stale ok, and the
  // unknown/legacy/missing/nil cases must show NEITHER box (no clean verdict from
  // un-evaluated data). This is the only check that the new green badge displays
  // correctly — the Go suite sees the JSON contract, never the pixels.
  const cont = await page.evaluate(() => {
    const okBox = continuityBox({ continuity: { status: "ok" } }, false);
    const gapBox = continuityBox({ gap_lost: { at: "2026-06-22 12:00:00", detail: "unfillable binlog gap" } }, false);
    const gapWins = continuityBox({ gap_lost: { at: "t", detail: "d" }, continuity: { status: "ok" } }, false);
    const unknownBox = continuityBox({ continuity: { status: "unknown" } }, false);
    const missingBox = continuityBox({ mode: "gtid" }, false); // legacy backend: no continuity field
    const nilBox = continuityBox(null, false);
    // The green box must actually be GREEN — append it and read the computed border
    // color, so a CSS-cascade break (class present, color not applied) is caught.
    let okBorder = "";
    if (okBox) { document.body.appendChild(okBox); okBorder = getComputedStyle(okBox).borderColor; okBox.remove(); }
    return {
      okGreenClass: !!okBox && okBox.classList.contains("ok-box"),
      okGreenText: !!okBox && /No gaps in captured stream/.test(okBox.textContent) && /doesn't mean the stream is currently running/.test(okBox.textContent),
      okBorder,
      gapRed: !!gapBox && gapBox.classList.contains("error-box") && /permanently lost/i.test(gapBox.textContent),
      gapPrecedence: !!gapWins && gapWins.classList.contains("error-box"),
      unknownNeither: unknownBox === null,
      missingNeither: missingBox === null,
      nilNeither: nilBox === null,
    };
  });
  cont.okGreenClass ? ok("continuity: ok renders the green ok-box") : bad("continuity: ok renders the green ok-box", "no .ok-box");
  cont.okGreenText ? ok("continuity: green box scoped to contiguity (not a liveness claim)") : bad("continuity: green box scoped to contiguity (not a liveness claim)", "wording missing/overclaims");
  // any non-default, non-transparent border color proves the .ok-box class resolved (--insert green).
  (cont.okBorder && cont.okBorder !== "rgba(0, 0, 0, 0)" && cont.okBorder !== "rgb(0, 0, 0)")
    ? ok("continuity: green ok-box actually renders green (CSS applied)")
    : bad("continuity: green ok-box actually renders green (CSS applied)", `borderColor=${cont.okBorder}`);
  cont.gapRed ? ok("continuity: gap_lost renders the red error-box") : bad("continuity: gap_lost renders the red error-box", "no .error-box");
  cont.gapPrecedence ? ok("continuity: gap_lost takes precedence over a stale ok") : bad("continuity: gap_lost takes precedence over a stale ok", "green won over a gap");
  cont.unknownNeither ? ok("continuity: unknown shows neither box") : bad("continuity: unknown shows neither box", "rendered a box for unknown");
  cont.missingNeither ? ok("continuity: missing continuity (legacy backend) shows no green") : bad("continuity: missing continuity (legacy backend) shows no green", "rendered green without continuity");
  cont.nilNeither ? ok("continuity: nil stream shows neither box") : bad("continuity: nil stream shows neither box", "rendered a box for nil stream");

  // Scenario 9 — Storage page AWS-credentials card (#681). credentialsCard is
  // pure (like pgHealthCard/continuityBox): it must lead with a plain-language
  // summary of which credential source is active, never leave the raw signals
  // as the only content, and each of the mutually-favored signals (static
  // env keys > IAM role > shared config > none) must produce distinct copy —
  // an IAM-role or shared-config setup must never read as "no credentials".
  const cred = await page.evaluate(() => {
    const mk = (aws) => credentialsCard({ aws });
    const none = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: false });
    const keys = mk({ access_key_env: true, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: false });
    const ecs = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: true, web_identity: false });
    const irsa = mk({ access_key_env: false, profile: "", region_env: "", shared_config: false, container_creds: false, web_identity: true });
    const shared = mk({ access_key_env: false, profile: "default", region_env: "", shared_config: true, container_creds: false, web_identity: false });
    const adv = none.querySelector("details.form-advanced");
    return {
      noneSummary: /No credentials set directly/.test(none.textContent),
      keysSummary: /access keys set in an environment variable/.test(keys.textContent),
      ecsSummary: /found an ECS task role/.test(ecs.textContent),
      irsaSummary: /EKS service-account role/.test(irsa.textContent),
      sharedSummary: /shared ~\/\.aws config file/.test(shared.textContent),
      hasDisclosure: !!adv,
      disclosureCollapsed: !!adv && !adv.open,
      hasToggle: !!none.querySelector("summary.form-adv-summary"),
      rawRowCount: none.querySelectorAll(".kv").length,
    };
  });
  cred.noneSummary ? ok("aws-creds: no signals renders the ambient-chain summary") : bad("aws-creds: no signals renders the ambient-chain summary", "wording missing");
  cred.keysSummary ? ok("aws-creds: static env keys take priority in the summary") : bad("aws-creds: static env keys take priority in the summary", "wording missing");
  cred.ecsSummary ? ok("aws-creds: ECS task role reads as an IAM role, not an error") : bad("aws-creds: ECS task role reads as an IAM role, not an error", "wording missing");
  cred.irsaSummary ? ok("aws-creds: EKS IRSA reads as an IAM role, not an error") : bad("aws-creds: EKS IRSA reads as an IAM role, not an error", "wording missing");
  cred.sharedSummary ? ok("aws-creds: shared ~/.aws config/profile never reads as 'no credentials'") : bad("aws-creds: shared ~/.aws config/profile never reads as 'no credentials'", "wording missing/contradicts raw signal");
  cred.hasDisclosure ? ok("aws-creds: raw signals are folded behind a details disclosure") : bad("aws-creds: raw signals are folded behind a details disclosure", "no details.form-advanced");
  cred.disclosureCollapsed ? ok("aws-creds: raw-signals disclosure is collapsed by default") : bad("aws-creds: raw-signals disclosure is collapsed by default", "rendered open");
  cred.hasToggle ? ok("aws-creds: disclosure uses the app's toggle style") : bad("aws-creds: disclosure uses the app's toggle style", "missing summary.form-adv-summary");
  cred.rawRowCount === 4 ? ok("aws-creds: all four raw signal rows still render") : bad("aws-creds: all four raw signal rows still render", `rawRowCount=${cred.rawRowCount}`);

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
