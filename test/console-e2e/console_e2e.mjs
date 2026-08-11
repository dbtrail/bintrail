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
// Read-fixture coordinates (seeded by run.sh into the boot index; see the
// fixture block there). FIX is the seeded schema; TT_AT is a timestamp just
// after the last seeded event, inside the hour whose partition exists — using
// the default `at` (now) would race the top-of-hour partition boundary.
const FIX = process.env.E2E_FIX_SCHEMA || "e2eshop";
const TT_AT = process.env.E2E_TT_AT || "";
// The query_text canary run.sh embeds in the seeded UPDATE event. query_text /
// query_hash are captured index data but NEVER cross the DTO layer (#699);
// this string appearing anywhere in the page or an export is a leak.
const CANARY = "e2e-canary-query-text";

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

  // Scenario 0b — the sidebar footer reports the running build (#1221). This
  // harness builds the daemon without -ldflags, so the server reports "dev";
  // the assertion is against the SERVER's own value, not a hardcoded string,
  // so a CONSOLE_BIN=<released build> run still passes. What it really guards
  // is the blank/garbage artifact: an empty row, or a label glued together as
  // "v" + a missing value.
  const sideVer = await page.evaluate(() => ({
    label: (document.querySelector("#meta-version b") || {}).textContent || "",
    reported: typeof capsCache !== "undefined" ? capsCache.version : undefined,
  }));
  const wantVer = /^v?\d+\.\d+\.\d+/.test(String(sideVer.reported || ""))
    ? "v" + String(sideVer.reported).replace(/^v/, "")
    : (String(sideVer.reported || "") || "dev");
  sideVer.label === wantVer
    ? ok("sidebar: running version shown")
    : bad("sidebar: running version shown", `label=${JSON.stringify(sideVer.label)} want=${JSON.stringify(wantVer)} (capabilities version=${JSON.stringify(sideVer.reported)})`);

  // Scenario 0c — the shapes the live daemon cannot produce, driven through the
  // REAL function (same technique as the ext-view scenarios below: stub
  // capsCache, call the production code, read the DOM). This harness builds
  // without -ldflags, so the server always reports the literal "dev" and 0b
  // above only ever exercises that branch; the released-build expectation has
  // to come from a FIXED input with a HARDCODED expectation, or dropping the
  // "v" prefix — or a regex that stops matching semver — ships green. (It does
  // NOT cover the leading-"v" strip: that is classification-only and produces
  // an identical string either way, which is why app.js says so in prose
  // instead of pretending a test pins it.) The other two shapes: `version` is
  // omitempty, so a Config with an empty Version sends no key at all and must
  // read "dev", never "" or "vundefined"; and a capabilities fetch that FAILED
  // is a different state — it must keep the "—" placeholder rather than report
  // "dev" for a build it never read.
  const degraded = await page.evaluate(() => {
    const keep = capsCache.version;
    const read = () => (document.querySelector("#meta-version b") || {}).textContent;
    capsCache.version = "1.2.3";
    updateSideVersion(true);
    const semver = read();
    delete capsCache.version;
    updateSideVersion(true);
    const unversioned = read();
    updateSideVersion(false);
    const unknown = read();
    capsCache.version = keep;
    updateSideVersion(true);
    return { semver, unversioned, unknown, restored: read() };
  });
  degraded.semver === "v1.2.3"
    ? ok("sidebar: a released version renders as vX.Y.Z")
    : bad("sidebar: a released version renders as vX.Y.Z", `got ${JSON.stringify(degraded.semver)} for capabilities version "1.2.3"`);
  degraded.unversioned === "dev"
    ? ok("sidebar: unversioned build falls back to 'dev'")
    : bad("sidebar: unversioned build falls back to 'dev'", `got ${JSON.stringify(degraded.unversioned)}`);
  degraded.unknown === "—"
    ? ok("sidebar: failed capabilities fetch never claims a version")
    : bad("sidebar: failed capabilities fetch never claims a version", `got ${JSON.stringify(degraded.unknown)}`);
  degraded.restored === wantVer
    ? ok("sidebar: version restored after the degraded probes")
    : bad("sidebar: version restored after the degraded probes", `got ${JSON.stringify(degraded.restored)}`);

  // Scenario 0d — the WIRING, not the function. 0c calls updateSideVersion(false)
  // directly, which proves the "—" branch exists but says nothing about the
  // caller ever reaching it: hardcoding updateSideVersion(true), or initialising
  // capsOK to true, passes 0b and 0c untouched and ships a console that reports
  // "dev" for a build it never read. So drive the real gateCapabilities() with a
  // FAILING /api/capabilities.
  // Two traps this is shaped around. The stub must answer 500, never 401 —
  // gateCapabilities RETHROWS a 401, and an unhandled rejection inside
  // page.evaluate surfaces as a harness crash instead of a failed assertion. And
  // it must restore with a real gateCapabilities() before returning: the failure
  // path sets capsCache = {}, which strips every cap-on class and would break the
  // control-plane scenarios below (scenario 10 models the same restore).
  const wiring = await page.evaluate(async () => {
    const realFetch = window.fetch;
    window.fetch = (p, o) => (typeof p === "string" && p.startsWith("/api/capabilities")
      ? Promise.resolve(new Response('{"error":"boom"}', { status: 500, headers: { "Content-Type": "application/json" } }))
      : realFetch(p, o));
    await gateCapabilities();
    window.fetch = realFetch;
    const onFailure = (document.querySelector("#meta-version b") || {}).textContent;
    await gateCapabilities();
    return { onFailure, restored: (document.querySelector("#meta-version b") || {}).textContent };
  });
  wiring.onFailure === "—"
    ? ok("sidebar: a failing capabilities fetch actually reaches the '—' branch")
    : bad("sidebar: a failing capabilities fetch actually reaches the '—' branch", `got ${JSON.stringify(wiring.onFailure)} — capsOK never false?`);
  wiring.restored === wantVer
    ? ok("sidebar: version repainted once capabilities recover")
    : bad("sidebar: version repainted once capabilities recover", `got ${JSON.stringify(wiring.restored)}`);

  // Scenario 0e — the coverage card's capture-liveness rendering (#1227),
  // driven through the REAL covCard with stub payloads (the technique the
  // ext-view scenarios use). The thing under test is not "a chip appeared" but
  // that the SAME lag number renders differently depending on the verdict: a
  // dead daemon and a quiet source used to paint identical amber, which is the
  // ambiguity this whole epic exists to remove.
  const cov = await page.evaluate(() => {
    const read = (c) => {
      const card = covCard(c);
      const chips = Array.from(card.querySelectorAll(".cov-chip")).map((n) => ({ text: n.textContent, cls: n.className }));
      const lines = Array.from(card.querySelectorAll(".cov-line")).map((n) => ({ text: n.textContent, cls: n.className }));
      return { chips, lines };
    };
    const base = { delta_from: "2026-08-01 00:00:00", delta_to: "2026-08-07 00:00:00", continuity: "ok", lag_seconds: 3600 };
    return {
      stalled: read({ ...base, freshness: "stalled", checkpoint_age_seconds: 3600 }),
      idle: read({ ...base, freshness: "idle" }),
      current: read({ ...base, freshness: "current", lag_seconds: 5 }),
      none: read({ delta_from: "2026-08-01 00:00:00", delta_to: "2026-08-07 00:00:00", continuity: "none", freshness: "none" }),
    };
  });
  const chipFor = (r, prefix) => r.chips.find((c) => c.text.startsWith(prefix)) || { text: "", cls: "" };

  // stalled: the lag chip must go RED, not amber — the window's upper edge is
  // frozen and changes since are not recoverable.
  chipFor(cov.stalled, "capture lag").cls.includes("bad")
    ? ok("coverage: a stalled stream paints the lag chip as an error")
    : bad("coverage: a stalled stream paints the lag chip as an error", `cls=${chipFor(cov.stalled, "capture lag").cls}`);
  cov.stalled.lines.some((l) => l.cls.includes("bad") && /STALLED/.test(l.text))
    ? ok("coverage: stalled renders an explicit error line")
    : bad("coverage: stalled renders an explicit error line", JSON.stringify(cov.stalled.lines));

  // idle: the SAME 3600s lag must NOT read as an error, and the card must say
  // it cannot tell a quiet source from a lagging one.
  chipFor(cov.idle, "capture lag").cls.includes("bad")
    ? bad("coverage: an idle stream is not an error state", "the lag chip is red — idle is not a fault")
    : ok("coverage: an idle stream is not an error state");
  cov.idle.lines.some((l) => /identical/.test(l.text))
    ? ok("coverage: idle admits it cannot tell quiet from lagging")
    : bad("coverage: idle admits it cannot tell quiet from lagging", JSON.stringify(cov.idle.lines));

  chipFor(cov.current, "capture lag").cls.includes("ok")
    ? ok("coverage: a current stream paints the lag chip green")
    : bad("coverage: a current stream paints the lag chip green", `cls=${chipFor(cov.current, "capture lag").cls}`);

  // "none" is a NON-CLAIM (a file-mode index ran no capture). Green there would
  // paint the absence of a claim as assurance — the same rule continuity follows.
  chipFor(cov.none, "capture ").cls.includes("ok")
    ? bad("coverage: a file-mode index never claims healthy capture", "the freshness chip is green for a no-claim")
    : ok("coverage: a file-mode index never claims healthy capture");

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

  // Scenario 8b — capture-degraded banner (#1296). captureHealthBox is pure
  // like continuityBox. What it must NOT do again: render advice written in
  // this file. The cause/remedy/scope prose is built by the backend
  // (status.ExplainCaptureSkips) and shipped in capture_health.explanation, so
  // `bintrail status` and the console cannot drift — and the half that drifted
  // would be the one saying what a remedy does NOT recover. The fixture drives
  // both the current backend (explanation present) and an older one (absent).
  const cap = await page.evaluate(() => {
    // schemaSnapshotButton() is gated on the capability and on a REAL registry
    // server (the reserved "default" is the daemon's own CLI stream, which the
    // control plane refuses). Force both, restored below: without them the
    // button is null and the placement assertions would pass by rendering
    // nothing, which is the shape this whole scenario exists to catch.
    const keepCaps = capsCache.schema_snapshot_trigger, keepServer = currentServer;
    capsCache.schema_snapshot_trigger = true;
    currentServer = "e2e-registry-server";
    const withExpl = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        skipped: { table_not_in_snapshot: { count: 3, tables: ["shop.plugin_log"] } },
        explanation: [
          "shop.plugin_log changed on the source but is missing from the schema snapshot.",
          "None of this recovers what was already skipped.",
        ],
      },
    });
    const legacy = captureHealthBox({
      capture_health: { status: "degraded", total_skipped: 3, skipped: { table_not_in_snapshot: { count: 3 } } },
    });
    const okBox = captureHealthBox({ capture_health: { status: "ok" } });
    const nilBox = captureHealthBox(null);
    // Historic vs active (#1312): the SAME tally, distinguished only by the
    // backend's skips_predate_snapshot. Historic must go quiet without going
    // away — the events are still permanently missing.
    const historic = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        snapshot_at: "2026-08-11 12:00:00", skips_predate_snapshot: true,
        skipped: { table_not_in_snapshot: { count: 3 } },
        explanation: ["Nothing has been skipped since the current schema snapshot was taken (2026-08-11 12:00:00)."],
      },
    });
    // Render it to confirm the per-paragraph spacing rule resolves — without it
    // the caveat merges into the wall of text above it. The lines now live in a
    // <details>, so it must be opened before measuring: a closed disclosure has
    // no layout, and a marginTop read from it would prove nothing.
    let lineGap = "", detailsClosedByDefault = null, buttonAtTopLevel = null, historicButtonInDetails = null;
    if (withExpl) {
      document.body.appendChild(withExpl);
      const det = withExpl.querySelector("details");
      detailsClosedByDefault = !!det && !det.open;
      // The action stays at the top level while the skips are ACTIVE — that is
      // the state where pressing it is the fix.
      buttonAtTopLevel = !!withExpl.querySelector(":scope > .warn-actions");
      if (det) det.open = true;
      const line = withExpl.querySelector(".warn-line");
      lineGap = line ? getComputedStyle(line).marginTop : "";
      withExpl.remove();
    }
    if (historic) {
      document.body.appendChild(historic);
      // ...and moves inside the disclosure once they are HISTORIC: it has been
      // pressed already, and a button under the headline invites pressing it
      // forever against a tally that will never move.
      historicButtonInDetails = !historic.querySelector(":scope > .warn-actions")
        && !!historic.querySelector("details .warn-actions");
      historic.remove();
    }
    capsCache.schema_snapshot_trigger = keepCaps;
    currentServer = keepServer;
    return {
      showsBackendProse: !!withExpl && /shop\.plugin_log/.test(withExpl.textContent)
        && /recovers what was already skipped/.test(withExpl.textContent),
      noOperatorBlame: !!withExpl && !/take a fresh snapshot on the source/.test(withExpl.textContent)
        && !/check the capture log/.test(withExpl.textContent),
      legacyFallback: !!legacy && /too old to say why/.test(legacy.textContent)
        && !/recovers what was already skipped/.test(legacy.textContent),
      okNone: okBox === null,
      nilNone: nilBox === null,
      lineGap,
      detailsClosedByDefault,
      buttonAtTopLevel,
      historicButtonInDetails,
      activeIsOrange: !!withExpl && withExpl.classList.contains("warn-box"),
      historicIsQuiet: !!historic && historic.classList.contains("ok-box")
        && !historic.classList.contains("warn-box"),
      historicStillStatesLoss: !!historic && /missing from the index for good/.test(historic.textContent),
      historicDatesTheSnapshot: !!historic && /2026-08-11 12:00:00/.test(historic.textContent),
      // An old daemon sends no anchor: no claim, so it must stay loud.
      legacyStaysLoud: !!legacy && legacy.classList.contains("warn-box"),
    };
  });
  cap.showsBackendProse ? ok("capture health: banner renders the backend's explanation (table named)") : bad("capture health: banner renders the backend's explanation (table named)", JSON.stringify(cap));
  cap.noOperatorBlame ? ok("capture health: the old blame-the-operator wording is gone") : bad("capture health: the old blame-the-operator wording is gone", "old copy still rendered");
  cap.legacyFallback ? ok("capture health: an old backend gets a fallback that promises nothing") : bad("capture health: an old backend gets a fallback that promises nothing", "fallback missing or invents advice");
  (cap.okNone && cap.nilNone) ? ok("capture health: ok/nil render no banner") : bad("capture health: ok/nil render no banner", `ok=${cap.okNone} nil=${cap.nilNone}`);
  (cap.lineGap && cap.lineGap !== "0px") ? ok("capture health: explanation paragraphs are spaced apart (CSS applied)") : bad("capture health: explanation paragraphs are spaced apart (CSS applied)", `marginTop=${cap.lineGap}`);
  cap.detailsClosedByDefault ? ok("capture health: the long explanation starts collapsed") : bad("capture health: the long explanation starts collapsed", "details rendered open — the banner is a wall of text again");
  cap.activeIsOrange ? ok("capture health: an active skip stays the orange alarm") : bad("capture health: an active skip stays the orange alarm", "active skips lost their alarm styling");
  cap.historicIsQuiet ? ok("capture health: skips that predate the current snapshot go quiet") : bad("capture health: skips that predate the current snapshot go quiet", "historic skips still render as a live alarm");
  cap.historicStillStatesLoss ? ok("capture health: the quiet box still states the events are gone for good") : bad("capture health: the quiet box still states the events are gone for good", "going quiet dropped the permanent-loss statement");
  cap.historicDatesTheSnapshot ? ok("capture health: the quiet box names the snapshot it compared against") : bad("capture health: the quiet box names the snapshot it compared against", "no snapshot date — the operator cannot check the claim");
  cap.legacyStaysLoud ? ok("capture health: a backend with no anchor stays loud") : bad("capture health: a backend with no anchor stays loud", "missing anchor was read as 'quiet' — that hides a live failure");
  cap.buttonAtTopLevel ? ok("capture health: the fix button is under the headline while skips are active") : bad("capture health: the fix button is under the headline while skips are active", "the actionable state buried its action");
  cap.historicButtonInDetails ? ok("capture health: the quiet box moves the button into the disclosure") : bad("capture health: the quiet box moves the button into the disclosure", "a pressed button still sits under the headline");

  // Scenario 8c — the remedy is reachable from the UI (#1296). The whole point
  // of the issue: the banner named an action with no button anywhere. It must
  // appear for a monitored REGISTRY server, and never for the reserved boot
  // entry (which the endpoint refuses — a button that 409s is worse than none).
  const capBtn = await page.evaluate(async () => {
    const servers = (await api("/api/servers")).servers || [];
    const registryServer = servers.find((s) => s.kind !== "ephemeral");
    const before = currentServer;
    const probe = (id) => {
      currentServer = id;
      const box = captureHealthBox({ capture_health: { status: "degraded", total_skipped: 1, skipped: {} } });
      const btn = box ? Array.from(box.querySelectorAll("button")).find((b) => /Refresh schema snapshot/.test(b.textContent)) : null;
      return !!btn;
    };
    try {
      return {
        capOn: !!capsCache.schema_snapshot_trigger,
        // Reported, not assumed: with no registry server the button check would
        // otherwise pass by testing nothing.
        foundRegistryServer: !!registryServer,
        onRegistry: registryServer ? probe(registryServer.id) : false,
        onBoot: probe("default"),
      };
    } finally {
      // Restore even on a throw: page.evaluate throwing crashes this harness,
      // but a half-restored selection would silently point every later scenario
      // at the wrong server.
      currentServer = before;
    }
  });
  capBtn.capOn ? ok("capture health: schema_snapshot_trigger capability reaches the frontend") : bad("capture health: schema_snapshot_trigger capability reaches the frontend", "capsCache.schema_snapshot_trigger falsy");
  (capBtn.foundRegistryServer && capBtn.onRegistry)
    ? ok("capture health: Refresh-schema-snapshot button renders for a registry server")
    : bad("capture health: Refresh-schema-snapshot button renders for a registry server",
      capBtn.foundRegistryServer ? "no button in the banner" : "no registry server in the fixture — the check tested nothing");
  !capBtn.onBoot ? ok("capture health: no Refresh button for the command-line entry") : bad("capture health: no Refresh button for the command-line entry", "offered an action the endpoint refuses");

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


  // Scenario 10 — extension views (embedding builds). The stock binary ships no
  // ConsoleViewProvider, so this fixture-drives the pure client wiring (like
  // scenarios 7-9): inject an advertised view into capsCache, then assert the
  // nav item appears, the route "ext-<id>" mounts the module and calls its
  // render(mount, {apiBase, api}), and a server switch both drops the nav item
  // and abandons an in-flight render (serverGen staleness). Script() is stubbed
  // with a blob-URL ES module — a same-document dynamic import, which works
  // because the console's Content-Security-Policy allows script-src 'self'
  // blob: (securityHeaders, #848); real extension views are same-origin
  // ('self', ext.ConsoleViewProvider.Script), and blob: keeps this stub and
  // client-side-minted module URLs importable. Dropping blob: from script-src
  // breaks this scenario — that is the contract this test pins.
  const extv = await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const modURL = (marker) => URL.createObjectURL(new Blob(
      [`export function render(mount, ctx){ window.${marker} = !!(mount && ctx && typeof ctx.api === "function" && ctx.apiBase); mount.append(document.createTextNode("ext-content")); }`],
      { type: "text/javascript" },
    ));

    // 0) Exercise gateCapabilities END-TO-END: the manual steps below fixture-drive
    //    capsCache/extViews directly, which BYPASSES the one real backend→frontend
    //    wiring line (extViews = capsCache.extension_views inside gateCapabilities).
    //    Stub /api/capabilities so it advertises an extension view, call the real
    //    gateCapabilities(), and assert extViews + the nav were populated FROM the
    //    parsed response — so a one-sided read of a different property (or a rename)
    //    fails a test instead of silently disabling the feature.
    const realFetch = window.fetch;
    window.fetch = (path, opts) => {
      if (typeof path === "string" && path.startsWith("/api/capabilities")) {
        // A realistic caps shape (incl. the always-present `auth` object) so the
        // whole gateCapabilities() tail — applyAuthGate/updateSrvNote — runs as it
        // would against the real backend, not just the extension_views read.
        return Promise.resolve(new Response(
          JSON.stringify({
            monitor: true,
            auth: { password_set: false, auth_kind: "token" },
            extension_views: [{ id: "gated", label: "Gated View", script: modURL("__extgate") }],
          }),
          { status: 200, headers: { "Content-Type": "application/json" } },
        ));
      }
      return realFetch(path, opts);
    };
    await gateCapabilities();
    window.fetch = realFetch;
    const gatedFromCaps = extViews.length === 1 && extViews[0].id === "gated";
    const gatedNav = !!document.querySelector('.nav-item[data-route="ext-gated"]');

    // 1) Advertise one view and sync the nav (mirrors gateCapabilities).
    const script = modURL("__ext1");
    capsCache = { ...capsCache, extension_views: [{ id: "demo", label: "Demo View", script }] };
    extViews = capsCache.extension_views;
    window.__ext1 = undefined;
    syncExtNav();
    const navItem = document.querySelector('.nav-item[data-route="ext-demo"]');
    const navText = navItem ? navItem.textContent.trim() : null;
    const known = isKnownRoute("ext-demo");

    // 2) Navigate → the module mounts and render() runs with apiBase + api.
    navigate("ext-demo");
    await sleep(400);
    const onRoute = location.pathname === "/ext-demo";
    const mount = document.querySelector(".ext-view-mount");
    const mountedText = mount ? mount.textContent : "";
    const rendered = window.__ext1;
    const navActive = navItem ? navItem.classList.contains("active") : false;

    // 3) Server switch → the nav item is dropped and the now-unknown ext route
    //    redirects to overview (unmount across a switch).
    serverGen++;
    extViews = [];
    capsCache = { ...capsCache, extension_views: [] };
    syncExtNav();
    const navGone = !document.querySelector('.nav-item[data-route="ext-demo"]');
    const redirected = routeFromLocation() === "overview";

    // 4) Mid-import staleness: start a render, bump serverGen synchronously
    //    (before the dynamic import resolves), and assert render() never runs.
    extViews = [{ id: "demo2", label: "Demo Two", script: modURL("__ext2") }];
    window.__ext2 = undefined;
    const p = renderExtensionView(extViews[0]); // captures gen synchronously
    serverGen++;                                // switch before the import settles
    await p;
    const staleAborted = window.__ext2 === undefined;

    // Restore the console's REAL capabilities (the stock binary ships no provider,
    // so this clears the stubbed ext nav) and leave the UI on a known-good route
    // for the final no-errors assertion.
    await gateCapabilities();
    navigate("overview");
    return { gatedFromCaps, gatedNav, navText, known, onRoute, mounted: !!mount, mountedText, rendered, navActive, navGone, redirected, staleAborted };
  });
  extv.gatedFromCaps ? ok("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views") : bad("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views", "extViews not set from the parsed caps response");
  extv.gatedNav ? ok("ext-view: gateCapabilities injects the nav item from the fetched caps") : bad("ext-view: gateCapabilities injects the nav item from the fetched caps", "no ext-gated nav item after gateCapabilities");
  extv.navText === "Demo View" ? ok("ext-view: nav item injected with the provider label") : bad("ext-view: nav item injected with the provider label", `navText=${extv.navText}`);
  extv.known ? ok("ext-view: isKnownRoute accepts a live ext-<id> route") : bad("ext-view: isKnownRoute accepts a live ext-<id> route", "ext-demo not known");
  extv.onRoute ? ok("ext-view: navigate routes to /ext-<id>") : bad("ext-view: navigate routes to /ext-<id>", "wrong route");
  extv.mounted ? ok("ext-view: a mount node is created") : bad("ext-view: a mount node is created", "no .ext-view-mount");
  extv.rendered ? ok("ext-view: the module render() runs with {apiBase, api}") : bad("ext-view: the module render() runs with {apiBase, api}", `rendered=${extv.rendered}`);
  /ext-content/.test(extv.mountedText) ? ok("ext-view: module content lands in the mount") : bad("ext-view: module content lands in the mount", `mountedText=${extv.mountedText}`);
  extv.navActive ? ok("ext-view: the nav item is marked active on its route") : bad("ext-view: the nav item is marked active on its route", "not .active");
  extv.navGone ? ok("ext-view: server switch removes the ext nav item (idempotent)") : bad("ext-view: server switch removes the ext nav item (idempotent)", "stale nav item remained");
  extv.redirected ? ok("ext-view: an ext route the new server lacks redirects to overview") : bad("ext-view: an ext route the new server lacks redirects to overview", "did not redirect");
  extv.staleAborted ? ok("ext-view: a server switch mid-import abandons the render (serverGen staleness)") : bad("ext-view: a server switch mid-import abandons the render (serverGen staleness)", "render ran after the switch");


  // Scenario 11 — CSV formula-injection guard (#965). The events CSV export
  // includes attacker-controlled values (pk_values/row_before/row_after from
  // the monitored DB); a leading =, +, -, @, tab or CR must be neutralized
  // with a leading quote or Excel/Sheets executes it on open. Calls the REAL
  // embedded csvCell — deleting the guard line in app.js turns this red.
  const csv = await page.evaluate(() => ({
    formula: csvCell("=HYPERLINK(\"http://evil.example\",\"click\")"),
    plus: csvCell("+1"),
    minus: csvCell("-2"),
    at: csvCell("@cmd"),
    tab: csvCell("\tx"),
    cr: csvCell("\rx"),
    plain: csvCell("hello"),
    number: csvCell(42),
    quoted: csvCell('a,"b'),
    objFormula: csvCell({ id: 1 }), // JSON starts with { — must NOT be prefixed
  }));
  csv.formula === '"\'=HYPERLINK(""http://evil.example"",""click"")"' ? ok("csv: leading = is quote-prefixed (and CSV-quoted)") : bad("csv: leading = is quote-prefixed (and CSV-quoted)", csv.formula);
  csv.plus === "'+1" && csv.minus === "'-2" && csv.at === "'@cmd" ? ok("csv: leading +/-/@ are quote-prefixed") : bad("csv: leading +/-/@ are quote-prefixed", JSON.stringify([csv.plus, csv.minus, csv.at]));
  csv.tab === "'\tx" ? ok("csv: leading tab is quote-prefixed") : bad("csv: leading tab is quote-prefixed", JSON.stringify(csv.tab));
  csv.cr === '"\'\rx"' ? ok("csv: leading CR is quote-prefixed and still CSV-quoted") : bad("csv: leading CR is quote-prefixed and still CSV-quoted", JSON.stringify(csv.cr));
  csv.plain === "hello" && csv.number === "42" ? ok("csv: benign values pass through untouched") : bad("csv: benign values pass through untouched", JSON.stringify([csv.plain, csv.number]));
  csv.quoted === '"a,""b"' ? ok("csv: existing quoting logic unchanged") : bad("csv: existing quoting logic unchanged", JSON.stringify(csv.quoted));
  csv.objFormula === '"{""id"":1}"' ? ok("csv: JSON object cells are not prefixed") : bad("csv: JSON object cells are not prefixed", JSON.stringify(csv.objFormula));

  // ── Scenarios 12-15: the primary READ workflows over REAL indexed data ─────
  // (#970/#686/#619). Everything below runs against the "byo-idx" server from
  // scenario 5 — its index is the boot db run.sh provisioned with `bintrail
  // init`, seeded events, and a real `bintrail baseline` snapshot (which the
  // daemon-level --baseline-dir exposes to every server, making this one
  // Time-travel-capable).
  await page.evaluate(async (id) => { await switchServer(id); }, byoId);

  // Scenario 12 — Events renders real rows, and the redaction contract holds
  // end-to-end (#970): query_text/query_hash are captured index data that must
  // NEVER reach the browser (#699 — the canary is IN the seeded UPDATE row),
  // while connection_id must PASS THROUGH (#701 D1 un-gated it; asserting
  // absence here would pin the stale pre-#701 contract).
  await page.evaluate(() => navigate("events"));
  await page.waitForSelector("#ev-rows .ev-row", { timeout: 10000 });
  const evr = await page.evaluate(({ FIX, CANARY }) => {
    const rows = Array.from(document.querySelectorAll("#ev-rows .ev-row"));
    const updRow = rows.find((r) => r.textContent.includes(FIX + ".orders") && r.textContent.includes("UPDATE"));
    let diffText = "";
    if (updRow) {
      updRow.click(); // expands and lazy-renders the before/after diff
      diffText = updRow.nextElementSibling ? updRow.nextElementSibling.textContent : "";
    }
    const upd = lastEvents.find((e) => e.event_type === "UPDATE" && e.schema_name === FIX && e.table_name === "orders") || {};
    return {
      rowCount: rows.length,
      updFound: !!updRow,
      diffText,
      domCanary: document.body.textContent.includes(CANARY),
      leakedKeys: lastEvents.filter((e) => ("query_text" in e) || ("query_hash" in e)).length,
      connId: upd.connection_id,
    };
  }, { FIX, CANARY });
  evr.rowCount === 6 ? ok("events: all 6 seeded events render") : bad("events: all 6 seeded events render", `rowCount=${evr.rowCount}`);
  evr.updFound ? ok("events: the seeded orders UPDATE row renders") : bad("events: the seeded orders UPDATE row renders", "row not found");
  (/shipped/.test(evr.diffText) && /new/.test(evr.diffText))
    ? ok("events: expanding a row shows the before/after diff")
    : bad("events: expanding a row shows the before/after diff", `diffText=${evr.diffText.slice(0, 120)}`);
  !evr.domCanary ? ok("events: query_text never reaches the DOM") : bad("events: query_text never reaches the DOM", "canary found in page text");
  evr.leakedKeys === 0 ? ok("events: no query_text/query_hash keys in the wire events") : bad("events: no query_text/query_hash keys in the wire events", `${evr.leakedKeys} event(s) carry them`);
  evr.connId === 777 ? ok("events: connection_id passes through (#701 D1)") : bad("events: connection_id passes through (#701 D1)", `connection_id=${JSON.stringify(evr.connId)}`);

  // Scenario 12b — the export path (#970): drive the REAL JSON/CSV buttons and
  // capture the blobs downloadBlob mints. Export is over the on-screen
  // redacted DTOs — so it must stay query_text-free WITH connection_id, and
  // the CSV header must stay in lockstep with EVENT_CSV_COLUMNS.
  const exp = await page.evaluate(async (CANARY) => {
    const btns = Array.from(document.querySelectorAll(".result-bar button"));
    const jsonBtn = btns.find((b) => b.textContent.trim() === "Export JSON");
    const csvBtn = btns.find((b) => b.textContent.trim() === "Export CSV");
    const blobs = [];
    const orig = URL.createObjectURL;
    URL.createObjectURL = (b) => { blobs.push(b); return orig.call(URL, b); };
    jsonBtn.click();
    csvBtn.click();
    // Export re-fetches the whole filtered set (#1297), so the blob appears a
    // round-trip after the click — poll instead of reading it synchronously.
    for (let i = 0; i < 100 && blobs.length < 2; i++) await new Promise((r) => setTimeout(r, 50));
    URL.createObjectURL = orig;
    const [jsonText, csvText] = await Promise.all(blobs.map((b) => b.text()));
    const arr = JSON.parse(jsonText);
    const header = csvText.split("\r\n")[0];
    return {
      n: arr.length,
      jsonLeak: arr.filter((e) => ("query_text" in e) || ("query_hash" in e)).length,
      jsonConn: arr.every((e) => "connection_id" in e),
      jsonCanary: jsonText.includes(CANARY),
      headerLockstep: header === EVENT_CSV_COLUMNS.join(","),
      headerConn: header.split(",").includes("connection_id"),
      headerLeak: /query_text|query_hash/.test(header),
      csvCanary: csvText.includes(CANARY),
      csvConnVal: csvText.split("\r\n").some((l) => l.split(",").includes("777")),
    };
  }, CANARY);
  exp.n === 6 ? ok("export: JSON blob carries every match of the search") : bad("export: JSON blob carries every match of the search", `n=${exp.n}`);
  (exp.jsonLeak === 0 && !exp.jsonCanary) ? ok("export: JSON blob is query_text/query_hash-free") : bad("export: JSON blob is query_text/query_hash-free", `leak=${exp.jsonLeak} canary=${exp.jsonCanary}`);
  exp.jsonConn ? ok("export: JSON blob keeps connection_id") : bad("export: JSON blob keeps connection_id", "some entry lacks the key");
  exp.headerLockstep ? ok("export: CSV header in lockstep with EVENT_CSV_COLUMNS") : bad("export: CSV header in lockstep with EVENT_CSV_COLUMNS", "header drifted");
  (exp.headerConn && !exp.headerLeak) ? ok("export: CSV columns include connection_id, never query_text") : bad("export: CSV columns include connection_id, never query_text", `conn=${exp.headerConn} leak=${exp.headerLeak}`);
  (!exp.csvCanary && exp.csvConnVal) ? ok("export: CSV rows redacted but carry the connection_id value") : bad("export: CSV rows redacted but carry the connection_id value", `canary=${exp.csvCanary} conn777=${exp.csvConnVal}`);

  // Scenario 12b — Events keyset paging (#1297). The view used to render one
  // window and stop: event 101 was unreachable except by inventing a filter,
  // and the header ("100 event(s) in the newest 100 events") restated the limit
  // instead of saying whether anything was behind it. The fixture seeds 6
  // events, so a page size of 5 splits them 5 + 1 — enough to prove the cursor
  // hands off without repeating or skipping a row, which is the only thing a
  // paging bug ever does.
  const pg1 = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.limit.value = "5";
    f.requestSubmit();
    // Wait for the re-render to settle on the smaller page.
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 5) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      ids: lastEvents.map((e) => e.event_id),
      note: ($("#ev-count-note", VIEW()) || {}).textContent || "",
      prevDisabled: document.getElementById("ev-prev").disabled,
      nextDisabled: document.getElementById("ev-next").disabled,
    };
  });
  pg1.rows === 5 ? ok("events paging: a page-size of 5 renders 5 of the 6 seeded events") : bad("events paging: a page-size of 5 renders 5 of the 6 seeded events", `rows=${pg1.rows}`);
  pg1.prevDisabled ? ok("events paging: Newer is disabled on the first page") : bad("events paging: Newer is disabled on the first page", "enabled");
  !pg1.nextDisabled ? ok("events paging: Older is enabled while events remain") : bad("events paging: Older is enabled while events remain", "disabled — event 6 is unreachable");
  /showing 1–5 of more/.test(pg1.note)
    ? ok("events paging: the header states a position, not the limit restated")
    : bad("events paging: the header states a position, not the limit restated", `note=${pg1.note}`);
  !/in the newest 5 events/.test(pg1.note)
    ? ok("events paging: the circular 'in the newest N' phrasing is gone")
    : bad("events paging: the circular 'in the newest N' phrasing is gone", `note=${pg1.note}`);

  const pg2 = await page.evaluate(async () => {
    document.getElementById("ev-next").click();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 1) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      ids: lastEvents.map((e) => e.event_id),
      note: ($("#ev-count-note", VIEW()) || {}).textContent || "",
      prevDisabled: document.getElementById("ev-prev").disabled,
      nextDisabled: document.getElementById("ev-next").disabled,
    };
  });
  pg2.rows === 1 ? ok("events paging: Older reaches the 6th event") : bad("events paging: Older reaches the 6th event", `rows=${pg2.rows}`);
  pg2.ids.every((id) => !pg1.ids.includes(id))
    ? ok("events paging: page 2 repeats no event from page 1 (keyset cut is exclusive)")
    : bad("events paging: page 2 repeats no event from page 1 (keyset cut is exclusive)", `p1=${pg1.ids} p2=${pg2.ids}`);
  pg1.ids.length + pg2.ids.length === 6
    ? ok("events paging: the two pages together cover all 6 events (nothing skipped)")
    : bad("events paging: the two pages together cover all 6 events (nothing skipped)", `p1=${pg1.ids} p2=${pg2.ids}`);
  pg2.nextDisabled ? ok("events paging: Older is disabled at the end of the stream") : bad("events paging: Older is disabled at the end of the stream", "still enabled past the last event");
  !pg2.prevDisabled ? ok("events paging: Newer is enabled off the first page") : bad("events paging: Newer is enabled off the first page", "disabled — the operator cannot get back");
  /showing 6–6 of 6 \(end\)/.test(pg2.note)
    ? ok("events paging: the last page reports the exact total it walked")
    : bad("events paging: the last page reports the exact total it walked", `note=${pg2.note}`);

  // The export decision (#1297): the buttons export every match of the SEARCH,
  // not the page on screen. Paged to page 2 (1 row rendered), an export must
  // still carry all 6 — silently handing an operator a sixth of their evidence
  // would be worse than the un-paged behavior this replaces.
  const expAll = await page.evaluate(async () => {
    const btns = Array.from(document.querySelectorAll(".result-bar button"));
    const jsonBtn = btns.find((b) => b.textContent.trim() === "Export JSON");
    const blobs = [];
    const orig = URL.createObjectURL;
    URL.createObjectURL = (b) => { blobs.push(b); return orig.call(URL, b); };
    jsonBtn.click();
    for (let i = 0; i < 100 && blobs.length < 1; i++) await new Promise((r) => setTimeout(r, 50));
    URL.createObjectURL = orig;
    return {
      n: blobs.length ? JSON.parse(await blobs[0].text()).length : -1,
      rendered: document.querySelectorAll("#ev-rows .ev-row").length,
      label: jsonBtn.textContent.trim(),
      title: jsonBtn.title,
    };
  });
  expAll.n === 6 && expAll.rendered === 1
    ? ok("export: exports every match of the search, not the 1 row on screen")
    : bad("export: exports every match of the search, not the 1 row on screen", `n=${expAll.n} rendered=${expAll.rendered}`);
  (expAll.label === "Export JSON" && /all matches/i.test(expAll.title))
    ? ok("export: the button states its scope in the UI")
    : bad("export: the button states its scope in the UI", `label=${expAll.label} title=${expAll.title}`);

  // A filter edit must DROP the cursor: a cursor names a row that need not
  // exist in the next search, so keeping it would serve a page from the middle
  // of a search the operator never ran.
  const reset = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.limit.value = "";
    f.requestSubmit();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-rows .ev-row").length === 6) break;
    }
    return {
      rows: document.querySelectorAll("#ev-rows .ev-row").length,
      prevDisabled: document.getElementById("ev-prev").disabled,
    };
  });
  (reset.rows === 6 && reset.prevDisabled)
    ? ok("events paging: a filter edit resets to page 1")
    : bad("events paging: a filter edit resets to page 1", `rows=${reset.rows} prevDisabled=${reset.prevDisabled}`);

  // Scenario 13 — Recover actually SUBMITS and renders the reversal SQL
  // (#970). Scenario 6 above only checks the form's DOM exists.
  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  await page.evaluate((FIX) => {
    const f = document.getElementById("recover-form");
    f.elements.schema.value = FIX;
    f.elements.table.value = "orders";
    f.elements.pk.value = "1";
    f.requestSubmit();
  }, FIX);
  await page.waitForSelector("#recover-out #sql-panel", { timeout: 8000 });
  const rec = await page.evaluate(() => ({
    sql: (document.querySelector("#recover-out #sql-panel .code") || {}).textContent || "",
    meta: (document.querySelector("#recover-out #sql-panel .lbl") || {}).textContent || "",
    cascadeBanner: !!document.querySelector("#recover-out .ctx-banner"),
  }));
  (/UPDATE/.test(rec.sql) && rec.sql.includes("'new'"))
    ? ok("recover: submit renders the reversal SQL")
    : bad("recover: submit renders the reversal SQL", `sql=${rec.sql.slice(0, 160)}`);
  /1 statement\(s\) from 1 event\(s\)/.test(rec.meta)
    ? ok("recover: meta line reports statement/event counts")
    : bad("recover: meta line reports statement/event counts", rec.meta);
  !rec.cascadeBanner ? ok("recover: a plain undo shows no CASCADE banner") : bad("recover: a plain undo shows no CASCADE banner", "banner rendered for a non-parent target");

  // Scenario 13b — the cascade-detected banner (#619): the seeded parent DELETE
  // has two child INSERTs behind an ON DELETE CASCADE fk_constraints row, so
  // /api/recover auto-detects and the positive-half rendering (banner + counts
  // meta) must run. During #617 a missing ')' in this exact block broke the
  // whole SPA and only a manual boot probe caught it — this is that guard.
  await page.evaluate(() => {
    const f = document.getElementById("recover-form");
    f.elements.table.value = "parent";
    f.elements.pk.value = "";
    f.requestSubmit();
  });
  await page.waitForFunction(() => {
    const b = document.querySelector("#recover-out .ctx-banner .badge");
    return b && b.textContent === "CASCADE";
  }, { timeout: 8000 });
  const cas = await page.evaluate(() => ({
    banner: (document.querySelector("#recover-out .ctx-banner") || {}).textContent || "",
    sql: (document.querySelector("#recover-out #sql-panel .code") || {}).textContent || "",
    meta: (document.querySelector("#recover-out #sql-panel .lbl") || {}).textContent || "",
  }));
  cas.banner.includes("restores 2 related row(s)")
    ? ok("cascade: banner counts the repaired child rows")
    : bad("cascade: banner counts the repaired child rows", cas.banner);
  (cas.meta.includes("2 cascade child row(s)") && cas.meta.includes("0 SET NULL restore(s)"))
    ? ok("cascade: meta line carries the cascade-aware counts")
    : bad("cascade: meta line carries the cascade-aware counts", cas.meta);
  (/INSERT INTO/.test(cas.sql) && cas.sql.includes("child") && cas.sql.includes("10") && cas.sql.includes("11"))
    ? ok("cascade: script re-inserts both cascade-deleted children")
    : bad("cascade: script re-inserts both cascade-deleted children", cas.sql.slice(0, 200));

  // Scenario 14 — the row-state half over the fixture baseline (#970), now
  // inside Restore (#1298). Pin the merge itself first: /timetravel must fold
  // into /recover (old bookmarks and Back entries), and a server WITHOUT a
  // baseline must still render the panel with an explanation instead of
  // silently dropping it — an operator who cannot find it has no way to learn
  // that a baseline is what is missing.
  const ttGate = await page.evaluate(() => {
    const had = !!capsCache.reconstruct;
    navigate("timetravel");
    const merged = location.pathname === "/recover";
    capsCache.reconstruct = false;
    navigate("recover");
    const sec = document.querySelector(".state-section");
    const explains = !!sec && !document.querySelector('[name="state_at"]') && !!sec.querySelector(".state-note");
    capsCache.reconstruct = had;
    return { had, merged, explains };
  });
  ttGate.had ? ok("restore: a baseline-configured server reports the reconstruct capability") : bad("restore: a baseline-configured server reports the reconstruct capability", "capsCache.reconstruct falsy on byo-idx");
  ttGate.merged ? ok("restore: /timetravel folds into /recover") : bad("restore: /timetravel folds into /recover", "did not land on /recover");
  ttGate.explains ? ok("restore: no baseline explains itself instead of vanishing") : bad("restore: no baseline explains itself instead of vanishing", "panel absent or still offering controls");

  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  // pk=1: baseline row (status new) + a later UPDATE (status shipped) — the
  // event fold half. email only ever existed in the baseline's full row image.
  // The target is entered ONCE, on the undo form — that single target driving
  // both halves is the merge (#1298).
  await page.evaluate(async ({ FIX, TT_AT }) => {
    const f = document.getElementById("recover-form");
    f.elements.schema.value = FIX;
    await loadTables(f);
    f.elements.table.value = "orders";
    f.elements.pk.value = "1";
    const at = document.querySelector('[name="state_at"]');
    if (TT_AT && at) at.value = TT_AT;
    await runState(f, false);
  }, { FIX, TT_AT });
  await page.waitForSelector("#state-out .statetable", { timeout: 10000 });
  const tt1 = await page.evaluate(() => {
    const cells = {};
    document.querySelectorAll("#state-out .statetable tr").forEach((tr) => {
      cells[tr.querySelector("th").textContent] = tr.querySelector("td").textContent;
    });
    return { cells, meta: (document.querySelector("#state-out .meta-line") || {}).textContent || "" };
  });
  (tt1.cells.status === "shipped" && tt1.cells.email === "a@example.com")
    ? ok("restore: reconstructed row folds the event over the baseline")
    : bad("restore: reconstructed row folds the event over the baseline", JSON.stringify(tt1.cells));
  /baseline /.test(tt1.meta) ? ok("restore: meta line names the baseline anchor") : bad("restore: meta line names the baseline anchor", tt1.meta);

  // pk=4: exists ONLY in the baseline (no events) — a binlog-only reconstruct
  // cannot resolve it, so this pins the baseline half of baseline+deltas.
  await page.evaluate(async () => { const f = document.getElementById("recover-form"); f.elements.pk.value = "4"; await runState(f, false); });
  await page.waitForFunction(() => /d@example\.com/.test((document.getElementById("state-out") || {}).textContent || ""), { timeout: 10000 });
  const tt4 = await page.evaluate(() => {
    const cells = {};
    document.querySelectorAll("#state-out .statetable tr").forEach((tr) => {
      cells[tr.querySelector("th").textContent] = tr.querySelector("td").textContent;
    });
    return cells;
  });
  (tt4.status === "new" && tt4.email === "d@example.com")
    ? ok("restore: a never-touched row resolves from the baseline alone")
    : bad("restore: a never-touched row resolves from the baseline alone", JSON.stringify(tt4));

  // pk=2: in the baseline, then DELETEd — must render the deleted note, not an
  // empty table and not the stale baseline value.
  await page.evaluate(async () => { const f = document.getElementById("recover-form"); f.elements.pk.value = "2"; await runState(f, false); });
  await page.waitForFunction(() => !!document.querySelector("#state-out .deleted-note"), { timeout: 10000 });
  const tt2 = await page.evaluate(() => (document.querySelector("#state-out .deleted-note") || {}).textContent || "");
  /Row was deleted/.test(tt2)
    ? ok("restore: a deleted row renders the deleted note")
    : bad("restore: a deleted row renders the deleted note", tt2);

  // The bridge that makes the two halves one errand: the state on screen
  // becomes the undo window that produces it. since = at + 1s reverses
  // precisely the events AFTER `at`, because reconstruct applies <= at while
  // recover reverses >= since. Passing `at` itself would be off by every event
  // sharing that second — these indexes routinely carry dozens.
  const bridge = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    f.elements.pk.value = "1";
    await runState(f, false);
    const btn = document.querySelector(".state-actions .btn-primary");
    if (!btn) return { err: "no Restore-to-this-state button on a found row" };
    const at = (document.querySelector("#state-out .meta-line") || {}).textContent || "";
    btn.click();
    return { since: f.elements.since.value, until: f.elements.until.value, at };
  });
  {
    const m = /as of (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/.exec(bridge.at || "");
    const want = m ? new Date(Date.parse(m[1].replace(" ", "T") + "Z") + 1000).toISOString().slice(0, 19).replace("T", " ") : null;
    (want && bridge.since === want && bridge.until === "")
      ? ok("restore: 'Restore to this state' sets the undo window to at+1s")
      : bad("restore: 'Restore to this state' sets the undo window to at+1s", JSON.stringify({ ...bridge, want }));
  }

  // The timeline is how an operator picks an instant without leaving to read
  // one off Events. Its own restore button used to route through undoEvent,
  // which sets `until` — reversing everything UP TO that point and landing the
  // row before its whole history, the opposite end from the state the button
  // names. Pin both actions against a real render.
  const tl = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    f.elements.pk.value = "1";
    f.elements.since.value = "";
    f.elements.until.value = "2000-01-01 00:00:00"; // stale bound that must be cleared
    await runState(f, true);
    const nodes = Array.from(document.querySelectorAll(".tl-node"));
    if (!nodes.length) return { err: "no timeline nodes" };
    const last = nodes[nodes.length - 1];
    const time = (last.querySelector(".tl-time") || {}).textContent || "";
    const restore = last.querySelector(".tl-restore");
    const use = last.querySelector(".tl-use");
    if (!use) return { err: "no 'Use this moment' action" };
    use.click();
    const at = (document.querySelector('[name="state_at"]') || {}).value || "";
    if (!restore) return { time, at, skippedRestore: true };
    restore.click();
    return { time, at, since: f.elements.since.value, until: f.elements.until.value };
  });
  if (tl.err) {
    bad("restore: timeline offers per-node actions", tl.err);
  } else {
    (tl.at === tl.time)
      ? ok("restore: 'Use this moment' fills the At field from the timeline")
      : bad("restore: 'Use this moment' fills the At field from the timeline", JSON.stringify(tl));
    if (tl.skippedRestore) {
      ok("restore: timeline restore skipped (baseline-only node)");
    } else {
      const want = new Date(Date.parse(tl.time.replace(" ", "T") + "Z") + 1000).toISOString().slice(0, 19).replace("T", " ");
      (tl.since === want && tl.until === "")
        ? ok("restore: timeline 'Restore to this state' aims AFTER the instant, not before it")
        : bad("restore: timeline 'Restore to this state' aims AFTER the instant, not before it", JSON.stringify({ ...tl, want }));
    }
  }

  // Scenario 14c — the Overview against the LIVE daemon (#1300). The fixture
  // scenario below drives buildOverview directly, so it cannot catch a route
  // that was never registered, an authz table that refuses it, or a JSON shape
  // that drifted from the struct. This one renders the real page: two tiles
  // must come back carrying a real period, and the window line must name the
  // aggregate's own bounds rather than the "—" a failed fetch would leave.
  await page.evaluate(() => navigate("overview"));
  // Wait on the TILES, not on the scope lines: waiting on the thing under test
  // turns a missing scope into a driver timeout that aborts the rest of the run
  // instead of one legible failure.
  await page.waitForFunction(() => document.querySelectorAll(".ov-stat").length === 4, { timeout: 10000 });
  const ovLive = await page.evaluate(() => ({
    scopes: Array.from(document.querySelectorAll(".ov-stat")).map((n) => (n.querySelector(".ov-stat-scope") || {}).textContent || ""),
    win: (document.querySelector(".ov-coverage") || {}).textContent || "",
    warns: Array.from(document.querySelectorAll(".warn-item")).map((n) => n.textContent),
  }));
  ovLive.scopes.every((s) => s.trim() !== "")
    ? ok("overview (live): every rendered tile carries a scope line")
    : bad("overview (live): every rendered tile carries a scope line", JSON.stringify(ovLive.scopes));
  (ovLive.scopes.filter((s) => /^last \d+ h/.test(s)).length === 2)
    ? ok("overview (live): the window tiles carry a real period from /api/activity")
    : bad("overview (live): the window tiles carry a real period from /api/activity", JSON.stringify(ovLive));
  (/window\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(ovLive.win) && !ovLive.warns.some((w) => /window counts could not be loaded/.test(w)))
    ? ok("overview (live): the window line states the aggregate's own bounds")
    : bad("overview (live): the window line states the aggregate's own bounds", JSON.stringify(ovLive));

  // Scenario 15 — Overview scope honesty (#686 + #1300): fixture-drive the REAL
  // buildOverview and pin that (a) every tile states its OWN scope, (b) the
  // period-scoped numbers come from the /api/activity aggregate rather than
  // from the handful of events the Recent-changes list renders, and (c) the
  // window line uses the AGGREGATE's bounds — a reintroduced
  // status.coverage.oldest fallback (#679/#684) fails here and nowhere else,
  // since the Go suite never renders app.js.
  //
  // The fixture is built so the two sources DISAGREE on purpose: the events
  // array holds 1 delete across 1 table, the aggregate says 17 deletes across
  // 5. A tile that recomputes from the events (the pre-#1300 behaviour) prints
  // 1 and 1 and fails.
  const mkev = (ts, type, pk) => ({ event_timestamp: ts, schema_name: "ovfix", table_name: "t", event_type: type, pk_values: pk, changed_columns: [] });
  const ovFix = {
    events: [mkev("2026-03-01 10:30:00", "DELETE", "9"), mkev("2026-03-01 10:00:00", "INSERT", "8")],
    // oldest is a decade before the aggregate's window: if it ever reaches the
    // window line or a tile, the number is attributed to the wrong span.
    status: { coverage: { oldest: "2020-01-01 00:00:00", newest: "2026-06-30 00:00:00", total_events: 123456 }, total_events_estimate: 3121 },
    activity: {
      period: "24h", label: "last 24 h",
      since: "2026-03-01 09:00:00", until: "2026-03-02 09:00:00",
      total: 640, inserts: 400, updates: 223, deletes: 17, other: 0,
      tables: 5, complete: true,
      top_tables: [{ schema: "ovfix", table: "orders", insert: 300, update: 200, delete: 10, total: 510 }],
    },
  };

  const ov = await page.evaluate((fx) => {
    const readTiles = () => Array.from(document.querySelectorAll(".ov-stat")).map((n) => ({
      v: (n.querySelector(".ov-stat-v") || {}).textContent || "",
      k: (n.querySelector(".ov-stat-k") || {}).textContent || "",
      scope: (n.querySelector(".ov-stat-scope") || {}).textContent || "",
    }));
    const notes = () => Array.from(document.querySelectorAll(".warn-item")).map((n) => n.textContent);
    buildOverview(fx.status, { events: fx.events }, null, fx.activity);
    const full = {
      tiles: readTiles(),
      win: (document.querySelector(".ov-coverage") || {}).textContent || "",
    };
    // Degraded arms, rendered through the SAME real function: a partial
    // aggregate must SAY partial on the tiles it scopes (a narrower number
    // under a wider label is the bug), and a failed aggregate must render "—"
    // rather than a zero nobody measured.
    buildOverview(fx.status, { events: fx.events }, null,
      Object.assign({}, fx.activity, { complete: false, notes: ["3 hour(s) of this window have been archived to Parquet and are NOT counted here."] }));
    const partial = { tiles: readTiles(), notes: notes() };
    buildOverview(fx.status, { events: fx.events }, null, null);
    const missing = { tiles: readTiles(), notes: notes() };
    return { full, partial, missing };
  }, ovFix);

  const tileBy = (tiles, key) => tiles.find((t) => t.k === key) || { v: "", k: "", scope: "" };
  (ov.full.tiles.length === 4 && ov.full.tiles.every((t) => t.scope.trim() !== ""))
    ? ok("overview: every tile states its own scope")
    : bad("overview: every tile states its own scope", JSON.stringify(ov.full.tiles));
  (tileBy(ov.full.tiles, "deletes").v === "17" && tileBy(ov.full.tiles, "deletes").scope.includes("last 24 h"))
    ? ok("overview: the deletes tile is the server aggregate, labelled with its period")
    : bad("overview: the deletes tile is the server aggregate, labelled with its period", JSON.stringify(tileBy(ov.full.tiles, "deletes")));
  (tileBy(ov.full.tiles, "tables touched").v === "5" && tileBy(ov.full.tiles, "tables touched").scope.includes("last 24 h"))
    ? ok("overview: the tables-touched tile is the server aggregate, labelled with its period")
    : bad("overview: the tables-touched tile is the server aggregate, labelled with its period", JSON.stringify(tileBy(ov.full.tiles, "tables touched")));
  // total_events_estimate is information_schema TABLE_ROWS — an InnoDB
  // estimate. It sits beside three exact counts, so the tile has to say so.
  (tileBy(ov.full.tiles, "changes indexed").scope.includes("all time") && /estimate/i.test(tileBy(ov.full.tiles, "changes indexed").scope))
    ? ok("overview: the all-time tile declares both its scope and that it is an estimate")
    : bad("overview: the all-time tile declares both its scope and that it is an estimate", JSON.stringify(tileBy(ov.full.tiles, "changes indexed")));
  (ov.full.win.includes("2026-03-01 09:00:00") && ov.full.win.includes("2026-03-02 09:00:00") && !ov.full.win.includes("2020-01-01"))
    ? ok("overview: window line uses the aggregate's own bounds")
    : bad("overview: window line uses the aggregate's own bounds", ov.full.win);
  (tileBy(ov.partial.tiles, "deletes").scope.includes("partial") && ov.partial.notes.some((n) => /archived/.test(n)))
    ? ok("overview: an incomplete window is marked partial on the tile and explained")
    : bad("overview: an incomplete window is marked partial on the tile and explained", JSON.stringify(ov.partial));
  (tileBy(ov.missing.tiles, "deletes").v === "—" && tileBy(ov.missing.tiles, "tables touched").v === "—" && ov.missing.notes.length > 0)
    ? ok("overview: a failed aggregate shows no number, never a zero")
    : bad("overview: a failed aggregate shows no number, never a zero", JSON.stringify(ov.missing));

  // Scenario 15b — Storage page live (#686): with the daemon opted in
  // (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1) and this server baseline-configured,
  // the Create-baseline button must render enabled, and the fixture snapshot
  // (1 table, anchored at binlog.000001:50) must be listed.
  await page.evaluate(() => navigate("storage"));
  await page.waitForFunction(() => Array.from(document.querySelectorAll(".stg-row")).some((r) => r.textContent.includes("binlog.000001:50")), { timeout: 10000 });
  const stg = await page.evaluate(() => {
    const heads = Array.from(document.querySelectorAll(".ov-panel-head"));
    const bHead = heads.find((h) => /Baseline snapshots/.test(h.textContent));
    const btn = bHead ? Array.from(bHead.querySelectorAll("button")).find((b) => b.textContent === "Create baseline") : null;
    const row = Array.from(document.querySelectorAll(".stg-row")).find((r) => r.textContent.includes("binlog.000001:50"));
    return { capOn: !!capsCache.baseline_trigger, btnPresent: !!btn, btnEnabled: btn ? !btn.disabled : false, rowText: row ? row.textContent : "" };
  });
  stg.capOn ? ok("storage: baseline_trigger capability reaches the frontend") : bad("storage: baseline_trigger capability reaches the frontend", "capsCache.baseline_trigger falsy");
  (stg.btnPresent && stg.btnEnabled) ? ok("storage: Create-baseline button renders enabled when both gates pass") : bad("storage: Create-baseline button renders enabled when both gates pass", `present=${stg.btnPresent} enabled=${stg.btnEnabled}`);
  stg.rowText.includes("1 table(s)") ? ok("storage: the fixture snapshot is listed with its table count") : bad("storage: the fixture snapshot is listed with its table count", stg.rowText);

  // Scenario 15c — the button's other gate arms, fixture-driven through the
  // REAL baselinesPanel (destination-missing can't exist live once the daemon
  // sets a default --baseline-dir): no destination → no button + the setup
  // empty state; capability off → no button even with a destination.
  const gates = await page.evaluate(() => {
    const servers = [{ id: "srv-fix", name: "fixture" }];
    const keepCur = currentServer;
    currentServer = "srv-fix";
    const cfgOff = baselinesPanel({ configured: false }, servers);
    const keepCap = capsCache.baseline_trigger;
    capsCache.baseline_trigger = false;
    const capOff = baselinesPanel({ configured: true, source: "/tmp/baselines", snapshots: [] }, servers);
    capsCache.baseline_trigger = keepCap;
    currentServer = keepCur;
    const hasBtn = (n) => Array.from(n.querySelectorAll("button")).some((b) => b.textContent === "Create baseline");
    return {
      cfgOffBtn: hasBtn(cfgOff),
      cfgOffEmpty: /No baselines configured/.test(cfgOff.textContent),
      capOffBtn: hasBtn(capOff),
      capOffEmpty: /no snapshots found/.test(capOff.textContent),
    };
  });
  (!gates.cfgOffBtn && gates.cfgOffEmpty)
    ? ok("storage: no baseline destination → no button, setup empty state")
    : bad("storage: no baseline destination → no button, setup empty state", JSON.stringify(gates));
  (!gates.capOffBtn && gates.capOffEmpty)
    ? ok("storage: baseline_trigger off → no button even with a destination")
    : bad("storage: baseline_trigger off → no button even with a destination", JSON.stringify(gates));

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
