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
// The #1365 archive fixture (run.sh): an index whose history is mostly
// rotated into Parquet, with a manufactured coverage-gap hour. Deliberately
// no fallback default — an unset env must fail the scenario loudly, never
// skip it.
const ARC_DB = process.env.E2E_ARC_DB || "";
const ARC_GAP_SINCE = process.env.E2E_ARC_GAP_SINCE || "";
const ARC_GAP_UNTIL = process.env.E2E_ARC_GAP_UNTIL || "";
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
    // Acknowledged (#1314): the same permanent loss, retired by an operator.
    // It must go MUTED (not the green "nothing since the snapshot" box — that
    // one is a claim about capture, this one is a claim about a human), keep
    // stating the loss, and stop offering the button it already consumed.
    const acked = captureHealthBox({
      capture_health: {
        status: "degraded", total_skipped: 3, last_skip_at: "2026-08-04 19:49:33",
        acknowledged: true, acknowledged_at: "2026-08-11 20:14:00",
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
    let ackedGap = "", ackedPaint = null;
    if (acked) {
      document.body.appendChild(acked);
      const det = acked.querySelector("details");
      if (det) det.open = true;
      const line = acked.querySelector(".warn-line");
      ackedGap = line ? getComputedStyle(line).marginTop : "";
      // The whole point of the muted box is that it is QUIET, and a CSS
      // custom property that does not exist resolves to nothing — the rule
      // matches, the spacing assertion above passes, and the box paints with
      // the page's default colours. (That is not hypothetical: the first
      // draft of this rule referenced three tokens this stylesheet has never
      // defined.) Read the real computed paint and compare it against the
      // orange alarm's.
      const mc = getComputedStyle(acked);
      let wc = null;
      if (withExpl) {
        document.body.appendChild(withExpl);
        const w = getComputedStyle(withExpl);
        wc = { color: w.color, bg: w.backgroundColor, border: w.borderTopColor };
        withExpl.remove();
      }
      ackedPaint = {
        color: mc.color, bg: mc.backgroundColor, border: mc.borderTopColor,
        transparentBg: mc.backgroundColor === "rgba(0, 0, 0, 0)" || mc.backgroundColor === "transparent",
        differsFromAlarm: !!wc && (mc.color !== wc.color && mc.backgroundColor !== wc.bg && mc.borderTopColor !== wc.border),
      };
      acked.remove();
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
      // Mark-as-read (#1314). The button must be reachable WITHOUT opening the
      // disclosure while the record is unacknowledged: it is the one thing an
      // operator staring at a permanent record actually wants.
      ackButtonAtTopLevel: !!withExpl && !!withExpl.querySelector(":scope > .ack-actions button"),
      ackedIsMuted: !!acked && acked.classList.contains("muted-box")
        && !acked.classList.contains("warn-box") && !acked.classList.contains("ok-box"),
      // ...and gone once acknowledged: pressing it again would 400.
      ackedHasNoAckButton: !!acked && !acked.querySelector(".ack-actions"),
      ackedStillStatesLoss: !!acked && /missing from the index for good/.test(acked.textContent),
      ackedNamesTheMoment: !!acked && /2026-08-11 20:14:00/.test(acked.textContent),
      // The muted box must inherit the paragraph spacing, or its disclosure is
      // the wall of text the orange one stopped being.
      ackedGap,
      ackedPaint,
    };
  });
  // Scenario 8b-bis — /api/events warnings must REACH the screen (#1311). The
  // server computed the archive-exclusion notice correctly and the events view
  // dropped `data.warnings` on the floor, so the flagship case -- a profiled
  // session's default browse -- was silent in the UI while the API said the
  // right thing. A server-side test cannot see that; this can.
  const evWarn = await page.evaluate(() => {
    const box = document.createElement("div");
    box.id = "probe-warnings";
    document.body.appendChild(box);
    renderWarnings(box, ["LIVE INDEX ONLY probe notice"]);
    const rendered = /LIVE INDEX ONLY probe notice/.test(box.textContent);
    box.remove();
    return {
      rendered,
      // The container the events view must build, by id. Without it
      // renderWarnings has nowhere to write and the notice is lost.
      hasEventsContainer: /id:\s*"ev-warnings"/.test(renderEvents.toString()),
      // ...and it must actually be filled from the response.
      wiredToResponse: /#ev-warnings/.test(runEventsQuery.toString())
        && /data\.warnings/.test(runEventsQuery.toString()),
      // previewRecover shares #recover-warnings with generateUndo; clearing it
      // made the notice vanish the moment a filter was adjusted.
      previewKeepsServerWarnings: /data\.warnings/.test(previewRecover.toString())
        && !/clear\(warns\)/.test(previewRecover.toString()),
    };
  });
  evWarn.rendered ? ok("warnings: renderWarnings paints a notice into a container") : bad("warnings: renderWarnings paints a notice into a container", JSON.stringify(evWarn));
  evWarn.hasEventsContainer ? ok("warnings: the events view builds a warnings container") : bad("warnings: the events view builds a warnings container", "no #ev-warnings — an API notice has nowhere to land");
  evWarn.wiredToResponse ? ok("warnings: the events query renders the response's own warnings") : bad("warnings: the events query renders the response's own warnings", "data.warnings is computed server-side and dropped at the browser");
  evWarn.previewKeepsServerWarnings ? ok("warnings: Preview keeps the server's notices instead of clearing them") : bad("warnings: Preview keeps the server's notices instead of clearing them", "re-previewing wipes the scope caveat");

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
  cap.ackButtonAtTopLevel ? ok("capture health: Mark-as-read is reachable without opening the disclosure") : bad("capture health: Mark-as-read is reachable without opening the disclosure", "the only action on a permanent record is buried");
  cap.ackedIsMuted ? ok("capture health: an acknowledged record renders muted, not as an alarm") : bad("capture health: an acknowledged record renders muted, not as an alarm", "acknowledging did not calm the box");
  cap.ackedHasNoAckButton ? ok("capture health: an acknowledged record stops offering Mark-as-read") : bad("capture health: an acknowledged record stops offering Mark-as-read", "a button that would now 400 is still on screen");
  cap.ackedStillStatesLoss ? ok("capture health: acknowledging keeps the permanent-loss statement on screen") : bad("capture health: acknowledging keeps the permanent-loss statement on screen", "going quiet erased the evidence");
  cap.ackedNamesTheMoment ? ok("capture health: the acknowledged box names when it was acknowledged") : bad("capture health: the acknowledged box names when it was acknowledged", "no timestamp — the operator cannot tell who saw what when");
  (cap.ackedGap && cap.ackedGap !== "0px") ? ok("capture health: the muted box inherits the paragraph spacing (CSS applied)") : bad("capture health: the muted box inherits the paragraph spacing (CSS applied)", `marginTop=${cap.ackedGap}`);
  (cap.ackedPaint && !cap.ackedPaint.transparentBg && cap.ackedPaint.differsFromAlarm)
    ? ok("capture health: the muted box paints its own quiet palette (every token resolves)")
    : bad("capture health: the muted box paints its own quiet palette (every token resolves)", JSON.stringify(cap.ackedPaint));

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
      // The stub exercises the CONTRACT, not just its key names: it calls
      // ctx.ui.dateField and inspects what comes back. A presence check would
      // have passed with the binding moved to a sibling key, or with the
      // builder's parameters reordered — both of which hand an extension a
      // broken widget and no error. Only a real call can tell.
      [`export function render(mount, ctx){
         const f = ctx && ctx.ui && ctx.ui.dateField;
         const node = typeof f === "function" ? f("Since (UTC)", "since", "md", "YYYY-MM-DD HH:MM:SS") : null;
         window.${marker} = !!(mount && ctx && typeof ctx.api === "function" && ctx.apiBase);
         window.${marker}_ui = !!(node
           && node.classList.contains("field")
           && node.querySelector(".dt-trigger")
           && node.querySelector("input") && node.querySelector("input").name === "since"
           && node.querySelector(".field-label")
           && node.querySelector(".field-label").textContent.includes("Since (UTC)"));
         mount.append(document.createTextNode("ext-content"));
       }`],
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
    window.__ext1_ui = undefined;
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
    const renderedUI = window.__ext1_ui;
    const navActive = navItem ? navItem.classList.contains("active") : false;

    // 2b) The OTHER extension surface. Settings panels take the same contract
    //     from the same builder, and until now nothing here drove them at all
    //     — which mattered because the settings surface is the one that was
    //     forgotten when the shared builder was introduced: the view's call
    //     was widened and the panel's was left hand-rolled. A guard comment
    //     claiming "the browser tier covers the shape" was true of the view
    //     and false of this, so the leg exists rather than the caveat.
    //
    //     Driven through renderExtensionSettings directly: the panel's nav
    //     lives under Settings and its routing is exercised elsewhere; what
    //     is unproven is the contract it hands its module.
    window.__extset = undefined;
    window.__extset_ui = undefined;
    await renderExtensionSettings({ id: "demoset", label: "Demo Panel", script: modURL("__extset") });
    await sleep(200);
    const setRendered = window.__extset;
    const setRenderedUI = window.__extset_ui;

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
    return { gatedFromCaps, gatedNav, navText, known, onRoute, mounted: !!mount, mountedText, rendered, renderedUI, setRendered, setRenderedUI, navActive, navGone, redirected, staleAborted };
  });
  extv.gatedFromCaps ? ok("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views") : bad("ext-view: gateCapabilities populates extViews from /api/capabilities.extension_views", "extViews not set from the parsed caps response");
  extv.gatedNav ? ok("ext-view: gateCapabilities injects the nav item from the fetched caps") : bad("ext-view: gateCapabilities injects the nav item from the fetched caps", "no ext-gated nav item after gateCapabilities");
  extv.navText === "Demo View" ? ok("ext-view: nav item injected with the provider label") : bad("ext-view: nav item injected with the provider label", `navText=${extv.navText}`);
  extv.known ? ok("ext-view: isKnownRoute accepts a live ext-<id> route") : bad("ext-view: isKnownRoute accepts a live ext-<id> route", "ext-demo not known");
  extv.onRoute ? ok("ext-view: navigate routes to /ext-<id>") : bad("ext-view: navigate routes to /ext-<id>", "wrong route");
  extv.mounted ? ok("ext-view: a mount node is created") : bad("ext-view: a mount node is created", "no .ext-view-mount");
  extv.rendered ? ok("ext-view: the module render() runs with {apiBase, api, ui}") : bad("ext-view: the module render() runs with {apiBase, api, ui}", `rendered=${extv.rendered}`);
  // The widget half, asserted by USING it. The Go guards pin the binding's
  // spelling in app.js; only this can say the extension receives something
  // that builds the console's own date field — right wrapper, right label,
  // right input name, calendar attached.
  extv.renderedUI ? ok("ext-view: ctx.ui.dateField builds the console's own date field") : bad("ext-view: ctx.ui.dateField builds the console's own date field", `renderedUI=${extv.renderedUI}`);
  // Same two assertions for the settings surface, because "both surfaces get
  // the same contract" is the claim and one of them was already forgotten once.
  extv.setRendered ? ok("ext-settings: the panel module render() runs with {apiBase, api, ui}") : bad("ext-settings: the panel module render() runs with {apiBase, api, ui}", `setRendered=${extv.setRendered}`);
  extv.setRenderedUI ? ok("ext-settings: ctx.ui.dateField builds the console's own date field") : bad("ext-settings: ctx.ui.dateField builds the console's own date field", `setRenderedUI=${extv.setRenderedUI}`);
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

  // Scenario 12tz — timezone discipline on Events (#1354): the column header
  // declares the zone once, and each row keeps the EXACT wire text — so it
  // copy-pastes into Since/Until/At unchanged — with the viewer's local
  // equivalent as a hover tooltip. Never an unlabeled browser-local render.
  const evtz = await page.evaluate(() => {
    const head = document.querySelector(".ev-head span");
    const t = document.querySelector("#ev-rows .ev-time");
    return {
      headCol: head ? head.textContent : "",
      text: t ? t.textContent : "",
      title: t ? t.getAttribute("title") || "" : "",
    };
  });
  evtz.headCol === "time (UTC)"
    ? ok("events: the time column declares UTC")
    : bad("events: the time column declares UTC", JSON.stringify(evtz));
  (/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/.test(evtz.text) && evtz.title.startsWith("UTC — in your local time:"))
    ? ok("events: rows keep exact UTC text with a local-time tooltip")
    : bad("events: rows keep exact UTC text with a local-time tooltip", JSON.stringify(evtz));

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
    // Arrive carrying the Undo bridge's per-row cap, which is what an operator
    // who used Undo first would have in the field.
    f.elements.limit_per_pk.value = "1";
    btn.click();
    return { since: f.elements.since.value, until: f.elements.until.value, cap: f.elements.limit_per_pk.value, at };
  });
  {
    const m = /as of (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})/.exec(bridge.at || "");
    const want = m ? new Date(Date.parse(m[1].replace(" ", "T") + "Z") + 1000).toISOString().slice(0, 19).replace("T", " ") : null;
    (want && bridge.since === want && bridge.until === "")
      ? ok("restore: 'Restore to this state' sets the undo window to at+1s")
      : bad("restore: 'Restore to this state' sets the undo window to at+1s", JSON.stringify({ ...bridge, want }));
    // The two bridges set opposite scopes on one form. This action reverses
    // EVERY change after the instant — that is what makes the row land on the
    // state shown — so a cap inherited from Undo would reverse only the newest
    // and land it elsewhere, silently, with the button still naming the state
    // it did not produce.
    bridge.cap === ""
      ? ok("restore: 'Restore to this state' clears the Undo bridge's per-row cap")
      : bad("restore: 'Restore to this state' clears the Undo bridge's per-row cap", `limit_per_pk=${bridge.cap}`);
  }

  // The other direction: arriving through Undo must land the cap in the field,
  // because that prefill is what makes the button reverse ONE change instead
  // of the row's whole history. Driven through the real bridge — pendingRecover
  // plus a navigate — rather than by calling the prefill, since the prefill
  // runs inside setSelectWhenReady's schema callback and a direct call would
  // skip the path that can actually break.
  //
  // And watch the POST while it happens. Every other check here reads
  // `limit_per_pk` off the form, which is order-blind: swapping the prefill
  // and the generateUndo call beneath it restores the original #1404 defect —
  // a request with no cap — while leaving the field holding "1" by the time
  // anything polls it. generateUndo snapshots the form synchronously at entry,
  // so the only witness that dies on that swap is the body it sent.
  const sentBodies = [];
  await page.route("**/api/recover", async (route) => {
    sentBodies.push(route.request().postData() || "");
    await route.continue();
  });
  const undoBridge = await page.evaluate(async (fixSchema) => {
    // Snapshot every field, not the schema alone: the scenarios below inherit
    // whatever this form already held (the timeline one sets only `pk` and
    // relies on schema+table being filled), and a re-render blanks all of it.
    {
      const f0 = document.getElementById("recover-form");
      window.__preUndo = {};
      if (f0) for (const n of ["schema", "table", "pk", "limit_per_pk", "since", "until"]) {
        window.__preUndo[n] = f0.elements[n] ? f0.elements[n].value : "";
      }
    }
    pendingRecover = { schema: fixSchema, table: "orders", pk: "1", type: "update", time: "2026-01-01 00:00:00" };
    navigate("recover");
    for (let i = 0; i < 40; i++) {
      const f = document.getElementById("recover-form");
      if (f && f.elements.pk.value === "1") break;
      await new Promise((r) => setTimeout(r, 100));
    }
    const f = document.getElementById("recover-form");
    if (!f) return { err: "no recover form" };
    const eyebrow = (document.querySelector(".ctx-banner .ctx-eyebrow") || {}).textContent || "";
    const detail = Array.from(document.querySelectorAll(".ctx-banner .ctx-detail")).map((n) => n.textContent).join(" ");
    return { cap: f.elements.limit_per_pk.value, until: f.elements.until.value, eyebrow, detail };
  }, FIX);
  (undoBridge.cap === "1" && undoBridge.until === "2026-01-01 00:00:00")
    ? ok("restore: the Undo bridge prefills a per-row cap of 1 alongside the ceiling")
    : bad("restore: the Undo bridge prefills a per-row cap of 1 alongside the ceiling", JSON.stringify(undoBridge));
  // Give the generate its POST, then stop intercepting.
  for (let i = 0; i < 40 && !sentBodies.length; i++) await new Promise((r) => setTimeout(r, 100));
  await page.unroute("**/api/recover");
  {
    const parsed = sentBodies.map((b) => { try { return JSON.parse(b); } catch { return {}; } });
    const capped = parsed.filter((b) => b.limit_per_pk === 1);
    (sentBodies.length > 0 && capped.length === parsed.length)
      ? ok("restore: the Undo bridge's generate SENDS limit_per_pk — the cap is on the wire, not just in the field")
      : bad("restore: the Undo bridge's generate SENDS limit_per_pk — the cap is on the wire, not just in the field",
            JSON.stringify({ sent: sentBodies }));
  }
  // The prefill changes what the button reverses, so the banner has to say it.
  // A prefill the banner does not mention is a silent narrowing.
  (/one change/.test(undoBridge.eyebrow) && /Latest per row is set to 1/.test(undoBridge.detail) && /clear it/.test(undoBridge.detail))
    ? ok("restore: the Undo banner states the cap it prefilled and how to clear it")
    : bad("restore: the Undo banner states the cap it prefilled and how to clear it", JSON.stringify(undoBridge));
  // The two bridges collide one level above the fields. With the Undo banner
  // on screen — the only place in the run where it really is — driving
  // "Restore to this state" must retire it: the banner states "Latest per row
  // is set to 1" as a fact about this form, and that action clears the field.
  // Left up, the operator reads a one-change scope over a script that reverses
  // everything after the instant.
  const staleBanner = await page.evaluate(async () => {
    const f = document.getElementById("recover-form");
    const before = !!document.getElementById("undo-ctx-banner");
    f.elements.pk.value = "1";
    await runState(f, false);
    const btn = document.querySelector(".state-actions .btn-primary");
    if (!btn) return { err: "no Restore-to-this-state button on a found row" };
    btn.click();
    return { before, after: !!document.getElementById("undo-ctx-banner"),
             cap: f.elements.limit_per_pk.value };
  });
  if (staleBanner.err) {
    bad("restore: 'Restore to this state' retires the Undo banner it contradicts", staleBanner.err);
  } else {
    // `before` is asserted too: if the banner were never up, `after === false`
    // would pass while proving nothing.
    (staleBanner.before === true && staleBanner.after === false && staleBanner.cap === "")
      ? ok("restore: 'Restore to this state' retires the Undo banner it contradicts")
      : bad("restore: 'Restore to this state' retires the Undo banner it contradicts", JSON.stringify(staleBanner));
  }

  // Leave the page as this scenario found it. pendingRecover survives a
  // re-render, and the prefilled cap makes every later generate 400 with
  // "the latest-per-row filter needs a PK" the moment a scenario clears the PK
  // — which is a correct server refusal and a wrong reason for fourteen
  // unrelated checks to go red.
  const undoRestore = await page.evaluate(async (fixSchema) => {
    pendingRecover = null;
    navigate("recover");
    for (let i = 0; i < 40; i++) {
      const f = document.getElementById("recover-form");
      if (f && !document.querySelector(".ctx-banner")) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    const f = document.getElementById("recover-form");
    if (!f || !window.__preUndo) return { schemaReady: false, want: null, got: null };
    // Schema first and on its own tick: the cascade listener repopulates the
    // table datalist and CLEARS a stale table value, so restoring table before
    // the cascade settles loses it again.
    // Through setSelectWhenReady, not by assigning .value: populateSchemas
    // fills the options asynchronously after the re-render, and assigning a
    // value the select does not yet carry silently leaves it blank — which is
    // what the Time-travel scenarios below then read as "no rows". The app
    // already owns this wait; using it is also what the real prefill does.
    const schemaReady = await new Promise((resolve) => {
      setSelectWhenReady(f, "schema", fixSchema, () => resolve(true));
      setTimeout(() => resolve(false), 5000);
    });
    await new Promise((r) => setTimeout(r, 300));
    for (const n of ["table", "pk", "limit_per_pk", "since", "until"]) {
      if (f.elements[n]) f.elements[n].value = window.__preUndo[n] || "";
    }
    // Report what the restore actually achieved instead of proceeding as if it
    // had. The schema is compared against the SNAPSHOT, not against fixSchema:
    // reinstalling a hardcoded value while claiming to restore is how this
    // silently installs the wrong schema the day a preceding scenario picks
    // another one.
    return { schemaReady, want: window.__preUndo.schema, got: f.elements.schema.value };
  }, FIX);
  (undoRestore && undoRestore.schemaReady && undoRestore.got === undoRestore.want)
    ? ok("restore: the Undo scenario hands the form back as it found it")
    : bad("restore: the Undo scenario hands the form back as it found it — the checks below inherit it",
          JSON.stringify(undoRestore));

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


  // ── Scenario 17d — the JavaScript half of reduced motion (#1392) ──
  // Placed here, out of numeric order, because this is the one point in the
  // run with a REAL rendered timeline. The stagger under test is scheduled by
  // renderTimeline's own rAF loop; the synthesised DOM 17c uses never runs it.
  //
  // The Go guard and 17c both stop at the stylesheet. This stagger is driven
  // from app.js — one setTimeout per node — so honouring the preference in CSS
  // alone removes the SMOOTHNESS while the loop still moves every node: under
  // `reduce` a long timeline snapped one node at a time across seconds of
  // shifting content, a jumpier version of the motion the preference asked to
  // remove. Only matchMedia can see this, and only at run time.
  //
  // NOT covered: the two scrollIntoView call sites gated on the same helper.
  // Their smooth/auto difference is a browser-internal scroll with no
  // observable DOM state, so this scenario makes no claim about them.
  const staggerProbe = async (mode) => {
    await page.emulateMedia({ reducedMotion: mode });
    return page.evaluate(async () => {
      const f = document.getElementById("recover-form");
      f.elements.pk.value = "1";
      f.elements.since.value = "";
      f.elements.until.value = "2000-01-01 00:00:00";
      await runState(f, true);
      // Record the poll tick at which each node FIRST carries `.in`, instead
      // of sampling once at a fixed instant.
      //
      // What is under test is a relative ordering — setTimeout(60) fires
      // before setTimeout(115) however loaded the runner is — and an ordering
      // survives a stall that a fixed window does not. The single snapshot
      // this replaces needed the second node's 115ms to still be in the
      // future when it read, and this fixture renders exactly two nodes, so
      // that margin was the whole assertion. It also gets STRONGER as the
      // fixture grows rather than weaker.
      const firstSeen = [];
      let arrived = 0, total = 0;
      for (let tick = 0; tick < 90; tick++) {
        const nodes = document.querySelectorAll(".tl-node");
        total = nodes.length;
        arrived = 0;
        nodes.forEach((n, i) => {
          if (!n.classList.contains("in")) return;
          arrived++;
          if (firstSeen[i] === undefined) firstSeen[i] = tick;
        });
        if (total > 0 && arrived === total) break;
        await new Promise((r) => setTimeout(r, 10));
      }
      return { total, arrived, firstSeen };
    });
  };
  const rmTl = await staggerProbe("reduce");
  const npTl = await staggerProbe("no-preference");
  await page.emulateMedia({ reducedMotion: null });

  (rmTl.total > 0 && rmTl.arrived === rmTl.total
    && rmTl.firstSeen.every((t) => t === rmTl.firstSeen[0]))
    ? ok("reduced motion: every timeline node arrives on the same tick — no JS stagger")
    : bad("reduced motion: every timeline node arrives on the same tick — no JS stagger", JSON.stringify(rmTl));
  // The other half, and the reason the arm above is not satisfied by a broken
  // render: under no-preference the nodes must arrive IN SEQUENCE.
  //
  // Two nodes are enough for an order claim, so unlike the snapshot version
  // there is no fixture size at which this quietly stops asserting — and a
  // fixture that drops below two is reported as a failure rather than waved
  // through, because at that point the scenario has proved nothing.
  (npTl.total >= 2 && npTl.arrived === npTl.total && npTl.firstSeen[1] > npTl.firstSeen[0])
    ? ok("reduced motion: under no-preference the timeline arrives one node at a time")
    : bad("reduced motion: under no-preference the timeline arrives one node at a time", JSON.stringify(npTl));

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
  // The coverage card fills independently (#1352); its freshness clock is part
  // of what this scenario pins, so wait for that fill too.
  await page.waitForFunction(() => !!document.querySelector(".cov-card .cov-asof"), { timeout: 10000 });
  const ovLive = await page.evaluate(() => ({
    scopes: Array.from(document.querySelectorAll(".ov-stat")).map((n) => (n.querySelector(".ov-stat-scope") || {}).textContent || ""),
    win: (document.querySelector(".ov-coverage") || {}).textContent || "",
    warns: Array.from(document.querySelectorAll(".warn-item")).map((n) => n.textContent),
    asof: (document.querySelector(".cov-card .cov-asof") || {}).textContent || "",
    tzChips: document.querySelectorAll(".tz-chip").length,
  }));
  ovLive.scopes.every((s) => s.trim() !== "")
    ? ok("overview (live): every rendered tile carries a scope line")
    : bad("overview (live): every rendered tile carries a scope line", JSON.stringify(ovLive.scopes));
  // The window is the live retention (#1352): "live retention · ~N h/d" when a
  // dated partition names the floor, the stated 24 h fallback otherwise.
  (ovLive.scopes.filter((s) => /^(live retention · ~\d+ [hd]|last 24 h)/.test(s)).length === 2)
    ? ok("overview (live): the window tiles carry a real window from /api/activity")
    : bad("overview (live): the window tiles carry a real window from /api/activity", JSON.stringify(ovLive));
  // The aggregate is a server-side materialization (#1352): its freshness must
  // be rendered with its numbers, or a stale count reads as live.
  (ovLive.scopes.filter((s) => / · as of \d{2}:\d{2}:\d{2} UTC/.test(s)).length === 2)
    ? ok("overview (live): the window tiles disclose the aggregate's refresh time, labeled UTC")
    : bad("overview (live): the window tiles disclose the aggregate's refresh time, labeled UTC", JSON.stringify(ovLive));
  (/window \(UTC\)\s+\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/.test(ovLive.win) && !ovLive.warns.some((w) => /window counts could not be loaded/.test(w)))
    ? ok("overview (live): the window line states the aggregate's own bounds and their zone")
    : bad("overview (live): the window line states the aggregate's own bounds and their zone", JSON.stringify(ovLive));
  // Timezone discipline (#1354): the freshness clock is the labeled UTC clock
  // — nowClock() was toLocaleTimeString(), which put an unlabeled browser-local
  // time directly above data timestamps that are all UTC, so the same instant
  // read hours apart on one page. And the sections that render bare data
  // timestamps each carry a UTC chip.
  /as of \d{2}:\d{2}:\d{2} UTC$/.test(ovLive.asof)
    ? ok("overview (live): the freshness clock is UTC and says so")
    : bad("overview (live): the freshness clock is UTC and says so", JSON.stringify(ovLive.asof));
  (ovLive.tzChips >= 2)
    ? ok("overview (live): timestamp-bearing sections carry a UTC chip")
    : bad("overview (live): timestamp-bearing sections carry a UTC chip", `tz chips: ${ovLive.tzChips}`);

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
      label: "live retention · ~24 h", refreshed_at: "2026-03-02 09:00:00",
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
      Object.assign({}, fx.activity, { complete: false, truncated: true, notes: ["This index has more than 20000 table/event-type groups in the window; the counts below are a floor, not the total."] }));
    const partial = { tiles: readTiles(), notes: notes() };
    buildOverview(fx.status, { events: fx.events }, null, null);
    const missing = { tiles: readTiles(), notes: notes() };
    return { full, partial, missing };
  }, ovFix);

  const tileBy = (tiles, key) => tiles.find((t) => t.k === key) || { v: "", k: "", scope: "" };
  (ov.full.tiles.length === 4 && ov.full.tiles.every((t) => t.scope.trim() !== ""))
    ? ok("overview: every tile states its own scope")
    : bad("overview: every tile states its own scope", JSON.stringify(ov.full.tiles));
  (tileBy(ov.full.tiles, "deletes").v === "17" && tileBy(ov.full.tiles, "deletes").scope.includes("live retention") && tileBy(ov.full.tiles, "deletes").scope.includes("as of 09:00:00 UTC"))
    ? ok("overview: the deletes tile is the server aggregate, labelled with its window and freshness")
    : bad("overview: the deletes tile is the server aggregate, labelled with its window and freshness", JSON.stringify(tileBy(ov.full.tiles, "deletes")));
  (tileBy(ov.full.tiles, "tables touched").v === "5" && tileBy(ov.full.tiles, "tables touched").scope.includes("live retention"))
    ? ok("overview: the tables-touched tile is the server aggregate, labelled with its window")
    : bad("overview: the tables-touched tile is the server aggregate, labelled with its window", JSON.stringify(tileBy(ov.full.tiles, "tables touched")));
  // total_events_estimate is information_schema TABLE_ROWS — an InnoDB
  // estimate. It sits beside three exact counts, so the tile has to say so.
  (tileBy(ov.full.tiles, "changes indexed").scope.includes("all time") && /estimate/i.test(tileBy(ov.full.tiles, "changes indexed").scope))
    ? ok("overview: the all-time tile declares both its scope and that it is an estimate")
    : bad("overview: the all-time tile declares both its scope and that it is an estimate", JSON.stringify(tileBy(ov.full.tiles, "changes indexed")));
  (ov.full.win.includes("2026-03-01 09:00:00") && ov.full.win.includes("2026-03-02 09:00:00") && !ov.full.win.includes("2020-01-01"))
    ? ok("overview: window line uses the aggregate's own bounds")
    : bad("overview: window line uses the aggregate's own bounds", ov.full.win);
  (tileBy(ov.partial.tiles, "deletes").scope.includes("partial") && ov.partial.notes.some((n) => /floor/.test(n)))
    ? ok("overview: an incomplete window is marked partial on the tile and explained")
    : bad("overview: an incomplete window is marked partial on the tile and explained", JSON.stringify(ov.partial));
  (tileBy(ov.missing.tiles, "deletes").v === "—" && tileBy(ov.missing.tiles, "tables touched").v === "—" && ov.missing.notes.length > 0)
    ? ok("overview: a failed aggregate shows no number, never a zero")
    : bad("overview: a failed aggregate shows no number, never a zero", JSON.stringify(ov.missing));

  // Scenario 15b — Baselines page live (#686, moved off Storage by #1384):
  // with the daemon opted in (BINTRAIL_CONSOLE_BASELINE_TRIGGER=1) and this
  // server baseline-configured, the Create-baseline button must render
  // enabled, and the fixture snapshot (1 table, anchored at
  // binlog.000001:50) must be listed.
  await page.evaluate(() => navigate("baselines"));
  await page.waitForFunction(() => Array.from(document.querySelectorAll(".stg-row")).some((r) => r.textContent.includes("binlog.000001:50")), { timeout: 10000 });
  const stg = await page.evaluate(() => {
    const heads = Array.from(document.querySelectorAll(".ov-panel-head"));
    const bHead = heads.find((h) => /Baseline snapshots/.test(h.textContent));
    const btn = bHead ? Array.from(bHead.querySelectorAll("button")).find((b) => b.textContent === "Create baseline") : null;
    const row = Array.from(document.querySelectorAll(".stg-row")).find((r) => r.textContent.includes("binlog.000001:50"));
    return { capOn: !!capsCache.baseline_trigger, btnPresent: !!btn, btnEnabled: btn ? !btn.disabled : false, rowText: row ? row.textContent : "" };
  });
  stg.capOn ? ok("baselines: baseline_trigger capability reaches the frontend") : bad("baselines: baseline_trigger capability reaches the frontend", "capsCache.baseline_trigger falsy");
  (stg.btnPresent && stg.btnEnabled) ? ok("baselines: Create-baseline button renders enabled when both gates pass") : bad("baselines: Create-baseline button renders enabled when both gates pass", `present=${stg.btnPresent} enabled=${stg.btnEnabled}`);
  stg.rowText.includes("1 table(s)") ? ok("baselines: the fixture snapshot is listed with its table count") : bad("baselines: the fixture snapshot is listed with its table count", stg.rowText);

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
    ? ok("baselines: no baseline destination → no button, setup empty state")
    : bad("baselines: no baseline destination → no button, setup empty state", JSON.stringify(gates));
  (!gates.capOffBtn && gates.capOffEmpty)
    ? ok("baselines: baseline_trigger off → no button even with a destination")
    : bad("baselines: baseline_trigger off → no button even with a destination", JSON.stringify(gates));

  // Scenario 15d — Protect group (#1384). Baselines and verification moved off
  // Settings > Storage into their own routes. Two halves must hold TOGETHER,
  // and only the first is obvious: each route renders its panel, AND Storage
  // no longer carries it. A move that left a copy behind would sail past a
  // "does /baselines work" check — which is the shape of the regression 15b
  // caught when this scenario did not exist.
  //
  // waitForFunction rather than a sleep: renderBaselines awaits two fetches,
  // and a fixed delay is the classic way this suite goes intermittently red.
  const protectNav = await page.evaluate(() => ({
    baselines: !!document.querySelector('.nav-item[data-route="baselines"]'),
    verification: !!document.querySelector('.nav-item[data-route="verification"]'),
  }));
  (protectNav.baselines && protectNav.verification)
    ? ok("protect: both nav entries render")
    : bad("protect: both nav entries render", JSON.stringify(protectNav));

  await page.evaluate(() => navigate("baselines"));
  await page.waitForFunction(() => location.pathname === "/baselines"
    && Array.from(document.querySelectorAll(".ov-panel-title")).some((h) => /Baseline snapshots/.test(h.textContent)),
    { timeout: 10000 });
  ok("protect: /baselines renders the snapshot panel");

  await page.evaluate(() => navigate("verification"));
  // NOT .ov-panel-title: the page suppresses the panel's own h2 (hideTitle) so
  // it does not sit under an identical h1, which makes that selector
  // unsatisfiable here. Anchor on the page heading and the mode control — the
  // things whose absence would mean the route did not render.
  await page.waitForFunction(() => location.pathname === "/verification"
    && Array.from(document.querySelectorAll("h1.page-title")).some((h) => /Verification/.test(h.textContent))
    && document.querySelector(".vfy-panel-full") !== null,
    { timeout: 10000 });
  ok("protect: /verification renders the verification panel");

  await page.evaluate(() => navigate("storage"));
  await page.waitForFunction(() => location.pathname === "/storage"
    && Array.from(document.querySelectorAll(".ov-panel-title")).some((h) => /S3 archiving/.test(h.textContent)),
    { timeout: 10000 });
  const storageTitles = await page.evaluate(() =>
    Array.from(document.querySelectorAll(".ov-panel-title")).map((h) => h.textContent));
  (!storageTitles.some((t) => /Baseline snapshots|Verification/.test(t)))
    ? ok("protect: Storage no longer carries the moved panels")
    : bad("protect: Storage no longer carries the moved panels", JSON.stringify(storageTitles));

  // Scenario 15e — motion that cannot be seen from Go (#1385).
  //
  // Both of these shipped BROKEN and a text-grep guard in the Go suite passed
  // them. That is the whole reason they live here: both are CASCADE-PRECEDENCE
  // bugs, a class no source-text assertion can observe. ci.yml says as much
  // about this suite — "go test never renders assets/*, so CSS/DOM/presentation
  // bugs are invisible to the Go suite".
  //
  //  1. `animation: … both` filled `transform: none` forward, and animations
  //     outrank normal author declarations, so `.ov-stat:hover { transform }`
  //     never applied — measured matrix(1,0,0,1,0,0). The box-shadow half DID
  //     apply, so it looked half-alive rather than broken.
  //  2. Re-declaring the `transition` SHORTHAND on `.nav-item .ni-icon` reset
  //     transition-property to `transform` alone, so the icon colour snapped.
  await page.evaluate(() => navigate("overview"));
  await page.waitForSelector(".ov-stat", { timeout: 10000 });
  // Let the entrance finish first: a RUNNING animation outranks the author rule
  // too, so hovering early measures the animation rather than the bug.
  await page.waitForTimeout(700);
  await page.hover(".ov-stat");
  await page.waitForTimeout(400); // the lift transitions over .22s
  const lift = await page.evaluate(() => getComputedStyle(document.querySelector(".ov-stat")).transform);
  // translateY(-3px) is the last component of the 2D matrix.
  /-3\)$/.test(lift.replace(/\s/g, ""))
    ? ok("motion: hovering a stat tile actually lifts it")
    : bad("motion: hovering a stat tile actually lifts it",
        `${lift} — an animation filling transform forward outranks the :hover rule`);

  const iconTransition = await page.evaluate(() => {
    const icon = document.querySelector(".nav-item .ni-icon");
    return icon ? getComputedStyle(icon).transitionProperty : "(no icon)";
  });
  (iconTransition.includes("color") && iconTransition.includes("transform"))
    ? ok("motion: the nav icon still transitions colour as well as transform")
    : bad("motion: the nav icon still transitions colour as well as transform",
        `${iconTransition} — re-declaring the transition shorthand resets transition-property`);

  // Scenario 16 — Restore Table combobox (#1364). The Table field must be an
  // input + datalist fed from /api/schemas?schema=… (the same endpoint the
  // events table picker consumes): suggestions from the fixture's tables,
  // swapped on a schema switch, a stale value cleared on switch — and still
  // free text, because recover legitimately targets dropped tables whose
  // events are indexed (that half is pinned in scenario 17's typed-submit leg).
  await page.evaluate(() => navigate("recover"));
  await page.waitForSelector("#recover-form", { timeout: 8000 });
  await page.waitForFunction((FIX) => {
    const f = document.getElementById("recover-form");
    return f && Array.from(f.elements.schema.options).some((o) => o.value === FIX);
  }, FIX, { timeout: 8000 });
  const combo = await page.evaluate(async (FIX) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const f = document.getElementById("recover-form");
    const input = f.elements.table;
    const listId = input.getAttribute("list");
    const dl = listId ? document.getElementById(listId) : null;
    const read = () => (dl ? Array.from(dl.options).map((o) => o.value).sort() : []);
    const hintText = () => (f.querySelector(".combo-hint") || {}).textContent || "";
    const waitFor = async (pred) => { for (let i = 0; i < 80 && !pred(); i++) await sleep(50); return pred(); };
    const pick = (schema) => { f.elements.schema.value = schema; f.elements.schema.dispatchEvent(new Event("change")); };

    // Hint geometry (#1369): the hint is out of flow, so with the hint EMPTY
    // the combo field's box must match its siblings (under .filters'
    // align-items:flex-end an in-flow empty hint pushed the label+input above
    // the row), and SHOWING the hint must not change the field's box. Deltas
    // are relative to the schema field so the view-enter animation (which
    // translates the whole filter panel) can't skew a comparison across time.
    const geom = () => {
      const c = input.closest(".field").getBoundingClientRect();
      const s = f.elements.schema.closest(".field").getBoundingClientRect();
      return { dTop: c.top - s.top, comboH: c.height, sibH: s.height };
    };
    const geomEmpty = geom(); // fresh form: hint is empty

    // The dropped-table flow: a name typed BEFORE the first schema selection
    // must survive that selection — clearing here would make the one recovery
    // a closed <select> can't do impossible from this combobox too.
    input.value = "preseeded_tbl";
    pick(FIX);
    await waitFor(() => read().length > 0);
    const tablesA = read();
    const preseededKept = input.value === "preseeded_tbl";

    // A value from the old schema not in the new listing: suggestions must
    // swap and the stale value must clear (never silently kept).
    input.value = "child";
    pick("e2estock");
    await waitFor(() => read().length > 0 && read().join() !== tablesA.join());
    const tablesB = read();
    const clearedOnSwitch = input.value === "";

    // The keep-arm: "orders" exists in BOTH schemas (shared fixture name), so
    // switching must KEEP it — an unconditional clear-on-switch fails here.
    pick(FIX);
    await waitFor(() => read().join() === tablesA.join());
    input.value = "orders";
    pick("e2estock");
    await waitFor(() => read().join() === tablesB.join());
    const sharedKept = input.value === "orders";

    // A → "— select —" → B must still clear: the empty option must not reset
    // the switch marker and launder B into a "first selection".
    pick(FIX);
    await waitFor(() => read().join() === tablesA.join());
    input.value = "parent";
    pick("");
    await sleep(120);
    const keptOnEmpty = input.value === "parent"; // deselecting alone never clears
    pick("e2estock");
    await waitFor(() => read().join() === tablesB.join());
    const clearedViaEmpty = input.value === "";

    // A FAILED listing: value intact, field usable, aria-busy gone, and the
    // persistent aria-live hint says to type the name (the toast is 2.2s).
    const realFetch = window.fetch;
    window.fetch = (p, o) => (typeof p === "string" && p.startsWith("/api/schemas")
      ? Promise.resolve(new Response('{"error":"boom"}', { status: 500, headers: { "Content-Type": "application/json" } }))
      : realFetch(p, o));
    tablesCache.delete(FIX);
    input.value = "typed_under_failure";
    pick(FIX);
    await waitFor(() => /type the table name/.test(hintText()));
    const geomHint = geom(); // the persistent failure note is showing
    const failed = {
      valueKept: input.value === "typed_under_failure",
      enabled: !input.disabled,
      ariaBusyGone: !input.hasAttribute("aria-busy"),
      hint: hintText(),
    };
    window.fetch = realFetch;
    // ...and a submit still reaches the backend despite the failed listing.
    document.getElementById("recover-out").replaceChildren();
    f.elements.pk.value = "";
    f.elements.since.value = ""; f.elements.until.value = "";
    f.requestSubmit();
    await waitFor(() => !!document.querySelector("#recover-out #sql-panel") && !document.querySelector("#modal .busy-modal"));
    const failedSubmit = !!document.querySelector("#recover-out #sql-panel");
    return {
      isInput: input.tagName === "INPUT",
      hasList: !!dl,
      hasHint: !!f.querySelector(".combo-hint"),
      disabled: input.disabled,
      preseededKept, tablesA, tablesB, clearedOnSwitch, sharedKept,
      keptOnEmpty, clearedViaEmpty, failed, failedSubmit,
      geomEmpty, geomHint,
    };
  }, FIX);
  (Math.abs(combo.geomEmpty.dTop) <= 1 && Math.abs(combo.geomEmpty.comboH - combo.geomEmpty.sibH) <= 1)
    ? ok("restore combo: empty hint reserves no height — field box matches its siblings (#1369)")
    : bad("restore combo: empty hint reserves no height — field box matches its siblings (#1369)", JSON.stringify(combo.geomEmpty));
  (Math.abs(combo.geomHint.dTop) <= 1 && Math.abs(combo.geomHint.comboH - combo.geomEmpty.comboH) <= 1)
    ? ok("restore combo: a showing hint never changes the field box or moves the row (#1369)")
    : bad("restore combo: a showing hint never changes the field box or moves the row (#1369)", JSON.stringify({ empty: combo.geomEmpty, hint: combo.geomHint }));
  (combo.isInput && combo.hasList)
    ? ok("restore combo: Table is an input+datalist combobox, not a closed select")
    : bad("restore combo: Table is an input+datalist combobox, not a closed select", JSON.stringify(combo));
  combo.preseededKept
    ? ok("restore combo: a name typed before the first schema selection survives it")
    : bad("restore combo: a name typed before the first schema selection survives it", "first listing load cleared the typed value");
  combo.tablesA.join(",") === "child,orders,parent"
    ? ok("restore combo: selecting a schema populates suggestions from its listing")
    : bad("restore combo: selecting a schema populates suggestions from its listing", JSON.stringify(combo.tablesA));
  combo.tablesB.join(",") === "inventory,orders"
    ? ok("restore combo: switching schemas swaps the suggestions")
    : bad("restore combo: switching schemas swaps the suggestions", JSON.stringify(combo.tablesB));
  combo.clearedOnSwitch
    ? ok("restore combo: a stale table value is cleared on schema switch")
    : bad("restore combo: a stale table value is cleared on schema switch", "kept the previous schema's table");
  combo.sharedKept
    ? ok("restore combo: a value present in the NEW schema's listing survives the switch")
    : bad("restore combo: a value present in the NEW schema's listing survives the switch", "cleared a value that belongs to the new schema");
  (combo.keptOnEmpty && combo.clearedViaEmpty)
    ? ok("restore combo: routing a switch through '— select —' still clears the stale value")
    : bad("restore combo: routing a switch through '— select —' still clears the stale value", JSON.stringify({ keptOnEmpty: combo.keptOnEmpty, clearedViaEmpty: combo.clearedViaEmpty }));
  (combo.failed.valueKept && combo.failed.enabled && combo.failed.ariaBusyGone)
    ? ok("restore combo: a failed listing keeps the value and the field usable")
    : bad("restore combo: a failed listing keeps the value and the field usable", JSON.stringify(combo.failed));
  /type the table name/.test(combo.failed.hint)
    ? ok("restore combo: a failed listing shows the persistent aria-live hint")
    : bad("restore combo: a failed listing shows the persistent aria-live hint", JSON.stringify(combo.failed.hint));
  combo.failedSubmit
    ? ok("restore combo: a submit still reaches the backend after a failed listing")
    : bad("restore combo: a submit still reaches the backend after a failed listing", "no reversal panel rendered");
  !combo.disabled
    ? ok("restore combo: the field is never disabled")
    : bad("restore combo: the field is never disabled", "input.disabled");

  // Scenario 17 — Generate-undo busy modal (#1363). A slowed /api/recover must
  // open the modal immediately (dialog semantics, facts stated, button
  // disabled), block a second click, close on completion with focus on the
  // reversal.sql header; Cancel and ESC must abort the fetch and restore the
  // pre-click state; an error must render IN the modal, then dismiss cleanly.
  // The stub honors AbortController like real fetch. That alone proves
  // nothing — teardown is synchronous, so the modal closes either way; the
  // wiring is proven by leg B sleeping PAST the stub's delay after Cancel:
  // if api() dropped opts.signal, the un-aborted response lands, renders a
  // panel and moves focus, and the post-delay assertions fail.
  const busyRun = await page.evaluate(async (FIX) => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const f = document.getElementById("recover-form");
    const genBtn = f.querySelector('button[type="submit"]');
    const prevBtn = Array.from(f.querySelectorAll(".filter-actions button")).find((b) => b.textContent === "Preview rows");
    const modalSel = () => document.querySelector("#modal .busy-modal");
    const realFetch = window.fetch;
    const calls = [];
    let mode = "delay", delay = 800;
    window.fetch = (p, o) => {
      if (!(typeof p === "string" && p.startsWith("/api/recover"))) return realFetch(p, o);
      calls.push(o && o.body ? String(o.body) : "");
      if (mode === "error") {
        return Promise.resolve(new Response('{"error":"the index server went away mid-read — check the connection and retry"}',
          { status: 500, headers: { "Content-Type": "application/json" } }));
      }
      return new Promise((resolve, reject) => {
        const t = setTimeout(() => resolve(realFetch(p, o)), delay);
        if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
      });
    };
    try {
      // A) slow generate: modal + facts + disabled button + second click inert.
      f.elements.schema.value = FIX;
      f.elements.table.value = "orders";
      f.elements.pk.value = "1";
      f.elements.since.value = ""; f.elements.until.value = "";
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const m = modalSel();
      const open = {
        present: !!m,
        role: m ? m.getAttribute("role") : "",
        busyAttr: m ? m.getAttribute("aria-busy") : "",
        facts: m ? m.textContent : "",
        btnDisabled: genBtn.disabled,
        prevDisabled: prevBtn ? prevBtn.disabled : false,
        focusInModal: m ? m.contains(document.activeElement) : false,
      };
      genBtn.click();    // disabled click — inert
      f.requestSubmit(); // a keyboard/programmatic submit must hit the re-entry guard
      await sleep(80);
      const secondBlocked = calls.length === 1;
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const done = {
        modalGone: !modalSel(),
        panel: !!document.querySelector("#recover-out #sql-panel"),
        calls: calls.length,
        btnEnabled: !genBtn.disabled,
        focusOnResult: !!document.activeElement && document.activeElement.classList.contains("code-head"),
      };

      // B) Cancel restores the pre-click state — and the abort is PROVEN by
      // sleeping past the stub's short delay afterwards: an un-aborted fetch
      // resolves then, renders a panel and moves focus, failing below.
      document.getElementById("recover-out").replaceChildren();
      delay = 600;
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const cbtn = Array.from((modalSel() || document.createElement("i")).querySelectorAll("button")).find((b) => b.textContent === "Cancel");
      if (cbtn) cbtn.click();
      await sleep(150);
      const afterCancel = {
        modalGone: !modalSel(),
        noPanel: !document.querySelector("#recover-out #sql-panel"),
        btnEnabled: !genBtn.disabled,
        focusBack: document.activeElement === genBtn,
        calls: calls.length, // 2: the canceled call fired once, never retried
      };
      await sleep(900); // PAST the stub's delay
      afterCancel.noPanelAfterDelay = !document.querySelector("#recover-out #sql-panel");
      afterCancel.focusStillBack = document.activeElement === genBtn;

      // B2) ESC aborts the same way (same past-delay proof).
      genBtn.focus(); genBtn.click();
      await sleep(120);
      const escOpened = !!modalSel();
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(150);
      const afterEsc = { opened: escOpened, modalGone: !modalSel(), btnEnabled: !genBtn.disabled, noPanel: !document.querySelector("#recover-out #sql-panel") };
      await sleep(900); // PAST the stub's delay
      afterEsc.noPanelAfterDelay = !document.querySelector("#recover-out #sql-panel");

      // C) an error renders IN the modal — and a FAILED generation must not
      // leave the PREVIOUS run's script on screen with Download live (those
      // bytes answer a filter nobody named). Seed a successful result first,
      // then fail the next generation and assert the stale panel and the
      // download buffer are gone.
      mode = "delay"; delay = 0;
      genBtn.focus(); genBtn.click();
      for (let i = 0; i < 100 && (modalSel() || !document.querySelector("#recover-out #sql-panel")); i++) await sleep(50);
      const seeded = { panel: !!document.querySelector("#recover-out #sql-panel"), sql: lastSQL.length > 0 };
      mode = "error";
      f.elements.pk.value = "2"; // a DIFFERENT filter than the seeded script answers
      genBtn.focus(); genBtn.click();
      await sleep(200);
      const em = modalSel();
      const errShown = {
        seeded,
        stillOpen: !!em,
        busyAttr: em ? em.getAttribute("aria-busy") : "",
        text: em ? em.textContent : "",
        stalePanelCleared: !document.querySelector("#recover-out #sql-panel"),
        staleSQLCleared: lastSQL === "",
      };
      const dismiss = em ? Array.from(em.querySelectorAll("button")).find((b) => b.textContent === "Dismiss") : null;
      if (dismiss) dismiss.click();
      await sleep(80);
      const afterErr = { modalGone: !modalSel(), noPanel: !document.querySelector("#recover-out #sql-panel"), btnEnabled: !genBtn.disabled };

      // D) a hand-typed table NOT in the listing still submits (#1364) and
      // reaches the backend — dropped tables with indexed events are a
      // legitimate recover target.
      mode = "delay"; delay = 0;
      f.elements.table.value = "vanished_tbl";
      f.elements.pk.value = "";
      genBtn.focus(); genBtn.click();
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const typed = {
        submitted: calls.some((c) => c.includes('"table":"vanished_tbl"')),
        panel: !!document.querySelector("#recover-out #sql-panel"),
      };

      // E) Preview rows adopts the same busy mechanism (same latency source).
      window.fetch = (p, o) => {
        if (!(typeof p === "string" && p.startsWith("/api/events"))) return realFetch(p, o);
        return new Promise((resolve, reject) => {
          const t = setTimeout(() => resolve(realFetch(p, o)), 700);
          if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
        });
      };
      f.elements.table.value = "orders";
      f.elements.pk.value = "1";
      prevBtn.focus(); prevBtn.click();
      await sleep(120);
      const pm = modalSel();
      const previewBusy = { modalOpen: !!pm, title: pm ? pm.textContent : "" };
      for (let i = 0; i < 100 && modalSel(); i++) await sleep(50);
      const previewDone = {
        modalGone: !modalSel(),
        rendered: !!document.querySelector("#recover-preview .events"),
        btnEnabled: !prevBtn.disabled,
      };
      window.fetch = (p, o) => {
        if (!(typeof p === "string" && p.startsWith("/api/recover"))) return realFetch(p, o);
        calls.push(o && o.body ? String(o.body) : "");
        return new Promise((resolve, reject) => {
          const t = setTimeout(() => resolve(realFetch(p, o)), delay);
          if (o && o.signal) o.signal.addEventListener("abort", () => { clearTimeout(t); reject(new DOMException("Aborted", "AbortError")); });
        });
      };

      // F) the ⌘K palette stacks ABOVE the dialog in its own mount and owns
      // its keys: Escape with the palette open must close the palette ONLY —
      // neither the modal's capture trap (which would beat the palette
      // input's own handler to the event) nor globalKeydown's generic
      // #modal-emptying fall-through (the same Escape that closed the
      // palette bubbles on) may touch the busy dialog.
      delay = 2000;
      f.elements.table.value = "orders"; f.elements.pk.value = "1";
      genBtn.focus(); genBtn.click();
      await sleep(100);
      const cmdkMount = document.getElementById("cmdk-mount");
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "k", metaKey: true, bubbles: true, cancelable: true }));
      await sleep(100);
      const paletteOpened = !!cmdkMount.firstChild;
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(100);
      const cmdkLeg = {
        paletteOpened,
        paletteClosed: !cmdkMount.firstChild,
        modalSurvived: !!modalSel(),
      };
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true, cancelable: true }));
      await sleep(150);
      cmdkLeg.canceledAfter = !modalSel();
      cmdkLeg.btnEnabled = !genBtn.disabled;

      // G) another dialog CLOBBERING the shared #modal slot (openServersModal
      // replaces its children without our teardown) must dissolve the trap on
      // the next keystroke: buttons re-enabled, re-entry flag reset, and the
      // occupying dialog left alone.
      delay = 1200;
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(100);
      const clobberBefore = !!modalSel();
      openServersModal();
      for (let i = 0; i < 60 && !document.getElementById("servers-list"); i++) await sleep(50);
      const busyGone = !modalSel();
      // The click path can arrive before any keystroke: the re-entry guard
      // must self-heal on read when the flag is set but no .busy-modal is in
      // the slot.
      const selfHealed = !busyModalActive() && !busyModalOpen;
      document.activeElement.dispatchEvent(new KeyboardEvent("keydown", { key: "Tab", bubbles: true, cancelable: true }));
      await sleep(80);
      const clobberLeg = {
        before: clobberBefore,
        busyGone,
        selfHealed,
        btnEnabled: !genBtn.disabled,
        flagReset: !busyModalOpen,
        serversIntact: !!document.getElementById("servers-list"),
      };
      closeServersModal();
      // Let the orphaned (never-aborted) request land before the next leg.
      for (let i = 0; i < 100 && !document.querySelector("#recover-out #sql-panel"); i++) await sleep(50);

      // H) a server switch mid-flight closes the dialog and renders nothing.
      delay = 700;
      document.getElementById("recover-out").replaceChildren();
      genBtn.focus(); genBtn.click();
      await sleep(100);
      serverGen++;
      for (let i = 0; i < 60 && modalSel(); i++) await sleep(50);
      const switchLeg = {
        modalGone: !modalSel(),
        noPanel: !document.querySelector("#recover-out #sql-panel"),
        btnEnabled: !genBtn.disabled,
      };
      return { open, secondBlocked, done, afterCancel, afterEsc, errShown, afterErr, typed, previewBusy, previewDone, cmdkLeg, clobberLeg, switchLeg };
    } finally {
      window.fetch = realFetch;
    }
  }, FIX);
  (busyRun.open.present && busyRun.open.role === "dialog" && busyRun.open.busyAttr === "true")
    ? ok("busy modal: opens immediately as an aria-busy dialog")
    : bad("busy modal: opens immediately as an aria-busy dialog", JSON.stringify(busyRun.open));
  (busyRun.open.facts.includes(FIX + ".orders") && /pk/.test(busyRun.open.facts))
    ? ok("busy modal: states what is being generated (target + pk)")
    : bad("busy modal: states what is being generated (target + pk)", busyRun.open.facts.slice(0, 200));
  (busyRun.open.btnDisabled && busyRun.open.prevDisabled)
    ? ok("busy modal: the Restore action buttons are disabled while open")
    : bad("busy modal: the Restore action buttons are disabled while open", JSON.stringify(busyRun.open));
  busyRun.open.focusInModal
    ? ok("busy modal: keyboard focus moves into the dialog")
    : bad("busy modal: keyboard focus moves into the dialog", "focus stayed outside");
  busyRun.secondBlocked
    ? ok("busy modal: a second click/submit queues no second generation")
    : bad("busy modal: a second click/submit queues no second generation", "a second /api/recover fired");
  (busyRun.done.modalGone && busyRun.done.panel && busyRun.done.calls === 1)
    ? ok("busy modal: closes on completion with the reversal panel rendered (one call total)")
    : bad("busy modal: closes on completion with the reversal panel rendered (one call total)", JSON.stringify(busyRun.done));
  (busyRun.done.btnEnabled && busyRun.done.focusOnResult)
    ? ok("busy modal: success re-enables the button and focuses the reversal.sql header")
    : bad("busy modal: success re-enables the button and focuses the reversal.sql header", JSON.stringify(busyRun.done));
  (busyRun.afterCancel.modalGone && busyRun.afterCancel.noPanel && busyRun.afterCancel.btnEnabled && busyRun.afterCancel.calls === 2)
    ? ok("busy modal: Cancel aborts the fetch and restores the pre-click state")
    : bad("busy modal: Cancel aborts the fetch and restores the pre-click state", JSON.stringify(busyRun.afterCancel));
  (busyRun.afterCancel.noPanelAfterDelay && busyRun.afterCancel.focusStillBack)
    ? ok("busy modal: the abort actually reaches the fetch (nothing renders after the stub's delay)")
    : bad("busy modal: the abort actually reaches the fetch (nothing renders after the stub's delay)", JSON.stringify(busyRun.afterCancel));
  busyRun.afterCancel.focusBack
    ? ok("busy modal: Cancel restores focus to the Generate button")
    : bad("busy modal: Cancel restores focus to the Generate button", "focus elsewhere");
  (busyRun.afterEsc.opened && busyRun.afterEsc.modalGone && busyRun.afterEsc.btnEnabled && busyRun.afterEsc.noPanel && busyRun.afterEsc.noPanelAfterDelay)
    ? ok("busy modal: ESC aborts and restores like Cancel")
    : bad("busy modal: ESC aborts and restores like Cancel", JSON.stringify(busyRun.afterEsc));
  (busyRun.errShown.stillOpen && busyRun.errShown.busyAttr === "false" && /went away mid-read/.test(busyRun.errShown.text))
    ? ok("busy modal: an error renders the server's actionable text IN the modal")
    : bad("busy modal: an error renders the server's actionable text IN the modal", JSON.stringify(busyRun.errShown).slice(0, 300));
  (busyRun.errShown.seeded.panel && busyRun.errShown.seeded.sql && busyRun.errShown.stalePanelCleared && busyRun.errShown.staleSQLCleared)
    ? ok("busy modal: a failed generation clears the previous script and its download buffer")
    : bad("busy modal: a failed generation clears the previous script and its download buffer", JSON.stringify(busyRun.errShown).slice(0, 300));
  (busyRun.afterErr.modalGone && busyRun.afterErr.noPanel && busyRun.afterErr.btnEnabled)
    ? ok("busy modal: Dismiss after an error returns to the pre-click state")
    : bad("busy modal: Dismiss after an error returns to the pre-click state", JSON.stringify(busyRun.afterErr));
  (busyRun.typed.submitted && busyRun.typed.panel)
    ? ok("restore combo: a typed table not in the listing still submits end-to-end")
    : bad("restore combo: a typed table not in the listing still submits end-to-end", JSON.stringify(busyRun.typed));

  // ── persistent failure notices (#1381) ───────────────────────────────────
  // A failure notice that fades is a failure nobody saw. The baseline privilege
  // refusal is ~550 characters of remediation, and at the old 2.2s auto-hide it
  // was unreadable and unrecoverable — it was reported from a screenshot caught
  // by luck. These assertions exist so a re-added setTimeout, or an ESC handler
  // that stops discriminating, cannot ship silently.
  const toastRun = await page.evaluate(async () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
    const t = document.getElementById("toast-error");
    const transient = document.getElementById("toast");
    const shown = () => !t.hidden;

    toastError("Refusal one: grant LOCK TABLES.");
    const immediately = shown();
    // Comfortably past the 2.2s auto-hide a transient toast would have used.
    await sleep(2600);
    const survivesAutoHide = shown();

    // A success notice must not silently delete an unread failure — and must
    // itself still render, which the shared-node version could not guarantee.
    toast("Baseline started…");
    const survivesSuccess = shown() && /Refusal one/.test(t.textContent);
    const successStillRenders = !transient.hidden && /Baseline started/.test(transient.textContent);

    // A second failure stacks rather than overwrites.
    toastError("Refusal two: BACKUP_ADMIN cannot be granted on RDS.");
    const stacked = /Refusal one/.test(t.textContent) && /Refusal two/.test(t.textContent);

    // ESC belongs to an open dialog first. Put something in the shared modal
    // slot and confirm the notice survives that Escape.
    const modalMount = document.getElementById("modal");
    modalMount.append(document.createElement("div"));
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const survivesModalEsc = shown();
    modalMount.replaceChildren();

    // Third door: the date picker renders into document.body, not #modal, and
    // closes itself on ESC without stopping propagation. Enumerating #modal and
    // #cmdk-mount was not enough — this is the case that got through.
    const pop = document.createElement("div");
    pop.className = "dt-pop";
    document.body.append(pop);
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const survivesDatePickerEsc = shown();
    pop.remove();

    // With nothing else open, ESC dismisses it.
    document.dispatchEvent(new KeyboardEvent("keydown", { key: "Escape", bubbles: true }));
    await sleep(50);
    const escDismisses = t.hidden;

    // And so does the close button.
    toastError("Refusal three.");
    const closeBtn = t.querySelector(".toast-close");
    if (closeBtn) closeBtn.click();
    await sleep(50);
    // Only `hidden` decides visibility: #toast-error carries the toast-error
    // class permanently in index.html now that failures have their own node,
    // so asserting the class is absent would assert the markup is wrong.
    const closeDismisses = t.hidden && t.textContent === "";

    // A repeat of a message already showing is counted, not dropped: several
    // failures carry no server name, so a second one would otherwise change
    // nothing on screen and read as success.
    toastError("Refusal four.");
    toastError("Refusal four.");
    const counted = /\(×2\)/.test(t.textContent);
    dismissToast();

    // A success notice still fades on its own.
    toast("Done");
    const successShown = !transient.hidden;
    await sleep(2600);
    return {
      immediately, survivesAutoHide, survivesSuccess, successStillRenders, stacked, counted,
      survivesModalEsc, survivesDatePickerEsc, escDismisses, closeDismisses,
      successShown, successFaded: transient.hidden,
    };
  });

  (toastRun.immediately && toastRun.survivesAutoHide)
    ? ok("toast: a failure notice is still on screen well past the transient auto-hide")
    : bad("toast: a failure notice is still on screen well past the transient auto-hide", JSON.stringify(toastRun));
  (toastRun.survivesSuccess && toastRun.successStillRenders)
    ? ok("toast: a success notice neither overwrites an unread failure nor is suppressed by it")
    : bad("toast: a success notice neither overwrites an unread failure nor is suppressed by it", JSON.stringify(toastRun));
  toastRun.counted
    ? ok("toast: a repeated failure is counted rather than silently dropped")
    : bad("toast: a repeated failure is counted rather than silently dropped", JSON.stringify(toastRun));
  toastRun.stacked
    ? ok("toast: concurrent failures stack instead of replacing each other")
    : bad("toast: concurrent failures stack instead of replacing each other", JSON.stringify(toastRun));
  (toastRun.survivesModalEsc && toastRun.survivesDatePickerEsc)
    ? ok("toast: an ESC meant for an open dialog or popover does not destroy the failure notice")
    : bad("toast: an ESC meant for an open dialog or popover does not destroy the failure notice", JSON.stringify(toastRun));
  (toastRun.escDismisses && toastRun.closeDismisses)
    ? ok("toast: ESC and the close button both dismiss the failure notice")
    : bad("toast: ESC and the close button both dismiss the failure notice", JSON.stringify(toastRun));
  (toastRun.successShown && toastRun.successFaded)
    ? ok("toast: success notices still fade on their own")
    : bad("toast: success notices still fade on their own", JSON.stringify(toastRun));
  (busyRun.previewBusy.modalOpen && /Previewing affected rows/.test(busyRun.previewBusy.title))
    ? ok("busy modal: Preview rows adopts the same busy affordance")
    : bad("busy modal: Preview rows adopts the same busy affordance", JSON.stringify(busyRun.previewBusy).slice(0, 200));
  (busyRun.previewDone.modalGone && busyRun.previewDone.rendered && busyRun.previewDone.btnEnabled)
    ? ok("busy modal: preview closes on completion with the rows rendered")
    : bad("busy modal: preview closes on completion with the rows rendered", JSON.stringify(busyRun.previewDone));
  (busyRun.cmdkLeg.paletteOpened && busyRun.cmdkLeg.paletteClosed && busyRun.cmdkLeg.modalSurvived)
    ? ok("busy modal: Escape over the ⌘K palette closes the palette only — the dialog survives")
    : bad("busy modal: Escape over the ⌘K palette closes the palette only — the dialog survives", JSON.stringify(busyRun.cmdkLeg));
  (busyRun.cmdkLeg.canceledAfter && busyRun.cmdkLeg.btnEnabled)
    ? ok("busy modal: the next Escape (palette closed) cancels the dialog as usual")
    : bad("busy modal: the next Escape (palette closed) cancels the dialog as usual", JSON.stringify(busyRun.cmdkLeg));
  (busyRun.clobberLeg.before && busyRun.clobberLeg.busyGone && busyRun.clobberLeg.serversIntact)
    ? ok("busy modal: another dialog can claim the shared #modal slot (fixture precondition)")
    : bad("busy modal: another dialog can claim the shared #modal slot (fixture precondition)", JSON.stringify(busyRun.clobberLeg));
  (busyRun.clobberLeg.btnEnabled && busyRun.clobberLeg.flagReset)
    ? ok("busy modal: a clobbered dialog dissolves its trap — buttons re-enabled, re-entry flag reset")
    : bad("busy modal: a clobbered dialog dissolves its trap — buttons re-enabled, re-entry flag reset", JSON.stringify(busyRun.clobberLeg));
  busyRun.clobberLeg.selfHealed
    ? ok("busy modal: the re-entry guard self-heals on read after a clobber")
    : bad("busy modal: the re-entry guard self-heals on read after a clobber", JSON.stringify(busyRun.clobberLeg));
  (busyRun.switchLeg.modalGone && busyRun.switchLeg.noPanel && busyRun.switchLeg.btnEnabled)
    ? ok("busy modal: a server switch mid-flight closes the dialog and renders nothing")
    : bad("busy modal: a server switch mid-flight closes the dialog and renders nothing", JSON.stringify(busyRun.switchLeg));

  // Scenario 17b — the reduced-motion arm (#1363, same rule as the #1353
  // skeletons): under prefers-reduced-motion the CSS animation is replaced by
  // the static note; under no-preference the animation shows and the note
  // hides. Driven through the REAL openBusyModal against emulated media.
  const rmProbe = async () => page.evaluate(() => {
    const busy = openBusyModal(document.getElementById("recover-form"),
      { title: "probe", errTitle: "probe", facts: [], onCancel: () => {} });
    const res = {
      anim: getComputedStyle(document.querySelector("#modal .busy-anim")).display,
      note: getComputedStyle(document.querySelector("#modal .busy-static")).display,
    };
    busy.close(false);
    return res;
  });
  await page.emulateMedia({ reducedMotion: "no-preference" });
  const rmOff = await rmProbe();
  await page.emulateMedia({ reducedMotion: "reduce" });
  const rmOn = await rmProbe();
  await page.emulateMedia({ reducedMotion: null });
  (rmOff.anim === "flex" && rmOff.note === "none")
    ? ok("busy modal: animation renders under no-preference (static note hidden)")
    : bad("busy modal: animation renders under no-preference (static note hidden)", JSON.stringify(rmOff));
  (rmOn.anim === "none" && rmOn.note === "block")
    ? ok("busy modal: prefers-reduced-motion collapses the animation to the static note")
    : bad("busy modal: prefers-reduced-motion collapses the animation to the static note", JSON.stringify(rmOn));

  // Scenario 17c — resting states under prefers-reduced-motion (#1392).
  //
  // The whole-file guard in assets_reducedmotion_test.go proves every
  // animation SITS inside the reduced-motion block. It cannot prove the rule
  // left OUTSIDE is a usable resting state, and that is the failure that costs
  // information rather than polish: `ul` defines only a `to` frame, so leaving
  // its scaleX(0) start on the base rule WOULD have hidden the underline
  // marking WHICH value changed — permanently, and with the text guard
  // green. A text checker cannot see that; it is a cascade question, so it is
  // asked of a real browser.
  //
  // The elements are synthesised rather than driven to through the UI on
  // purpose: the claim is about the stylesheet, and reaching a timeline with a
  // changed value through five navigations would test the route, not the CSS.
  const restProbe = async () => page.evaluate(() => {
    const host = document.createElement("div");
    host.innerHTML = '<div class="tl-node in"><span class="tl-dot"></span>'
      + '<div class="tl-body"><span class="pv changed">v</span></div></div>'
      + '<nav class="nav"><a class="nav-item active"><span>x</span></a></nav>'
      + '<button class="cov-refresh-btn spin"><span class="cov-refresh-ico">'
      + '<svg viewBox="0 0 24 24"></svg></span></button>'
      + '<div class="view-enter"><div>panel</div></div>'
      + '<div class="ov-stat"><div class="ov-stat-v">1</div></div>';
    document.body.appendChild(host);
    const cs = (sel, pseudo) => getComputedStyle(host.querySelector(sel), pseudo || null);
    const res = {
      underline: cs(".pv.changed", "::after").transform,
      underlineAnim: cs(".pv.changed", "::after").animationName,
      node: cs(".tl-node.in").transform,
      dot: cs(".tl-dot").transform,
      railHeight: cs(".nav-item.active", "::before").height,
      railAnim: cs(".nav-item.active", "::before").animationName,
      spinAnim: cs(".cov-refresh-ico svg").animationName,
      spinOpacity: cs(".cov-refresh-btn.spin").opacity,
      riseAnim: cs(".view-enter > div").animationName,
      // The Go orphan check anchors on @keyframes, which leaves two blind
      // spots these four fields cover. A guarded TRANSITION has no keyframes
      // to orphan; and `rise` is declared on BOTH .view-enter > * and
      // .ov-stat, so deleting either one leaves the name still "driven".
      nodeTransDur: cs(".tl-node.in").transitionDuration,
      dotTransDur: cs(".tl-dot").transitionDuration,
      dotAnim: cs(".tl-dot").animationName,
      ovAnim: cs(".ov-stat").animationName,
      ovTransDur: cs(".ov-stat").transitionDuration,
    };
    host.remove();
    return res;
  });
  await page.emulateMedia({ reducedMotion: "reduce" });
  const rest = await restProbe();
  await page.emulateMedia({ reducedMotion: "no-preference" });
  const moved = await restProbe();
  await page.emulateMedia({ reducedMotion: null });

  // Every element must ARRIVE without its animation. "none" is the computed
  // transform of an element with none declared — i.e. scaleX(1), full size,
  // final position.
  (rest.underline === "none" && rest.underlineAnim === "none")
    ? ok("reduced motion: the changed-value underline is VISIBLE without its animation")
    : bad("reduced motion: the changed-value underline is VISIBLE without its animation", JSON.stringify(rest));
  (rest.node === "none" && rest.dot === "none" && rest.dotAnim === "none"
    && rest.nodeTransDur === "0s" && rest.dotTransDur === "0s")
    ? ok("reduced motion: timeline node and dot rest at their final position, untimed")
    : bad("reduced motion: timeline node and dot rest at their final position, untimed", JSON.stringify(rest));
  (rest.ovAnim === "none" && rest.ovTransDur === "0s")
    ? ok("reduced motion: the stat tile neither enters nor transitions")
    : bad("reduced motion: the stat tile neither enters nor transitions", JSON.stringify(rest));
  rest.railHeight !== "0px"
    ? ok("reduced motion: the active rail keeps its height without railpop")
    : bad("reduced motion: the active rail keeps its height without railpop", rest.railHeight);
  (rest.railAnim === "none" && rest.riseAnim === "none")
    ? ok("reduced motion: neither the rail nor the view entrance animates")
    : bad("reduced motion: neither the rail nor the view entrance animates", JSON.stringify(rest));
  // The spin carries nearly all the in-flight signal — the :disabled dim is
  // subtle and the cursor change invisible without a pointer — so its guard
  // owes a replacement rather than a plain removal.
  (rest.spinAnim === "none" && rest.spinOpacity === "0.55")
    ? ok("reduced motion: the refresh spinner is replaced by a dimmed button, not by nothing")
    : bad("reduced motion: the refresh spinner is replaced by a dimmed button, not by nothing", JSON.stringify(rest));

  // The other half: moving the rules into the guard must not have deleted the
  // motion for everyone else. Without this, emptying the block passes above.
  (moved.underlineAnim === "ul" && moved.railAnim === "railpop" && moved.spinAnim === "covspin"
    && moved.riseAnim === "rise" && moved.dotAnim === "dotpop")
    ? ok("reduced motion: under no-preference every animation this probe covers still runs")
    : bad("reduced motion: under no-preference every animation this probe covers still runs", JSON.stringify(moved));
  // Asserted as "nonzero", not as an exact duration. The exact form caught the
  // deletion this exists for, but it ALSO went red on a legitimate retune
  // (.45s -> .5s) with a message accusing the author of a deletion that never
  // happened — the cry-wolf shape this file argues against everywhere else.
  (parseFloat(moved.nodeTransDur) > 0 && parseFloat(moved.dotTransDur) > 0
    && parseFloat(moved.ovTransDur) > 0)
    ? ok("reduced motion: the guarded transitions still have a duration under no-preference")
    : bad("reduced motion: the guarded transitions still have a duration under no-preference", JSON.stringify(moved));
  moved.ovAnim === "rise"
    ? ok("reduced motion: the stat tile keeps its own entrance (rise has a second driver)")
    : bad("reduced motion: the stat tile keeps its own entrance (rise has a second driver)", moved.ovAnim);
  moved.spinOpacity === "1"
    ? ok("reduced motion: the dimmed-button stand-in does not leak into no-preference")
    : bad("reduced motion: the dimmed-button stand-in does not leak into no-preference", moved.spinOpacity);

  // ── Scenario 17e — who may wear the brand gradient (#1385) ──
  //
  // The Go guards in assets_brandpaint_test.go prove the gradient is opt-in,
  // that its transparent ink stays behind @supports, and that app.js grants the
  // class from exactly one place. None of them can prove the gate in that one
  // place points the right way: rewriting it to grant the class
  // unconditionally keeps the count at one and leaves every text guard green.
  // Verified by mutation, which is why this scenario exists.
  //
  // What is at stake is not decoration. A gradient has to satisfy its contrast
  // bar at every point along the sweep, and this one bottoms out at 3.53:1 on
  // the studio tile ground it actually paints on (3.70:1 on white) — so it can
  // be relied on at WCAG's LARGE-text bar of 3:1 and never at the 4.5:1 body
  // bar. (Its violet stop alone would clear 4.5:1; that buys nothing, the
  // other stops share the sweep.) A tile that takes the gradient without being large text is
  // unreadable, and the deletes tile additionally carries the semantic
  // --delete that the brand palette must never repaint.
  //
  // ovStat is called for real rather than synthesised: the gate lives inside
  // it, so a hand-built div would test the stylesheet and skip the bug.
  //
  // Two things this deliberately does NOT reach, so nobody reads more into a
  // green run than it earns. The scenario passes "danger" itself, so it proves
  // the GATE honours the modifier and not that fillOvActivity still sends one
  // — drop that argument at its call site and the deletes tile takes the
  // gradient with every check green. And the tiles are mounted on document.body
  // rather than inside a rendered Overview, so a rule scoped to an .ov-stats
  // or .view ancestor is invisible here in both directions. The wordmark
  // assertion below has neither limit: it reads the real element in place.
  const brandProbe = () => page.evaluate(() => {
    const host = document.createElement("div");
    document.body.appendChild(host);
    const tile = (v, key, mod) => {
      const n = ovStat(v, key, mod, "last 24 h");
      host.appendChild(n);
      return n.querySelector(".ov-stat-v");
    };
    const plain = tile("42", "changes indexed", "");
    const danger = tile("7", "deletes", "danger");
    // The `small` variant is built inline in fillOvEvents, not through ovStat.
    // Mirrored here because it is the surface the gradient would hurt MOST:
    // 19px at weight 500 is body text, held to 4.5:1, which the sweep as a
    // whole does not hold. It is also the exclusion the CSS cascade does NOT
    // protect — `.ov-stat-v.small` sets no colour, so the gradient rule would
    // win outright over it. Built by hand here on purpose, unlike the two
    // tiles above: this row asserts the STYLESHEET, that a bare
    // `.ov-stat-v.small` is left alone, which is the half no gate can supply.
    const small = el("div", { class: "ov-stat-v small", text: "2026-08-20 11:00:00" });
    host.appendChild(small);
    // A reference for the semantic colour, computed through the same pipeline
    // as the tile so the two strings are comparable whatever form the engine
    // serialises oklch into.
    const ref = el("span", { text: "x" });
    ref.style.color = "var(--delete)";
    host.appendChild(ref);
    const cs = (n) => getComputedStyle(n);
    const res = {
      plainImage: cs(plain).backgroundImage,
      plainColor: cs(plain).color,
      dangerImage: cs(danger).backgroundImage,
      dangerColor: cs(danger).color,
      smallImage: cs(small).backgroundImage,
      smallColor: cs(small).color,
      wordImage: cs(document.querySelector(".brand-name")).backgroundImage,
      wordColor: cs(document.querySelector(".brand-name")).color,
      deleteRef: cs(ref).color,
    };

    // The warmed loading bar (#1385). Measured off the ELEMENT, which is the
    // whole point and was got wrong first: an earlier version read --skel-warm
    // and --surface-2 off :root and compared those. That measures a token pair
    // the bar need not use — repointing .skel-line back at var(--line), the
    // literal state before the change, left it green.
    //
    // So: pull the first colour stop out of the bar's own computed gradient,
    // and composite it over the first painted ancestor rather than over black,
    // which keeps the arithmetic honest if the stop ever carries alpha.
    // Canvas rather than string comparison because the two values are a
    // color-mix and an oklch — they can never compare equal, so a string check
    // would be vacuous rather than merely different; only rendered pixels say
    // how far apart they are.
    const pending = ovStatPending("changes indexed", "all time");
    host.appendChild(pending);
    const bar = pending.querySelector(".skel-line");
    res.skelPainted = cs(bar).backgroundImage;
    res.skelStop = (res.skelPainted.match(/\b(?:rgba?|color|oklch|oklab|hsla?)\([^)]*\)|#[0-9a-fA-F]{3,8}\b/) || [])[0] || "";
    let ground = "rgba(0, 0, 0, 0)";
    for (let n = bar; n && (ground === "rgba(0, 0, 0, 0)" || ground === "transparent"); n = n.parentElement) {
      ground = cs(n).backgroundColor;
    }
    // Belt matching the fillStyle one below, for the same reason: if every
    // ancestor came back transparent the ground's luminance is 0 and the ratio
    // INFLATES past any floor, so the failure direction is a silent pass.
    // Unreachable while body paints --bg, which is exactly when a belt is
    // cheap. Recorded as null so the assertion has something to test.
    res.skelGround = (ground === "rgba(0, 0, 0, 0)" || ground === "transparent") ? null : ground;

    const cv = document.createElement("canvas");
    cv.width = cv.height = 1;
    const ctx = cv.getContext("2d", { willReadFrequently: true });
    // An unparseable fillStyle is IGNORED, leaving the previous value in place
    // — and the previous value would be the ground, so a rejected stop would
    // measure 1.0 and read as a failure, or worse against black would inflate
    // the ratio and pass. Checked explicitly instead of hoped for.
    ctx.fillStyle = "#010203";
    ctx.fillStyle = res.skelStop;
    res.skelParsed = ctx.fillStyle !== "#010203";
    const px = (c, over) => {
      ctx.clearRect(0, 0, 1, 1);
      ctx.fillStyle = over;
      ctx.fillRect(0, 0, 1, 1);
      ctx.fillStyle = c;
      ctx.fillRect(0, 0, 1, 1);
      return ctx.getImageData(0, 0, 1, 1).data;
    };
    const relLum = (d) => {
      const c = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
      return 0.2126 * c(d[0]) + 0.7152 * c(d[1]) + 0.0722 * c(d[2]);
    };
    const [hi, lo] = [relLum(px(res.skelStop, ground)), relLum(px(ground, ground))].sort((a, b) => b - a);
    res.skelRatio = (hi + 0.05) / (lo + 0.05);

    host.remove();
    return res;
  });

  const brand = await brandProbe();
  const transparent = "rgba(0, 0, 0, 0)";

  (brand.plainImage.includes("gradient") && brand.plainColor === transparent)
    ? ok("brand paint: the 32px Overview count wears the headline gradient")
    : bad("brand paint: the 32px Overview count wears the headline gradient", JSON.stringify(brand));
  // Both halves matter. No gradient is the gate doing its job; a colour that
  // is still --delete is the cascade doing its job if the gate ever stops.
  (brand.dangerImage === "none" && brand.dangerColor === brand.deleteRef)
    ? ok("brand paint: the deletes tile keeps the semantic --delete and takes no gradient")
    : bad("brand paint: the deletes tile keeps the semantic --delete and takes no gradient", JSON.stringify(brand));
  (brand.smallImage === "none" && brand.smallColor !== transparent)
    ? ok("brand paint: the 19px/500 timestamp tile is left as plain readable ink")
    : bad("brand paint: the 19px/500 timestamp tile is left as plain readable ink", JSON.stringify(brand));
  // The other painted surface. Read off the REAL sidebar wordmark rather than
  // a copy — it is static markup that has been on screen since the first
  // navigation, so there is nothing to synthesise. Dropping .brand-name from
  // the gradient rule used to leave every check green.
  (brand.wordImage.includes("gradient") && brand.wordColor === transparent)
    ? ok("brand paint: the sidebar wordmark wears the headline gradient")
    : bad("brand paint: the sidebar wordmark wears the headline gradient", JSON.stringify(brand));
  // A floor placed BETWEEN the two measured states, not at the measurement:
  // the warmed stop renders 1.60:1 on the studio tile and the plain --line it
  // replaced rendered 1.23:1, so 1.3 separates them. That is what makes the
  // check bind — reverting the declaration lands at 1.23 and rings. It is not
  // a general washout detector: the mix has to fall to roughly 5% pink before
  // 1.3 fires on its own, so a "make it subtler" retune is NOT caught here.
  //
  // Studio is measured because it is the shipped direction and also the worst
  // of the three grounds; paper and trail resolve to white, where the same
  // stop sits at 1.68:1.
  (brand.skelParsed && brand.skelGround && brand.skelPainted.includes("gradient") && brand.skelRatio >= 1.3)
    ? ok("brand paint: the warmed loading bar's stop stays clear of its panel")
    : bad("brand paint: the warmed loading bar's stop stays clear of its panel",
        JSON.stringify({ stop: brand.skelStop, ground: brand.skelGround || "NONE FOUND",
          ratio: Number(brand.skelRatio.toFixed(3)), parsed: brand.skelParsed }));

  // ── Scenario 17f — the Events skeleton is visible (#1397) ──
  //
  // .ev-skel-bar stood in for event rows at 1.09:1 against the page — fainter
  // than the 1.17:1 --line-soft hairline separating the rows it stood in for.
  // A loading list therefore rendered as an empty list with dividers: the
  // "blank list" that the #1353 comment introducing this feature says must
  // never be the busy state, on the page an operator opens mid-incident.
  //
  // This reads SCREENSHOT PIXELS, not computed style, and that is the whole
  // design. Two earlier drafts asserted a declared colour, and each time
  // review produced a one-declaration edit that re-broke the rendering while
  // the assertion sat unchanged at 1.64. `.ev-loading { opacity: .5 }` is the
  // one that settled it: opacity does NOT inherit — it composites the group —
  // so the bar's own computed opacity stays 1 while it renders at 1.267.
  // Chasing that with an ancestor walk would have left `filter`, a covering
  // ::after, and a background-image over the fill. A pixel has no such list.
  //
  // Every bar is measured (40 of them) because a rule can repaint SOME: the
  // nth-child rule reaches only even rows, and the five .ev-skel-* width
  // classes sit on the same element as .ev-skel-bar, so they repaint the bar
  // without naming it. All three directions are measured because stripping
  // fills is `paper`'s idiom — five existing rules do exactly that.
  //
  // The pulse is sampled rather than modelled. An earlier draft read the
  // keyframes and folded the minimum opacity into the maths, which is blind
  // to a pulse that animates any other property. Instead the animation is
  // frozen at five phases with a negative delay and photographed, so whatever
  // it animates is simply in the picture.
  //
  // Ground is sampled too, from the row's own padding a few pixels above the
  // bar, so both halves of the ratio come from the same photograph.
  //
  // The limit worth naming, since nothing here reaches it: renderEventsLoading
  // is called directly rather than through runEventsQuery. This proves the
  // skeleton is visible, not that the fetch path still paints one.
  const skelShot = async () => {
    const png = (await page.screenshot({ fullPage: true })).toString("base64");
    // Decoded by the browser's own PNG reader rather than a hand-rolled one:
    // the image goes back into the page, onto a canvas, and comes out as bytes.
    return page.evaluate(async (b64) => {
      const img = new Image();
      img.src = "data:image/png;base64," + b64;
      await img.decode();
      const cv = document.createElement("canvas");
      cv.width = img.width; cv.height = img.height;
      cv.getContext("2d").drawImage(img, 0, 0);
      const ctx = cv.getContext("2d", { willReadFrequently: true });
      const at = (x, y) => [...ctx.getImageData(Math.round(x), Math.round(y), 1, 1).data].slice(0, 3);
      // Document coordinates, to match a full-page shot. The ground point sits
      // inside the row's top padding, above the bar and well clear of the
      // bottom border, so it photographs whatever the bar is drawn on.
      return [...document.querySelectorAll(".ev-skel-bar")].map((b) => {
        const r = b.getBoundingClientRect();
        const row = b.closest(".ev-skel-row").getBoundingClientRect();
        const x = r.left + r.width / 2 + scrollX;
        return { bar: at(x, r.top + r.height / 2 + scrollY), ground: at(x, row.top + 3 + scrollY) };
      });
    }, png);
  };
  const relLum = (d) => {
    const c = (v) => { v /= 255; return v <= 0.04045 ? v / 12.92 : Math.pow((v + 0.055) / 1.055, 2.4); };
    return 0.2126 * c(d[0]) + 0.7152 * c(d[1]) + 0.0722 * c(d[2]);
  };
  const skelWorst = (samples) => samples.length === 0 ? null : samples.reduce((worst, s) => {
    const [hi, lo] = [relLum(s.bar), relLum(s.ground)].sort((a, b) => b - a);
    const ratio = (hi + 0.05) / (lo + 0.05);
    return !worst || ratio < worst.ratio ? { ratio, n: samples.length, bar: s.bar, ground: s.ground } : worst;
  }, null);

  // Resting first, under emulated reduced motion — a determinism choice, since
  // mid-pulse a photograph catches whatever frame it lands on. It is also the
  // honest worst case: the pulse is gated behind the same query, so a
  // reduced-motion user gets no movement at all to suggest the bar is there,
  // and the static value was the invisible one.
  await page.emulateMedia({ reducedMotion: "reduce" });
  // Repainted before every shot, not once for the run: renderEvents ends by
  // firing its own query, and that resolve calls buildEventRows, which clears
  // #ev-rows. Asserting the skeleton survived an await would be a flake in the
  // one scenario whose stated design is determinism.
  const skelPaint = () => page.evaluate(async () => {
    navigate("events");
    await new Promise((r) => setTimeout(r, 300));
    const rows = document.querySelector("#ev-rows");
    if (!rows) return 0;
    renderEventsLoading(rows);
    return document.querySelectorAll(".ev-skel-bar").length;
  });
  const skelSetDir = (dir) => page.evaluate((d) => {
    const root = document.documentElement;
    d === null ? root.removeAttribute("data-dir") : root.setAttribute("data-dir", d);
  }, dir);

  const skelPrevDir = await page.evaluate(() => document.documentElement.getAttribute("data-dir"));
  let skelBars = 0;
  const skelRest = {};
  for (const dir of ["studio", "paper", "trail"]) {
    await skelSetDir(dir);
    skelBars = await skelPaint();
    skelRest[dir] = skelWorst(await skelShot());
  }
  await skelSetDir(skelPrevDir);

  // Then the pulse, sampled rather than modelled: freeze the animation with a
  // negative delay and photograph it. Whatever it animates is in the picture,
  // so swapping the animated property cannot slip past — an earlier draft read
  // the keyframes' opacity and was blind to exactly that.
  //
  // A uniform tenth-of-a-cycle grid rather than the phases this keyframe
  // happens to use, since reading the offsets would put the sampler back
  // inside the model it is meant to escape. The cost is stated rather than
  // hidden: a dip narrower than a tenth of a cycle can fall between samples.
  // An eased pulse is flat around its extreme, so the grid finds today's, and
  // it would find one moved off 50% — which the previous five-phase grid,
  // clustered around this keyframe's own minimum, would not have.
  //
  // One direction, not three: the pulse dims whatever the fill is, so sweeping
  // it per direction would re-measure the same multiplier. A direction that
  // strips the fill is caught by the resting floor above, in the direction
  // that strips it.
  //
  // The freeze is verified rather than trusted. If the paused state or the
  // delay silently failed to take, every photograph would land on the RESTING
  // pixel, clear the floor, and report that the dimmest phase had been
  // measured when nothing was. That is a vacuous pass — the failure this
  // scenario exists to make impossible — so the computed values are read back
  // after being set, which checks the instrument rather than the outcome.
  await page.emulateMedia({ reducedMotion: "no-preference" });
  await skelPaint();
  const skelPhases = [];
  for (const phase of [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]) {
    const froze = await page.evaluate((p) => {
      let name = "none", frozen = true;
      for (const b of document.querySelectorAll(".ev-skel-bar")) {
        const before = getComputedStyle(b);
        name = before.animationName;
        const secs = parseFloat(before.animationDuration) || 0;
        b.style.animationPlayState = "paused";
        b.style.animationDelay = `${-p * secs}s`;
        const after = getComputedStyle(b);
        if (after.animationPlayState !== "paused") frozen = false;
        if (Math.abs(parseFloat(after.animationDelay) + p * secs) > 1e-6) frozen = false;
      }
      return { name, frozen };
    }, phase);
    skelPhases.push({ phase, ...froze, ...(skelWorst(await skelShot()) || {}) });
  }
  await page.emulateMedia({ reducedMotion: null });
  await page.evaluate(() => { const r = document.querySelector("#ev-rows"); if (r) clear(r); });

  const skelShots = Object.entries(skelRest);
  const skelEnough = (s) => s && s.n >= 30;
  const skelRestWorst = skelShots.every(([, s]) => skelEnough(s))
    ? skelShots.reduce((w, [dir, s]) => (!w || s.ratio < w.ratio ? { dir, ...s } : w), null)
    : null;
  const skelPulseWorst = skelPhases.every(skelEnough)
    ? skelPhases.reduce((w, s) => (!w || s.ratio < w.ratio ? s : w), null)
    : null;
  const skelR = (s) => s && +s.ratio.toFixed(3);
  const skelDetail = () => JSON.stringify({
    bars: skelBars,
    rest: Object.fromEntries(skelShots.map(([d, s]) => [d, skelR(s)])),
    pulse: skelPhases.map((p) => ({ at: p.phase, r: skelR(p), frozen: p.frozen, anim: p.name })),
    worstRest: skelRestWorst && { dir: skelRestWorst.dir, ratio: skelR(skelRestWorst) },
    worstPulse: skelPulseWorst && { at: skelPulseWorst.phase, ratio: skelR(skelPulseWorst) },
  });

  // Hoisted out of the two floors below so a broken fixture reads as a broken
  // fixture. An empty sample list has no worst member, and every floor is
  // satisfied by nothing at all — the vacuous direction. Each shot must also
  // carry its own bars, since the list is repainted per direction.
  (skelBars >= 30 && skelRestWorst && skelPulseWorst)
    ? ok("events skeleton: the loading state paints bars this scenario can photograph")
    : bad("events skeleton: the loading state paints bars this scenario can photograph", skelDetail());

  // A floor placed BETWEEN the two measured states rather than at either:
  // --surface-3 rendered 1.09 and the neutral mix renders 1.64, so 1.35 is
  // what makes this bind. Reverting rings, and so does reaching for any
  // weaker token on the ramp (--line-soft 1.17, --line 1.28). It tolerates an
  // ordinary retune of the mix's two inputs — 80/20 is 1.52, 90/10 is 1.40 —
  // and, like 17e's floor, it is NOT a general washout detector: a deliberate
  // "make it subtler" edit has room to reach roughly 91% --line first — the
  // PULSE floor below is what binds there, one point before this one. It has
  // no opinion about hue either; --skel-warm measures 1.68 and would sail
  // through, which is deliberate and explained at the token.
  //
  // Three directions, not four axes: [data-accent] is NOT swept, so an
  // accent-scoped rule on these bars would go unphotographed. Said plainly
  // because the directions ARE swept and a reader could assume the rest.
  (skelRestWorst && skelRestWorst.ratio >= 1.35)
    ? ok("events skeleton: every bar stays visible at rest, in every direction")
    : bad("events skeleton: every bar stays visible at rest, in every direction", skelDetail());
  // The pulse's own floor, separate because it fails to a different edit —
  // deepening the dip rather than weakening the fill. 1.15 sits between the
  // old RESTING value (1.09) and this bar's dip (1.24), so the claim that the
  // animation no longer carries the bar's visibility is held by a measurement
  // rather than by the comment making it. Deepening 0.45 -> 0.12 photographs
  // at 1.06, below where this whole change started.
  //
  // A removed pulse is a legitimate pass, not a hole: with no animation the
  // resting floor above already describes every frame there is. What is NOT
  // allowed is an animation that exists while the freeze did not take.
  (skelPulseWorst && skelPhases.every((p) => p.name === "none" || p.frozen) &&
    skelPulseWorst.ratio >= 1.15)
    ? ok("events skeleton: the pulse's dimmest phase stays above where the bar started")
    : bad("events skeleton: the pulse's dimmest phase stays above where the bar started", skelDetail());

  // ── Scenario 18 — advisory severity split on the archive fixture (#1365) ──
  // The archive-elision record must render in the INFO register (muted line,
  // no ⚠ icon, no amber, no alert classes) while a real coverage-gap warning
  // keeps the ALERT register — both driven end to end against the run.sh
  // archive fixture (real rotation, real gap). One visual register for both
  // was the bug: a note saying "nothing is missing" read as an incident.
  const arcId = await page.evaluate(async (db) => {
    const res = await api("/api/servers", { method: "POST", body: {
      name: "arc-idx", host: "127.0.0.1", port: "13306", user: "root", password: "testroot", dbname: db,
    } });
    return res.id;
  }, ARC_DB);
  await page.evaluate(async (id) => { await switchServer(id); }, arcId);

  // The wire shape first: the elision fact lands in `notes` on a filled fast
  // page (warnings clean), and a large page really reads the archives (the
  // premise that makes the elision meaningful) with no elision claimed.
  const arcWire = await page.evaluate(async () => {
    const fast = await api("/api/events?limit=5");
    const slow = await api("/api/events?limit=500");
    return {
      fastCount: fast.count,
      fastNotes: fast.notes || [],
      fastWarnings: fast.warnings || [],
      slowCount: slow.count,
      slowNotes: slow.notes || [],
    };
  });
  arcWire.fastCount === 5 && arcWire.slowCount > arcWire.fastCount
    ? ok("severity split: archive fixture premise holds (fast page filled, slow page reads archives)")
    : bad("severity split: archive fixture premise holds (fast page filled, slow page reads archives)", JSON.stringify(arcWire));
  arcWire.fastNotes.some((n) => /answered from the live index/.test(n))
    ? ok("severity split: the elision fact rides the response `notes` list")
    : bad("severity split: the elision fact rides the response `notes` list", JSON.stringify(arcWire.fastNotes));
  arcWire.fastWarnings.length === 0
    ? ok("severity split: the elision is NOT a warning on the wire")
    : bad("severity split: the elision is NOT a warning on the wire", JSON.stringify(arcWire.fastWarnings));
  arcWire.slowNotes.length === 0
    ? ok("severity split: no elision claimed on a page that read the archives")
    : bad("severity split: no elision claimed on a page that read the archives", JSON.stringify(arcWire.slowNotes));

  // The INFO register in the real DOM: a filled 5-event browse.
  await page.evaluate(() => navigate("events"));
  await page.waitForSelector("#ev-rows .ev-row", { timeout: 10000 });
  const arcInfo = await page.evaluate(async () => {
    const f = document.getElementById("ev-form");
    f.elements.since.value = ""; f.elements.until.value = "";
    f.elements.limit.value = "5";
    f.requestSubmit();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-notes .note-item").length) break;
    }
    const note = document.querySelector("#ev-notes .note-item");
    const cs = note ? getComputedStyle(note) : null;
    const bar = document.querySelector(".result-bar");
    const notesBox = document.getElementById("ev-notes");
    return {
      noteCount: document.querySelectorAll("#ev-notes .note-item").length,
      noteText: note ? note.textContent : "",
      warnCount: document.querySelectorAll("#ev-warnings .warn-item").length,
      hasIcon: note ? !!note.querySelector("svg") : true,
      alertClass: note ? /warn|alert|error/.test(note.className) : true,
      inAlertContainer: note ? !!note.closest(".warnings, .warn-box, .error-box") : true,
      bg: cs ? cs.backgroundColor : "",
      border: cs ? cs.borderTopStyle : "",
      underCountLine: !!(bar && notesBox && (bar.compareDocumentPosition(notesBox) & Node.DOCUMENT_POSITION_FOLLOWING)),
    };
  });
  arcInfo.noteCount === 1 && /answered from the live index/.test(arcInfo.noteText)
    ? ok("severity split: the elision note renders on the Events view")
    : bad("severity split: the elision note renders on the Events view", JSON.stringify(arcInfo));
  (!arcInfo.hasIcon && !arcInfo.alertClass && !arcInfo.inAlertContainer)
    ? ok("severity split: the note carries no alert classes and no ⚠ icon")
    : bad("severity split: the note carries no alert classes and no ⚠ icon", JSON.stringify(arcInfo));
  (arcInfo.bg === "rgba(0, 0, 0, 0)" && arcInfo.border === "none")
    ? ok("severity split: the note paints muted — no amber background, no border")
    : bad("severity split: the note paints muted — no amber background, no border", `bg=${arcInfo.bg} border=${arcInfo.border}`);
  arcInfo.warnCount === 0
    ? ok("severity split: no alert component renders for the benign fact")
    : bad("severity split: no alert component renders for the benign fact", `warnCount=${arcInfo.warnCount}`);
  arcInfo.underCountLine
    ? ok("severity split: the note sits under the result-count line")
    : bad("severity split: the note sits under the result-count line", "notes container precedes the result bar");

  // The ALERT register: a time range covering the manufactured gap hour must
  // keep the warning component — amber box, ⚠ icon — and claim no elision.
  const arcWarn = await page.evaluate(async ({ since, until }) => {
    const f = document.getElementById("ev-form");
    f.elements.since.value = since;
    f.elements.until.value = until;
    f.elements.limit.value = "50";
    f.requestSubmit();
    for (let i = 0; i < 60; i++) {
      await new Promise((r) => setTimeout(r, 50));
      if (document.querySelectorAll("#ev-warnings .warn-item").length) break;
    }
    const w = document.querySelector("#ev-warnings .warn-item");
    const cs = w ? getComputedStyle(w) : null;
    return {
      warnCount: document.querySelectorAll("#ev-warnings .warn-item").length,
      warnText: w ? w.textContent : "",
      hasIcon: w ? !!w.querySelector("svg") : false,
      bg: cs ? cs.backgroundColor : "",
      border: cs ? cs.borderTopStyle : "",
      noteCount: document.querySelectorAll("#ev-notes .note-item").length,
    };
  }, { since: ARC_GAP_SINCE, until: ARC_GAP_UNTIL });
  arcWarn.warnCount >= 1 && /rotated and not archived/.test(arcWarn.warnText)
    ? ok("severity split: a real coverage-gap warning renders")
    : bad("severity split: a real coverage-gap warning renders", JSON.stringify(arcWarn));
  (arcWarn.hasIcon && arcWarn.bg !== "rgba(0, 0, 0, 0)" && arcWarn.border === "solid")
    ? ok("severity split: the gap warning keeps the alert register (⚠, amber, border)")
    : bad("severity split: the gap warning keeps the alert register (⚠, amber, border)", `icon=${arcWarn.hasIcon} bg=${arcWarn.bg} border=${arcWarn.border}`);
  arcWarn.noteCount === 0
    ? ok("severity split: no elision note on a time-ranged read into the archives")
    : bad("severity split: no elision note on a time-ranged read into the archives", `noteCount=${arcWarn.noteCount}`);

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
