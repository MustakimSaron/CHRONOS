/* CHRONOS-102 demo-pack builder.
   Loaded only by the local builder (never by the public website). It opens the six calibration targets one after
   another, records every archive answer and survey image, checks the recording and saves it to website/demo/.
   A target that fails its check is NOT saved, so a good older copy is never overwritten by a bad one. */
(() => {
  "use strict";
  if (!/^(127\.0\.0\.1|localhost)$/.test(location.hostname)) return;

  const TARGETS = [["wise_0855", "WISE 0855"], ["barnards_star", "Barnard's Star"], ["eps_indi_b", "ε Indi Ba/Bb"],
                   ["rw_aurigae", "RW Aurigae"], ["r_draconis", "R Draconis"], ["planet_x_cand", "Planet X demo"]];
  const ATTEMPTS = 2;

  // ---------- progress window ----------
  const box = document.createElement("div");
  box.style.cssText = "position:fixed;inset:0;z-index:99999;background:#0B1322;color:#E6ECF5;font:16px/1.5 system-ui,sans-serif;display:flex;align-items:center;justify-content:center;padding:24px";
  box.innerHTML = `<div style="max-width:640px;width:100%">
    <h1 style="margin:0 0 6px;font:600 26px Georgia,serif">CHRONOS-102 · building the demo pack</h1>
    <p style="margin:0 0 18px;color:#8EA0BD">Keep this window open. Your computer downloads real NASA / ESA archive data for the six demonstration targets and saves it into <b>website/demo</b>. This can take 20–60 minutes on a slow connection.</p>
    <div style="height:10px;background:#1B2742;border-radius:6px;overflow:hidden"><div id="bp-bar" style="height:100%;width:2%;background:linear-gradient(90deg,#7C6CF0,#34C3B7);transition:width .4s"></div></div>
    <p id="bp-now" style="margin:14px 0 6px;font-weight:600">Starting…</p>
    <ul id="bp-list" style="list-style:none;margin:0;padding:0;color:#C3CEDF;font-size:14px"></ul>
    <p id="bp-end" style="margin:18px 0 0;font-weight:600"></p></div>`;
  document.body.appendChild(box);
  const $b = id => box.querySelector("#" + id);
  const rows = {};
  const addRow = (key, text) => { const li = document.createElement("li"); li.textContent = "· " + text; $b("bp-list").appendChild(li); rows[key] = li; return li; };
  const setRow = (key, mark, text) => { const li = rows[key] || addRow(key, text); li.textContent = mark + " " + text; };
  const tell = (path, body) => fetch(path, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body || {}) }).catch(() => { });
  const progress = f => { $b("bp-bar").style.width = Math.max(2, Math.min(100, f * 100)) + "%"; };

  const isLocalServer = () => backendAlive;
  async function blobToDataURL(b) { return await new Promise((res, rej) => { const r = new FileReader(); r.onload = () => res(r.result); r.onerror = rej; r.readAsDataURL(b); }); }

  async function captureSurvey(k) {
    const urls = hipsChain(k, sv.ra, sv.dec, sv.fov, 512), key = demoKey(urls[2]); if (!key || !DEMO.rec) return false;
    const img = $("survey-img");
    if (sv.state === "ok" && !window._svTainted && img.naturalWidth) { // the image on screen is readable: save exactly that
      try { const c = document.createElement("canvas"); c.width = img.naturalWidth; c.height = img.naturalHeight; c.getContext("2d").drawImage(img, 0, 0); DEMO.rec[key] = { __image: c.toDataURL("image/jpeg", 0.86) }; return true; } catch (e) { }
    }
    for (const u of urls.filter(isProxy)) { // otherwise fetch it through the local server's proxies (always readable)
      try { const r = await fetch(u, { cache: "no-store" }); if (!r.ok || !/image/.test(r.headers.get("content-type") || "")) continue; DEMO.rec[key] = { __image: await blobToDataURL(await r.blob()) }; return true; } catch (e) { }
    }
    return false;
  }
  async function saveDemoPack(name, pack) {
    const r = await fetch(`${backendBase()}/api/demo/save`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ name, pack }) });
    if (!r.ok) { let d = ""; try { d = (await r.json()).detail; } catch (e) { } throw new Error("HTTP " + r.status + (d ? " - " + d : "")); }
    return await r.json();
  }
  const waitFor = async (cond, ms, step = 300) => { const end = performance.now() + ms; while (performance.now() < end) { try { if (cond()) return true; } catch (e) { } await sleep(step); } return false; };

  // records one target; returns { pack, note } or null when the recording is not good enough
  async function recordTarget(key, label) {
    DEMO.rec = {};
    if (dataMode !== "sim") setDataMode("sim");
    fieldCache = null; currentTargetKey = null; // force a fresh load so every answer for this field is recorded
    selectBenchmark(key);
    await waitFor(() => fieldCache && !fieldCache.pending && model && currentTargetKey === key, 150000);
    await waitFor(() => Object.keys(DEMO.rec).some(k => k.startsWith("/api/archives?what=wise")), 100000);
    const starsOK = !!(fieldCache && !fieldCache.synthetic && fieldCache.gaia && fieldCache.gaia.length);
    if (!starsOK) { DEMO.rec = null; return null; } // Gaia did not answer: a pack made of synthetic stars would be worthless
    await checkKnownObjects(false); await coverHere();
    if (model.tgt && model.tgt.pos && model.tgt.pos.length > 1) { ruler = { p0: model.tgt.pos[0], p1: model.tgt.pos[1] }; updateRuler(); await crossCheckWISE(); }
    runAIScan(); if (scanResults.length) { selectScan(0); await crossCheckWISE(); }
    ruler = { p0: null, p1: null }; updateRuler();
    let imgs = 0; for (const k of ["wise", "2mass", "dss"]) { setSurvey(k); await waitFor(() => sv.state && sv.state !== "loading", 50000); if (await captureSurvey(k)) imgs++; }
    setSurvey("wise");
    setDataMode("real"); await waitFor(() => realState !== "loading" && realState !== "idle", 360000, 500);
    let real = "no real SPHEREx exposures";
    if (realState === "ready") {
      const deep1 = await waitFor(() => activeSpec && activeSpec.real && activeSpec.deep, 480000, 1000); // full spectrum of the default pixel
      real = `${realField.frames.length} real exposures${deep1 ? ", full spectrum" : ""}`;
      await checkKnownObjects(false); runAIScan();
      if (scanResults.length) { selectScan(0); await waitFor(() => activeSpec && activeSpec.real && activeSpec.deep, 300000, 1000); await crossCheckWISE(); }
    }
    setDataMode("sim");
    const rec = DEMO.rec; DEMO.rec = null;
    return { pack: { label, key, ra: BENCHMARKS[key].ra_deg, dec: BENCHMARKS[key].dec_deg, fov: model.fov, responses: rec }, note: `${fieldCache.gaia.length} stars, ${imgs}/3 survey images, ${real}`, imgs };
  }

  async function run() {
    if (!isLocalServer()) { $b("bp-now").textContent = "The local data server is not running. Start the builder with Build-Demo-Pack."; return; }
    const keep = { mode: dataMode, view: viewMode, survey: sv.key };
    const saved = [], failed = []; let bytes = 0;
    try {
      $b("bp-now").textContent = "Sky coverage map and data release…";
      DEMO.rec = {}; await loadCoverage(true); await syncIRSA(true);
      const rec0 = DEMO.rec; DEMO.rec = null;
      if (!Object.keys(rec0).length) failed.push("Sky coverage"); else { const g = await saveDemoPack("global", { label: "Sky coverage", ra: null, dec: null, fov: null, responses: rec0 }); Object.assign(DEMO.map, rec0); bytes += g.bytes; saved.push("global"); }
      setRow("global", Object.keys(rec0).length ? "✓" : "✕", "Sky coverage map");
      progress(1 / (TARGETS.length + 1));
      for (let i = 0; i < TARGETS.length; i++) {
        const [key, label] = TARGETS[i]; setRow(key, "…", label); $b("bp-now").textContent = `Downloading ${label} (${i + 1} of ${TARGETS.length})…`;
        let res = null, why = "";
        for (let a = 1; a <= ATTEMPTS && !res; a++) {
          try { res = await recordTarget(key, label); if (!res) why = "star catalogue did not answer"; } catch (e) { DEMO.rec = null; why = e && e.message || "error"; }
          if (!res && a < ATTEMPTS) { setRow(key, "…", `${label} — retrying (${why})`); await sleep(4000); }
        }
        if (res) {
          const r = await saveDemoPack(key, res.pack); bytes += r.bytes; saved.push(key);
          Object.assign(DEMO.map, res.pack.responses); if (!demoField(res.pack.ra, res.pack.dec)) DEMO.fields.push({ name: key, label, ra: res.pack.ra, dec: res.pack.dec, fov: res.pack.fov });
          setRow(key, "✓", `${label} — ${res.note} (${(r.bytes / 1048576).toFixed(1)} MB)`);
        } else { failed.push(label); setRow(key, "✕", `${label} — not saved (${why}); an older saved copy, if any, is kept`); }
        progress((i + 2) / (TARGETS.length + 1));
      }
    } catch (e) { DEMO.rec = null; failed.push("stopped: " + (e && e.message)); }
    try { setDataMode(keep.mode); setViewMode(keep.view); setSurvey(keep.survey); } catch (e) { }
    const fin = await (await fetch(`${backendBase()}/api/demo/finish`, { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ saved, failed }) }).catch(() => null))?.json().catch(() => null);
    const ok = failed.length === 0 && fin && fin.ok;
    $b("bp-now").textContent = ok ? "Finished." : "Finished with problems.";
    $b("bp-end").style.color = ok ? "#7CE0B0" : "#F5C76A";
    $b("bp-end").textContent = ok ? `All ${saved.length - 1} targets and the sky map are saved (${(bytes / 1048576).toFixed(1)} MB). You can close this window and copy the whole folder to GitHub.`
      : `${failed.join(", ")} could not be saved. Run the builder again (already-saved targets are kept), ideally with a better internet connection.`;
    progress(1);
  }

  (async () => { while (!(typeof bootDone !== "undefined" && bootDone && model)) await sleep(500); run(); })();
})();
