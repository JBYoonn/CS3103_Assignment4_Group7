/*
 * app.js - LiveRecall browser client.
 *
 * Responsibilities (caption rules themselves live in store.js):
 *   - open the WebSocket and send CLIENT_HELLO with the resume point
 *   - pass CAPTION_UPDATE / HISTORY messages to the store
 *   - draw the live view (one line per segment, revised in place)
 *   - "What did I miss?" panel: 30 s by default, 15 / 30 / 60 s tabs,
 *     built only from captions already held locally
 *   - reconnect automatically with backoff and show connection state
 *
 * URL overrides (handy for the demo / Ubuntu VM):
 *   index.html?server=ws://192.168.64.5:8765&session=L1
 */
(function () {
  "use strict";
 
  // ---------- Configuration ----------
  const params = new URLSearchParams(location.search);
  const WS_URL = params.get("server") || "ws://127.0.0.1:8765";
  const SESSION = params.get("session") || "L1";
  const HISTORY_REQUEST_SECONDS = 60;      // max the brief allows in CLIENT_HELLO
  const RECALL_DEFAULT_SECONDS = 30;       // brief: "initially display the previous 30 seconds"
  const RECONNECT_MIN_MS = 500;
  const RECONNECT_MAX_MS = 5000;
  const SIMULATED_DROP_MS = 8000;
  const PRUNE_EVERY_MS = 2000;
 
  // ---------- State ----------
  const store = new LiveRecallStore.CaptionStore(); // 90 s window (store.js)
  let ws = null;
  let everConnected = false;
  let reconnectAttempts = 0;
  let reconnectTimer = null;
  let countdownTimer = null;
  let pausedUntil = 0;          // used by "Simulate network drop"
  let recallSeconds = RECALL_DEFAULT_SECONDS;
  let renderQueued = false;
  const justFinalized = new Set();
  const stats = { reconnects: 0, recovered: 0, ignored: 0 };
 
  // ---------- Elements ----------
  const $ = (id) => document.getElementById(id);
  const statusEl = $("status");
  const statusText = $("status-text");
  const liveEl = $("live");
  const jumpBtn = $("jump-latest");
  const recallPanel = $("recall-panel");
  const recallList = $("recall");
  const recallMeta = $("recall-meta");
  const layout = document.querySelector(".layout");
  const toastEl = $("toast");
 
  $("session-label").textContent = `Session ${SESSION}`;
  $("d-server").textContent = WS_URL;
 
  // ---------- Helpers ----------
  function numericClientId() {
    const key = "liverecall-client-id";
    let value = null;
    try { value = sessionStorage.getItem(key); } catch (_) { /* storage blocked */ }
    if (!value) {
      value = String(Math.floor(100000 + Math.random() * 900000));
      try { sessionStorage.setItem(key, value); } catch (_) { /* ignore */ }
    }
    return Number(value);
  }
 
  function setStatus(state, text) {
    statusEl.dataset.state = state;
    statusText.textContent = text;
  }
 
  // Screen readers: the live area is NOT aria-live (it would read every
  // revision: "TCP uses", "TCP uses cumulative", ...). Instead, each FINAL
  // caption is announced once through this hidden region.
  const srEl = $("sr-announce");
  function announce(text) {
    srEl.textContent = text;
  }
 
  let toastTimer = null;
  function toast(message) {
    toastEl.textContent = message;
    toastEl.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => { toastEl.hidden = true; }, 3500);
  }
 
  // ---------- Connection ----------
  function connect() {
    clearTimeout(reconnectTimer);
    clearInterval(countdownTimer);
    setStatus(everConnected ? "reconnecting" : "connecting",
              everConnected ? "Reconnecting…" : "Connecting…");
 
    let sock;
    try {
      sock = new WebSocket(WS_URL);
    } catch (err) {
      console.error("Bad server URL", err);
      setStatus("offline", "Invalid server address");
      return;
    }
    ws = sock;
 
    sock.addEventListener("open", () => {
      if (sock !== ws) return; // an older socket; ignore
      setStatus("connected", "Live");
      const hello = {
        type: "CLIENT_HELLO",
        client_id: numericClientId(),
        session: SESSION,
        ...store.resumeState(),          // last_segment / last_revision (0/0 on first join)
        history_seconds: HISTORY_REQUEST_SECONDS,
      };
      sock.send(JSON.stringify(hello));
    });
 
    sock.addEventListener("message", (event) => {
      if (sock !== ws) return;
      let msg;
      try { msg = JSON.parse(event.data); } catch (_) { return; } // ignore junk
      handleMessage(msg);
    });
 
    sock.addEventListener("close", () => {
      if (sock !== ws) return;
      ws = null;
      scheduleReconnect();
    });
 
    // "error" is always followed by "close", which handles reconnecting.
    sock.addEventListener("error", () => {});
  }
 
  function scheduleReconnect() {
    clearTimeout(reconnectTimer);
    clearInterval(countdownTimer);
 
    const now = Date.now();
    let delay;
    if (pausedUntil > now) {
      delay = pausedUntil - now;                      // simulated drop
    } else {
      // Exponential backoff 0.5 s -> 1 -> 2 -> 4 -> 5 s max, plus small jitter
      // so many clients don't all reconnect in the same instant.
      delay = Math.min(RECONNECT_MAX_MS, RECONNECT_MIN_MS * 2 ** reconnectAttempts);
      delay += Math.random() * 250;
      reconnectAttempts++;
    }
 
    const deadline = now + delay;
    const showCountdown = () => {
      const secs = Math.max(0, Math.ceil((deadline - Date.now()) / 1000));
      if (pausedUntil > Date.now()) setStatus("offline", `Offline (simulated) · retry in ${secs}s`);
      else if (!navigator.onLine) setStatus("offline", "Offline · waiting for network");
      else setStatus("reconnecting", secs > 0 ? `Disconnected · retrying in ${secs}s` : "Reconnecting…");
    };
    showCountdown();
    countdownTimer = setInterval(showCountdown, 500);
    reconnectTimer = setTimeout(connect, delay);
  }
 
  // Network came back: don't wait for the backoff timer.
  window.addEventListener("online", () => {
    if (!ws && pausedUntil <= Date.now()) connect();
  });
 
  // ---------- Messages ----------
  function handleMessage(msg) {
    if (msg.session && msg.session !== SESSION) return;
 
    if (msg.type === "CAPTION_UPDATE") {
      const result = store.apply(msg);          // also advances the lecture clock
      if (result.changed) {
        if (result.reason === "restart") {
          justFinalized.clear();
          toast("A new lecture started · captions reset");
        }
        if (msg.final === true) {
          justFinalized.add(msg.segment);
          announce(msg.text);                    // screen readers hear finals only
        }
        queueRender();
      } else {
        stats.ignored++;
        queueRender();
      }
    } else if (msg.type === "HISTORY") {
      const items = Array.isArray(msg.items) ? msg.items : [];
      const added = store.mergeHistory(items);
      stats.recovered += added;
      stats.ignored += items.length - added;
      reconnectAttempts = 0; // server answered properly; reset backoff
 
      if (!everConnected) {
        toast(added > 0 ? `Joined late · loaded ${added} recent caption${added === 1 ? "" : "s"}`
                        : "Connected · waiting for captions");
      } else {
        stats.reconnects++;
        toast(added > 0 ? `Back online · recovered ${added} missed caption${added === 1 ? "" : "s"}`
                        : "Back online · nothing missed");
      }
      everConnected = true;
      queueRender();
    }
    // Any other message type (e.g. future extensions) is ignored safely.
  }
 
  // ---------- Rendering ----------
  function queueRender() {
    if (renderQueued) return;
    renderQueued = true;
    requestAnimationFrame(() => {
      renderQueued = false;
      renderLive();
      if (!recallPanel.hidden) renderRecall();
      renderStats();
    });
  }
 
  function isNearBottom() {
    return liveEl.scrollHeight - liveEl.scrollTop - liveEl.clientHeight < 60;
  }
 
  function renderLive() {
    const stick = isNearBottom();
    const rows = store.viewRows();
    const frag = document.createDocumentFragment();
 
    if (rows.length === 0) {
      const p = document.createElement("p");
      p.className = "empty";
      p.textContent = "Waiting for the lecture to start…";
      frag.appendChild(p);
    }
 
    for (const row of rows) {
      if (row.gap) {
        const g = document.createElement("div");
        g.className = "gap";
        const n = row.to - row.from + 1;
        g.textContent = `⋯ ${n} caption${n === 1 ? "" : "s"} unavailable (missed while offline)`;
        frag.appendChild(g);
        continue;
      }
      const p = document.createElement("p");
      p.className = `line ${row.final ? "final" : "provisional"}`;
      if (justFinalized.has(row.segment)) p.classList.add("just-final");
      p.dataset.segment = row.segment;
      p.dataset.revision = row.revision;
      p.textContent = row.text;
      if (row.recovered) {
        const tag = document.createElement("span");
        tag.className = "tag";
        tag.textContent = "recovered";
        p.appendChild(tag);
      }
      frag.appendChild(p);
    }
    justFinalized.clear();
 
    liveEl.replaceChildren(frag);
    if (stick) liveEl.scrollTop = liveEl.scrollHeight;
    jumpBtn.hidden = isNearBottom();
  }
 
  liveEl.addEventListener("scroll", () => { jumpBtn.hidden = isNearBottom(); });
  jumpBtn.addEventListener("click", () => { liveEl.scrollTop = liveEl.scrollHeight; });
 
  // ---------- "What did I miss?" ----------
  function openRecall(seconds) {
    recallSeconds = seconds;
    recallPanel.hidden = false;
    layout.classList.add("with-recall");
    renderRecall();
  }
 
  function closeRecall() {
    recallPanel.hidden = true;
    layout.classList.remove("with-recall");
  }
 
  function renderRecall() {
    for (const tab of recallPanel.querySelectorAll("[data-seconds]")) {
      tab.setAttribute("aria-selected", String(Number(tab.dataset.seconds) === recallSeconds));
    }
    const now = store.lectureNow();
    const items = store.recall(recallSeconds, now);   // local data only, no server request
 
    recallMeta.textContent = items.length
      ? `Last ${recallSeconds} s · ${items.length} caption${items.length === 1 ? "" : "s"}`
      : `Nothing finalized in the last ${recallSeconds} s.`;
 
    const frag = document.createDocumentFragment();
    for (const e of items) {
      const li = document.createElement("li");
      const ago = document.createElement("span");
      ago.className = "ago";
      ago.textContent = `${Math.max(0, Math.round(now - e.source_ts))} s ago${e.recovered ? " · recovered" : ""}`;
      li.append(ago, document.createTextNode(e.text));
      frag.appendChild(li);
    }
    recallList.replaceChildren(frag);
  }
 
  $("miss-btn").addEventListener("click", () => openRecall(RECALL_DEFAULT_SECONDS));
  $("recall-close").addEventListener("click", closeRecall);
  for (const tab of recallPanel.querySelectorAll("[data-seconds]")) {
    tab.addEventListener("click", () => openRecall(Number(tab.dataset.seconds)));
  }
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeRecall(); });
 
  // ---------- Stats / demo tools ----------
  function renderStats() {
    const r = store.resumeState();
    $("d-resume").textContent = `segment ${r.last_segment}, revision ${r.last_revision}`;
    $("d-held").textContent = String(store.segments.size);
    $("d-reconnects").textContent = String(stats.reconnects);
    $("d-recovered").textContent = String(stats.recovered);
    $("d-ignored").textContent = String(stats.ignored);
  }
 
  $("drop-btn").addEventListener("click", () => {
    pausedUntil = Date.now() + SIMULATED_DROP_MS;
    if (ws) ws.close(4000, "simulated network drop");  // close handler schedules reconnect
    else scheduleReconnect();
  });
 
  // ---------- Housekeeping ----------
  // Keep memory bounded and keep the recall "s ago" labels moving.
  setInterval(() => {
    const removed = store.prune();
    if (removed > 0 || !recallPanel.hidden) queueRender();
  }, PRUNE_EVERY_MS);
 
  connect();
})();
 
