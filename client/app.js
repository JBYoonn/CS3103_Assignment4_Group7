// LiveRecall browser client.
//
//   connect -> CLIENT_HELLO(resume state) -> HISTORY (merge) -> live CAPTION_UPDATEs
//   close   -> reconnect with backoff, resume from the latest FINALIZED caption held
//
// Server URL: ws://<page host>:8765 by default; override with ?server=ws://host:port

import { CaptionStore } from "./store.js";

const params = new URLSearchParams(location.search);
const SERVER = params.get("server") || `ws://${location.hostname || "127.0.0.1"}:8765`;
const SESSION = params.get("session") || "L1";
const HISTORY_SECONDS = 60;          // ask for the full recall window on (re)join
const MAX_DOM_LINES = 400;           // transcript shows recent lines; the store keeps more
const BACKOFF_MS = [500, 1000, 2000, 4000, 5000];

const $ = (id) => document.getElementById(id);
const els = {
  conn: $("conn"), connText: $("conn-text"), retry: $("retry"), session: $("session"),
  transcript: $("transcript"), lines: $("lines"), empty: $("empty"), jump: $("jump"),
  miss: $("miss"), recall: $("recall"), recallTitle: $("recall-title"), recallMeta: $("recall-meta"),
  recallBody: $("recall-body"), recallNote: $("recall-note"), recallClose: $("recall-close"),
  toast: $("toast"), simDrop: $("sim-drop"),
  stServer: $("st-server"), stHeld: $("st-held"), stLast: $("st-last"),
  stRecovered: $("st-recovered"), stReconnects: $("st-reconnects"),
};

const store = new CaptionStore({ keepSeconds: 600 });
const lineEls = new Map();           // segment -> <li>
const nowS = () => Date.now() / 1000;

let ws = null;
let attempt = 0;
let reconnectTimer = null;
let countdownTimer = null;
let everConnected = false;
let holdUntil = 0;                   // demo: simulated network drop
let recallSeconds = 30;
let recoveredTotal = 0;
let reconnects = 0;
let connectedAt = null;

function clientId() {
  const key = "liverecall-client-id";
  try {
    let v = sessionStorage.getItem(key);
    if (!v) { v = String(100000 + Math.floor(Math.random() * 900000)); sessionStorage.setItem(key, v); }
    return Number(v);
  } catch { return 100000 + Math.floor(Math.random() * 900000); }
}
const CLIENT_ID = clientId();

// ---------------------------------------------------------------- connection

function setConn(state, text) {
  els.conn.dataset.state = state;
  els.connText.textContent = text;
  els.retry.hidden = !(state === "reconnecting" || state === "offline");
}

function connect() {
  clearTimeout(reconnectTimer);
  clearInterval(countdownTimer);
  if (nowS() < holdUntil) { scheduleReconnect(); return; }
  setConn(everConnected ? "reconnecting" : "connecting", everConnected ? "Reconnecting…" : "Connecting…");

  if (ws) { const old = ws; ws = null; try { old.close(); } catch { /* already closed */ } }
  let sock;
  try { sock = new WebSocket(SERVER); } catch { scheduleReconnect(); return; }
  ws = sock;
  let gotHistory = false;

  sock.addEventListener("open", () => {
    if (ws !== sock) return;           // superseded by a newer attempt (e.g. "Retry now")
    setConn("syncing", "Syncing…");
    sock.send(JSON.stringify({
      type: "CLIENT_HELLO", client_id: CLIENT_ID, session: SESSION,
      ...store.resumeState(), history_seconds: HISTORY_SECONDS,
    }));
  });

  sock.addEventListener("message", (ev) => {
    if (ws !== sock) return;
    let msg;
    try { msg = JSON.parse(ev.data); } catch { return; }
    if (msg.type === "CAPTION_UPDATE") {
      onCaption(msg);
    } else if (msg.type === "HISTORY") {
      const firstOnThisConnection = !gotHistory;
      gotHistory = true;
      onHistory(msg, firstOnThisConnection);
      if (firstOnThisConnection) {
        const wasReconnect = everConnected;
        everConnected = true;
        attempt = 0;
        connectedAt = nowS();
        if (wasReconnect) reconnects += 1;
        setConn("live", "Live");
      }
    } else if (msg.type === "SESSION_RESET") {
      resetView();
      toast("A new lecture started.");
    }
    // TICK and unknown types: ignore (forward compatible).
  });

  sock.addEventListener("close", () => {
    if (ws !== sock) return;
    ws = null;
    scheduleReconnect();
  });
  sock.addEventListener("error", () => { /* close follows */ });
}

function scheduleReconnect() {
  const held = Math.max(0, holdUntil - nowS()) * 1000;
  const base = BACKOFF_MS[Math.min(attempt, BACKOFF_MS.length - 1)];
  const delay = Math.max(held, base * (0.75 + Math.random() * 0.5));     // jitter avoids reconnect storms
  attempt += 1;
  const due = Date.now() + delay;
  const offline = navigator.onLine === false;
  const tick = () => {
    const s = Math.max(0, Math.ceil((due - Date.now()) / 1000));
    if (offline) setConn("offline", `Offline — retrying in ${s}s`);
    else setConn("reconnecting", s > 0 ? `Reconnecting in ${s}s` : "Reconnecting…");
  };
  tick();
  clearInterval(countdownTimer);
  countdownTimer = setInterval(tick, 250);
  clearTimeout(reconnectTimer);
  reconnectTimer = setTimeout(connect, delay);
}

els.retry.addEventListener("click", () => { holdUntil = 0; attempt = 0; connect(); });
window.addEventListener("online", () => { if (!ws) { attempt = 0; connect(); } });

els.simDrop.addEventListener("click", () => {
  holdUntil = nowS() + 8;
  els.simDrop.disabled = true;
  setTimeout(() => { els.simDrop.disabled = false; }, 8000);
  if (ws) ws.close(4000, "simulated network drop");
});

// ---------------------------------------------------------------- messages

function onCaption(msg) {
  const res = store.apply(msg, nowS());
  if (res.kind === "ignored") return;
  render(res.entry, res.kind);
  afterChange();
}

function onHistory(msg, firstOnThisConnection) {
  const isReconnect = firstOnThisConnection && everConnected;
  const applied = store.mergeHistory(msg.items, nowS(), isReconnect);
  for (const e of applied) render(e, "final");
  if (isReconnect) {
    recoveredTotal += applied.length;
    toast(applied.length
      ? `Reconnected — recovered ${applied.length} missed caption${applied.length === 1 ? "" : "s"}.`
      : "Reconnected — nothing was missed.");
  } else if (firstOnThisConnection && applied.length) {
    toast(`Caught up on the last ${HISTORY_SECONDS} s (${applied.length} caption${applied.length === 1 ? "" : "s"}).`);
  }
  afterChange();
}

// ---------------------------------------------------------------- rendering

function fmtTime(ts) {
  if (typeof ts !== "number") return "";
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false });
}

function render(entry, kind) {
  const atBottom = isNearBottom();
  let li = lineEls.get(entry.segment);
  if (!li) {
    li = document.createElement("li");
    li.className = "line";
    li.dataset.segment = String(entry.segment);
    li.innerHTML = '<time></time><span><span class="text"></span><span class="tag"></span></span>';
    insertInOrder(li, entry.segment);
    lineEls.set(entry.segment, li);
  }
  const wasProvisional = li.classList.contains("provisional");
  li.querySelector("time").textContent = fmtTime(entry.source_ts);
  li.querySelector(".text").textContent = entry.text;
  li.classList.toggle("provisional", !entry.final);
  li.classList.toggle("recovered", entry.final && entry.recovered);
  const tag = li.querySelector(".tag");
  tag.textContent = !entry.final ? "drafting" : entry.recovered ? "recovered" : "";
  tag.hidden = !tag.textContent;
  if (entry.final && wasProvisional) {
    li.classList.remove("settled");
    void li.offsetWidth;               // restart the "settled" highlight
    li.classList.add("settled");
  }
  els.empty.hidden = true;
  trimDom();
  if (atBottom) scrollToLive(); else els.jump.hidden = false;
}

function insertInOrder(li, segment) {
  // Usually appends; HISTORY after a reconnect may need to slot in earlier.
  let node = els.lines.lastElementChild;
  while (node && Number(node.dataset.segment) > segment) node = node.previousElementSibling;
  els.lines.insertBefore(li, node ? node.nextElementSibling : els.lines.firstElementChild);
}

function trimDom() {
  while (els.lines.childElementCount > MAX_DOM_LINES) {
    const first = els.lines.firstElementChild;
    lineEls.delete(Number(first.dataset.segment));
    first.remove();
  }
}

function isNearBottom() {
  const t = els.transcript;
  return t.scrollHeight - t.scrollTop - t.clientHeight < 80;
}

function scrollToLive() {
  els.transcript.scrollTop = els.transcript.scrollHeight;
  els.jump.hidden = true;
}

els.transcript.addEventListener("scroll", () => { if (isNearBottom()) els.jump.hidden = true; });
els.jump.addEventListener("click", scrollToLive);

function resetView() {
  store.reset();
  lineEls.clear();
  els.lines.replaceChildren();
  els.empty.hidden = false;
  afterChange();
}

// ---------------------------------------------------------------- recall

function openRecall() {
  els.recall.hidden = false;
  els.miss.setAttribute("aria-expanded", "true");
  renderRecall();
}

function closeRecall() {
  els.recall.hidden = true;
  els.miss.setAttribute("aria-expanded", "false");
}

function renderRecall() {
  if (els.recall.hidden) return;
  // Uses captions already held in this browser -- no server request in normal operation.
  const lectureNow = store.lectureNow(nowS());
  const items = store.recall(recallSeconds, lectureNow);
  els.recallTitle.textContent = `Last ${recallSeconds} seconds`;
  els.recallMeta.textContent = `${items.length} caption${items.length === 1 ? "" : "s"}`;
  if (!items.length) {
    els.recallBody.innerHTML = '<p class="recall-empty">Nothing was finalized in this window yet.</p>';
  } else {
    els.recallBody.replaceChildren(...items.map((e) => {
      const p = document.createElement("p");
      const t = document.createElement("time");
      t.textContent = fmtTime(e.source_ts);
      p.append(t, document.createTextNode(e.text));
      return p;
    }));
  }
  const oldest = store.oldestFinalTs();
  const partial = oldest !== null && oldest > lectureNow - recallSeconds + 3
    && connectedAt !== null && nowS() - connectedAt < recallSeconds;
  els.recallNote.hidden = !partial;
  if (partial) els.recallNote.textContent = "You joined recently — showing everything available so far.";
}

els.miss.addEventListener("click", () => (els.recall.hidden ? openRecall() : closeRecall()));
els.recallClose.addEventListener("click", () => { closeRecall(); els.miss.focus(); });
for (const btn of document.querySelectorAll(".segmented button")) {
  btn.addEventListener("click", () => {
    recallSeconds = Number(btn.dataset.seconds);
    for (const b of document.querySelectorAll(".segmented button")) {
      b.setAttribute("aria-checked", String(b === btn));
    }
    openRecall();
  });
}
document.addEventListener("keydown", (e) => {
  if (e.target instanceof HTMLInputElement || e.metaKey || e.ctrlKey || e.altKey) return;
  if (e.key === "m" || e.key === "M") { els.recall.hidden ? openRecall() : closeRecall(); }
  if (e.key === "Escape" && !els.recall.hidden) { closeRecall(); els.miss.focus(); }
});

// ---------------------------------------------------------------- misc

let toastTimer = null;
function toast(text) {
  els.toast.textContent = text;
  els.toast.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => { els.toast.hidden = true; }, 4000);
}

function afterChange() {
  renderRecall();
  const last = store.resumeState();
  els.stHeld.textContent = String(store.entries().length);
  els.stLast.textContent = last.last_segment ? `segment ${last.last_segment} (rev ${last.last_revision})` : "–";
  els.stRecovered.textContent = String(recoveredTotal);
  els.stReconnects.textContent = String(reconnects);
}

setInterval(() => {
  store.prune(store.lectureNow(nowS()));
  renderRecall();               // the window slides even when nothing new arrives
}, 5000);

els.stServer.textContent = SERVER;
els.session.textContent = `Lecture ${SESSION}`;
connect();
