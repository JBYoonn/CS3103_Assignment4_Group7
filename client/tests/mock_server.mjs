// mock_server.mjs - tiny stand-in for solution_server.py, for CLIENT testing only.
//
// It speaks the protocol from Section 4 of the brief:
//   client -> CLIENT_HELLO   server -> HISTORY (finals after last_segment, within history_seconds)
//   server -> CAPTION_UPDATE (provisional revisions, then final=true)
// No npm packages needed (plain Node 18+).
//
//   node client/tests/mock_server.mjs            # listens on ws://127.0.0.1:8765
//   node client/tests/mock_server.mjs --port 9000
//
// This is NOT our real server; it just lets the client team test reconnect,
// late join and HISTORY before solution_server.py implements them.

import http from "node:http";
import crypto from "node:crypto";

const portArg = process.argv.indexOf("--port");
const PORT = portArg > 0 ? Number(process.argv[portArg + 1]) : 8765;
const HISTORY_KEEP_SECONDS = 120;

const WORDS = ("TCP uses cumulative acknowledgements so the receiver reports the next byte it expects " +
  "WebSockets run over TCP which means every message arrives in order and reliably " +
  "when a packet is lost the sender retransmits and later data waits behind it " +
  "this is called head of line blocking and it increases tail latency " +
  "a slow client should not be allowed to delay every other client in the lecture").split(" ");

// ---------- caption generator ----------
let segment = 0;
let wordPos = 0;
let active = null;          // latest revision of the segment in progress
const finals = [];          // bounded history of final captions
const clients = new Set();

function nextSegment() {
  segment++;
  const len = 4 + (segment % 4);                 // 4..7 words per segment
  const words = [];
  for (let i = 0; i < len; i++) words.push(WORDS[(wordPos + i) % WORDS.length]);
  wordPos += len;
  let rev = 0;
  const step = 2;                                // reveal ~2 words per revision
  const timer = setInterval(() => {
    rev++;
    const shown = Math.min(words.length, rev * step);
    const final = shown >= words.length;
    const text = words.slice(0, shown).join(" ") + (final ? "." : "");
    const msg = { type: "CAPTION_UPDATE", session: "L1", segment, revision: rev,
                  source_ts: Date.now() / 1000, final, text };
    broadcast(msg);
    if (final) {
      clearInterval(timer);
      active = null;
      finals.push(msg);
      const cutoff = Date.now() / 1000 - HISTORY_KEEP_SECONDS;
      while (finals.length && finals[0].source_ts < cutoff) finals.shift();
      setTimeout(nextSegment, 300);
    } else {
      active = msg;
    }
  }, 350);
}

function broadcast(msg) {
  const data = JSON.stringify(msg);
  for (const c of clients) if (c.ready) sendText(c.socket, data);
}

// ---------- minimal WebSocket framing ----------
function sendText(socket, str) {
  if (socket.destroyed) return;
  const payload = Buffer.from(str);
  let header;
  if (payload.length < 126) header = Buffer.from([0x81, payload.length]);
  else if (payload.length < 65536) { header = Buffer.alloc(4); header[0] = 0x81; header[1] = 126; header.writeUInt16BE(payload.length, 2); }
  else { header = Buffer.alloc(10); header[0] = 0x81; header[1] = 127; header.writeBigUInt64BE(BigInt(payload.length), 2); }
  socket.write(Buffer.concat([header, payload]));
}

function parseFrames(client, chunk) {
  client.buf = Buffer.concat([client.buf, chunk]);
  for (;;) {
    const b = client.buf;
    if (b.length < 2) return;
    const opcode = b[0] & 0x0f;
    let len = b[1] & 0x7f, off = 2;
    if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; }
    else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
    const masked = (b[1] & 0x80) !== 0;
    const need = off + (masked ? 4 : 0) + len;
    if (b.length < need) return;
    let payload = b.subarray(off + (masked ? 4 : 0), need);
    if (masked) {
      const mask = b.subarray(off, off + 4);
      payload = Buffer.from(payload.map((x, i) => x ^ mask[i % 4]));
    }
    client.buf = b.subarray(need);
    if (opcode === 0x8) { client.socket.end(Buffer.from([0x88, 0])); return; }   // close
    if (opcode === 0x9) { client.socket.write(Buffer.concat([Buffer.from([0x8a, payload.length]), payload])); continue; } // ping->pong
    if (opcode === 0x1) onMessage(client, payload.toString());
  }
}

function onMessage(client, text) {
  let msg;
  try { msg = JSON.parse(text); } catch { return; }
  if (msg.type !== "CLIENT_HELLO") return;
  const last = Number(msg.last_segment) || 0;
  const secs = Math.min(60, Number(msg.history_seconds) || 30);
  const from = Date.now() / 1000 - secs;
  const items = finals.filter((f) => f.segment > last && f.source_ts >= from);
  sendText(client.socket, JSON.stringify({ type: "HISTORY", items }));
  if (active) sendText(client.socket, JSON.stringify(active)); // current in-progress caption
  client.ready = true;
  console.log(`client ${msg.client_id} hello last_segment=${last} -> HISTORY ${items.length} item(s)` +
              (items.length ? ` [${items[0].segment}..${items.at(-1).segment}]` : ""));
}

const server = http.createServer((_, res) => { res.writeHead(426); res.end("WebSocket only"); });
server.on("upgrade", (req, socket) => {
  const key = req.headers["sec-websocket-key"];
  const accept = crypto.createHash("sha1").update(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").digest("base64");
  socket.write("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n" +
               `Sec-WebSocket-Accept: ${accept}\r\n\r\n`);
  const client = { socket, buf: Buffer.alloc(0), ready: false };
  clients.add(client);
  socket.on("data", (c) => parseFrames(client, c));
  socket.on("close", () => clients.delete(client));
  socket.on("error", () => clients.delete(client));
});

server.listen(PORT, "0.0.0.0", () => {
  console.log(`mock LiveRecall server on ws://127.0.0.1:${PORT}  (Ctrl+C to stop)`);
  nextSegment();
});
