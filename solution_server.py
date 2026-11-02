#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server.

Required evaluator command:
    python solution_server.py --port 8765

Design overview
---------------
Source side
  * One source connection sends CAPTION_UPDATE messages. The server keeps the
    newest accepted revision of every recent segment and ignores stale or
    duplicate revisions (an older revision never overwrites a newer one, and
    nothing overwrites a final). A finalized-segment watermark also rejects
    old updates after their revision entries have been evicted. The source
    completes segments in increasing order, as in caption_source.py.
  * Each accepted update is JSON-encoded once and the same string is handed to
    every client without per-client serialization.

Bounded recovery history
  * Finalized captions are kept in a deque bounded both by age
    (HISTORY_KEEP_SECONDS, comfortably above the 60 s the protocol allows a
    client to ask for) and by count (HISTORY_MAX_ITEMS). Nothing else about the
    lecture is retained.

Per-client delivery
  * Every client has its own pending map (segment -> newest encoded update)
    drained by its own sender task. The source handler never awaits a client
    send, so a slow client cannot delay the source or other clients.
  * Semantic coalescing happens naturally in the pending map: if a client is
    still behind when a newer revision of the same segment arrives, the newer
    revision replaces the waiting provisional one in place. A final is never
    replaced while queued. Overflow closes the connection and recovery is
    limited to the retained history window. Under normal load the map is empty
    when each new revision arrives, so clients see every revision.
  * The map is bounded (CLIENT_MAX_PENDING segments). A client that falls
    that far behind is disconnected with code 1013; it reconnects and resumes
    from its last finalized caption via HISTORY, which is bounded too.
  * Ping-based flow control (see Client.run_sender) limits how much is in
    flight to each client. Delayed pongs prioritize finals, but don't prove
    that the client application has processed or displayed captions.
  * The kernel send buffer and the websockets write buffer are kept small so
    that backpressure shows up in the pending map (where it can be
    coalesced) instead of hiding megabytes of stale data in socket buffers.

Client handshake
  * On CLIENT_HELLO the server takes the HISTORY snapshot and registers the
    client in one synchronous step (no await in between), so no live update
    can be missed or duplicated between HISTORY and the live stream.
  * HISTORY holds finalized captions newer than (last_segment, last_revision)
    received within the last history_seconds (capped at 60), oldest first.
    If a segment is currently in progress, its newest provisional revision is
    queued right after HISTORY so a late joiner sees the current sentence.
  * One session is selected by the first valid hello, with one active source.
    Restart the server for a new lecture; a reconnecting source must continue
    its segment numbering rather than restart from segment 1.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import socket
import time
from collections import OrderedDict, deque
from contextlib import suppress
from typing import Any

from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

# ---- Tunable bounds -------------------------------------------------------
MAX_HISTORY_SECONDS = 60          # protocol maximum for history_seconds
HISTORY_KEEP_SECONDS = 90         # retained window (a bit above 60 s)
HISTORY_MAX_ITEMS = 2000          # hard cap even if the source is very bursty
SEGMENT_STATE_MAX = 4096          # recent segments whose revision we remember
CLIENT_MAX_PENDING = 256          # segments waiting for one client
CLIENT_SNDBUF_BYTES = 32 * 1024   # kernel send buffer per client socket
WRITE_LIMIT_BYTES = 16 * 1024     # websockets user-space write buffer
PING_EVERY = 4                    # messages between flow-control pings
MAX_UNACKED_PINGS = 1             # gate sending on outstanding transport pings
SLOW_BLOCK_SECONDS = 1.0          # window wait that counts as a slow-reader sign
SLOW_BLOCKS_NEEDED = 2            # consecutive long waits before acting
SLOW_PING_AGE_SECONDS = 4.0       # an unanswered ping this old = slow reader
FINALS_ONLY_HOLD_SECONDS = 10.0   # finals-only period once a reader is slow
RESET_FINALS_ONLY_SECONDS = 60.0  # finals-only period after a reset client reconnects
RESET_SLOW_CLIENTS = True         # reset-and-resume; --no-reset-slow turns it off
RESET_COOLDOWN_SECONDS = 60.0     # at most one reset per client per minute
SLOW_MEMORY_SECONDS = 300.0       # remember a reset client_id this long
SLOW_IDS_MAX = 10000              # bound on remembered client ids
LAG_SECONDS = 1.0                 # unanswered ping older than this = lagging
STATS_INTERVAL = 10.0             # seconds between status log lines


def _seg_rev_newer(seg: int, rev: int, base_seg: int, base_rev: int) -> bool:
    return seg > base_seg or (seg == base_seg and rev > base_rev)


class Client:
    """Delivery state for one connected student client."""

    __slots__ = ("ws", "client_id", "pending", "wakeup", "sender",
                 "sent", "coalesced", "closing", "slow_episodes",
                 "finals_only_until", "slow_blocks")

    def __init__(self, ws, client_id: Any) -> None:
        self.ws = ws
        self.client_id = client_id
        # key -> (encoded message, is_final). Keys are segment numbers, plus
        # the string "HISTORY" for the initial history reply.
        self.pending: OrderedDict[Any, tuple[str, bool]] = OrderedDict()
        self.wakeup = asyncio.Event()
        self.sender: asyncio.Task | None = None
        self.sent = 0
        self.coalesced = 0
        self.closing = False
        self.slow_episodes = 0
        self.finals_only_until = 0.0    # slow-reader mode deadline (monotonic)
        self.slow_blocks = 0            # consecutive long window waits

    def enqueue(self, key: Any, raw: str, final: bool) -> bool:
        """Queue one message. Returns False if the client is hopelessly behind."""
        waiting = self.pending.get(key)
        if waiting is not None:
            if waiting[1]:
                return True               # a final is already queued: keep it
            self.pending[key] = (raw, final)   # replace in place (keeps order)
            self.coalesced += 1
        else:
            if len(self.pending) >= CLIENT_MAX_PENDING:
                return False
            self.pending[key] = (raw, final)
        self.wakeup.set()
        return True

    async def run_sender(self) -> None:
        """Drain the pending map with ping-based flow control.

        After every PING_EVERY messages we send a WebSocket ping. A delayed
        pong signals transport lag; it isn't an application acknowledgement.
        If more than MAX_UNACKED_PINGS are unanswered we stop sending and wait.
        Waiting updates remain in our pending map, where they can be coalesced.
        The receiving WebSocket stack may already have buffered earlier data.
        A client whose oldest unanswered ping is
        older than LAG_SECONDS is sent finals first (see below).
        """
        waiters: deque[tuple[float, asyncio.Future]] = deque()   # (sent_at, pong)
        since_ping = 0
        min_rtt = 1.0              # best ping round trip seen for this client
        try:
            while True:
                while waiters and waiters[0][1].done():
                    _, fut = waiters.popleft()
                    rtt = None if fut.cancelled() or fut.exception() else fut.result()
                    if isinstance(rtt, float):
                        min_rtt = min(min_rtt, rtt)
                # Lagging = the client has not answered a ping sent over
                # LAG_SECONDS ago. While lagging, send finals only: the
                # provisional revisions stay queued (and keep being replaced
                # by newer ones), so the reader's limited bandwidth goes to
                # durable state and it can catch up.
                now = time.monotonic()
                # A ping unanswered for several seconds means the client has
                # stopped reading (its library queue is full), however the
                # window waits looked. Catches severe and bursty slow readers.
                if waiters and now - waiters[0][0] > self._ping_age_limit(min_rtt):
                    if self._mark_slow(now):
                        return
                lagging = (now < self.finals_only_until or
                           (bool(waiters) and now - waiters[0][0] > LAG_SECONDS))
                key = self._next_key(finals_only=lagging)

                if key is None:
                    if self.pending:
                        # Only provisionals left and the client is lagging:
                        # re-check shortly, or sooner if a new final arrives.
                        self.wakeup.clear()
                        with suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(self.wakeup.wait(), 0.1)
                    else:
                        self.wakeup.clear()
                        await self.wakeup.wait()
                    continue

                raw, final = self.pending.pop(key)
                await self.ws.send(raw)
                self.sent += 1
                since_ping += 1
                if since_ping >= PING_EVERY:
                    since_ping = 0
                    waiters.append((time.monotonic(), await self.ws.ping()))
                    if len(waiters) > MAX_UNACKED_PINGS:
                        blocked_at = time.monotonic()
                        while len(waiters) > MAX_UNACKED_PINGS:
                            sent_at, fut = waiters.popleft()
                            limit = self._ping_age_limit(min_rtt)
                            remaining = sent_at + limit - time.monotonic()
                            if remaining > 0:
                                await asyncio.wait([fut], timeout=remaining)
                            if not fut.done():
                                # Still no pong after the limit: slow reader.
                                if self._mark_slow(time.monotonic()):
                                    return
                            rtt = await fut
                            if isinstance(rtt, float):
                                min_rtt = min(min_rtt, rtt)
                        # The window was full: we produce faster than this
                        # client reads. A wait well beyond its normal round
                        # trip means a slow reader (not just network delay),
                        # so send it finals only for a while. Finals arrive
                        # at about a third of the revision rate, which lets
                        # the client drain buffers we cannot see.
                        # One long wait can be a TCP retransmission stall on
                        # a lossy link, so only consecutive long waits count.
                        now = time.monotonic()
                        if now - blocked_at > max(SLOW_BLOCK_SECONDS, 4 * min_rtt):
                            self.slow_blocks += 1
                        else:
                            self.slow_blocks = 0
                        if self.slow_blocks >= SLOW_BLOCKS_NEEDED:
                            # Reset and resume: the client's own WebSocket
                            # library still holds ~16 stale messages we can't
                            # withdraw. Closing the connection discards them;
                            # the client reconnects with its last final and
                            # gets everything it missed in ONE HISTORY message,
                            # then starts in finals-only mode (see register).
                            if self._mark_slow(now):
                                return
        except ConnectionClosed:
            pass

    @staticmethod
    def _ping_age_limit(min_rtt: float) -> float:
        return max(SLOW_PING_AGE_SECONDS, 8 * min_rtt)

    def _mark_slow(self, now: float) -> bool:
        """Put a slow reader in finals-only mode; True if it was reset."""
        if now >= self.finals_only_until:
            self.slow_episodes += 1
        self.finals_only_until = now + FINALS_ONLY_HOLD_SECONDS
        return STATE.reset_slow_client(self)

    def _next_key(self, finals_only: bool) -> Any:
        if not self.pending:
            return None
        if not finals_only:
            return next(iter(self.pending))
        for key, (_raw, final) in self.pending.items():
            if final:
                return key
        return None


class LiveRecallServer:
    def __init__(self, session: str | None = None) -> None:
        self.session = session
        self.source = None
        self.clients: set[Client] = set()
        # Finalized captions: (server_recv_time, segment, revision, item_dict)
        self.history: deque[tuple[float, int, int, dict]] = deque()
        # segment -> (revision, final) for recent segments, for dedup/stale checks
        self.segment_state: OrderedDict[int, tuple[int, bool]] = OrderedDict()
        # The protocol source finishes segments in increasing order. Keep this
        # constant-size guard even when old per-segment entries are evicted.
        self.last_final_segment = 0
        # Newest provisional update of the segment currently in progress
        self.active: tuple[int, int, str] | None = None   # (seg, rev, raw)
        self.updates_in = 0
        self.updates_ignored = 0
        # client_id -> (remember_until, last_reset) for clients reset as slow
        self.slow_ids: OrderedDict[Any, tuple[float, float]] = OrderedDict()
        self.slow_disconnects = 0
        self.slow_resets = 0

    # ---- source side ----------------------------------------------------
    def accept_update(self, msg: dict[str, Any]) -> None:
        try:
            seg = msg["segment"]
            rev = msg["revision"]
            final = msg["final"]
            source_ts = msg["source_ts"]
            if (type(seg) is not int or type(rev) is not int
                    or type(final) is not bool
                    or type(source_ts) not in (int, float)
                    or not math.isfinite(source_ts)
                    or not isinstance(msg["text"], str)
                    or msg.get("session") != self.session):
                raise ValueError("Invalid caption fields or session")
            item = {
                "type": "CAPTION_UPDATE",
                "session": msg.get("session"),
                "segment": seg,
                "revision": rev,
                "source_ts": float(source_ts),
                "final": final,
                "text": msg["text"],
            }
        except (KeyError, TypeError, ValueError, OverflowError):
            self.updates_ignored += 1
            return
        if seg < 1 or rev < 1 or seg <= self.last_final_segment:
            self.updates_ignored += 1
            return

        known = self.segment_state.get(seg)
        if known is not None:
            known_rev, known_final = known
            if known_final or rev <= known_rev:
                self.updates_ignored += 1   # stale, duplicate, or after final
                return
        self.segment_state[seg] = (rev, final)
        self.segment_state.move_to_end(seg)
        while len(self.segment_state) > SEGMENT_STATE_MAX:
            self.segment_state.popitem(last=False)

        self.updates_in += 1
        raw = json.dumps(item, separators=(",", ":"))
        now = time.time()

        if final:
            self.last_final_segment = seg
            self.history.append((now, seg, rev, item))
            self.prune_history(now)
            if self.active is not None and self.active[0] <= seg:
                self.active = None
        elif self.active is None or seg >= self.active[0]:
            self.active = (seg, rev, raw)

        too_slow = [c for c in self.clients if not c.enqueue(seg, raw, final)]
        for client in too_slow:
            self.drop_slow_client(client)

    def prune_history(self, now: float) -> None:
        cutoff = now - HISTORY_KEEP_SECONDS
        h = self.history
        while h and (h[0][0] < cutoff or len(h) > HISTORY_MAX_ITEMS):
            h.popleft()

    # ---- client side ----------------------------------------------------
    def history_for(self, last_seg: int, last_rev: int, seconds: float) -> list[dict]:
        now = time.time()
        self.prune_history(now)
        cutoff = now - seconds
        return [item for (t, seg, rev, item) in self.history
                if t >= cutoff and _seg_rev_newer(seg, rev, last_seg, last_rev)]

    def register(self, client: Client, last_seg: int, last_rev: int,
                 seconds: float) -> int:
        """Snapshot HISTORY and start live delivery atomically."""
        items = self.history_for(last_seg, last_rev, seconds)
        client.pending["HISTORY"] = (
            json.dumps({"type": "HISTORY", "items": items}, separators=(",", ":")),
            True,
        )
        if self.active is not None:
            seg, rev, raw = self.active
            if _seg_rev_newer(seg, rev, last_seg, last_rev):
                client.pending[seg] = (raw, False)
        cid = client.client_id
        known = self.slow_ids.get(cid) if isinstance(cid, (int, str)) else None
        if known is not None and time.monotonic() < known[0]:
            # A client we reset as slow: start it in finals-only mode so it
            # doesn't refill its buffers with provisionals straight away.
            client.finals_only_until = time.monotonic() + RESET_FINALS_ONLY_SECONDS
        client.wakeup.set()
        self.clients.add(client)
        return len(items)

    def reset_slow_client(self, client: Client) -> bool:
        """Close a slow reader so it resumes via HISTORY. False = not reset."""
        cid = client.client_id
        if not RESET_SLOW_CLIENTS or not isinstance(cid, (int, str)):
            return False
        now = time.monotonic()
        known = self.slow_ids.get(cid)
        if known is not None and now - known[1] < RESET_COOLDOWN_SECONDS:
            return False                 # reset recently: don't churn
        self.slow_ids[cid] = (now + SLOW_MEMORY_SECONDS, now)
        self.slow_ids.move_to_end(cid)
        while len(self.slow_ids) > SLOW_IDS_MAX:
            self.slow_ids.popitem(last=False)
        self.slow_resets += 1
        self.drop_slow_client(client, "slow reader; reconnect to resume from HISTORY")
        return True

    def drop_slow_client(self, client: Client,
                         reason: str = "client too slow; reconnect to resume") -> None:
        if client.closing:
            return
        client.closing = True
        self.clients.discard(client)
        client.pending.clear()
        self.slow_disconnects += 1
        print(f"client {client.client_id} disconnected: {reason}", flush=True)
        if client.sender is not None:
            client.sender.cancel()
        asyncio.get_running_loop().create_task(
            client.ws.close(code=1013, reason=reason))

    async def stats_loop(self) -> None:
        while True:
            await asyncio.sleep(STATS_INTERVAL)
            self.prune_history(time.time())
            if not self.clients and not self.updates_in:
                continue
            backlog = max((len(c.pending) for c in self.clients), default=0)
            print(f"[stats] clients={len(self.clients)} updates={self.updates_in} "
                  f"ignored={self.updates_ignored} history={len(self.history)} "
                  f"max_backlog={backlog} slow_drops={self.slow_disconnects} slow_resets={self.slow_resets}",
                  flush=True)


STATE = LiveRecallServer()


def _shrink_send_buffer(ws) -> None:
    sock = ws.transport.get_extra_info("socket") if ws.transport else None
    if sock is None:
        return
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, CLIENT_SNDBUF_BYTES)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    except OSError:
        pass


async def handle_source(ws, hello: dict[str, Any]) -> None:
    print(f"source connected: session={hello.get('session')}", flush=True)
    try:
        async for raw in ws:
            if STATE.source is not ws:
                break                  # replaced by a newer source connection
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if not isinstance(msg, dict):
                continue
            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                STATE.accept_update(msg)
            elif msg_type == "SOURCE_DONE":
                print("source finished", flush=True)
            # Anything else is ignored for forward compatibility.
    except ConnectionClosed:
        pass
    finally:
        if STATE.source is ws:
            STATE.source = None
        print("source disconnected", flush=True)


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return default


async def handle_client(ws, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")
    last_seg = max(0, _int(hello.get("last_segment")))
    last_rev = max(0, _int(hello.get("last_revision")))
    seconds = hello.get("history_seconds", 30)
    try:
        seconds = min(MAX_HISTORY_SECONDS, max(0.0, float(seconds)))
    except (TypeError, ValueError, OverflowError):
        seconds = 30.0

    _shrink_send_buffer(ws)
    client = Client(ws, client_id)
    n_items = STATE.register(client, last_seg, last_rev, seconds)
    client.sender = asyncio.create_task(client.run_sender())
    print(f"client connected: id={client_id} resume={last_seg}/{last_rev} "
          f"history_items={n_items}; clients={len(STATE.clients)}", flush=True)
    try:
        # Clients don't send anything after hello; just wait for close.
        async for _ in ws:
            pass
    except ConnectionClosed:
        pass
    finally:
        STATE.clients.discard(client)
        if client.sender is not None:
            client.sender.cancel()
            with suppress(asyncio.CancelledError):
                await client.sender
        print(f"client disconnected: id={client_id} sent={client.sent} "
              f"coalesced={client.coalesced} slow_episodes={client.slow_episodes}; clients={len(STATE.clients)}",
              flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
        if not isinstance(hello, dict):
            raise ValueError
    except (asyncio.TimeoutError, json.JSONDecodeError, ValueError, ConnectionClosed):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type")
    if msg_type not in ("SOURCE_HELLO", "CLIENT_HELLO"):
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")
        return
    session = hello.get("session")
    if not isinstance(session, str) or not session.strip():
        await ws.close(code=1008, reason="Expected a nonempty session")
        return
    if STATE.session is not None and session != STATE.session:
        await ws.close(code=1008, reason="One session per server run; restart for a new lecture")
        return
    # No await between checking and claiming the session/source.
    STATE.session = session
    if msg_type == "SOURCE_HELLO":
        old = STATE.source
        STATE.source = ws
        if old is not None:
            # A reconnecting source replaces the old connection at once.
            # Waiting for the old one to time out could lose up to
            # ping_interval + ping_timeout seconds of captions.
            print("new source connected; closing the previous source", flush=True)
            asyncio.get_running_loop().create_task(
                old.close(code=1012, reason="replaced by a new source connection"))
        await handle_source(ws, hello)
    else:
        await handle_client(ws, hello)


class _IgnoreProbeHandshakes(logging.Filter):
    """Test harnesses probe the port with plain TCP connects; don't log those."""

    def filter(self, record: logging.LogRecord) -> bool:
        return "opening handshake failed" not in record.getMessage()


async def main_async(port: int) -> None:
    logging.basicConfig(level=logging.WARNING)
    logging.getLogger("websockets.server").addFilter(_IgnoreProbeHandshakes())
    async with serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
        write_limit=WRITE_LIMIT_BYTES,
        # Captions are ~200-byte JSON. Per-message deflate would compress every
        # message separately for every client, which dominates CPU at scale.
        compression=None,
    ):
        print(f"LiveRecall server listening on ws://0.0.0.0:{port}", flush=True)
        stats = asyncio.create_task(STATE.stats_loop())
        try:
            await asyncio.Future()
        finally:
            stats.cancel()
            with suppress(asyncio.CancelledError):
                await stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--no-reset-slow", action="store_true",
                        help="keep slow readers connected instead of reset-and-resume")
    args = parser.parse_args()
    if args.no_reset_slow:
        global RESET_SLOW_CLIENTS
        RESET_SLOW_CLIENTS = False
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
