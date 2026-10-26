#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall naive working baseline.

This starter is intentionally useful but incomplete. It provides the required
CLI and a basic WebSocket broadcaster so that caption_source.py -> server ->
browser/load clients works on day one.

It DOES NOT implement the graded networking mechanisms:
  - revision-state management or deduplication
  - recent-history recovery or late-join recovery
  - bounded per-client application queues
  - slow-client isolation
  - semantic coalescing / backpressure
  - constrained-bandwidth overload handling

Required evaluator command:
    python solution_server.py --port 8765

Teams should replace or substantially extend the implementation while keeping
this command-line interface and the protocol in the assignment specification.
"""

from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any
from collections import OrderedDict, deque
import time
import websockets
from websockets.exceptions import ConnectionClosed

# Bounds
MAX_HISTORY_SECONDS = 60
MASTER_HISTORY_SECONDS = 120
CLIENT_MAX_PENDING = 128
LAG_SECONDS = 1.0
MAX_UNACKED_PINGS = 1
PING_EVERY = 4
SEGMENT_STATE_MAX = 2048
HISTORY_MAX_ITEMS = 2048

def _newer(seg: int, rev: int, base_seg: int, base_rev: int) -> bool:
    return seg > base_seg or (seg == base_seg and rev > base_rev)


class Client:
    """State of one client"""
    __slots__ = ("ws", "client_id", "pending", "wakeup", "sender", "sent", "coalesced", "closing")

    def __init__(self, ws, client_id: Any) -> None:
        self.ws = ws
        self.client_id = client_id

        # key->(msg,is_final), where key is segment number + "HISTORY" for initial history
        self.pending: OrderedDict[Any, tuple[str, bool]] = OrderedDict()
        self.wakeup = asyncio.Event()
        self.sender: asyncio.Task | None = None
        self.sent = 0
        self.coalesced = 0
        self.closing = False

    def enqueue(self, key: Any, raw: str, final: bool) -> bool:
        """Queues one msg. Return false if client is too far behind"""
        waiting = self.pending.get(key)
        if waiting is not None:
            if waiting[1]:
                return True # a final was queued
            self.pending[key] = (raw, final)
            self.coalesced += 1
        else:
            if len(self.pending) >= CLIENT_MAX_PENDING:
                return False
            self.pending[key] = (raw, final)
        self.wakeup.set()
        return True

    async def run_sender(self) -> None:
        """Empty pending map.
        
        After every ping via WebSocket ping. If more than MAX_UNACKED_PINGS are unanswered,
        we stop sending and wait. This will limit the amount of traffic to the slower client,
        while the pending map fills up, where it will coalesced with newer segments.
        A client whose oldest unanswered ping is older than LAG_SECONDS is only sent finals.
        """
        waiters: deque[tuple[float, asyncio.Future]] = deque() # (send_at, pong)
        since_ping = 0
        try:
            while True:
                while waiters and waiters[0][1].done():
                    waiters.popleft()

                lagging = bool(waiters) and time.monotonic() - waiters[0][0] > LAG_SECONDS
                key = self._next_key(finals_only=lagging)

                if key is None:
                    if self.pending:
                        # only provisionals left and client lagging
                        # wait for client to catch up or new final arrive
                        await asyncio.wait([waiters[0][1]], timeout=0.1)
                    else:
                        self.wakeup.clear()
                        await self.wakeup.wait()
                    continue

                raw, final = self.pending.pop(key)
                await self.ws.send(raw)
                self.sent += 1
                since_ping += 1
                if since_ping > PING_EVERY:
                    since_ping = 0
                    waiters.append((time.monotonic(), await self.ws.ping()))
                    while len(waiters) > MAX_UNACKED_PINGS:
                        await waiters[0][1]
                        waiters.popleft()
        except ConnectionClosed:
            pass

    def _next_key(self, finals_only: bool) -> Any:
        if not self.pending:
            return None
        if not finals_only:
            return next(iter(self.pending))
        for key, (_raw, final) in self.pending.items(): # finals_only True
            if final:
                return key
        return None


class LiveRecallServer:
    def __init__(self) -> None:
        self.clients: set[Client] = set()
        # finalized captions -> (server_recv_time, segment, revision, item_dict)
        self.history: deque[tuple[float, int, int, dict]] = deque()
        # segment -> (revision, final) for recent segments
        self.segment_state: OrderedDict[int, tuple[int, bool]] = OrderedDict()
        # provisional update of the segment currently in progress
        self.active: tuple[int, int, str] | None = None # (seg, rev, raw)
        self.updates_in = 0
        self.updates_ignored = 0
        self.slow_disconnects = 0

    # source side
    def accept_update(self, msg: dict[str, Any]) -> None:
        try:
            seg = int(msg["segment"])
            rev = int(msg["revision"])
            final = bool(msg.get("final", False))
            item = {
                "type": "CAPTION_UPDATE",
                "session": msg.get("session"),
                "segment": seg,
                "revision": rev,
                "source_ts": float(msg["source_ts"]),
                "final": final,
                "text": str(msg.get("text", "")),
            }
        except (KeyError, TypeError, ValueError):
            self.updates_ignored += 1
            return

        if seg < 1 or rev < 1:
            self.updates_ignored += 1
            return

        known = self.segment_state.get(seg)
        if known is not None:
            known_rev, known_final = known
            if known_final or rev <= known_rev: # ignore duplicates/stale updates
                self.updates_ignored += 1 
                return
        # perform update
        self.segment_state[seg] = (rev, final)
        self.segment_state.move_to_end(seg)

        while len(self.segment_state) > SEGMENT_STATE_MAX:
            self.segment_state.popitem(last=False)

        self.updates_in += 1
        raw = json.dumps(item, separators=(",",":"))
        now = time.time()

        if final:
            self.history.append((now, seg, rev, item))
            self.prune_history(now)
            if self.active is not None and self.active[0] <= seg:
                self.active = None
        elif self.active is None or seg >= self.active[0]:
            self.active = (seg, rev, raw)

        slow = [c for c in self.clients if not c.enqueue(seg, raw, final)]
        for client in slow:
            self.drop_slowpoke(client)

    def prune_history(self, now: float) -> None:
        cutoff = now - MASTER_HISTORY_SECONDS
        h = self.history
        while h and (h[0][0] < cutoff or len(h) > HISTORY_MAX_ITEMS):
            h.popleft()

    # client-side
    

# to be deleted
async def broadcast_naively(raw_message: str) -> None:
    """Send one message directly to every connected client.

    This is intentionally a naive baseline. The source handler waits for all
    sends to complete. Under pressure, this design may accumulate delay and is
    not the target architecture for the assignment.
    """
    if not CLIENTS:
        return

    snapshot = list(CLIENTS)
    results = await asyncio.gather(
        *(client.send(raw_message) for client in snapshot),
        return_exceptions=True,
    )
    for client, result in zip(snapshot, results):
        if isinstance(result, Exception):
            CLIENTS.discard(client)


async def handle_source(ws, hello: dict[str, Any]) -> None:
    session = hello.get("session")
    print(f"source connected: session={session}", flush=True)

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type")
            if msg_type == "CAPTION_UPDATE":
                # Baseline behavior: forward exactly what was received.
                # No history, state, validation, coalescing, or bounded queues.
                await broadcast_naively(raw)
            elif msg_type == "SOURCE_DONE":
                # Test-control message. The specification does not require it
                # to be forwarded to clients.
                print("source finished", flush=True)
            else:
                # Forward compatibility: ignore fields/messages this baseline
                # doesn't understand rather than crashing.
                continue
    finally:
        print("source disconnected", flush=True)


async def handle_client(ws, hello: dict[str, Any]) -> None:
    client_id = hello.get("client_id")

    # The baseline has no stored recovery state. Sending an empty HISTORY makes
    # the connection behavior visible and keeps the starter easy to inspect.
    # Students must implement correct HISTORY semantics for the assignment.
    await ws.send(json.dumps({"type": "HISTORY", "items": []}))

    CLIENTS.add(ws)
    print(f"client connected: id={client_id}; clients={len(CLIENTS)}", flush=True)
    try:
        # Student clients normally don't send further messages after hello.
        # Keep the WebSocket open until the client disconnects.
        async for _ in ws:
            pass
    finally:
        CLIENTS.discard(ws)
        print(f"client disconnected: id={client_id}; clients={len(CLIENTS)}", flush=True)


async def handler(ws) -> None:
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=5)
        hello = json.loads(raw)
    except (asyncio.TimeoutError, json.JSONDecodeError):
        await ws.close(code=1008, reason="Expected a valid hello message")
        return

    msg_type = hello.get("type")
    if msg_type == "SOURCE_HELLO":
        await handle_source(ws, hello)
    elif msg_type == "CLIENT_HELLO":
        await handle_client(ws, hello)
    else:
        await ws.close(code=1008, reason="Expected SOURCE_HELLO or CLIENT_HELLO")


async def main_async(port: int) -> None:
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        max_size=2**20,
        ping_interval=20,
        ping_timeout=20,
    ):
        print(f"LiveRecall baseline listening on ws://0.0.0.0:{port}", flush=True)
        await asyncio.Future()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args.port))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
