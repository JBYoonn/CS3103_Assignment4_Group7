"""LiveRecall WebSocket server: wiring between the source, state, and clients.

Data path for one caption revision:

    source ws --> handle_source --> CaptionState.apply   (dedupe / durable finals / history)
                                         |
                                     Hub.publish          (synchronous, O(clients), never awaits)
                                         |
                    +--------------------+--------------------+
                    v                    v                    v
              ClientOutbox         ClientOutbox         ClientOutbox     (one per client, bounded,
                    |                    |                    |           coalesces obsolete revisions)
              sender task          sender task          sender task     (one per client; paced by
                    |                    |                    |           FlowController when lagging)
                client ws            client ws            client ws

Because the source handler only ever touches in-memory outboxes, a slow or
stuck client can delay nobody but itself.
"""

from __future__ import annotations

import asyncio
import json
import math
import os
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

import websockets
from websockets.exceptions import ConnectionClosed

from .flow import FlowController
from .outbox import ClientOutbox, FifoOutbox, history_message
from .state import Caption, CaptionState

MAX_SESSIONS = 32


@dataclass
class Profile:
    """Which overload mechanisms are on. 'full' is what gets evaluated."""
    name: str = "full"
    coalesce: bool = True        # semantic outbox (one pending entry per segment)
    flow: bool = True            # ping-based lag detection, rate control, HISTORY batching
    ping_interval: float = 0.25  # min seconds between lag probes on a busy connection
    limited_ticks: int = 3       # TICKs after each caption to a limited client (see run())
    probation_s: float = 15.0    # new connections also get TICKs for this long (early detection)
    write_limit: int = 16 * 1024 # websockets user-space write buffer high-water mark

    @classmethod
    def from_name(cls, name: str) -> "Profile":
        name = (name or "full").strip().lower()
        if name == "basic":      # conventional per-client FIFO queues, nothing semantic
            return cls(name="basic", coalesce=False, flow=False, limited_ticks=0)
        if name == "noflow":     # coalescing only, no lag detection/pacing/batching
            return cls(name="noflow", flow=False, limited_ticks=0)
        if name == "noticks":    # full design without TICK padding (to measure its effect)
            return cls(name="noticks", limited_ticks=0)
        return cls()


TICK = '{"type":"TICK"}'


def _int_field(msg: dict, key: str, default: int = 0) -> int:
    v = msg.get(key, default)
    return v if isinstance(v, int) and not isinstance(v, bool) and v >= 0 else default


class RateMeter:
    """Events per second over a sliding window (the source's offered rate)."""

    def __init__(self, window: float = 5.0):
        self.window = window
        self.events: deque[float] = deque()

    def mark(self, now: float) -> None:
        self.events.append(now)
        self._trim(now)

    def rate(self, now: float) -> float:
        self._trim(now)
        return len(self.events) / self.window

    def _trim(self, now: float) -> None:
        while self.events and self.events[0] < now - self.window:
            self.events.popleft()


class ClientSession:
    def __init__(self, hub: "Hub", ws, client_id: Any, profile: Profile):
        self.hub = hub
        self.ws = ws
        self.client_id = client_id
        self.profile = profile
        self.outbox = ClientOutbox() if profile.coalesce else FifoOutbox()
        self.flow = FlowController() if profile.flow else None
        self.wake = asyncio.Event()
        self.pong_waiter: asyncio.Future | None = None
        self.ping_sent_at = 0.0
        self.last_ping_at = -math.inf
        self.notice: str | None = None     # control message to send before any caption
        self.blocked = False               # True while ws.send() waits for TCP to drain

    # Called synchronously by Hub.publish -- must never block.
    def push(self, cap: Caption) -> None:
        # Pressure = our sender is stuck waiting for TCP to drain, or the flow
        # controller has decided this client is behind. Only then are obsolete
        # provisional revisions discarded.
        self.outbox.push(cap, coalesce=self.blocked or self.limited)
        if self.outbox.overflowed and self.blocked:
            # The sender is stuck inside ws.send() (full TCP buffer), so it cannot
            # run its own overflow check: drop the connection now to stay bounded.
            transport = getattr(self.ws, "transport", None)
            if transport is not None:
                transport.abort()
        self.wake.set()

    def reset(self, notice: str) -> None:
        self.outbox = ClientOutbox() if self.profile.coalesce else FifoOutbox()
        self.notice = notice
        self.wake.set()

    @property
    def limited(self) -> bool:
        return bool(self.flow and self.flow.limited)

    async def run(self, first_message: str) -> None:
        loop = asyncio.get_running_loop()
        ws = self.ws
        probation_until = loop.time() + self.profile.probation_s
        try:
            await ws.send(first_message)
            while True:
                self.wake.clear()
                now = loop.time()
                self._poll_pong(now)

                if self.outbox.overflowed:
                    # Hopelessly behind: drop our copy and make the client resume
                    # through CLIENT_HELLO/HISTORY instead of buffering forever.
                    await ws.close(1013, "too far behind; reconnect to resume")
                    return

                if self.notice is not None:
                    notice, self.notice = self.notice, None
                    await ws.send(notice)
                    continue

                timeout = None
                outstanding = self.ping_sent_at if self.pong_waiter is not None else None
                if self.flow and self.flow.gated(now, outstanding):
                    pass        # visibly backlogged: hold everything until the pong comes back
                elif self.outbox.pending:
                    delay = self.flow.ready_in(now) if self.flow else 0.0
                    if delay <= 0.0:
                        msg = self.outbox.take(limited=self.limited)
                        if msg is not None:
                            tick = self.limited or now < probation_until
                            self.blocked = True
                            try:
                                await ws.send(msg)
                                if tick:
                                    # A client's WebSocket stack bounds its receive queue in
                                    # *messages* (Python websockets: 16) and only stops reading
                                    # TCP -- which is what makes our ping late -- once it is
                                    # full. TICKs cost the reader nothing (ignored, no work)
                                    # but fill that queue, so a lagging client's backlog shows
                                    # up ~4 captions deep instead of ~16. Sent to clients we
                                    # already limit, and to every client during its first
                                    # probation_s seconds so a slow reader is caught early.
                                    for _ in range(self.profile.limited_ticks):
                                        await ws.send(TICK)
                            finally:
                                self.blocked = False
                            now = loop.time()
                            if self.flow:
                                self.flow.on_sent(now)
                                await self._maybe_ping(now)
                            continue
                    else:
                        timeout = delay
                if self.pong_waiter is not None:      # wake up to check the ping's age
                    timeout = 0.25 if timeout is None else min(timeout, 0.25)

                if timeout is None:
                    await self.wake.wait()
                else:
                    handle = loop.call_later(timeout, self.wake.set)
                    try:
                        await self.wake.wait()
                    finally:
                        handle.cancel()
        except ConnectionClosed:
            return
        except Exception as exc:          # never let one client's sender die silently
            print(f"client {self.client_id}: sender error {exc!r}; closing", flush=True)
            try:
                await ws.close(1011, "internal error")
            except Exception:
                pass

    async def _maybe_ping(self, now: float) -> None:
        if self.pong_waiter is None and now - self.last_ping_at >= self.profile.ping_interval:
            self.pong_waiter = await self.ws.ping()
            self.pong_waiter.add_done_callback(lambda _f: self.wake.set())
            self.ping_sent_at = self.last_ping_at = now

    def _poll_pong(self, now: float) -> None:
        w = self.pong_waiter
        if w is None or self.flow is None:
            return
        if not w.done():
            self.flow.on_ping_age(now, self.ping_sent_at)
            return
        self.pong_waiter = None
        if w.cancelled() or w.exception() is not None:
            return
        pong_at = self.ping_sent_at + float(w.result())   # result = measured latency
        self.flow.on_pong(pong_at, self.ping_sent_at, self.hub.offered.rate(now))


class Hub:
    """One lecture session: its state plus everyone subscribed to it."""

    def __init__(self, session: str):
        self.session = session
        self.state = CaptionState()
        self.clients: set[ClientSession] = set()
        self.offered = RateMeter()

    def publish(self, cap: Caption, now: float) -> None:
        self.offered.mark(now)
        for client in self.clients:
            client.push(cap)

    def reset(self) -> None:
        self.state.reset()
        notice = json.dumps({"type": "SESSION_RESET", "session": self.session})
        for client in self.clients:
            client.reset(notice)


class LiveRecallServer:
    def __init__(self, profile: Profile | None = None, verbose: bool = False):
        self.profile = profile or Profile.from_name(os.environ.get("LIVERECALL_PROFILE", "full"))
        self.verbose = verbose
        self.hubs: dict[str, Hub] = {}

    def hub(self, session: Any) -> Hub | None:
        session = session if isinstance(session, str) and session else "L1"
        hub = self.hubs.get(session)
        if hub is None:
            if len(self.hubs) >= MAX_SESSIONS:
                return None
            hub = self.hubs[session] = Hub(session)
        return hub

    async def start(self, host: str, port: int):
        return await websockets.serve(
            self.handler, host, port,
            max_size=2**20,
            ping_interval=20,
            ping_timeout=20,
            write_limit=self.profile.write_limit,
        )

    async def handler(self, ws) -> None:
        try:
            hello = json.loads(await asyncio.wait_for(ws.recv(), timeout=5))
            if not isinstance(hello, dict):
                raise ValueError
        except (asyncio.TimeoutError, ValueError, TypeError):
            await ws.close(1008, "expected SOURCE_HELLO or CLIENT_HELLO")
            return
        except ConnectionClosed:
            return

        kind = hello.get("type")
        hub = self.hub(hello.get("session"))
        if hub is None or kind not in ("SOURCE_HELLO", "CLIENT_HELLO"):
            await ws.close(1008, "expected SOURCE_HELLO or CLIENT_HELLO")
            return
        if kind == "SOURCE_HELLO":
            await self.handle_source(ws, hub)
        else:
            await self.handle_client(ws, hub, hello)

    async def handle_source(self, ws, hub: Hub) -> None:
        loop = asyncio.get_running_loop()
        print(f"source connected: session={hub.session}", flush=True)
        first_update = True
        try:
            async for raw in ws:
                try:
                    msg = json.loads(raw)
                except (ValueError, TypeError, RecursionError):   # bad JSON/UTF-8, huge ints
                    continue
                if not isinstance(msg, dict):
                    continue
                kind = msg.get("type")
                if kind == "CAPTION_UPDATE":
                    now = loop.time()
                    if first_update:
                        first_update = False
                        seg = msg.get("segment")
                        # Segments start at 1, so a new source connection that starts at
                        # segment 1 after the lecture got further is a new lecture. Anything
                        # else (e.g. replaying a recent segment after a blip) is a resume.
                        if seg == 1 and hub.state.max_segment > 1:
                            print(f"new lecture detected on {hub.session}: resetting state", flush=True)
                            hub.reset()
                    cap = hub.state.apply(msg, now)
                    if cap is not None:
                        hub.publish(cap, now)
                elif kind == "SOURCE_DONE":
                    print(f"source finished: session={hub.session}", flush=True)
                # Anything else: ignore (forward compatibility).
        except ConnectionClosed:
            pass
        finally:
            print(f"source disconnected: session={hub.session}", flush=True)

    async def handle_client(self, ws, hub: Hub, hello: dict) -> None:
        loop = asyncio.get_running_loop()
        last_segment = _int_field(hello, "last_segment")
        last_revision = _int_field(hello, "last_revision")
        if last_segment > hub.state.max_segment:
            # Resume state from before a lecture restart (or nonsense): it refers to
            # segments this lecture has not reached, so treat the client as new.
            last_segment = last_revision = 0
        hs = hello.get("history_seconds", 30)
        valid_hs = isinstance(hs, (int, float)) and not isinstance(hs, bool) and math.isfinite(hs)
        history_seconds = hs if valid_hs else 30

        client = ClientSession(hub, ws, hello.get("client_id"), self.profile)

        # Snapshot + register with no await in between, so no update can fall
        # into the gap between "what HISTORY contains" and "what live sends".
        now = loop.time()
        items = hub.state.history_for(last_segment, last_revision, history_seconds, now)
        client.outbox.prime(last_segment, last_revision)
        client.outbox.mark_sent(items)
        for cap in hub.state.active_captions():
            client.push(cap)          # current partial line, so a late joiner sees "now" immediately
        hub.clients.add(client)

        if self.verbose:
            print(f"client connected: id={client.client_id} resume={last_segment}/{last_revision} "
                  f"history={len(items)} clients={len(hub.clients)}", flush=True)

        sender = asyncio.create_task(client.run(history_message(items)))
        try:
            async for _ in ws:
                pass                  # clients send nothing after CLIENT_HELLO
        except ConnectionClosed:
            pass
        finally:
            hub.clients.discard(client)
            sender.cancel()
            try:
                await sender
            except (asyncio.CancelledError, Exception):
                pass
            if self.verbose:
                print(f"client disconnected: id={client.client_id} clients={len(hub.clients)}", flush=True)

    async def report_forever(self, every: float = 10.0) -> None:
        """Periodic one-line status: handy in the demo and for the report."""
        while True:
            await asyncio.sleep(every)
            for hub in self.hubs.values():
                clients = list(hub.clients)
                if not clients and not len(hub.state):
                    continue
                limited = sum(c.limited for c in clients)
                backlog = max((c.outbox.pending for c in clients), default=0)
                coalesced = sum(c.outbox.coalesced for c in clients)
                batched = sum(c.outbox.batched for c in clients)
                print(
                    f"[{time.strftime('%H:%M:%S')}] session={hub.session} clients={len(clients)} "
                    f"limited={limited} max_backlog={backlog} coalesced={coalesced} batched_finals={batched} "
                    f"history={len(hub.state)} active={len(hub.state.active)} "
                    f"offered={hub.offered.rate(asyncio.get_running_loop().time()):.1f}/s",
                    flush=True,
                )
