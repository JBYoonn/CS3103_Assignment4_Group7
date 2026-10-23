"""End-to-end protocol behaviour over real WebSockets (liverecall/server.py)."""

import asyncio
import json

import websockets

from liverecall.server import LiveRecallServer


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, timeout=10))


class Harness:
    async def __aenter__(self):
        self.app = LiveRecallServer()
        self.server = await self.app.start("127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        self.uri = f"ws://127.0.0.1:{port}"
        return self

    async def __aexit__(self, *exc):
        self.server.close()
        await self.server.wait_closed()

    async def source(self):
        ws = await websockets.connect(self.uri)
        await ws.send(json.dumps({"type": "SOURCE_HELLO", "session": "L1"}))
        return ws

    async def client(self, last_segment=0, last_revision=0, history_seconds=30, cid=1):
        ws = await websockets.connect(self.uri)
        await ws.send(json.dumps({
            "type": "CLIENT_HELLO", "client_id": cid, "session": "L1",
            "last_segment": last_segment, "last_revision": last_revision,
            "history_seconds": history_seconds,
        }))
        return ws


def upd(seg, rev, final=False, **extra):
    return json.dumps({
        "type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
        "source_ts": 1.0, "final": final, "text": f"s{seg}r{rev}", **extra,
    })


async def recv_json(ws, timeout=2.0):
    """Next protocol message, skipping TICKs exactly as a real client must."""
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout))
        if msg.get("type") != "TICK":
            return msg


async def settle():
    await asyncio.sleep(0.15)


def key(m):
    return (m["segment"], m["revision"])


def test_client_gets_history_first_then_live_revisions():
    async def go():
        async with Harness() as h:
            c = await h.client()
            assert (await recv_json(c)) == {"type": "HISTORY", "items": []}
            src = await h.source()
            for r, final in [(1, False), (2, False), (3, True)]:
                await src.send(upd(1, r, final))
            got = [key(await recv_json(c)) for _ in range(3)]
            assert got == [(1, 1), (1, 2), (1, 3)]
    run(go())


def test_stale_and_duplicate_source_revisions_are_not_forwarded():
    async def go():
        async with Harness() as h:
            c = await h.client()
            await recv_json(c)
            src = await h.source()
            for seg, rev, final in [(1, 2, False), (1, 1, False), (1, 2, False), (1, 3, True), (1, 4, False), (2, 1, False)]:
                await src.send(upd(seg, rev, final))
            got = [key(await recv_json(c)) for _ in range(3)]
            assert got == [(1, 2), (1, 3), (2, 1)]
    run(go())


def test_late_joiner_gets_recent_finals_then_current_provisional_then_live():
    async def go():
        async with Harness() as h:
            src = await h.source()
            for seg in (1, 2, 3):
                await src.send(upd(seg, 3, True))
            await src.send(upd(4, 1))
            await settle()
            c = await h.client()
            hist = await recv_json(c)
            assert hist["type"] == "HISTORY"
            assert [key(i) for i in hist["items"]] == [(1, 3), (2, 3), (3, 3)]
            assert key(await recv_json(c)) == (4, 1)
            await src.send(upd(4, 2))
            assert key(await recv_json(c)) == (4, 2)
    run(go())


def test_reconnecting_client_recovers_only_missed_finals():
    async def go():
        async with Harness() as h:
            src = await h.source()
            for seg in range(1, 7):
                await src.send(upd(seg, 3, True))
            await settle()
            c = await h.client(last_segment=4, last_revision=3)
            hist = await recv_json(c)
            assert [key(i) for i in hist["items"]] == [(5, 3), (6, 3)]
    run(go())


def test_history_contains_only_final_captions():
    async def go():
        async with Harness() as h:
            src = await h.source()
            await src.send(upd(1, 3, True))
            await src.send(upd(2, 1))
            await settle()
            c = await h.client()
            hist = await recv_json(c)
            assert all(i["final"] for i in hist["items"])
    run(go())


def test_source_extra_fields_and_unknown_messages_are_ignored():
    async def go():
        async with Harness() as h:
            c = await h.client()
            await recv_json(c)
            src = await h.source()
            await src.send(json.dumps({"type": "SOMETHING_NEW", "x": 1}))
            await src.send("not json")
            await src.send(upd(1, 1, phase="burst"))
            msg = await recv_json(c)
            assert key(msg) == (1, 1) and "phase" not in msg
    run(go())


def test_source_done_is_not_forwarded():
    async def go():
        async with Harness() as h:
            c = await h.client()
            await recv_json(c)
            src = await h.source()
            await src.send(json.dumps({"type": "SOURCE_DONE", "session": "L1"}))
            await src.send(upd(1, 1))
            assert (await recv_json(c))["type"] == "CAPTION_UPDATE"
    run(go())


def test_invalid_first_message_is_rejected_with_policy_close():
    async def go():
        async with Harness() as h:
            ws = await websockets.connect(h.uri)
            await ws.send(json.dumps({"type": "HELLO?"}))
            try:
                await asyncio.wait_for(ws.recv(), 2)
            except websockets.ConnectionClosed as e:
                assert e.rcvd.code == 1008
            else:
                raise AssertionError("expected close")
    run(go())


def test_new_lecture_from_restarted_source_resets_state():
    async def go():
        async with Harness() as h:
            src = await h.source()
            for seg in range(1, 6):
                await src.send(upd(seg, 3, True))
            await src.send(json.dumps({"type": "SOURCE_DONE", "session": "L1"}))
            await src.close()
            src2 = await h.source()
            await src2.send(upd(1, 1))
            await settle()
            c = await h.client()
            hist = await recv_json(c)
            assert hist["items"] == []
            assert key(await recv_json(c)) == (1, 1)
    run(go())


def test_slow_client_does_not_delay_normal_client():
    async def go():
        async with Harness() as h:
            slow = await websockets.connect(h.uri, max_queue=1)
            await slow.send(json.dumps({"type": "CLIENT_HELLO", "client_id": 9, "session": "L1",
                                        "last_segment": 0, "last_revision": 0, "history_seconds": 30}))
            fast = await h.client(cid=2)
            await recv_json(fast)
            src = await h.source()
            big = "x" * 2000
            for seg in range(1, 301):        # ~600 KB the slow client never reads
                await src.send(json.dumps({"type": "CAPTION_UPDATE", "session": "L1", "segment": seg,
                                           "revision": 1, "source_ts": 1.0, "final": True, "text": big}))
                msg = await recv_json(fast)
                assert msg["segment"] == seg
            slow.transport.abort()        # it never reads, so a polite close would hang
    run(go())


class FakeWS:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)

    async def ping(self):
        return asyncio.get_running_loop().create_future()


def test_limited_client_gets_tick_messages_after_each_caption():
    # Ticks make a slow client's bounded receive queue (counted in messages)
    # fill sooner, so its backlog becomes visible to the ping probe sooner.
    from liverecall.server import ClientSession, Hub, Profile, TICK
    from liverecall.state import CaptionState

    async def go():
        session = ClientSession(Hub("L1"), FakeWS(), 1, Profile(limited_ticks=3))
        session.flow.limited = True
        session.flow.rate = 100.0
        cap = CaptionState().apply(json.loads(upd(1, 1)), now=0.0)
        session.push(cap)
        task = asyncio.create_task(session.run('{"type":"HISTORY","items":[]}'))
        await asyncio.sleep(0.05)
        task.cancel()
        sent = session.ws.sent
        assert sent[1] == cap.wire
        assert sent[2:5] == [TICK] * 3
    run(go())


def test_normal_client_after_probation_gets_no_ticks():
    from liverecall.server import ClientSession, Hub, Profile, TICK
    from liverecall.state import CaptionState

    async def go():
        session = ClientSession(Hub("L1"), FakeWS(), 1, Profile(limited_ticks=3, probation_s=0.0))
        cap = CaptionState().apply(json.loads(upd(1, 1)), now=0.0)
        session.push(cap)
        task = asyncio.create_task(session.run('{"type":"HISTORY","items":[]}'))
        await asyncio.sleep(0.05)
        task.cancel()
        assert TICK not in session.ws.sent
    run(go())


def test_new_client_gets_ticks_during_probation_then_stops():
    from liverecall.server import ClientSession, Hub, Profile, TICK
    from liverecall.state import CaptionState

    async def go():
        session = ClientSession(Hub("L1"), FakeWS(), 1, Profile(limited_ticks=2, probation_s=0.2))
        task = asyncio.create_task(session.run('{"type":"HISTORY","items":[]}'))
        session.push(CaptionState().apply(json.loads(upd(1, 1)), now=0.0))
        await asyncio.sleep(0.05)
        assert session.ws.sent.count(TICK) == 2           # inside probation
        await asyncio.sleep(0.3)
        session.push(CaptionState().apply(json.loads(upd(2, 1)), now=0.0))
        await asyncio.sleep(0.05)
        task.cancel()
        assert session.ws.sent.count(TICK) == 2           # probation over, client not limited
    run(go())


# --- review findings ------------------------------------------------------------

def test_resuming_client_from_previous_lecture_gets_new_lecture():
    async def go():
        async with Harness() as h:
            src = await h.source()
            for seg in range(1, 6):
                await src.send(upd(seg, 3, True))
            await src.close()
            src2 = await h.source()
            await src2.send(upd(1, 3, True))          # new lecture starts again at 1
            await settle()
            c = await h.client(last_segment=5, last_revision=3)   # still holds old lecture state
            hist = await recv_json(c)
            assert [key(i) for i in hist["items"]] == [(1, 3)]
    run(go())


class BrokenWS(FakeWS):
    def __init__(self):
        super().__init__()
        self.closed_with = None

    async def send(self, msg):
        if self.sent:
            raise ValueError("boom")                  # e.g. an encoding error mid-stream
        self.sent.append(msg)

    async def close(self, code=1000, reason=""):
        self.closed_with = code


def test_unexpected_send_error_closes_connection_instead_of_silently_killing_sender():
    from liverecall.server import ClientSession, Hub, Profile
    from liverecall.state import CaptionState

    async def go():
        ws = BrokenWS()
        session = ClientSession(Hub("L1"), ws, 1, Profile(probation_s=0.0))
        session.push(CaptionState().apply(json.loads(upd(1, 1)), now=0.0))
        await asyncio.wait_for(session.run('{"type":"HISTORY","items":[]}'), 1.0)
        assert ws.closed_with == 1011
    run(go())


class StuckWS(FakeWS):
    """send() blocks forever after the first message, like a full TCP buffer."""
    def __init__(self):
        super().__init__()
        self.aborted = False
        outer = self

        class Transport:
            def abort(self_inner):
                outer.aborted = True
        self.transport = Transport()

    async def send(self, msg):
        self.sent.append(msg)
        if len(self.sent) > 1:
            await asyncio.Event().wait()


def test_overflow_while_sender_is_blocked_aborts_the_connection():
    from liverecall.outbox import ClientOutbox
    from liverecall.server import ClientSession, Hub, Profile
    from liverecall.state import CaptionState

    async def go():
        ws = StuckWS()
        session = ClientSession(Hub("L1"), ws, 1, Profile(probation_s=0.0))
        session.outbox = ClientOutbox(max_pending_finals=5)
        task = asyncio.create_task(session.run('{"type":"HISTORY","items":[]}'))
        st = CaptionState()
        session.push(st.apply(json.loads(upd(1, 1, True)), now=0.0))
        await asyncio.sleep(0.05)                    # sender now stuck in send()
        for seg in range(2, 12):
            session.push(st.apply(json.loads(upd(seg, 1, True)), now=0.0))
        assert ws.aborted
        task.cancel()
    run(go())


def test_source_reconnect_mid_lecture_does_not_wipe_state():
    async def go():
        async with Harness() as h:
            src = await h.source()
            for seg in range(1, 6):
                await src.send(upd(seg, 3, True))
            await src.close()
            src2 = await h.source()
            await src2.send(upd(4, 3, True))          # replays an old segment after a blip
            await src2.send(upd(6, 1))
            await settle()
            c = await h.client()
            hist = await recv_json(c)
            assert [key(i) for i in hist["items"]] == [(s, 3) for s in range(1, 6)]
    run(go())
