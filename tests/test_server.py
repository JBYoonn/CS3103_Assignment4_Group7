"""Regression checks for revision, recovery, and connection invariants."""

import asyncio
import json
import time
import unittest
from unittest.mock import patch

from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

import solution_server as server


def caption(segment, revision=1, final=False, session="L1", **fields):
    return {
        "type": "CAPTION_UPDATE", "session": session,
        "segment": segment, "revision": revision, "final": final,
        "source_ts": time.time(), "text": "A caption", **fields,
    }


class StateTests(unittest.TestCase):
    def setUp(self):
        self.state = server.LiveRecallServer("L1")

    def test_old_final_cannot_reappear_after_revision_eviction(self):
        with patch.object(server, "SEGMENT_STATE_MAX", 2):
            for segment in range(1, 5):
                self.state.accept_update(caption(segment, 3, True))
        self.assertNotIn(1, self.state.segment_state)
        for revision, final in [(1, False), (99, False), (99, True)]:
            self.state.accept_update(caption(1, revision, final))
        self.assertEqual(self.state.updates_in, 4)
        self.assertEqual(self.state.updates_ignored, 3)
        self.assertIsNone(self.state.active)
        self.assertEqual(len(self.state.history), 4)

    def test_revisions_are_increasing_and_finals_are_immutable(self):
        for revision, final in [(2, False), (1, False), (2, False),
                                (3, True), (4, False), (5, True)]:
            self.state.accept_update(caption(1, revision, final))
        self.assertEqual(self.state.updates_in, 2)
        self.assertEqual(self.state.history[0][3]["revision"], 3)

    def test_invalid_fields_and_cross_session_updates_leave_state_unchanged(self):
        for fields in [dict(segment=True), dict(revision=1.5),
                       dict(final="false"), dict(source_ts=float("nan")),
                       dict(source_ts=float("inf")), dict(text=None),
                       dict(session="L2")]:
            update = caption(1)
            update.update(fields)
            self.state.accept_update(update)
        self.assertEqual(self.state.updates_in, 0)
        self.assertFalse(self.state.segment_state)

    def test_history_filters_age_cursor_and_count(self):
        with patch.object(server.time, "time", return_value=1000):
            for segment, age in [(1, 100), (2, 70), (3, 50), (4, 20)]:
                self.state.history.append((1000-age, segment, 1,
                                           caption(segment, final=True)))
            self.assertEqual([m["segment"] for m in self.state.history_for(0, 0, 60)], [3, 4])
            self.assertEqual([m["segment"] for m in self.state.history_for(3, 1, 60)], [4])
            with patch.object(server, "HISTORY_MAX_ITEMS", 1):
                self.state.prune_history(1000)
                self.assertEqual(len(self.state.history), 1)

    def test_coalescing_keeps_final_and_overflow_is_reported(self):
        client = server.Client(None, "test")
        client.enqueue(1, "provisional", False)
        client.enqueue(1, "newer", False)
        client.enqueue(1, "final", True)
        client.enqueue(1, "stale", False)
        self.assertEqual(client.pending[1], ("final", True))
        with patch.object(server, "CLIENT_MAX_PENDING", 1):
            self.assertFalse(client.enqueue(2, "next", False))


class ConnectionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.previous_state = server.STATE
        server.STATE = server.LiveRecallServer()
        self.sockets = []
        self.listener = await serve(server.handler, "127.0.0.1", 0)
        self.uri = f"ws://127.0.0.1:{self.listener.sockets[0].getsockname()[1]}"

    async def asyncTearDown(self):
        for ws in self.sockets:
            await ws.close()
        self.listener.close()
        await self.listener.wait_closed()
        self.assertFalse(server.STATE.clients)
        self.assertIsNone(server.STATE.source)
        server.STATE = self.previous_state

    async def open(self, role, session="L1", **fields):
        ws = await connect(self.uri)
        self.sockets.append(ws)
        await ws.send(json.dumps({"type": role, "session": session, **fields}))
        return ws

    async def receive(self, ws):
        return json.loads(await asyncio.wait_for(ws.recv(), 2))

    async def assert_closed(self, ws, code):
        with self.assertRaises(ConnectionClosed):
            await asyncio.wait_for(ws.recv(), 2)
        self.assertEqual(ws.close_code, code)

    async def test_late_join_transition_and_finalized_resume(self):
        source = await self.open("SOURCE_HELLO")
        first = await self.open("CLIENT_HELLO")
        self.assertEqual((await self.receive(first))["items"], [])
        for update in [caption(1, 3, True), caption(2, 1)]:
            await source.send(json.dumps(update))
            self.assertEqual(await self.receive(first), update)
        late = await self.open("CLIENT_HELLO", history_seconds=60)
        history = await self.receive(late)
        self.assertEqual([m["segment"] for m in history["items"]], [1])
        self.assertEqual((await self.receive(late))["segment"], 2)
        await source.send(json.dumps(caption(2, 2, True)))
        self.assertTrue((await self.receive(late))["final"])
        self.assertTrue((await self.receive(first))["final"])
        await first.close()
        await source.send(json.dumps(caption(3, 1, True)))
        self.assertEqual((await self.receive(late))["segment"], 3)
        resumed = await self.open("CLIENT_HELLO", last_segment=2, last_revision=2)
        self.assertEqual([m["segment"] for m in (await self.receive(resumed))["items"]], [3])

    async def test_second_source_and_other_session_are_rejected(self):
        client = await self.open("CLIENT_HELLO")
        await self.receive(client)
        source = await self.open("SOURCE_HELLO")
        await source.send(json.dumps(caption(1, final=True)))
        await self.receive(client)
        second = await self.open("SOURCE_HELLO")
        await self.assert_closed(second, 1008)
        for role in ["CLIENT_HELLO", "SOURCE_HELLO"]:
            other = await self.open(role, session="L2")
            await self.assert_closed(other, 1008)
        await source.close()
        # Wait for the source handler to release its slot before reconnecting.
        for _ in range(50):
            if server.STATE.source is None:
                break
            await asyncio.sleep(.01)
        resumed = await self.open("SOURCE_HELLO")
        await resumed.send(json.dumps(caption(2, final=True)))
        self.assertEqual((await self.receive(client))["segment"], 2)

    async def test_hello_caps_history_and_handles_unrepresentable_seconds(self):
        now = time.time()
        for segment, age in [(1, 65), (2, 50), (3, 20)]:
            server.STATE.history.append((now-age, segment, 1,
                                         caption(segment, final=True)))
        for seconds, expected in [(10000, [2, 3]), (10**400, [3]), (-1, [])]:
            client = await self.open("CLIENT_HELLO", history_seconds=seconds)
            history = await self.receive(client)
            self.assertEqual([m["segment"] for m in history["items"]], expected)

    async def test_overflow_closes_one_client_without_delaying_another(self):
        slow = await self.open("CLIENT_HELLO", client_id="slow")
        await self.receive(slow)
        fast = await self.open("CLIENT_HELLO", client_id="fast")
        await self.receive(fast)
        slow_state = next(c for c in server.STATE.clients if c.client_id == "slow")
        slow_state.sender.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await slow_state.sender
        with patch.object(server, "CLIENT_MAX_PENDING", 2):
            for segment in range(1, 4):
                server.STATE.accept_update(caption(segment, final=True))
                self.assertEqual((await self.receive(fast))["segment"], segment)
        await self.assert_closed(slow, 1013)
        self.assertEqual(server.STATE.slow_disconnects, 1)


if __name__ == "__main__":
    unittest.main()
