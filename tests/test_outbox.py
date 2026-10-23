"""Per-client outbox: coalescing, no stale/duplicate sends, batching (liverecall/outbox.py)."""

import json

from liverecall.outbox import ClientOutbox
from liverecall.state import CaptionState


def caps(*specs):
    """specs: (segment, revision, final) -> list[Caption] built through real state."""
    out = []
    for seg, rev, final in specs:
        st = CaptionState()
        out.append(st.apply({
            "type": "CAPTION_UPDATE", "session": "L1", "segment": seg, "revision": rev,
            "source_ts": 1.0, "final": final, "text": f"s{seg}r{rev}",
        }, now=0.0))
    return out


def ids(wire):
    m = json.loads(wire)
    if m["type"] == "CAPTION_UPDATE":
        return [(m["segment"], m["revision"])]
    return [(i["segment"], i["revision"]) for i in m["items"]]


def drain(box, limited=False):
    sent = []
    while (msg := box.take(limited=limited)) is not None:
        sent.append(msg)
    return sent


def test_normal_mode_sends_every_revision_in_order():
    box = ClientOutbox()
    a, = caps((1, 1, False))
    box.push(a)
    assert ids(box.take(limited=False)) == [(1, 1)]
    b, c = caps((1, 2, False), (1, 3, True))
    box.push(b)
    assert ids(box.take(limited=False)) == [(1, 2)]
    box.push(c)
    assert ids(box.take(limited=False)) == [(1, 3)]
    assert box.take(limited=False) is None


def test_burst_without_backpressure_keeps_every_revision():
    # A bursty source must not lose revisions for a client that is keeping up.
    box = ClientOutbox()
    for c in caps((1, 1, False), (1, 2, False), (1, 3, True)):
        box.push(c)
    assert [ids(m) for m in drain(box)] == [[(1, 1)], [(1, 2)], [(1, 3)]]


def test_pending_provisional_is_coalesced_by_newer_revision_under_backpressure():
    box = ClientOutbox()
    for c in caps((1, 1, False), (1, 2, False), (1, 3, False)):
        box.push(c, coalesce=True)
    assert [ids(m) for m in drain(box)] == [[(1, 3)]]
    assert box.coalesced == 2


def test_final_replaces_pending_provisional_of_same_segment_under_backpressure():
    box = ClientOutbox()
    for c in caps((1, 1, False), (1, 3, True)):
        box.push(c, coalesce=True)
    assert [ids(m) for m in drain(box)] == [[(1, 3)]]


def test_coalescing_keeps_segment_order():
    box = ClientOutbox()
    for c in caps((1, 1, False), (2, 1, False), (1, 2, True)):
        box.push(c, coalesce=True)
    assert [ids(m) for m in drain(box)] == [[(1, 2)], [(2, 1)]]


def test_backlog_beyond_soft_limit_is_compacted_even_without_pressure_flag():
    box = ClientOutbox(soft_limit=4)
    for c in caps(*[(1, r, False) for r in range(1, 4)], *[(2, r, False) for r in range(1, 4)]):
        box.push(c)
    assert box.pending <= 4
    assert [ids(m) for m in drain(box)][-1] == [(2, 3)]


def test_finals_survive_compaction():
    box = ClientOutbox(soft_limit=2)
    for c in caps((1, 1, False), (1, 2, True), (2, 1, False), (2, 2, False), (3, 1, True)):
        box.push(c)
    sent = [k for m in drain(box) for k in ids(m)]
    assert (1, 2) in sent and (3, 1) in sent


def test_never_sends_older_or_equal_revision_after_newer_was_sent():
    box = ClientOutbox()
    new, old, same = caps((1, 3, False), (1, 2, False), (1, 3, False))
    box.push(new)
    drain(box)
    box.push(old)
    box.push(same)
    assert box.take(limited=False) is None


def test_prime_suppresses_everything_the_client_already_holds():
    box = ClientOutbox()
    box.prime(last_segment=5, last_revision=3)
    for c in caps((4, 9, False), (5, 4, False), (6, 1, False)):
        box.push(c)
    assert [ids(m) for m in drain(box)] == [[(6, 1)]]


def test_mark_sent_counts_history_items_as_delivered():
    box = ClientOutbox()
    hist = caps((7, 3, True))
    box.mark_sent(hist)
    box.push(hist[0])
    assert box.take(limited=False) is None


def test_limited_mode_batches_pending_finals_into_one_history_message():
    box = ClientOutbox()
    for c in caps((1, 3, True), (2, 3, True), (3, 3, True), (4, 1, False)):
        box.push(c)
    first = box.take(limited=True)
    msg = json.loads(first)
    assert msg["type"] == "HISTORY"
    assert all(i["final"] for i in msg["items"])
    assert ids(first) == [(1, 3), (2, 3), (3, 3)]
    assert ids(box.take(limited=True)) == [(4, 1)]


def test_limited_mode_single_final_goes_as_caption_update():
    box = ClientOutbox()
    for c in caps((1, 3, True), (2, 1, False)):
        box.push(c)
    assert json.loads(box.take(limited=True))["type"] == "CAPTION_UPDATE"


def test_limited_mode_sends_only_newest_provisional_and_drops_older_segments():
    box = ClientOutbox()
    for c in caps((1, 1, False), (2, 2, False), (3, 1, False)):
        box.push(c)
    assert [ids(m) for m in drain(box, limited=True)] == [[(3, 1)]]


def test_batch_size_is_capped():
    box = ClientOutbox(max_batch=2)
    for c in caps(*[(s, 1, True) for s in range(1, 6)]):
        box.push(c)
    sizes = [len(ids(m)) for m in drain(box, limited=True)]
    assert sizes == [2, 2, 1]


def test_overflow_flag_when_pending_finals_exceed_bound():
    box = ClientOutbox(max_pending_finals=3)
    for c in caps(*[(s, 1, True) for s in range(1, 4)]):
        box.push(c)
    assert not box.overflowed
    box.push(caps((4, 1, True))[0])
    assert box.overflowed


def test_sent_revision_memory_is_bounded():
    box = ClientOutbox(remember_segments=10)
    for c in caps(*[(s, 1, True) for s in range(1, 101)]):
        box.push(c)
        drain(box)
    assert len(box.sent_rev) <= 10


def test_pending_count_reports_queue_size():
    box = ClientOutbox()
    for c in caps((1, 1, False), (2, 1, False)):
        box.push(c)
    assert box.pending == 2


# --- "basic" profile used for the before/after comparison -------------------

def test_fifo_outbox_keeps_every_revision_even_when_backlogged():
    from liverecall.outbox import FifoOutbox
    box = FifoOutbox()
    for c in caps((1, 1, False), (1, 2, False), (1, 3, True)):
        box.push(c)
    assert [ids(m) for m in drain(box)] == [[(1, 1)], [(1, 2)], [(1, 3)]]


def test_fifo_outbox_still_never_sends_stale_or_already_held_state():
    from liverecall.outbox import FifoOutbox
    box = FifoOutbox()
    box.prime(last_segment=2, last_revision=3)
    for c in caps((2, 3, True), (3, 2, False), (3, 1, False)):
        box.push(c)
    assert [ids(m) for m in drain(box)] == [[(3, 2)]]
