"""Revision state, bounded history, and HISTORY selection (liverecall/state.py)."""

import json

import pytest

from liverecall.state import CaptionState


def update(segment, revision, final=False, text=None, source_ts=1000.0, **extra):
    return {
        "type": "CAPTION_UPDATE",
        "session": "L1",
        "segment": segment,
        "revision": revision,
        "source_ts": source_ts,
        "final": final,
        "text": text if text is not None else f"s{segment} r{revision}",
        **extra,
    }


def make_state(**kw):
    kw.setdefault("history_keep_seconds", 90)
    kw.setdefault("max_history_items", 1000)
    return CaptionState(**kw)


# --- revision semantics -----------------------------------------------------

def test_first_provisional_revision_is_accepted():
    st = make_state()
    cap = st.apply(update(1, 1), now=0.0)
    assert cap is not None
    assert (cap.segment, cap.revision, cap.final) == (1, 1, False)


def test_newer_revision_replaces_and_older_or_equal_is_rejected():
    st = make_state()
    assert st.apply(update(1, 2), now=0.0) is not None
    assert st.apply(update(1, 1), now=0.1) is None      # stale
    assert st.apply(update(1, 2), now=0.2) is None      # duplicate
    assert st.apply(update(1, 3), now=0.3) is not None


def test_final_is_durable_and_never_overwritten():
    st = make_state()
    st.apply(update(1, 3, final=True), now=0.0)
    assert st.apply(update(1, 4), now=0.1) is None
    assert st.apply(update(1, 2), now=0.1) is None
    assert st.active_captions() == []


def test_revision_numbers_need_not_be_contiguous_or_fixed_count():
    st = make_state()
    assert st.apply(update(1, 1), now=0.0) is not None
    assert st.apply(update(1, 7), now=0.1) is not None
    assert st.apply(update(1, 9, final=True), now=0.2) is not None


@pytest.mark.parametrize("bad", [
    {"segment": 0}, {"segment": -1}, {"segment": "3"}, {"segment": True},
    {"revision": 0}, {"revision": 1.5}, {"text": None}, {"final": "yes"},
])
def test_malformed_updates_are_ignored(bad):
    st = make_state()
    msg = update(1, 1)
    msg.update(bad)
    assert st.apply(msg, now=0.0) is None


def test_unknown_fields_are_ignored_and_not_forwarded():
    st = make_state()
    cap = st.apply(update(1, 1, phase="warmup", junk=[1, 2]), now=0.0)
    wire = json.loads(cap.wire)
    assert "phase" not in wire and "junk" not in wire
    assert wire == {
        "type": "CAPTION_UPDATE", "session": "L1", "segment": 1, "revision": 1,
        "source_ts": 1000.0, "final": False, "text": "s1 r1",
    }


def test_missing_source_ts_uses_server_receive_wall_clock():
    st = make_state(wall_clock=lambda: 5555.5)
    msg = update(1, 1)
    del msg["source_ts"]
    cap = st.apply(msg, now=0.0)
    assert cap.source_ts == 5555.5


# --- active (provisional) state --------------------------------------------

def test_active_captions_hold_latest_provisional_per_segment():
    st = make_state()
    st.apply(update(4, 1), now=0.0)
    st.apply(update(4, 2), now=0.1)
    assert [(c.segment, c.revision) for c in st.active_captions()] == [(4, 2)]


# --- HISTORY selection ------------------------------------------------------

def finals(st, segs, start=0.0, step=1.0):
    for i, s in enumerate(segs):
        st.apply(update(s, 3, final=True), now=start + i * step)


def test_new_client_gets_finals_within_requested_seconds_oldest_first():
    st = make_state()
    finals(st, range(1, 11), start=0.0, step=10.0)     # finals at t=0..90
    items = st.history_for(last_segment=0, last_revision=0, history_seconds=30, now=95.0)
    assert [c.segment for c in items] == [8, 9, 10]     # finals at t=70,80,90 are within 30 s of t=95


def test_history_seconds_is_clamped_to_60():
    st = make_state()
    finals(st, range(1, 11), start=0.0, step=10.0)
    items = st.history_for(0, 0, history_seconds=500, now=95.0)
    assert [c.segment for c in items] == [5, 6, 7, 8, 9, 10]   # only t>=35, not 500 s back


def test_resuming_client_gets_only_finals_it_missed():
    st = make_state()
    finals(st, range(1, 11), start=0.0, step=1.0)
    items = st.history_for(last_segment=6, last_revision=3, history_seconds=30, now=10.0)
    assert [c.segment for c in items] == [7, 8, 9, 10]


def test_resume_returns_everything_missed_still_retained_even_beyond_history_seconds():
    # A client away for 45 s asking for 30 s must not silently lose 15 s of finals.
    st = make_state()
    finals(st, range(1, 11), start=0.0, step=5.0)      # t=0..45
    items = st.history_for(last_segment=1, last_revision=3, history_seconds=30, now=46.0)
    assert [c.segment for c in items] == list(range(2, 11))


def test_resume_same_segment_with_older_revision_includes_that_final():
    st = make_state()
    st.apply(update(5, 4, final=True), now=0.0)
    items = st.history_for(last_segment=5, last_revision=2, history_seconds=30, now=1.0)
    assert [(c.segment, c.revision) for c in items] == [(5, 4)]


def test_history_never_contains_provisional_captions():
    st = make_state()
    finals(st, [1, 2])
    st.apply(update(3, 1), now=3.0)
    assert all(c.final for c in st.history_for(0, 0, 60, now=4.0))


# --- boundedness ------------------------------------------------------------

def test_history_is_pruned_by_age():
    st = make_state(history_keep_seconds=90)
    finals(st, range(1, 101), start=0.0, step=1.0)     # t=0..99
    st.prune(now=200.0)
    assert len(st) == 0
    st.apply(update(101, 1, final=True), now=200.0)
    assert len(st) == 1


def test_history_is_capped_by_count():
    st = make_state(max_history_items=50)
    finals(st, range(1, 201), start=0.0, step=0.01)
    assert len(st) == 50
    assert st.history_for(0, 0, 60, now=2.0)[0].segment == 151


def test_updates_for_segments_older_than_retained_history_are_rejected():
    st = make_state(max_history_items=5)
    finals(st, range(1, 21), start=0.0, step=0.1)
    assert st.apply(update(3, 9), now=3.0) is None
    assert st.apply(update(3, 9, final=True), now=3.0) is None


def test_active_segments_are_bounded():
    st = make_state(max_active_segments=8)
    for s in range(1, 100):
        st.apply(update(s, 1), now=0.0)
    assert len(st.active_captions()) == 8
    assert st.active_captions()[-1].segment == 99


# --- lecture restart --------------------------------------------------------

def test_reset_clears_everything():
    st = make_state()
    finals(st, range(1, 5))
    st.apply(update(5, 1), now=5.0)
    st.reset()
    assert len(st) == 0 and st.active_captions() == [] and st.max_segment == 0
    assert st.apply(update(1, 1), now=6.0) is not None


def test_max_segment_tracks_highest_seen():
    st = make_state()
    st.apply(update(3, 1), now=0.0)
    st.apply(update(2, 1), now=0.0)
    assert st.max_segment == 3


# --- hostile / malformed source input (review findings) ---------------------

@pytest.mark.parametrize("ts", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_source_ts_falls_back_to_wall_clock_and_wire_is_strict_json(ts):
    st = make_state(wall_clock=lambda: 4242.0)
    cap = st.apply(update(1, 1, source_ts=ts), now=0.0)
    assert cap.source_ts == 4242.0
    json.loads(cap.wire, parse_constant=lambda c: (_ for _ in ()).throw(ValueError(c)))


def test_text_that_cannot_be_utf8_encoded_is_rejected():
    st = make_state()
    assert st.apply(update(1, 1, text="bad \ud800 surrogate"), now=0.0) is None


def test_oversized_text_is_rejected():
    st = make_state()
    assert st.apply(update(1, 1, text="x" * 20_000), now=0.0) is None
