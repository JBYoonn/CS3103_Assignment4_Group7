"""Lecture caption state: revision tracking, bounded history, HISTORY selection.

One CaptionState holds everything the server remembers about a lecture:

  active   segment -> latest provisional Caption (bounded by max_active_segments)
  history  deque of finalized Captions, oldest first (bounded by age AND count)
  floor    highest segment that has been pruned out of history; anything at or
           below it is too old to matter and is rejected outright

Invariants:
  * a final caption is durable: nothing replaces it, older or newer;
  * a revision is accepted only if it is newer than what we hold for that segment;
  * memory is bounded no matter how long the lecture runs.
"""

from __future__ import annotations

import json
import math
import time
from collections import OrderedDict, deque
from dataclasses import dataclass
from typing import Any, Callable

MAX_RECALL_SECONDS = 60
MAX_TEXT_CHARS = 8000      # a caption is a sentence; anything bigger is junk or abuse


@dataclass(frozen=True, slots=True)
class Caption:
    segment: int
    revision: int
    final: bool
    text: str
    source_ts: float
    session: str
    recv_ts: float          # server monotonic receive time (history age is measured on this)
    wire: str               # pre-serialised CAPTION_UPDATE, built once and shared by all clients


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def parse_update(msg: dict, wall_clock: Callable[[], float]) -> dict | None:
    """Validate a CAPTION_UPDATE and keep only the required fields."""
    seg, rev, final, text = msg.get("segment"), msg.get("revision"), msg.get("final"), msg.get("text")
    if not (_is_int(seg) and seg >= 1 and _is_int(rev) and rev >= 1):
        return None
    if not isinstance(final, bool) or not isinstance(text, str) or len(text) > MAX_TEXT_CHARS:
        return None
    try:
        text.encode("utf-8")              # lone surrogates would break every later send
    except UnicodeEncodeError:
        return None
    ts = msg.get("source_ts")
    if not isinstance(ts, (int, float)) or isinstance(ts, bool) or not math.isfinite(ts):
        ts = wall_clock()                 # NaN/inf would serialise to invalid JSON
    session = msg.get("session") if isinstance(msg.get("session"), str) else "L1"
    return {
        "type": "CAPTION_UPDATE", "session": session, "segment": seg, "revision": rev,
        "source_ts": float(ts), "final": final, "text": text,
    }


class CaptionState:
    def __init__(
        self,
        history_keep_seconds: float = 90.0,
        max_history_items: int = 2000,
        max_active_segments: int = 64,
        wall_clock: Callable[[], float] = time.time,
    ):
        # Keep a little more than the 60 s recall window so a client that was
        # away slightly longer than 60 s can still resume without a gap.
        self.history_keep_seconds = history_keep_seconds
        self.max_history_items = max_history_items
        self.max_active_segments = max_active_segments
        self.wall_clock = wall_clock
        self.reset()

    def reset(self) -> None:
        self.active: OrderedDict[int, Caption] = OrderedDict()
        self.history: deque[Caption] = deque()
        self.final_rev: dict[int, int] = {}      # segment -> final revision, mirrors history
        self.floor = 0
        self.max_segment = 0

    def __len__(self) -> int:
        return len(self.history)

    # -- source side ---------------------------------------------------------

    def apply(self, msg: dict, now: float) -> Caption | None:
        """Apply one source update. Returns the accepted Caption, or None if it
        was malformed, stale, a duplicate, or targets an already-final segment."""
        fields = parse_update(msg, self.wall_clock)
        if fields is None:
            return None
        seg, rev = fields["segment"], fields["revision"]

        if seg <= self.floor or seg in self.final_rev:
            return None
        current = self.active.get(seg)
        if current is not None and rev <= current.revision:
            return None

        cap = Caption(
            segment=seg, revision=rev, final=fields["final"], text=fields["text"],
            source_ts=fields["source_ts"], session=fields["session"], recv_ts=now,
            wire=json.dumps(fields, separators=(",", ":"), ensure_ascii=False, allow_nan=False),
        )
        self.max_segment = max(self.max_segment, seg)

        if cap.final:
            self.active.pop(seg, None)
            self.history.append(cap)
            self.final_rev[seg] = rev
            self.prune(now)
        else:
            self.active[seg] = cap
            self.active.move_to_end(seg)
            while len(self.active) > self.max_active_segments:
                self.active.popitem(last=False)
        return cap

    def prune(self, now: float) -> None:
        cutoff = now - self.history_keep_seconds
        while self.history and (
            self.history[0].recv_ts < cutoff or len(self.history) > self.max_history_items
        ):
            old = self.history.popleft()
            self.final_rev.pop(old.segment, None)
            self.floor = max(self.floor, old.segment)
        # Provisional segments that fell below the floor can never be finalised now.
        for seg in [s for s in self.active if s <= self.floor]:
            del self.active[seg]

    # -- client side ---------------------------------------------------------

    def history_for(self, last_segment: int, last_revision: int,
                    history_seconds: float, now: float) -> list[Caption]:
        """Finalized captions for a CLIENT_HELLO, oldest first.

        * New client (last_segment == 0): the last `history_seconds` (max 60).
        * Resuming client: every retained final it has not reported, even if
          that spans more than `history_seconds` -- a resume should not
          silently drop captions the server still has.
        """
        self.prune(now)
        if last_segment <= 0:
            window = max(0.0, min(float(history_seconds), MAX_RECALL_SECONDS))
            cutoff = now - window
            return [c for c in self.history if c.recv_ts >= cutoff]
        return [
            c for c in self.history
            if c.segment > last_segment or (c.segment == last_segment and c.revision > last_revision)
        ]

    def active_captions(self) -> list[Caption]:
        return sorted(self.active.values(), key=lambda c: c.segment)
