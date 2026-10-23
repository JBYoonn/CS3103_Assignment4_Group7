"""Per-client outbound buffer with semantic coalescing.

Each connected client owns one ClientOutbox. The source never writes to a
socket directly; it pushes Captions into every outbox, and each client's own
sender task drains its outbox as fast as that client can take it.

Pending captions are kept per segment, in segment order. What happens when a
newer revision arrives for a segment that still has something pending depends
on whether this client is under pressure:

  * no pressure (client keeping up): the revision is queued behind the older
    one, so a bursty source still delivers every revision -- this protects
    Normal Update Coverage;
  * pressure (the sender is blocked on TCP, the flow controller has limited
    this client, or more than `soft_limit` revisions are pending): older
    pending *provisional* revisions of that segment are discarded -- they are
    obsolete, the client would overwrite them immediately anyway.

A final caption is never discarded or replaced, and nothing is ever sent that
is older than (or equal to) what this client already has.

In *limited* mode (see flow.py) one take() produces one message chosen for
usefulness per message:

  * 2+ pending finals  -> one HISTORY batch (finals only, oldest first)
  * 1 pending final    -> that CAPTION_UPDATE
  * only provisionals  -> the newest one; older provisional segments are dropped
"""

from __future__ import annotations

from collections import OrderedDict, deque

from .state import Caption


def history_message(captions: list[Caption], catchup: bool = False) -> str:
    extra = ',"catchup":true' if catchup else ""
    return '{"type":"HISTORY"%s,"items":[%s]}' % (extra, ",".join(c.wire for c in captions))


class ClientOutbox:
    def __init__(self, max_pending_finals: int = 1024, max_batch: int = 256,
                 remember_segments: int = 256, soft_limit: int = 64):
        self.max_pending_finals = max_pending_finals
        self.max_batch = max_batch
        self.remember_segments = remember_segments
        self.soft_limit = soft_limit       # compaction trigger, not a cap (finals are capped below)
        self.queue: OrderedDict[int, list[Caption]] = OrderedDict()  # segment -> pending revisions
        self._count = 0
        self.pending_finals = 0
        self.sent_rev: OrderedDict[int, int] = OrderedDict()          # segment -> highest revision sent
        self.floor = 0          # client already holds everything at or below this segment
        self.coalesced = 0      # obsolete provisionals suppressed (for logs/report)
        self.batched = 0        # finals delivered inside HISTORY catch-up batches

    @property
    def pending(self) -> int:
        return self._count

    @property
    def overflowed(self) -> bool:
        return self.pending_finals > self.max_pending_finals

    def prime(self, last_segment: int, last_revision: int) -> None:
        """Record the finalized state the client reported in CLIENT_HELLO."""
        if last_segment > 0:
            self.floor = max(self.floor, last_segment)

    def mark_sent(self, captions: list[Caption]) -> None:
        for c in captions:
            if c.revision > self.sent_rev.get(c.segment, 0):
                self.sent_rev[c.segment] = c.revision
                self.sent_rev.move_to_end(c.segment)
        while len(self.sent_rev) > self.remember_segments:
            seg, _ = self.sent_rev.popitem(last=False)
            self.floor = max(self.floor, seg)

    def push(self, cap: Caption, coalesce: bool = False) -> None:
        seg = cap.segment
        if seg <= self.floor or cap.revision <= self.sent_rev.get(seg, 0):
            return
        revs = self.queue.get(seg)
        if revs is None:
            self.queue[seg] = [cap]
        else:
            last = revs[-1]
            if last.final or last.revision >= cap.revision:
                return
            if coalesce:
                self.coalesced += len(revs)
                self._count -= len(revs)
                revs.clear()
            revs.append(cap)
        self._count += 1
        if cap.final:
            self.pending_finals += 1
        if self._count > self.soft_limit:
            self.compact()

    def compact(self) -> None:
        """Keep only the newest pending revision of every segment."""
        for revs in self.queue.values():
            if len(revs) > 1:
                dropped = len(revs) - 1
                self.coalesced += dropped
                self._count -= dropped
                del revs[:-1]

    def _pop_segment(self, seg: int) -> Caption:
        """Remove a segment entirely; returns its newest pending revision."""
        revs = self.queue.pop(seg)
        self._count -= len(revs)
        newest = revs[-1]
        if newest.final:
            self.pending_finals -= 1
        return newest

    def take(self, limited: bool) -> str | None:
        """Remove and return the next wire message, or None if nothing is pending."""
        if not self.queue:
            return None
        if not limited:
            seg, revs = next(iter(self.queue.items()))
            cap = revs.pop(0)
            self._count -= 1
            if not revs:
                del self.queue[seg]
            if cap.final:
                self.pending_finals -= 1
            self.mark_sent([cap])
            return cap.wire

        self.compact()
        finals = [s for s, revs in self.queue.items() if revs[-1].final][: self.max_batch]
        if len(finals) >= 2:
            batch = [self._pop_segment(s) for s in finals]
            self.mark_sent(batch)
            self.batched += len(batch)
            return history_message(batch, catchup=True)
        if finals:
            cap = self._pop_segment(finals[0])
            self.mark_sent([cap])
            return cap.wire

        newest = next(reversed(self.queue))
        for seg in list(self.queue):
            if seg != newest:
                self._pop_segment(seg)
                self.coalesced += 1
        cap = self._pop_segment(newest)
        self.mark_sent([cap])
        return cap.wire


class FifoOutbox(ClientOutbox):
    """Conventional per-client FIFO queue: no coalescing, no batching.

    Used only by the "basic" profile for the before/after comparison. It still
    refuses stale/duplicate state, so the comparison isolates the overload
    policy rather than correctness.
    """

    def __init__(self, **kw):
        super().__init__(**kw)
        self.fifo: deque[Caption] = deque()

    @property
    def pending(self) -> int:
        return len(self.fifo)

    def push(self, cap: Caption, coalesce: bool = False) -> None:
        if cap.segment <= self.floor or cap.revision <= self.sent_rev.get(cap.segment, 0):
            return
        self.mark_sent([cap])          # "queued" counts as sent for stale filtering
        self.fifo.append(cap)
        if cap.final:
            self.pending_finals += 1

    def take(self, limited: bool) -> str | None:
        if not self.fifo:
            return None
        cap = self.fifo.popleft()
        if cap.final:
            self.pending_finals -= 1
        return cap.wire
