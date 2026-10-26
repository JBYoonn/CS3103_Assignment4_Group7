/*
 * store.js - LiveRecall caption state (no network code, no DOM code).
 *
 * The store keeps ONE entry per caption segment:
 *     segment -> { segment, revision, final, text, source_ts, recovered }
 *
 * Core rule (used for live updates AND recovered HISTORY items):
 *   1. If we already hold the FINAL for this segment  -> ignore the update.
 *        (finals are durable; this also blocks duplicate finals after reconnect)
 *   2. If the update's revision <= the revision we hold -> ignore it (stale).
 *   3. Otherwise the update REPLACES what we hold for that segment.
 *
 * Because every update goes through this one function (apply), an older
 * revision can never overwrite a newer one, no matter what order messages
 * arrive in (live, recovered history, or both overlapping).
 *
 * Bounded memory: entries older than HISTORY_SECONDS of lecture time are
 * pruned, so a long lecture never grows the store (or the page) forever.
 */
(function (root) {
  "use strict";
 
  // Team decision: client keeps 90 s of captions (brief requires >= 60 s).
  const DEFAULT_HISTORY_SECONDS = 90;
  // Hard safety cap in case timestamps are ever wrong.
  const DEFAULT_MAX_ENTRIES = 2000;
 
  class CaptionStore {
    constructor(options = {}) {
      this.historySeconds = options.historySeconds ?? DEFAULT_HISTORY_SECONDS;
      this.maxEntries = options.maxEntries ?? DEFAULT_MAX_ENTRIES;
      this.reset();
    }
 
    reset() {
      this.segments = new Map();   // segment number -> entry
      this.lastFinalSegment = 0;   // resume point sent in CLIENT_HELLO
      this.lastFinalRevision = 0;
      this.prunedUpTo = 0;         // segments <= this were dropped by pruning
      // Lecture clock: latest source_ts seen live, and when (local ms) we saw it.
      this.clockSourceTs = null;
      this.clockLocalMs = null;
    }
 
    /**
     * Apply one CAPTION_UPDATE.
     * Returns { changed: boolean, reason: string }.
     *   reason is one of: "new", "revised", "finalized",
     *                     "duplicate-final", "stale", "too-old", "invalid"
     */
    apply(update, { recovered = false } = {}) {
      if (!isValidUpdate(update)) return { changed: false, reason: "invalid" };
 
      // Lecture restarted (caption source / server restarted): segment numbers
      // start again from 1. Without this check every new caption would look
      // "old" and be ignored, freezing the page on the previous lecture.
      let restarted = false;
      if (!recovered && this.isRestart(update)) {
        this.reset();
        restarted = true;
      }
 
      const seg = update.segment;
      const held = this.segments.get(seg);
 
      // Live updates move the lecture clock forward (after the restart check).
      if (!recovered) this.observeClock(update.source_ts);
 
      // Already pruned out of our window: don't resurrect it.
      if (!held && seg <= this.prunedUpTo) return { changed: false, reason: "too-old" };
 
      if (held) {
        if (held.final) return { changed: false, reason: "duplicate-final" }; // rule 1
        if (update.revision <= held.revision) return { changed: false, reason: "stale" }; // rule 2
      }
 
      // Rule 3: replace.
      const entry = {
        segment: seg,
        revision: update.revision,
        final: update.final === true,
        text: String(update.text ?? ""),
        source_ts: Number(update.source_ts),
        recovered,
      };
      this.segments.set(seg, entry);
 
      if (entry.final && seg > this.lastFinalSegment) {
        this.lastFinalSegment = seg;
        this.lastFinalRevision = entry.revision;
      }
 
      let reason;
      if (restarted) reason = "restart";
      else if (!held) reason = entry.final ? "finalized" : "new";
      else reason = entry.final ? "finalized" : "revised";
      return { changed: true, reason };
    }
 
    /**
     * A live update means "new lecture" when BOTH are true:
     *   - its segment number is lower than the newest segment we hold, and we
     *     can't accept it normally (we hold its final, or it was pruned), and
     *   - its source_ts is NEWER than anything we've seen.
     * The timestamp check matters: a server may legitimately deliver segment
     * N's final just after segment N+1's first revision, but that final is
     * OLDER in time, so it is not mistaken for a restart.
     */
    isRestart(update) {
      if (this.clockSourceTs === null || this.segments.size === 0) return false;
      if (Number(update.source_ts) <= this.clockSourceTs) return false;
      const newest = Math.max(this.lastFinalSegment, ...this.segments.keys());
      if (update.segment >= newest) return false;
      const held = this.segments.get(update.segment);
      return !held || held.final;
    }
 
    /**
     * Merge a HISTORY message's items (finalized captions).
     * Items are applied oldest -> newest through the same apply() rule, so
     * anything we already hold is skipped and nothing is shown twice.
     * Returns the number of captions actually added/replaced.
     */
    mergeHistory(items) {
      if (!Array.isArray(items)) return 0;
      const sorted = items
        .filter((it) => it && it.final === true)
        .slice()
        .sort((a, b) => a.segment - b.segment);
      let added = 0;
      for (const item of sorted) {
        if (this.apply(item, { recovered: true }).changed) added++;
      }
      // Late join with no live update yet: start the lecture clock from history.
      if (this.clockSourceTs === null && sorted.length > 0) {
        this.observeClock(sorted[sorted.length - 1].source_ts);
      }
      return added;
    }
 
    /** Fields for CLIENT_HELLO: latest finalized caption we hold (0/0 if none). */
    resumeState() {
      return { last_segment: this.lastFinalSegment, last_revision: this.lastFinalRevision };
    }
 
    /**
     * Lecture clock. The browser's clock may differ from the server/source
     * machine, so instead of Date.now() vs source_ts we track:
     *   "lecture now" = latest live source_ts + time elapsed locally since then.
     */
    observeClock(sourceTs, localMs = Date.now()) {
      const ts = Number(sourceTs);
      if (!Number.isFinite(ts)) return;
      if (this.clockSourceTs === null || ts >= this.clockSourceTs) {
        this.clockSourceTs = ts;
        this.clockLocalMs = localMs;
      }
    }
 
    lectureNow(localMs = Date.now()) {
      if (this.clockSourceTs === null) return null;
      return this.clockSourceTs + (localMs - this.clockLocalMs) / 1000;
    }
 
    /** Drop entries older than the history window (bounded memory). */
    prune(now = this.lectureNow()) {
      let removed = 0;
      if (now !== null) {
        const cutoff = now - this.historySeconds;
        for (const [seg, entry] of this.segments) {
          if (entry.source_ts < cutoff) {
            this.segments.delete(seg);
            if (seg > this.prunedUpTo) this.prunedUpTo = seg;
            removed++;
          }
        }
      }
      // Safety cap: drop the oldest segments if we somehow hold too many.
      if (this.segments.size > this.maxEntries) {
        const extra = [...this.segments.keys()].sort((a, b) => a - b)
          .slice(0, this.segments.size - this.maxEntries);
        for (const seg of extra) {
          this.segments.delete(seg);
          if (seg > this.prunedUpTo) this.prunedUpTo = seg;
          removed++;
        }
      }
      return removed;
    }
 
    /** All held entries, oldest -> newest, for the live view. */
    ordered() {
      return [...this.segments.values()].sort((a, b) => a.segment - b.segment);
    }
 
    /**
     * Live view rows: entries plus gap markers where segment numbers jump
     * (captions we missed and could not recover).
     */
    viewRows() {
      const rows = [];
      let prev = null;
      for (const entry of this.ordered()) {
        if (prev !== null && entry.segment > prev + 1) {
          rows.push({ gap: true, from: prev + 1, to: entry.segment - 1 });
        }
        rows.push(entry);
        prev = entry.segment;
      }
      return rows;
    }
 
    /** Finalized captions from the last `seconds` of lecture time (local only). */
    recall(seconds, now = this.lectureNow()) {
      if (now === null) return [];
      const from = now - seconds;
      return this.ordered().filter((e) => e.final && e.source_ts >= from);
    }
  }
 
  function isValidUpdate(u) {
    return (
      u != null &&
      Number.isInteger(u.segment) && u.segment > 0 &&
      Number.isInteger(u.revision) && u.revision > 0 &&
      Number.isFinite(Number(u.source_ts))
    );
  }
 
  const api = { CaptionStore, DEFAULT_HISTORY_SECONDS };
  if (typeof module === "object" && module.exports) module.exports = api; // Node tests
  else root.LiveRecallStore = api;                                         // browser
})(typeof globalThis !== "undefined" ? globalThis : this);
 