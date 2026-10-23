// LiveRecall client-side caption state. No DOM here, so it can be unit-tested with Node.
//
// One entry per segment: { segment, revision, final, text, source_ts, recovered }.
// Rules (mirroring the server):
//   * a newer revision replaces the older one in place -- never a second line;
//   * an older or equal revision is ignored (no stale state);
//   * a final caption is durable: nothing overwrites it;
//   * HISTORY may only add finals we do not already hold (no duplicate finals).

const isInt = (v) => Number.isInteger(v);

function valid(msg) {
  return msg && isInt(msg.segment) && msg.segment >= 1 && isInt(msg.revision) && msg.revision >= 1
    && typeof msg.text === "string" && typeof msg.final === "boolean";
}

export class CaptionStore {
  constructor({ keepSeconds = 600, maxFinals = 2000 } = {}) {
    this.keepSeconds = Math.max(60, keepSeconds);   // spec: at least the previous 60 s
    this.maxFinals = maxFinals;
    this.reset();
  }

  reset() {
    this.map = new Map();           // segment -> entry
    this.offset = Infinity;         // browser clock - source clock (+ min one-way latency)
    this.lastFinal = { last_segment: 0, last_revision: 0 };
  }

  get(segment) { return this.map.get(segment); }

  entries() { return [...this.map.values()].sort((a, b) => a.segment - b.segment); }

  finals() { return this.entries().filter((e) => e.final); }

  /** Live CAPTION_UPDATE. Returns { kind: "new" | "update" | "final" | "ignored", entry }. */
  apply(msg, recvAt) {
    if (!valid(msg)) return { kind: "ignored" };
    const res = this.#put(msg, false);
    if (res.kind !== "ignored" && typeof msg.source_ts === "number") {
      this.offset = Math.min(this.offset, recvAt - msg.source_ts);
    }
    return res;
  }

  /** HISTORY items (finals only). Returns the entries actually added or finalized. */
  mergeHistory(items, recvAt, recovered) {
    const applied = [];
    for (const item of [...(items || [])].sort((a, b) => a.segment - b.segment)) {
      if (!valid(item) || !item.final) continue;
      const res = this.#put(item, recovered);
      if (res.kind !== "ignored") applied.push(res.entry);
    }
    return applied;
  }

  #put(msg, recovered) {
    const cur = this.map.get(msg.segment);
    if (cur && (cur.final || msg.revision <= cur.revision)) return { kind: "ignored" };
    const entry = {
      segment: msg.segment, revision: msg.revision, final: msg.final, text: msg.text,
      source_ts: typeof msg.source_ts === "number" ? msg.source_ts : null,
      recovered: Boolean(recovered),
    };
    this.map.set(msg.segment, entry);
    if (entry.final) {
      const lf = this.lastFinal;
      if (entry.segment > lf.last_segment
          || (entry.segment === lf.last_segment && entry.revision > lf.last_revision)) {
        this.lastFinal = { last_segment: entry.segment, last_revision: entry.revision };
      }
    }
    return { kind: !cur ? (entry.final ? "final" : "new") : (entry.final ? "final" : "update"), entry };
  }

  /** What CLIENT_HELLO should report: the latest FINALIZED state we hold. */
  resumeState() { return { ...this.lastFinal }; }

  /** Current time on the source's clock, estimated from live captions. */
  lectureNow(browserNow) {
    return Number.isFinite(this.offset) ? browserNow - this.offset : browserNow;
  }

  /** Finalized captions from the last `seconds` of the lecture, oldest first. */
  recall(seconds, lectureNow) {
    const cutoff = lectureNow - seconds;
    return this.finals().filter((e) => e.source_ts !== null && e.source_ts >= cutoff);
  }

  /** Oldest source_ts we hold a final for (to tell the user if a window is partial). */
  oldestFinalTs() {
    const f = this.finals();
    return f.length ? f[0].source_ts : null;
  }

  prune(lectureNow) {
    const cutoff = lectureNow - this.keepSeconds;
    const finals = this.finals();
    let excess = finals.length - this.maxFinals;
    for (const e of finals) {
      if (e.source_ts !== null && e.source_ts >= cutoff && excess <= 0) break;
      this.map.delete(e.segment);
      excess -= 1;
    }
    return this.map.size;
  }
}
