// Run from the repo root:   node --test client/tests/*.test.mjs
import test from "node:test";
import assert from "node:assert/strict";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);
const { CaptionStore } = require("../store.js");

const T0 = 1760000000; // fake lecture start time (seconds)
const upd = (segment, revision, final, text, ts = T0 + segment) =>
  ({ type: "CAPTION_UPDATE", session: "L1", segment, revision, final, text, source_ts: ts });

test("newer revision replaces older one (no duplicate line)", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 1, false, "TCP uses"));
  s.apply(upd(1, 2, false, "TCP uses cumulative"));
  s.apply(upd(1, 3, true, "TCP uses cumulative acknowledgements."));
  const rows = s.ordered();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].revision, 3);
  assert.equal(rows[0].final, true);
});

test("older revision arriving late is ignored", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3, false, "newest"));
  const r = s.apply(upd(1, 2, false, "older"));
  assert.equal(r.reason, "stale");
  assert.equal(s.ordered()[0].text, "newest");
});

test("final caption is never overwritten, even by a higher revision", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3, true, "final text"));
  const r = s.apply(upd(1, 9, false, "late provisional"));
  assert.equal(r.reason, "duplicate-final");
  assert.equal(s.ordered()[0].text, "final text");
});

test("final can arrive without ever seeing provisionals (server coalesced)", () => {
  const s = new CaptionStore();
  const r = s.apply(upd(5, 4, true, "straight to final"));
  assert.equal(r.reason, "finalized");
  assert.deepEqual(s.resumeState(), { last_segment: 5, last_revision: 4 });
});

test("resume state tracks the latest FINAL, not provisionals", () => {
  const s = new CaptionStore();
  assert.deepEqual(s.resumeState(), { last_segment: 0, last_revision: 0 });
  s.apply(upd(1, 2, true, "a"));
  s.apply(upd(2, 1, false, "b in progress"));
  assert.deepEqual(s.resumeState(), { last_segment: 1, last_revision: 2 });
});

test("HISTORY merge adds missed finals and skips ones already held", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 2, true, "one"));
  s.apply(upd(2, 2, true, "two"));
  // History overlaps what we hold (2) and adds 3, 4 - given out of order.
  const added = s.mergeHistory([upd(4, 1, true, "four"), upd(2, 2, true, "two"), upd(3, 3, true, "three")]);
  assert.equal(added, 2);
  assert.deepEqual(s.ordered().map((e) => e.segment), [1, 2, 3, 4]);
  assert.equal(s.ordered().filter((e) => e.segment === 2).length, 1);
  assert.equal(s.ordered().find((e) => e.segment === 3).recovered, true);
});

test("HISTORY final replaces a stale provisional we held before disconnect", () => {
  const s = new CaptionStore();
  s.apply(upd(3, 1, false, "half a sen"));
  s.mergeHistory([upd(3, 4, true, "half a sentence, completed.")]);
  const e = s.ordered()[0];
  assert.equal(e.final, true);
  assert.equal(e.text, "half a sentence, completed.");
});

test("HISTORY ignores non-final items", () => {
  const s = new CaptionStore();
  assert.equal(s.mergeHistory([upd(1, 1, false, "provisional")]), 0);
  assert.equal(s.ordered().length, 0);
});

test("pruning keeps memory bounded and pruned segments are not resurrected", () => {
  const s = new CaptionStore({ historySeconds: 90 });
  for (let i = 1; i <= 300; i++) s.apply(upd(i, 1, true, `seg ${i}`, T0 + i));
  s.prune(T0 + 300);
  const segs = s.ordered().map((e) => e.segment);
  assert.ok(segs.length <= 91, `held ${segs.length}`);
  assert.equal(segs[0], 210);
  const r = s.apply(upd(5, 1, true, "very old"));
  assert.equal(r.reason, "too-old");
});

test("recall returns only finals inside the chosen window", () => {
  const s = new CaptionStore();
  for (let i = 1; i <= 70; i++) s.apply(upd(i, 1, true, `s${i}`, T0 + i));
  s.apply(upd(71, 1, false, "still drafting", T0 + 71));
  const now = T0 + 71;
  assert.equal(s.recall(15, now).length, 15); // segments 56..70
  assert.equal(s.recall(30, now).length, 30);
  assert.equal(s.recall(60, now).length, 60);
  assert.ok(s.recall(60, now).every((e) => e.final));
});

test("lecture clock is independent of the browser clock", () => {
  const s = new CaptionStore();
  s.observeClock(T0 + 100, 5_000);          // browser clock says 5 s
  assert.equal(s.lectureNow(7_000), T0 + 102); // 2 s later locally
  s.observeClock(T0 + 50, 8_000);           // older ts must not move clock back
  assert.equal(s.lectureNow(8_000), T0 + 103);
});

test("gap markers show where segments could not be recovered", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 1, true, "a"));
  s.apply(upd(5, 1, true, "e"));
  const gap = s.viewRows().find((r) => r.gap);
  assert.deepEqual({ from: gap.from, to: gap.to }, { from: 2, to: 4 });
  s.mergeHistory([upd(2, 1, true, "b"), upd(3, 1, true, "c"), upd(4, 1, true, "d")]);
  assert.equal(s.viewRows().some((r) => r.gap), false);
});

test("lecture restart (segments back to 1, newer time) resets the store", () => {
  const s = new CaptionStore();
  for (let i = 1; i <= 50; i++) s.apply(upd(i, 1, true, `old ${i}`, T0 + i));
  // Caption source restarted: segment 1 again, but later in time.
  const r = s.apply(upd(1, 1, false, "new lecture", T0 + 500));
  assert.equal(r.reason, "restart");
  assert.deepEqual(s.ordered().map((e) => e.text), ["new lecture"]);
  assert.deepEqual(s.resumeState(), { last_segment: 0, last_revision: 0 });
  s.apply(upd(1, 2, true, "new lecture begins.", T0 + 501));
  assert.deepEqual(s.resumeState(), { last_segment: 1, last_revision: 2 });
});

test("late final of the previous segment is NOT mistaken for a restart", () => {
  const s = new CaptionStore();
  s.apply(upd(7, 1, false, "seven so far", T0 + 70));
  s.apply(upd(8, 1, false, "eight starts", T0 + 80));   // server sent N+1 first
  const r = s.apply(upd(7, 3, true, "seven done.", T0 + 75)); // older timestamp
  assert.equal(r.reason, "finalized");
  assert.equal(s.ordered().length, 2);
});

test("HISTORY items never trigger a restart", () => {
  const s = new CaptionStore();
  for (let i = 1; i <= 10; i++) s.apply(upd(i, 1, true, `s${i}`, T0 + i));
  s.mergeHistory([upd(2, 1, true, "s2", T0 + 999)]);
  assert.equal(s.ordered().length, 10);
});

test("invalid messages are rejected", () => {
  const s = new CaptionStore();
  assert.equal(s.apply({ segment: 0, revision: 1, source_ts: T0 }).reason, "invalid");
  assert.equal(s.apply({ segment: 1, revision: "x", source_ts: T0 }).reason, "invalid");
  assert.equal(s.ordered().length, 0);
});
