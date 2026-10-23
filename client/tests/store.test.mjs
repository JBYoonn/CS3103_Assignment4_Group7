// Run: node --test client/tests/
import test from "node:test";
import assert from "node:assert/strict";
import { CaptionStore } from "../store.js";

const upd = (segment, revision, final = false, ts = 1000, text) => ({
  type: "CAPTION_UPDATE", session: "L1", segment, revision, final,
  source_ts: ts, text: text ?? `s${segment} r${revision}`,
});

test("newer revision replaces older one in place (no duplicate line)", () => {
  const s = new CaptionStore();
  assert.equal(s.apply(upd(1, 1), 1000).kind, "new");
  assert.equal(s.apply(upd(1, 2), 1000).kind, "update");
  assert.equal(s.entries().length, 1);
  assert.equal(s.get(1).text, "s1 r2");
});

test("older or equal revision is ignored", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3), 1000);
  assert.equal(s.apply(upd(1, 2), 1000).kind, "ignored");
  assert.equal(s.apply(upd(1, 3), 1000).kind, "ignored");
  assert.equal(s.get(1).revision, 3);
});

test("final is durable: nothing overwrites it", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3, true), 1000);
  assert.equal(s.apply(upd(1, 4), 1000).kind, "ignored");
  assert.equal(s.get(1).final, true);
});

test("history merge never duplicates a final already held", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3, true), 1000);
  s.apply(upd(2, 1), 1000);
  const applied = s.mergeHistory([upd(1, 3, true), upd(2, 3, true), upd(3, 3, true)], 1001, true);
  assert.deepEqual(applied.map((e) => e.segment), [2, 3]);
  assert.equal(s.finals().length, 3);
  assert.equal(s.get(2).recovered, true);
});

test("history items that are not final are rejected", () => {
  const s = new CaptionStore();
  assert.deepEqual(s.mergeHistory([upd(1, 1, false)], 1000, false), []);
});

test("resume state is the latest finalized segment held", () => {
  const s = new CaptionStore();
  assert.deepEqual(s.resumeState(), { last_segment: 0, last_revision: 0 });
  s.apply(upd(1, 3, true), 1000);
  s.apply(upd(2, 2), 1000);              // provisional does not count
  assert.deepEqual(s.resumeState(), { last_segment: 1, last_revision: 3 });
});

test("recall returns finals within the window, oldest first", () => {
  const s = new CaptionStore();
  for (let i = 1; i <= 10; i++) s.apply(upd(i, 1, true, 1000 + i * 10), 1000 + i * 10);
  const now = 1100;                        // lecture clock (offset ~0 here)
  assert.deepEqual(s.recall(30, now).map((e) => e.segment), [7, 8, 9, 10]);
  assert.deepEqual(s.recall(15, now).map((e) => e.segment), [9, 10]);
  assert.deepEqual(s.recall(60, now).map((e) => e.segment), [4, 5, 6, 7, 8, 9, 10]);
});

test("lecture clock compensates for source/browser clock skew", () => {
  const s = new CaptionStore();
  // Source clock is 500 s behind the browser; captions arrive ~0.05 s after source_ts.
  s.apply(upd(1, 1, true, 500), 1000.05);
  s.apply(upd(2, 1, true, 510), 1010.2);
  assert.ok(Math.abs(s.lectureNow(1010.05) - 510) < 0.1);
});

test("finals older than keepSeconds are pruned but at least 60 s are kept", () => {
  const s = new CaptionStore({ keepSeconds: 60 });
  for (let i = 0; i <= 100; i++) s.apply(upd(i + 1, 1, true, 1000 + i), 1000 + i);
  s.prune(1100);
  const segs = s.finals().map((e) => e.segment);
  assert.equal(segs[0], 41);               // ts 1040 .. 1100 retained
  assert.equal(segs.at(-1), 101);
});

test("reset clears everything", () => {
  const s = new CaptionStore();
  s.apply(upd(1, 3, true), 1000);
  s.reset();
  assert.equal(s.entries().length, 0);
  assert.deepEqual(s.resumeState(), { last_segment: 0, last_revision: 0 });
});

test("malformed messages are ignored", () => {
  const s = new CaptionStore();
  assert.equal(s.apply({ type: "CAPTION_UPDATE", segment: "x", revision: 1 }, 1).kind, "ignored");
  assert.equal(s.apply({ type: "CAPTION_UPDATE", segment: 1, revision: 1, text: 5 }, 1).kind, "ignored");
});
