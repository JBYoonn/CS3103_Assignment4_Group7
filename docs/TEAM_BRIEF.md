# Team brief — what we need to decide and own

Deadline **Wed 4 Nov 2026**. Demo and individual viva **Fri 6 Nov**, 15-minute slot; all four must attend.
The viva is **20 marks and marked individually**: anyone can be asked about any part of the code.

## 1. Decisions we need from everyone (please reply in the group chat)

1. **Viva slot.** Who books it, and which time works for all four? (Use the SharePoint link in the handout
   §15; enter only our team number.)
2. **Ubuntu environment.** Who has a working Ubuntu VM (the Mininet one, Multipass, or native)? We need at
   least one person to run T5 (`tc/netem`) and the final `submission_check.py --final` on Ubuntu.
3. **Ownership for the viva.** Each person owns one area below and must be able to explain it, change it live,
   and say where it lives in the code. Agree the split by **Tue 27 Oct**.
4. **Report sections.** Who writes which part of the 4–5 page `report.pdf`, and who does the final merge and
   PDF export? A draft with real numbers is in `docs/DESIGN.md`.
5. **Demo video (5–7 min).** Who records it, and who narrates? Script: README "Mandatory deterministic
   application demo".
6. **AI usage.** Everyone lists the tools they actually used and what for, in `AI_USAGE.md`. It must include
   at least one AI suggestion we rejected or changed, with the reason.
7. **Contribution %.** Agree honestly and record it in `CONTRIBUTIONS.md`. Git history is the evidence, so
   commit your own work from your own account.
8. **Optional ASR bonus (+3).** Is anyone keen? It must not touch `requirements.txt` or the server path.

## 2. Suggested ownership

| Owner | Area | Files | Must be able to answer |
|---|---|---|---|
| A | Revision state, history, recovery | `liverecall/state.py`, HISTORY path in `server.py` | Why can an older revision never overwrite a newer one? Exactly what happens when a client reconnects after 10 s offline? How is history bounded? |
| B | Per-client outbox and overload policy | `liverecall/outbox.py`, sender loop in `server.py` | Where is per-client buffering, and how is it bounded? Which provisional revisions can be dropped safely, and why? Why are finals batched? |
| C | Lag detection and rate control; T3/T5 experiments | `liverecall/flow.py`, `experiments/` | How do we know a client is slow? Why subtract the minimum RTT? Why do TICK messages help? What happens if the bandwidth is halved? |
| D | Browser client, demo video, report merge | `client/`, `README.md`, `docs/` | How does the client avoid duplicate finals after HISTORY? What resume state does it send, and why only *finalized* state? How does the recall window work without the server? |

Everyone should be able to explain the **whole data path** (source → state → hub → outbox → sender → client),
read the server's status line, and run `run_demo.py`.

## 3. Viva practice questions (from the handout, plus likely follow-ups)

- Why can an older revision never overwrite a newer one? → `CaptionState.apply` and `ClientOutbox.push`.
- What exactly happens when a client reconnects after 10 s offline? → `CLIENT_HELLO(last_segment)` →
  `history_for()` → only missed finals → current partial line → live.
- Where is per-client buffering implemented, and how is it bounded? → `ClientOutbox`: under pressure, one
  pending entry per segment; above 64 pending revisions it compacts the same way (a trigger, not a cap); at
  most 1,024 pending finals, past which we disconnect and the client resumes through HISTORY.
- Why did P95/P99 increase under packet loss even though TCP is reliable? → retransmission timeouts and
  head-of-line blocking delay every later message on that connection.
- What happens when a client consumes more slowly than captions are generated?
- Which provisional revisions can safely be dropped, and why?
- How would behaviour change if bandwidth were halved?
- Show the code that implements the overload/backpressure policy.
- Explain one experimental result that surprised the team. Candidates: the 16-message hidden queue in the
  client library, or the fragmentation idea that crashed the instructor's load client.
- Small live change: "make the server keep 120 s of history", "change the slow-client threshold",
  "send at most 1 revision per segment".

## 4. Timeline

| Date | Target |
|---|---|
| Sat 24 Oct | Everyone clones, runs the demo, reads the README + DESIGN.md |
| Tue 27 Oct | Ownership agreed; viva slot booked; each owner walks the others through their file |
| Thu 29 Oct | T1–T5 rerun on Ubuntu (3 runs each); numbers frozen |
| Sun 1 Nov  | Report draft complete; demo video recorded |
| Tue 3 Nov  | `submission_check.py --final` passes on Ubuntu; final review |
| Wed 4 Nov  | **Submit** (zip or repo URL to "miniProject-Assignment4-submission") |
| Fri 6 Nov  | Demo + viva — arrive 5 min early, with server/browser/tests ready |
