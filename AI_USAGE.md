# AI / Coding-Assistant Usage

> Draft from Wilson's session. **Every member must add the tools they used themselves** and check that
> the statements below match what they personally did before submission.

## Tools used

| Member | Tool | Used for |
|---|---|---|
| Wilson | Claude Code (Anthropic, Claude Opus) | Reading the starter kit and harness; designing and implementing `liverecall/` (state, outbox, flow control, server); pytest and node tests; browser client; experiment scripts; README/DESIGN drafts. Every design step was checked against measured runs of the public harness. |
| _member_ | | |
| _member_ | | |
| _member_ | | |

## One useful suggestion

**Use WebSocket ping/pong as a per-client "how far behind is the reader" signal.** The server cannot see when
a browser has *read* a message, but a pong is only produced after the client's WebSocket stack has read every
byte sent before the ping. Subtracting the smallest RTT seen on that connection turns pong time into queueing
delay, so a long but healthy path (netem delay) is not mistaken for a slow client.

*How we verified it:* T3 (10% slow readers at 500 ms). The naive server's slow clients reached P95 = 28 s
caption age and final completeness was 95.1% (below the 99% correctness gate). With lag detection and the
overload policy built on it, completeness is 100% and slow-client P95 is about 2 s, while normal clients
stay at about 30 ms P95. Numbers are in `docs/DESIGN.md`; reproduce them with
`./experiments/run_public_tests.sh t3 t3_basic`.

## Suggestions that were wrong, inefficient, or required modification

1. **"Coalesce every pending provisional revision as soon as a newer one arrives."**
   The first outbox replaced a pending revision whenever a newer one for the same segment arrived. Our own
   end-to-end test (`test_client_gets_history_first_then_live_revisions`) failed: when the source sends
   revisions back-to-back, the fast client's sender task has not run yet, so revisions 1 and 2 were silently
   merged into 3. That would cut **Normal Update Coverage** under a bursty source even with no overload.
   *Fix:* coalesce only under real pressure: the sender is blocked on TCP, the client is rate-limited, or
   more than 64 entries are pending (`ClientOutbox.push(..., coalesce=...)`).

2. **Textbook AIMD (halve the rate, then add a little each second).**
   Measured in T3, slow clients stayed **7–8 s behind for the whole run**. Reading the `websockets` client
   source explained why: it buffers about 16 messages before it stops reading from TCP, so pongs only get
   late once a client is already about 16 messages behind. Halving landed the rate exactly on the reader's
   capacity, so that hidden backlog never drained, and every additive probe rebuilt it unseen.
   *Fix (`liverecall/flow.py`):* a deep first cut (¼ of the send rate); **send nothing while the backlog is
   visible**, so pending finals leave as one batch; hold the rate for as long as recovery took; probe up only
   in ×1.25 steps, with exponential backoff after a failed probe.

3. **"Split each message into 4–8 WebSocket fragments so the client's 16-frame queue fills sooner."**
   This made the lag visible earlier and improved slow-client freshness, but with 8 fragments it **crashed
   the instructor's `load_clients.py`**:
   `AssertionError: cannot reset() while queue isn't empty` in `websockets/asyncio/messages.py`. When
   `load_clients.py`'s `wait_for(ws.recv(), 1 s)` times out mid-way through a fragmented message, the library
   hits an internal assertion. A crashed evaluator run would score zero, so we **rejected fragmentation
   entirely**. *Replacement:* separate tiny `TICK` messages, sent only to rate-limited clients and during each
   client's first 15 s. They fill the same queue, but each one is a complete message, so the race cannot happen.

## Responsibility statement

We understand that the team remains responsible for the submitted code, experimental results, and claims, and
that every member may be asked to explain or modify relevant code during the viva.
