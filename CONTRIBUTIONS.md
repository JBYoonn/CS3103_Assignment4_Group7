# Team Contributions

Repository: https://github.com/JBYoonn/CS3103_Assignment4_Group7 (git history is supporting evidence for this statement).

| Member | Main responsibilities | Experiments/report sections | Agreed contribution % |
|---|---|---|---:|
| Yoon Jia Bao | **Server implementation** in `solution_server.py`: the `Client` class (bounded per-client pending queue as an ordered dictionary) and the `LiveRecallServer` class (history deque, segment state, active segment), including the revision-ordering helper and `shrink_send_buffer` so newer revisions replace queued ones instead of being locked into the socket buffer. **Server testing and further optimisation** to handle client needs (e.g. server history reduced from 2 min to 90 s; branch `jb/optimise`). Repository owner; reviewed and merged the client and server pull requests. Set up the report and its table of contents; booked the demo slot. **Demo video.** | Ran the public stress tests with `run_demo.py` (100 and 200 clients, 3 runs, median; slow-client test). Report: Public Experiments, Optimisation Comparison, Results | _TODO_ |
| Ian Chin | **Server completion** in `solution_server.py`, building on Jia Bao's classes: `HISTORY` caption recovery (requests capped at 60 s; history bounded at 90 s / 2,000 finals), revision tracking, slow-client handling (pending provisionals replaced by newer revisions; queue overflow closes only that client; ping-based lag signal `since_ping`). Server regression tests (`tests/test_server.py`) and `SERVER_DESIGN.md`. **Final `README.md`.** | Single-client ordering check; diagnostic T1 (20 clients) and T3 (100 clients, 10% slow) runs. Report: Revision/Recovery Semantics, Backpressure/Overload Policy | _TODO_ |
| Wilson Soon | **Client integration**: early full-project draft (`wilson_draft1`); client v2 (pull request #2), built on Nicole's client with improvements (Syncing state, Retry now, recall scroll-position fix, restart detection after pruning, late-join history not tagged "recovered", 25 unit tests). `progress_log.md`: requirement tracking, notes for the server team, server + client test checklist. **`AI_USAGE.md`** and **final submission package**. | Report: Failures and Trade-offs, Limitations, Future Work / FYP Direction | _TODO_ |
| Nicole Teh May Xin | **Browser client** (`client/`), the baseline merged into `main`: caption store with in-place revision replacement, protection against stale revisions and duplicate finals, bounded 90 s history, lecture clock from `source_ts`; "What did I miss?" (30 s default, 15/30/60 s, local data only); automatic reconnect with status; resume from latest final; `HISTORY` merge; lecture-restart handling; screen-reader announcements; unit tests and mock server; `client/README.md`. **`CONTRIBUTIONS.md`.** | Browser end-to-end checks (live captions, recall, network drop, late join, restart). Report: Architecture, Protocol | _TODO_ |

Total contribution should be 100%.

## Shared / integration work

- **23 Oct meeting (all):** agreed the architecture: server history of finals, per-client send queues that replace redundant revisions, 90 s client history. Split work into server (Jia Bao, Ian) and client (Wilson, Nicole).
- **28 Oct meeting (all):** reviewed and merged the client pull request (Wilson's v2 on Nicole's client) and the server pull requests (Jia Bao's branch first, then Ian's, which builds on it). Ran the three components together (server, client web server, `caption_source.py`) and confirmed live revision replacement in the browser. Divided the report and documentation.
- **Results at integration:** 100% final completeness at 100 and 200 clients; slow-client P95 ≈ 9.8 s _(Jia Bao: update with final medians)_.
- **Review process:** all code and documentation merged into `main` through GitHub pull requests reviewed by another member.
- **AI assistance:** declared in `AI_USAGE.md`.
