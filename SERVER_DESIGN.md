# Server behavior and validation

Run `python solution_server.py --port 8765` after installing `requirements.txt`.

The first valid source or client hello selects the session for the process.
Other sessions and a second simultaneous source are rejected with code 1008.
Restart the server for a new lecture. A reconnecting source must continue its
segment numbering. As in the supplied source, segments finish in increasing
order; a finalized-segment watermark rejects older segments even after recent
revision entries are evicted.

Each client has a bounded map of pending segments and a separate sender task.
New revisions replace queued provisional revisions. Queue overflow closes only
that client with code 1013. The initial finalized HISTORY snapshot and live
registration happen without yielding, with the current provisional queued next.

History requests are capped at 60 seconds. Server history is bounded by 90
seconds and 2000 finalized captions. Recovery cannot restore captions outside
these bounds, and an arbitrarily slow client is not guaranteed every final.

Pings limit transport traffic in flight and provide a lag signal. A pong is not
an acknowledgement that captions were processed or displayed. The slow-reader
policy still needs evaluation with T3; do not describe it as guaranteeing fresh
captions for slow readers.

Run the regression checks with:

```powershell
python -m unittest discover -s tests -v
python submission_check.py
```

Use the unchanged public tests in `PUBLIC_TESTS.md` for performance. Report three
independent runs and their median; use Ubuntu for T5 network impairment.

The browser in `client/` remains the starter. Revision replacement, HISTORY
merging, finalized resume state, automatic reconnect, and local 15/30/60-second
recall are the next application work.

## Local diagnostic results (27 October 2026)

Validated on Windows with Python 3.13 and websockets 16.1.1, installed in a
temporary directory. Nine regression tests and `submission_check.py` passed.
The following used unchanged public measurement scripts and one independent
run each, so they are diagnostics rather than the required three-run medians.

| Experiment | Results |
| --- | --- |
| T1: 20 clients, 20 seconds | 440/440 finals; 1320/1320 revisions; P95 age 3.40 ms |
| T3: 100 clients, 60 seconds, 10% slow at 500 ms/read | 7470/7600 finals (98.29%); normal-client P95 age 8.31 ms; slow-client P95 age 10676.35 ms |

Both experiments had zero duplicate/stale deliveries or connection errors.
In T3, all 90 normal clients received every revision and final. Slow clients
had incomplete delivery at the deadline; the test does not establish permanent
loss, because pending captions are not drained beyond the experiment deadline.
This confirms isolation but leaves slow-reader freshness as an optimization task.

Experiment commands (use separate output stems for subsequent runs):

```powershell
python run_demo.py --clients 20 --duration 20 --runs 1 --output-stem implemented_t1
python run_demo.py --clients 100 --duration 60 --runs 1 --slow-percent 10 --slow-ms 500 --output-stem implemented_t3
```
