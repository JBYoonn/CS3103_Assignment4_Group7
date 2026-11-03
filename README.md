# Team 7 - LiveRecall

LiveRecall is Group 7's CS3103 Assignment 4 application for distributing provisional and finalised captions through a WebSocket server. This guide documents the implementation available in this repository and the commands used to evaluate it.

## Team members

- Yoon Jia Bao (A0307920J)
- Soon Fu Hong Wilson (A0273596M)
- Nicole Teh May Xin (A0342054N)
- Chin Shang Zer Ian (A0307824A)

Individual responsibilities and contribution percentages are documented in [CONTRIBUTIONS.md](CONTRIBUTIONS.md).

## Ubuntu setup

Use Ubuntu with Python 3.10 or later. On a fresh Ubuntu installation, install the mandatory system packages:

```bash
sudo apt update
sudo apt install -y git python3 python3-venv python3-pip iproute2
python3 --version
```

Obtain the project and enter its root directory:

```bash
git clone https://github.com/JBYoonn/CS3103_Assignment4_Group7.git
cd CS3103_Assignment4_Group7
```

If using the supplied project copy, enter that directory instead of cloning again. Run all commands below from the repository root. T5 requires Ubuntu/Linux `tc/netem` and permission to configure the loopback interface; run the server and test clients in the same Ubuntu environment. No ASR packages or frontend build tools are required.

For WSL, clone inside Ubuntu's Linux filesystem. The command above preserves Unix line endings; Windows checkouts with CRLF-encoded shell scripts cannot run the supplied network profiles directly.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

The mandatory Python dependency is `websockets>=13,<17`. In each additional terminal, enter the repository root and run `source venv/bin/activate` before using Python commands. The browser requires JavaScript to be enabled.

## Required server

```bash
python solution_server.py --port 8765
```

This starts the caption WebSocket server on all interfaces, port **8765**. Successful startup prints:

```text
LiveRecall server listening on ws://0.0.0.0:8765
```

Local clients connect to **`ws://127.0.0.1:8765`**. This port does not serve the browser webpage. Stop the server with `Ctrl+C`.

The source and browser use session `L1`. One session and one active source are supported per server process. A new source for that session takes over immediately; the previous source is closed with code `1012`. Takeover preserves revision and history state, so a reconnecting source must continue segment numbering. Restart the server before repeating the supplied source demonstration: `caption_source.py` starts at segment 1, while the server rejects updates for already finalised segments. Restarting clears in-memory history.

Adaptive slow-reader resets are enabled by default. For a manual comparison, start the server with:

```bash
python solution_server.py --port 8765 --no-reset-slow
```

This disables proactive slow-reader resets, not queue-overflow disconnections or finals-only scheduling. `run_demo.py` starts its own default server and does not forward this option; the comparison requires manually running the server, source, and load clients.

## Browser client

1. Start the WebSocket server using the command above.
2. In a second terminal, serve the browser files over HTTP:

   ```bash
   python -m http.server 8000 --directory client
   ```

3. Open **`http://127.0.0.1:8000/index.html`**, which serves `client/index.html`. The directory URL `http://127.0.0.1:8000/` also works. HTTP serving is the recommended launch method; no build step is needed.
4. Confirm that the status changes from **Connecting…** through **Syncing…** to **Live**. `client/app.js` opens a separate WebSocket connection and sends `CLIENT_HELLO` for session `L1`. The client becomes Live after merging the server's initial `HISTORY` response, which may be empty.

HTTP port **8000** serves the page; WebSocket port **8765** carries captions. By default, the browser connects to `ws://<page-host>:8765`, using `127.0.0.1` when opened through `localhost`. To open the client on another computer, use the server computer's address in the HTTP URL. The WebSocket address and session can also be overridden without changing code, for example:

```text
http://127.0.0.1:8000/index.html?server=ws://127.0.0.1:8765&session=L1
```

The live view shows one caption per segment, revises its text, and distinguishes provisional and final text visually. **What did I miss?** opens local finalised captions for the last 30 seconds, with 15/30/60-second tabs. **Connection details & demo tools** shows the resume point and recovery counters and provides a simulated network-drop button.

## Public deterministic tests

The following commands reproduce [PUBLIC_TESTS.md](PUBLIC_TESTS.md). Stop any manually started server on port 8765 and close browser clients before running them. `run_demo.py` launches its own server, simulated clients, and deterministic source, then analyses the results. **`--runs 3` performs three independent runs, restarting the server for each run, and prints medians of the main metrics.**

| Test                    | Configuration and purpose                                                             |
| ----------------------- | ------------------------------------------------------------------------------------- |
| T1 - Basic correctness  | 20 clients, 20 seconds; final completeness, revision coverage, duplicate/stale state. |
| T2 - Public scale       | 100 clients, 40 seconds; caption age and freshness at scale.                          |
| T3 - Slow clients       | 100 clients, 60 seconds; 10% delay processing by 500 ms per live update.              |
| T4 - Reconnection       | 100 clients, 60 seconds; 20% disconnect at 15 seconds for 5 seconds.                  |
| T5 - Network impairment | T2 configuration with delay, jitter, and packet loss on `lo`.                         |

Run T1-T4 without a network impairment profile applied:

```bash
# T1
python run_demo.py --clients 20 --duration 20 --runs 3 --output-stem t1

# T2
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t2

# T3
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --slow-percent 10 --slow-ms 500 --output-stem t3

# T4
python run_demo.py --clients 100 --duration 60 --runs 3 \
  --disconnect-percent 20 --disconnect-at 15 --disconnect-seconds 5 \
  --output-stem t4
```

For **T5**, run this Bash block in Ubuntu. It applies `network_profiles/delay_loss.sh` to `lo` and resets the profile when the subshell exits. Using `bash` avoids requiring executable file permissions on the supplied scripts.

```bash
(
  set -e
  trap 'sudo bash network_profiles/reset.sh lo' EXIT
  sudo bash network_profiles/delay_loss.sh lo
  python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem t5
)
```

The profile requires `sudo` and adds 120 ms delay, 30 ms jitter, and 3% packet loss. If cleanup is interrupted, reset manually before other tests:

```bash
sudo bash network_profiles/reset.sh lo
```

All five commands use the default seed `3103`, session `L1`, port `8765`, 30-second history requests, and a 500 ms freshness threshold. The runner selects the source segment count from the test duration. Caption content and client roles are reproducible; timings remain dependent on system and network conditions.

Per-run metrics and final medians appear in the terminal. CSV event logs and JSON summaries are saved beside `run_demo.py`, for example `t1_run01.csv` / `t1_run01.json` through `t1_run03.csv` / `t1_run03.json`. Reusing an output stem overwrites those files; choose a different stem to retain earlier results.

| Metric                                      | Interpretation                                                                                                                                                                                                          |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| P50/P95/P99 caption age                     | Source timestamp to receipt for accepted live updates; lower is better. T3 also separates normal and slow readers.                                                                                                      |
| Fresh Caption / Fresh Final Caption Ratio   | Live updates, or expected unique finals, received within 500 ms. Recovered finals count toward final freshness using their original source timestamp.                                                                   |
| Final completeness / Normal Update Coverage | Unique finals delivered live or recovered, and unique live segment/revision pairs, respectively, divided by expected totals across all simulated client roles. The coverage metric is not restricted to normal readers. |
| Recovery P95                                | Time from an established reconnect/late-join connection to receipt of `HISTORY`; excludes outage and connection setup. Assess alongside completeness and duplicate/stale counts.                                        |

**Verification on 2 November 2026:** commit `1f5dee3`, Ubuntu/WSL2, Python 3.12.3, websockets 16.1.1, Intel Core i9-13900HX. These are three-run medians from this machine, separate from the group's earlier optimisation comparison. All 15 runs reached 100% final completeness with no duplicate/stale deliveries.

| Test | Final completeness | Fresh finals | P95 age (ms) | P99 age (ms) | Connection errors (runs 1/2/3) |
| ---- | -----------------: | -----------: | -----------: | -----------: | ------------------------------ |
| T1   |            100.00% |      100.00% |         4.07 |         4.49 | 0/0/0                          |
| T2   |            100.00% |      100.00% |         9.49 |        11.97 | 0/0/0                          |
| T3   |            100.00% |       96.49% |         8.66 |      4360.45 | 10/10/10                       |
| T4   |            100.00% |       98.42% |         8.36 |        10.68 | 0/0/0                          |
| T5   |            100.00% |       86.90% |       637.96 |      1168.02 | 1/0/0                          |

T3 reset ten slow clients per run; normal-client P95 was 7.77 ms and slow-client P95 was 8558.28 ms. T4 median recovery P95 was 89.88 ms. T5 reset one client in its first run despite no configured slow readers, so no general absence of false detections is claimed. The netem profile was removed after testing. See [verification results and raw-data index](verification_results/2026-11-02_1f5dee3/RESULTS.md) for per-run records and limitations.

For optional RSS observation during a running Ubuntu experiment, use a separate terminal:

```bash
python monitor_server.py --match solution_server.py --duration 60 --output t3_memory.csv
```

The monitor attaches to one server process, so restart it with a different output name for each independent run. Existing server regression and startup checks are also available:

```bash
python -m unittest discover -s tests -v
python submission_check.py
```

The startup check verifies files and a WebSocket hello; it does not validate performance or browser functionality.

The existing regression suite contains an outdated source-rejection expectation: `test_second_source_and_other_session_are_rejected` expects the replacement source to close with code `1008`, whereas the merged server closes the previous source with `1012`. The recorded verification has one timeout and eight passing server tests. The separate takeover check in the linked verification results passed replacement-source delivery and cross-session rejection.

The client state tests require Node.js with the built-in test runner; Node.js is not needed to run the application or T1-T5. With Node.js installed, run:

```bash
node --test client/tests/store.test.mjs
```

These 25 tests cover revision replacement, immutable finals, history merging, resume cursors, local recall, pruning, and lecture-restart detection.

## Mandatory deterministic application demo

Use three terminals in the same repository and activate the virtual environment in each. The source supplies fixed caption text and revision order; actual delivery and reconnect timings depend on the environment.

1. **Start a fresh server in terminal 1.** Activate the virtual environment and run:

   ```bash
   python solution_server.py --port 8765
   ```

   Expect the startup message documented above. Use a fresh server for each repetition of this walkthrough.

2. **Open browser clients.** In terminal 2, run:

   ```bash
   python -m http.server 8000 --directory client
   ```

   Open `http://127.0.0.1:8000/index.html` in two tabs. Both should show **Live** before starting the source.

3. **Start the deterministic caption source in terminal 3.** The longer revision interval makes individual updates easier to observe:

   ```bash
   python caption_source.py --transcript sample_transcript.txt --duration 300 \
     --revision-delay 0.6 --segment-gap 0.6
   ```

   The source cycles the supplied transcript for approximately five minutes and prints its caption messages, followed by `SOURCE_DONE`. It uses session `L1` and `ws://127.0.0.1:8765` by default. Keep it running throughout the remaining steps.

4. **Observe revision replacement.** Watch a grey italic provisional sentence with a blinking drafting cursor. Its text expands as revisions arrive, occupying one line per segment rather than adding duplicate sentences. In developer tools, each caption row has `data-segment` and `data-revision` attributes if the identifiers need to be checked.

5. **Observe finalisation.** The completed sentence changes from grey italic drafting text to solid text with a blue side bar. It remains one caption row and is no longer changed by subsequent revisions of that segment. Both tabs should continue receiving new sentences.

6. **Use local recall.** After at least 65 seconds of streaming, click **What did I miss?**. The panel defaults to **30 s** and lists finalised captions with age labels. Switch between **15 s**, **30 s**, and **60 s**; the panel filters locally stored finals without requesting server history. Before enough context is available, it may indicate a partial window. **M** toggles the panel and **Escape** closes it.

7. **Demonstrate late joining.** Open a new tab at the same URL while captions continue. It requests up to 60 seconds of recent history, displays the available finalised context, and then follows live updates, including the active provisional caption if present. The toast reports **Joined late · loaded N recent captions** when context is available. Initial context is not marked as recovered.

8. **Demonstrate disconnection and automatic reconnect.** In an established tab, expand **Connection details & demo tools** and note the resume point, **Reconnects**, and **Recovered via history** counters. Click **Simulate 8 s network drop**. That tab shows **Offline (simulated)** with a countdown while the other tabs continue updating. Leave **Retry now** untouched so the full interruption occurs. After approximately eight seconds, the client reconnects automatically, synchronises history, and returns to **Live**.

9. **Verify recovered captions.** Expect a **Back online · recovered N missed captions** toast, an increased reconnect count, newly merged captions tagged **recovered**, and an advancing finalised resume point. Captions already held remain single entries. The recovery count should increase if finals were missed during the drop. The WebSocket Messages view can confirm that `CLIENT_HELLO` carries the last finalised cursor and that `HISTORY` supplies newer finals. The separate T4 command above provides CSV/JSON measurements of the reconnection path.

10. **Finish the demo.** Stop the source, WebSocket server, and HTTP server with `Ctrl+C` in their respective terminals. Repeat from a fresh server and newly loaded browser tabs for a clean run. Recovery is limited to available recent history; an arbitrarily long interruption cannot restore the full lecture.

## Design notes

- **Revision/deduplication state:** captions are identified by session and segment, with increasing revisions. The server retains revision/final metadata for up to 4096 segments and rejects stale or duplicate revisions. A finalised-segment watermark prevents later changes to completed segments, assuming increasing segment completion order. The browser store keeps one entry per segment, replaces it only with a newer revision, and preserves held finals. Live and history updates use the same acceptance rules, avoiding duplicate display rows.

- **Bounded server history:** a shared deque holds final captions, pruned by a 90-second server-receipt-time window and a 2000-item limit. Pruning runs on final arrival, history requests, and ten-second statistics ticks. Requests default to 30 seconds and are capped at 60 seconds; superseded provisional revisions are not retained for replay. Separately, the browser prunes its store every two seconds using a 90-second source-based lecture-time window and a 2000-entry safety cap; recall reads only local finals.

- **Reconnect/late-join recovery:** `CLIENT_HELLO` supplies the session, client ID, latest finalised segment/revision, and history window. The browser requests 60 seconds; new tabs start at `(0, 0)`. The server snapshots newer retained finals and registers live delivery without yielding, then queues the current provisional if applicable. The browser merges history through its revision rules and retries closed connections after 0.5/1/2/4/5 seconds, capped at five seconds plus up to 250 ms jitter; a successful history response resets the backoff. **Retry now** bypasses the delay. The simulated test clients instead use 30-second requests and a fixed 400 ms error retry. Recovery cannot restore captions outside the requested/retained window or after server restart. Browser caption/resume state is in memory, so reloading creates a new caption store. See [PROTOCOL_EXAMPLES.md](PROTOCOL_EXAMPLES.md) for message flows.

- **Per-client buffering/backpressure policy:** each client has an independent sender task and ordered pending map, capped at 256 entries including initial history. Provisional revisions coalesce by segment; queued finals are preserved. Pings every four messages gate sending when more than one is outstanding. A reader is marked slow after two consecutive window waits exceeding `max(1 s, 4 × RTT baseline)`, or an unanswered ping exceeding `max(4 s, 8 × RTT baseline)`. The existing one-second ping-lag rule also enables finals-only selection. Slow detection assigns a ten-second finals-only deadline, and recognised reset clients initially receive a 60-second deadline on reconnect. Provisionals remain queued and replaceable. Pongs measure transport progress, not application consumption.

- **Overload behaviour and trade-offs:** adaptive slow detection can proactively close a client with code `1013`, clearing pending work and cancelling its sender; the browser reconnects and resumes through one bounded `HISTORY` response. A separate reset occurs if a new pending entry would exceed 256 entries. Proactive resets have a 60-second per-ID cooldown while the ID remains recorded; up to 10,000 integer/string IDs are remembered with five-minute classification windows. The server requests a 32 KiB socket send buffer and 16 KiB WebSocket write high-water mark, with 20-second keepalive interval/timeout settings. Compression is explicitly disabled. Coalescing can skip provisional revisions, and transport buffers can still delay delivery. Completeness and freshness depend on retained history and successful client progress; entry-count bounds are not a fixed total-memory cap. Two implementation limits remain: a later slow event can shorten the initial 60-second hold to ten seconds, and the RTT baseline starts at one second and can only decrease.
