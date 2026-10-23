# Group 7 — LiveRecall

*"Never lose the lecture because you missed the last sentence."*

LiveRecall relays a live, revision-based caption stream from one caption source to many student
browsers over WebSockets. Its server keeps every client correct and fresh when clients are slow,
disconnect, join late, or share a constrained network.

```
caption_source.py ──WS──▶ solution_server.py ──WS──▶ browser clients / load_clients.py
                           state · bounded history · per-client outbox · lag-based flow control
```

## Team members

| Name | Student ID | Responsibility |
|---|---|---|
| _to fill_ | | |
| _to fill_ | | |
| _to fill_ | | |
| _to fill_ | | |

## Ubuntu setup (fresh Ubuntu 22.04/24.04)

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip iproute2 git
git clone https://github.com/JBYoonn/CS3103_Assignment4_Group7.git
cd CS3103_Assignment4_Group7
```

On macOS/Windows, use an Ubuntu VM (e.g. `multipass launch --name liverecall --cpus 2 --memory 4G --disk 15G`),
as described in `UBUNTU_SETUP.md`.

## Install

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt          # only `websockets`; nothing else is needed to run the server
```

## Required server

```bash
python solution_server.py --port 8765
```

The server listens on `0.0.0.0:<port>`, starts in well under a second, and needs no input. It prints one
status line every 10 s (clients, how many are rate-limited, backlog, coalesced/batched counts, history size).
Optional flags, for experiments only: `--profile basic|noflow|noticks`, `--stats-every 0`, `--verbose`.

## Browser client

```bash
python -m http.server 8000 --directory client
```

Open <http://127.0.0.1:8000> in two or more tabs. The page connects to `ws://<page host>:8765`; to use
another server add `?server=ws://HOST:PORT`. If you open the page from another machine (for example, the
browser on your Mac while the server runs in the VM), use the VM's IP for both the page and `?server=`.

## Public deterministic tests (T1–T5)

Each test is run with **3 independent runs**; `run_demo.py` restarts the server for every run and prints the
median. To run everything (≈30 min; results go to `results/<os>/`):

```bash
./experiments/run_public_tests.sh                  # T1-T4, extra stress tests, and before/after
./experiments/run_public_tests.sh t3 t3_basic      # a subset
```

Individual commands (identical to the assignment's):

```bash
python run_demo.py --clients 20  --duration 20 --runs 3 --output-stem results/t1                                  # T1
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem results/t2                                  # T2
python run_demo.py --clients 100 --duration 60 --runs 3 --slow-percent 10 --slow-ms 500 --output-stem results/t3  # T3
python run_demo.py --clients 100 --duration 60 --runs 3 --disconnect-percent 20 --disconnect-at 15 \
                   --disconnect-seconds 5 --output-stem results/t4                                                # T4
```

**T5 (network impairment, Ubuntu only).** Run T2 under each `tc/netem` profile, and always reset afterwards:

```bash
./experiments/run_netem_tests.sh                   # asks for sudo for tc; delay, loss, 2 Mbit/s, overload; resets lo
# or by hand:
sudo ./network_profiles/delay_loss.sh lo
python run_demo.py --clients 100 --duration 40 --runs 3 --output-stem results/t5_delay_loss
sudo ./network_profiles/reset.sh lo
```

**Before/after comparison.** Prefix any command with `LIVERECALL_PROFILE=basic` to run the same server with
conventional per-client FIFO queues (no coalescing, no lag detection, no batching). `run_demo.py` passes the
environment through to `solution_server.py`, so the measurement code stays untouched.

## Mandatory deterministic application demo

Three terminals (plus a browser):

```bash
python solution_server.py --port 8765                       # 1: server
python -m http.server 8000 --directory client               # 2: browser files
python caption_source.py --duration 300                     # 3: start this AFTER opening two tabs
```

Then, in the browser:

1. **Two clients, live revisions.** Two tabs show the same stream. A provisional caption is grey italic with a
   `DRAFTING` tag and a caret; each newer revision replaces the line in place; the final version turns solid
   with a brief highlight. No duplicate lines appear.
2. **What did I miss?** Press the button (or `M`). It shows the last 30 s by default; 15 s / 60 s switch the
   window. This uses captions already held in the browser, so it makes no server request.
3. **Disconnect and recover.** Open *Connection details & demo tools* and press *Simulate 8 s network drop*
   (or stop/start the server). The status pill shows `Reconnecting in Ns`; after reconnecting, a toast reports
   how many missed captions were recovered, and they appear tagged `RECOVERED`, in order, with no duplicates.
4. **Late join.** Open a third tab mid-lecture: it immediately shows the last 60 s of finals plus the current
   partial line, then continues live.

`DEMO_GUIDE.md` has the original checklist.

## Unit and protocol tests

```bash
pip install pytest
python -m pytest -q                      # server: state, outbox, flow control, end-to-end protocol
node --test client/tests/*.test.mjs      # browser caption store (Node 18+)
python submission_check.py               # instructor compatibility check
```

## Design notes

Full rationale and measurements: `docs/DESIGN.md`. In brief:

**Revision / deduplication state** (`liverecall/state.py`). One entry per segment. A revision is accepted
only if it is newer than what we hold. A final caption is durable: nothing replaces it. Unknown fields are
dropped, and every accepted update is serialised once and the same bytes are shared by all clients.

**Bounded server history.** A deque of finals, pruned to 90 s by age and capped at 2,000 items. Updates
for segments older than the retained window are rejected. Active provisional segments are capped at 64. Memory
does not grow with lecture length.

**Reconnect / late-join recovery.** `CLIENT_HELLO` → `HISTORY` (finals only, oldest first):
- A **new** client gets the last `history_seconds` (≤ 60).
- A **resuming** client gets every retained final after its `last_segment/last_revision`, so it never
  receives a duplicate and never silently loses a gap.

The snapshot and the client's registration happen with no `await` in between, so no update can fall into the
gap between HISTORY and live. The current partial line follows right after HISTORY.

**Per-client buffering / backpressure** (`liverecall/outbox.py`, `liverecall/server.py`). The source handler
only appends to in-memory outboxes and never awaits a socket, so a slow client delays nobody but itself.
Each client has its own sender task and an outbox that, under pressure, keeps at most one pending entry per
segment; above 64 pending revisions it compacts the same way. Pending finals are capped at 1,024; past that the
client is disconnected and resumes through HISTORY.

**Overload policy** (`liverecall/flow.py`). WebSocket ping/pong gives each client's *lag*: pong time minus the
minimum RTT ever seen, i.e. queueing delay. Above 0.5 s the client is rate-limited:
- Obsolete provisional revisions are coalesced.
- Pending finals are sent as one `HISTORY` catch-up batch.
- Nothing is sent while the backlog is visible.
- The rate is held, then probed up in ×1.25 steps, with exponential backoff after a failed probe (TCP-style).
- Once the client keeps up, it returns to full updates.

Clients that are not overloaded get every revision, which preserves Normal Update Coverage.

**Protocol extensions** (all ignorable by older clients):
- `HISTORY` may also arrive mid-stream with `"catchup": true` (finals only, never duplicates).
- `TICK` is padding sent to rate-limited clients and to every client during its first 15 s. Each caption is
  followed by 3 TICKs, so a slow reader's 16-message receive queue fills, and its lag becomes visible, about
  4 captions deep instead of about 16.
- `SESSION_RESET` is sent when a restarted source begins a new lecture.

## Repository layout

```
solution_server.py         required entry point (CLI: --port)
liverecall/state.py        revision state, bounded history, HISTORY selection
liverecall/outbox.py       per-client outbox: coalescing, catch-up batching, bounds
liverecall/flow.py         ping-based lag detection + rate control
liverecall/server.py       WebSocket wiring, hub, per-client sender tasks
client/                    browser client (index.html, styles.css, app.js, store.js)
tests/                     pytest suite;  client/tests/ node tests
experiments/               scripts that reproduce every number in the report
results/                   JSON/log summaries of our runs (raw CSVs are git-ignored)
docs/DESIGN.md             design rationale, experiments, trade-offs
```

## Optional ASR bonus

Not attempted yet. If we attempt it, its dependencies will live under `bonus_asr/` with their own
requirements file, so the mandatory install above stays as it is.
