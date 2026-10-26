# LiveRecall – Browser Client

Owner: Nicole Teh May Xin. This page explains what each file in `client/` does, how they fit together, and how to run and test them.

## Files at a glance

| File | What it does |
|---|---|
| `index.html` | Page layout: connection status pill, live captions area, "What did I miss?" button and panel (15 / 30 / 60 s tabs), toast messages, and a "Connection details & demo tools" section. |
| `styles.css` | Looks: provisional captions are grey italic, finals are solid with a blue bar, status pill colours, light/dark theme, mobile layout. |
| `store.js` | The caption "brain". Holds the caption state and all the rules. No network code and no screen code, so it can be tested on its own. |
| `app.js` | Connects everything: opens the WebSocket, sends `CLIENT_HELLO`, passes messages to `store.js`, draws the screen, and reconnects automatically. |
| `tests/store.test.mjs` | 16 automated tests that prove the rules in `store.js` work. |
| `tests/mock_server.mjs` | A small fake server for testing the client before `solution_server.py` is finished. It is **not** our real server and is not part of the submission's server. |

## How a message flows

```
caption_source.py ──► solution_server.py ──WebSocket──► app.js ──► store.js (rules)
														   │
														   └──► draws index.html
```

1. `app.js` connects and sends `CLIENT_HELLO` with the resume point from `store.js`.
2. The server replies with `HISTORY` (missed final captions), then live `CAPTION_UPDATE`s.
3. `app.js` gives every message to `store.js`. The store decides whether to accept or ignore it.
4. `app.js` redraws the page from the store's state. It redraws at most once per animation frame, so bursts of updates stay smooth.

## store.js – the rules

The store keeps **one entry per segment**: `segment → { revision, final, text, source_ts, recovered }`.

Every update, live or from `HISTORY`, goes through the same function, `apply()`:

1. If we already hold the **final** for this segment → ignore. Finals are never changed, and duplicate finals after a reconnect are dropped.
2. If the update's **revision is ≤ the one we hold** → ignore. It is stale.
3. Otherwise → **replace** what we hold.

Because there is only this one path, an older revision can never overwrite a newer one, whatever order messages arrive in.

Other jobs:

| Function | What it does |
|---|---|
| `mergeHistory(items)` | Sorts recovered finals oldest → newest and runs each through `apply()`. Ones we already hold are skipped. Returns how many were added. |
| `resumeState()` | Returns `{ last_segment, last_revision }` of the **latest final** we hold (0/0 for a new client). Sent in every `CLIENT_HELLO`. |
| `isRestart(update)` | Detects that the caption source/server restarted: a **lower** segment number with a **newer** timestamp. The store then resets instead of ignoring the new lecture. The timestamp check stops a late final of the previous segment from being mistaken for a restart. |
| `observeClock()` / `lectureNow()` | Tracks "lecture time" from `source_ts`, so recall works even if the browser's clock differs from the server's. |
| `prune()` | Drops captions older than **90 s** (team decision; the brief requires ≥ 60 s). Keeps memory bounded during a long lecture. |
| `recall(seconds)` | Final captions from the last `seconds` of lecture time. Uses local data only; no server request. |
| `viewRows()` | Captions in order, plus a gap marker where segments are missing and could not be recovered. |

## app.js – network and screen

| Part | What it does |
|---|---|
| **Connect** | Opens `ws://127.0.0.1:8765` (change it with `?server=ws://IP:PORT` in the page URL). |
| **CLIENT_HELLO** | Sends `client_id`, `session`, `last_segment`, `last_revision` (from `resumeState()`), and `history_seconds: 60`. |
| **Messages** | `CAPTION_UPDATE` → `store.apply()`. `HISTORY` → `store.mergeHistory()` and a toast like "Back online · recovered 6 missed captions". Unknown message types are ignored. |
| **Reconnect** | If the connection closes, it retries automatically: 0.5 s → 1 → 2 → 4 → 5 s max, with a little random jitter so many clients don't all retry at once. It retries immediately when the browser comes back online. |
| **Status pill** | Live (green), Connecting / Reconnecting (amber, with countdown), Offline (red). |
| **Live view** | One line per segment. A revision updates its line in place. It auto-scrolls only if you're already at the bottom; otherwise a "Jump to latest" button appears. |
| **What did I miss?** | Opens on the last **30 s**, with 15 / 30 / 60 s tabs. One sentence per line with "N s ago". |
| **Accessibility** | Screen readers hear each **final** caption once, not every revision. |
| **Demo tools** | Shows server address, resume point, captions held, reconnect count, recovered and ignored counts. The **Simulate 8 s network drop** button closes the connection to demo reconnect and recovery. |

## Settings (top of each file)

| Setting | File | Value |
|---|---|---|
| `DEFAULT_HISTORY_SECONDS` | `store.js` | 90 |
| `HISTORY_REQUEST_SECONDS` | `app.js` | 60 |
| `RECALL_DEFAULT_SECONDS` | `app.js` | 30 |
| `RECONNECT_MIN_MS` / `RECONNECT_MAX_MS` | `app.js` | 500 / 5000 |

## How to run

From the repo folder, each command in its own terminal.

**With the fake server (tests everything, including recovery):**
```
node client/tests/mock_server.mjs
python -m http.server 8000 --directory client
```

**With the real server:**
```
python solution_server.py
python -m http.server 8000 --directory client
python caption_source.py --duration 300
```

Then open http://localhost:8000. Open the page **before** starting `caption_source.py`.

**Run the tests:**
```
node --test client/tests/store.test.mjs
```
Expected: `pass 16`, `fail 0`.

## Notes for the server team

The client expects:
- After `CLIENT_HELLO`, the server sends `HISTORY` first, with **only final** captions where `segment > last_segment`, within `history_seconds`.
- Then live `CAPTION_UPDATE`s. Ideally the server also sends the latest revision of the segment currently in progress, so a reconnecting student sees it straight away.
- The client is safe if the server skips provisionals for a slow client, sends a final without its provisionals, or repeats something it already sent. `store.js` handles all of these.
