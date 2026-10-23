#!/usr/bin/env python3
"""
CS3103 Assignment 4 - LiveRecall server (Group 7).

Required evaluator command:
    python solution_server.py --port 8765

Design (details in README.md and the liverecall/ modules):
  liverecall/state.py   revision state, durable finals, bounded history, HISTORY selection
  liverecall/outbox.py  per-client bounded outbox with semantic coalescing / batching
  liverecall/flow.py    ping-based lag detection + per-client message-rate control
  liverecall/server.py  WebSocket wiring: source -> state -> hub -> per-client sender tasks

Optional, for experiments only (the evaluator uses the defaults):
  --profile basic    conventional per-client FIFO queues (the "before" configuration)
  --profile noflow   coalescing only, no lag detection / pacing / batching
  --profile noticks  full design without TICK padding
  LIVERECALL_PROFILE=basic python run_demo.py ...   same, through the public test runner
"""

from __future__ import annotations

import argparse
import asyncio
import os

from liverecall.server import LiveRecallServer, Profile


async def main_async(args) -> None:
    app = LiveRecallServer(Profile.from_name(args.profile), verbose=args.verbose)
    server = await app.start(args.host, args.port)
    print(f"LiveRecall listening on ws://{args.host}:{args.port} (profile={app.profile.name})", flush=True)
    if args.stats_every > 0:
        asyncio.create_task(app.report_forever(args.stats_every))
    async with server:
        await server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="LiveRecall caption server")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--profile", default=os.environ.get("LIVERECALL_PROFILE", "full"),
                        choices=["full", "basic", "noflow", "noticks"])
    parser.add_argument("--stats-every", type=float, default=10.0,
                        help="seconds between status lines (0 = off)")
    parser.add_argument("--verbose", action="store_true", help="log every client connect/disconnect")
    args = parser.parse_args()
    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
