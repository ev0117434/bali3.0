#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
runner_monitors.py — запускает мониторы в одном asyncio event loop.
  staleness_monitor, redis_monitor, telegram_alert, spread_monitor

Запускается как subprocess из run_all.py, pinned на cores 26-27.

Аргументы:
  --delay N     задержка перед стартом spread_monitor (секунд)
  --buckets     включить age-bucket отчёт в staleness_monitor
"""

import argparse
import asyncio
import importlib
import sys
import time
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).parent))

SCRIPT  = "runner_monitors"
LOG_DIR = Path(__file__).parent.parent / "logs"


def _log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


async def _run(name: str, *args) -> None:
    mod = importlib.import_module(name)
    _log("INFO", "collector_started", collector=name)
    try:
        await mod.main(*args)
    except Exception as e:
        _log("ERROR", "collector_crashed", collector=name, reason=str(e)[:120])


async def _run_spread_delayed(delay: float) -> None:
    if delay > 0:
        _log("INFO", "spread_monitor_waiting", delay_sec=delay,
             note="signals and snapshots will start after delay")
        await asyncio.sleep(delay)
    spread = importlib.import_module("spread_monitor")
    _log("INFO", "collector_started", collector="spread_monitor")
    try:
        await spread.main()
    except Exception as e:
        _log("ERROR", "collector_crashed", collector="spread_monitor", reason=str(e)[:120])


async def main(spread_delay: float = 0.0, with_buckets: bool = False) -> None:
    _log("INFO", "group_start", group=SCRIPT,
         spread_delay=spread_delay, buckets=with_buckets)

    staleness   = importlib.import_module("staleness_monitor")
    redis_mon   = importlib.import_module("redis_monitor")
    telegram    = importlib.import_module("telegram_alert")

    await asyncio.gather(
        staleness.main(with_buckets),
        redis_mon.main(),
        telegram.main(LOG_DIR),
        _run_spread_delayed(spread_delay),
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--delay",   type=float, default=0.0)
    parser.add_argument("--buckets", action="store_true")
    args = parser.parse_args()
    asyncio.run(main(args.delay, args.buckets))
