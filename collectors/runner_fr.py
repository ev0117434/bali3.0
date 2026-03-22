#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
runner_fr.py — запускает 5 FR-коллекторов в одном asyncio event loop.
Запускается как subprocess из run_all.py, pinned на cores 24-25.
"""

import asyncio
import importlib
import sys
import time
from pathlib import Path

import orjson

sys.path.insert(0, str(Path(__file__).parent))

SCRIPT = "runner_fr"

COLLECTORS = [
    "fr_binance_futures",
    "fr_bybit_futures",
    "fr_okx_futures",
    "fr_gate_futures",
    "fr_bitget_futures",
]


def _log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


async def _run(name: str) -> None:
    mod = importlib.import_module(name)
    _log("INFO", "collector_started", collector=name)
    try:
        await mod.main()
    except Exception as e:
        _log("ERROR", "collector_crashed", collector=name, reason=str(e)[:120])


async def main() -> None:
    _log("INFO", "group_start", group=SCRIPT, collectors=COLLECTORS)
    tasks = []
    for name in COLLECTORS:
        tasks.append(asyncio.create_task(_run(name)))
        await asyncio.sleep(1)
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
