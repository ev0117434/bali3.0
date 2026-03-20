#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — запускает все 8 WS-коллекторов + staleness_monitor в одном процессе.

Каждый коллектор работает как отдельная asyncio-задача.
Общий лог-поток: stdout в формате JSON (script поле указывает источник).

Запуск:
    python3 collectors/run_all.py
    python3 collectors/run_all.py --buckets     # staleness с корзинами
    python3 collectors/run_all.py 2>&1 | tee logs/collectors.log
"""

import argparse
import asyncio
import importlib
import sys
import time
from pathlib import Path

import orjson

COLLECTORS = [
    "binance_spot",
    "binance_futures",
    "bybit_spot",
    "bybit_futures",
    "okx_spot",
    "okx_futures",
    "gate_spot",
    "gate_futures",
]

# Добавляем папку collectors в путь
sys.path.insert(0, str(Path(__file__).parent))


def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": "run_all", "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


async def run_collector(name: str) -> None:
    mod = importlib.import_module(name)
    log("INFO", "collector_started", collector=name)
    try:
        await mod.main()
    except Exception as e:
        log("ERROR", "collector_crashed", collector=name, reason=str(e))


async def main(with_buckets: bool = False) -> None:
    log("INFO", "startup", collectors=COLLECTORS, staleness_buckets=with_buckets)
    tasks = [asyncio.create_task(run_collector(name)) for name in COLLECTORS]

    # Staleness monitor
    staleness = importlib.import_module("staleness_monitor")
    tasks.append(asyncio.create_task(staleness.main(with_buckets)))
    log("INFO", "collector_started", collector="staleness_monitor")

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log("INFO", "shutdown", msg="KeyboardInterrupt received")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log("INFO", "stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all market data collectors")
    parser.add_argument(
        "--buckets",
        action="store_true",
        help="Enable age-bucket distribution in staleness_monitor",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.buckets))
    except KeyboardInterrupt:
        pass
