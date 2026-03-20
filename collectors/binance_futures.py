#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Futures (USD-M) — market data collector.
WS: fstream.binance.com  Channel: @bookTicker
Redis key: md:bn:f:{SYMBOL}  Fields: b (bid), a (ask), t (exchange transaction time ms)
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

SCRIPT = "binance_futures"
EX, MKT = "bn", "f"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/binance/binance_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_BASE         = "wss://fstream.binance.com/stream?streams="

CHUNK_SIZE      = 300
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020
STATS_INTERVAL  = 30
RECONNECT_DELAY = 3


# ── logger ──────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── helpers ──────────────────────────────────────────────────────────────────

def load_symbols() -> list[str]:
    return [s for s in SUBSCRIBE_FILE.read_text().strip().splitlines() if s]


def chunk(lst: list, n: int) -> list[list]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def ws_url(syms: list[str]) -> str:
    return WS_BASE + "/".join(f"{s.lower()}@bookTicker" for s in syms)


def redis_key(sym: str) -> bytes:
    return f"md:{EX}:{MKT}:{sym}".encode()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush(r: aioredis.Redis, batch: list, counters: dict) -> None:
    t0 = time.monotonic()
    async with r.pipeline(transaction=False) as pipe:
        for key, mapping in batch:
            pipe.hset(key, mapping=mapping)
        await pipe.execute()
    lat_ms = (time.monotonic() - t0) * 1000
    counters["flushes"] += 1
    counters["lat_total"] += lat_ms


# ── per-chunk WS connection ───────────────────────────────────────────────────

async def collect_chunk(r: aioredis.Redis, syms: list[str], counters: dict) -> None:
    url = ws_url(syms)
    while True:
        batch: list = []
        last_flush = time.monotonic()
        try:
            log("INFO", "connecting", chunk_symbols=len(syms))
            async with websockets.connect(
                url,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=4096,
            ) as ws:
                log("INFO", "connected", chunk_symbols=len(syms))
                async for raw in ws:
                    now = time.monotonic()
                    try:
                        obj = orjson.loads(raw)
                        data = obj.get("data") or obj
                        sym = data.get("s")
                        bid = data.get("b")
                        ask = data.get("a")
                        if sym and bid and ask:
                            # T = transaction time from exchange; fall back to local
                            exch_ts = data.get("T")
                            ts_ms = str(exch_ts if exch_ts else int(time.time() * 1000)).encode()
                            batch.append((
                                redis_key(sym),
                                {b"b": bid.encode(), b"a": ask.encode(), b"t": ts_ms},
                            ))
                            counters["msgs"] += 1
                    except Exception:
                        pass
                    if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= BATCH_TIMEOUT):
                        await flush(r, batch, counters)
                        batch.clear()
                        last_flush = now

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            if batch:
                try:
                    await flush(r, batch, counters)
                except Exception:
                    pass

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


# ── stats ─────────────────────────────────────────────────────────────────────

async def stats_loop(counters: dict) -> None:
    prev_msgs = 0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs = counters["msgs"]
        rate = (msgs - prev_msgs) / STATS_INTERVAL
        avg_lat = counters["lat_total"] / counters["flushes"] if counters["flushes"] else 0.0
        log("INFO", "stats",
            msgs_total=msgs,
            msgs_per_sec=round(rate, 1),
            flushes_total=counters["flushes"],
            avg_pipeline_ms=round(avg_lat, 3))
        prev_msgs = msgs


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    chunks = chunk(syms, CHUNK_SIZE)
    log("INFO", "subscribing", total_symbols=len(syms), connections=len(chunks))

    tasks = [asyncio.create_task(collect_chunk(r, ch, counters)) for ch in chunks]
    tasks.append(asyncio.create_task(stats_loop(counters)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
