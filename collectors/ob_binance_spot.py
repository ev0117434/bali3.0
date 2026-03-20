#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Spot — order book collector (10 levels).
WS: stream.binance.com:9443  Channel: @depth10@100ms
Redis key: ob:bn:s:{SYMBOL}
Fields: b1..b10 (bid prices), bq1..bq10 (bid qty),
        a1..a10 (ask prices), aq1..aq10 (ask qty), t (local ms)

Protocol notes:
- Streams embedded in URL (combined stream), no subscribe message needed
- Response: data.bids / data.asks = [[price, qty], ...] up to 10 levels
- Binance sends snapshot every 100ms — no local book state needed
- CHUNK_SIZE=200 to keep URL length reasonable
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from ob_hist_writer import OBHistWriter

SCRIPT = "ob_binance_spot"
EX, MKT = "bn", "s"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/binance/binance_spot.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_BASE         = "wss://stream.binance.com:9443/stream?streams="

LEVELS          = 10
CHUNK_SIZE      = 200     # symbols per WS connection
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020   # 20 ms
STATS_INTERVAL  = 30
RECONNECT_DELAY = 3


# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── helpers ───────────────────────────────────────────────────────────────────

def load_symbols() -> list[str]:
    return [s for s in SUBSCRIBE_FILE.read_text().strip().splitlines() if s]


def chunk(lst: list, n: int) -> list[list]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def ws_url(syms: list[str]) -> str:
    return WS_BASE + "/".join(f"{s.lower()}@depth10@100ms" for s in syms)


def redis_key(sym: str) -> bytes:
    return f"ob:{EX}:{MKT}:{sym}".encode()


def build_mapping(bids: list, asks: list, ts_ms: bytes) -> dict:
    """Build HSET mapping from list of [price, qty] entries."""
    m = {b"t": ts_ms}
    for i, row in enumerate(bids[:LEVELS], 1):
        m[f"b{i}".encode()]  = str(row[0]).encode()
        m[f"bq{i}".encode()] = str(row[1]).encode()
    for i, row in enumerate(asks[:LEVELS], 1):
        m[f"a{i}".encode()]  = str(row[0]).encode()
        m[f"aq{i}".encode()] = str(row[1]).encode()
    return m


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush(r: aioredis.Redis, batch: list, hw: OBHistWriter, counters: dict) -> None:
    t0 = time.monotonic()
    now_sec = time.time()
    async with r.pipeline(transaction=False) as pipe:
        for key, mapping in batch:
            pipe.hset(key, mapping=mapping)
        hw.add_to_pipe(pipe, batch, now_sec)
        hw.ensure_config(pipe, now_sec)
        await pipe.execute()
    counters["flushes"]   += 1
    counters["lat_total"] += (time.monotonic() - t0) * 1000


# ── per-chunk WS connection ───────────────────────────────────────────────────

async def collect_chunk(r: aioredis.Redis, syms: list[str], hw: OBHistWriter, counters: dict) -> None:
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
                t_connected = time.monotonic()
                log("INFO", "connected", chunk_symbols=len(syms), note="streams_embedded_in_url")
                first_msg_logged = False
                async for raw in ws:
                    now = time.monotonic()
                    try:
                        obj  = orjson.loads(raw)
                        data = obj.get("data") or obj
                        bids = data.get("bids", [])
                        asks = data.get("asks", [])
                        # sym from stream field: "btcusdt@depth10@100ms"
                        stream = obj.get("stream", "")
                        sym = stream.split("@")[0].upper() if stream else data.get("s", "")
                        if sym and (bids or asks):
                            ts_ms = str(int(time.time() * 1000)).encode()
                            n_bid = len(bids[:LEVELS])
                            n_ask = len(asks[:LEVELS])
                            counters["levels_total"] += n_bid + n_ask
                            counters["levels_count"] += 2
                            if n_bid < LEVELS or n_ask < LEVELS:
                                counters["shallow"] += 1
                            batch.append((redis_key(sym), build_mapping(bids, asks, ts_ms)))
                            counters["msgs"] += 1
                            if not first_msg_logged:
                                log("INFO", "first_message",
                                    chunk_symbols=len(syms),
                                    ms_since_connected=round((now - t_connected) * 1000, 1),
                                    first_sym=sym)
                                first_msg_logged = True
                    except Exception:
                        pass
                    if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= BATCH_TIMEOUT):
                        await flush(r, batch, hw, counters)
                        batch.clear()
                        last_flush = now

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            if batch:
                try:
                    await flush(r, batch, hw, counters)
                except Exception:
                    pass

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


# ── stats ─────────────────────────────────────────────────────────────────────

async def stats_loop(counters: dict) -> None:
    prev_msgs = 0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs    = counters["msgs"]
        rate    = (msgs - prev_msgs) / STATS_INTERVAL
        avg_lat = counters["lat_total"] / counters["flushes"] if counters["flushes"] else 0.0
        avg_lvl = (counters["levels_total"] / counters["levels_count"]
                   if counters["levels_count"] else 0.0)
        log("INFO", "stats",
            msgs_total       = msgs,
            msgs_per_sec     = round(rate, 1),
            flushes_total    = counters["flushes"],
            avg_pipeline_ms  = round(avg_lat, 3),
            avg_levels       = round(avg_lvl, 2),
            shallow_warnings = counters["shallow"],
        )
        prev_msgs = msgs


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {
        "msgs":         0,
        "flushes":      0,
        "lat_total":    0.0,
        "levels_total": 0,
        "levels_count": 0,
        "shallow":      0,
    }
    chunks = chunk(syms, CHUNK_SIZE)
    log("INFO", "subscribing", total_symbols=len(syms), connections=len(chunks))

    hw = OBHistWriter(EX, MKT, syms)
    tasks = [asyncio.create_task(collect_chunk(r, ch, hw, counters)) for ch in chunks]
    tasks.append(asyncio.create_task(stats_loop(counters)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
