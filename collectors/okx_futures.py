#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX Futures (USDT Perpetual Swaps) — market data collector.
WS: ws.okx.com:8443/ws/v5/public  Channel: tickers
Redis key: md:ok:f:{SYMBOL}  Fields: b (bid), a (ask), t (exchange ts ms)

Protocol notes:
- Subscribe file has BTCUSDT; OKX requires BTC-USDT-SWAP (native format)
- Multiple connections, 300 instId per connection, 0.15s stagger between opens
- Custom ping: text "ping", response "pong" every 25s
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

SCRIPT = "okx_futures"
EX, MKT = "ok", "f"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/okx/okx_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://ws.okx.com:8443/ws/v5/public"

CHUNK_SIZE      = 300
CONNECT_DELAY   = 0.15
SUB_BATCH       = 20      # args per subscribe message (OKX frame limit ~4KB)
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020
PING_INTERVAL   = 25
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


def to_native(sym: str) -> str:
    """BTCUSDT → BTC-USDT-SWAP"""
    if sym.endswith("USDT"):
        return sym[:-4] + "-USDT-SWAP"
    if sym.endswith("USDC"):
        return sym[:-4] + "-USDC-SWAP"
    return sym


def normalize(inst_id: str) -> str:
    """BTC-USDT-SWAP → BTCUSDT"""
    s = inst_id
    if s.endswith("-SWAP"):
        s = s[:-5]
    return s.replace("-", "")


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


# ── ping loop ─────────────────────────────────────────────────────────────────

async def ping_loop(ws, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop.is_set():
                break
            await ws.send("ping")
    except Exception:
        pass


# ── per-chunk connection ──────────────────────────────────────────────────────

async def collect_chunk(
    r: aioredis.Redis,
    inst_ids: list[str],    # native OKX format: BTC-USDT-SWAP
    counters: dict,
    delay: float,
) -> None:
    if delay:
        await asyncio.sleep(delay)

    while True:
        stop = asyncio.Event()
        batch: list = []
        last_flush = time.monotonic()

        try:
            log("INFO", "connecting", chunk_symbols=len(inst_ids))
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_queue=4096,
            ) as ws:
                log("INFO", "connected", chunk_symbols=len(inst_ids))
                sub_batches = chunk(inst_ids, SUB_BATCH)
                for sb in sub_batches:
                    args = [{"channel": "tickers", "instId": iid} for iid in sb]
                    # OKX requires text frames (str), not binary (bytes)
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}).decode())
                    await asyncio.sleep(0.05)
                log("INFO", "subscribed", chunk_symbols=len(inst_ids), sub_batches=len(sub_batches))

                ping_task = asyncio.create_task(ping_loop(ws, stop))

                async for raw in ws:
                    if raw == "pong":
                        continue
                    now = time.monotonic()
                    try:
                        msg = orjson.loads(raw)
                        data_list = msg.get("data")
                        if not data_list:
                            continue
                        for item in data_list:
                            inst_id = item.get("instId", "")
                            bid = item.get("bidPx", "")
                            ask = item.get("askPx", "")
                            ts_str = item.get("ts", "")
                            if inst_id and bid and ask:
                                sym = normalize(inst_id)
                                ts_ms = ts_str.encode() if ts_str else str(int(time.time() * 1000)).encode()
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

                stop.set()
                ping_task.cancel()
                await asyncio.gather(ping_task, return_exceptions=True)

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            stop.set()
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

    native = [to_native(s) for s in syms]
    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    chunks = chunk(native, CHUNK_SIZE)
    log("INFO", "subscribing", total_symbols=len(syms), connections=len(chunks))

    tasks = [
        asyncio.create_task(collect_chunk(r, ch, counters, i * CONNECT_DELAY))
        for i, ch in enumerate(chunks)
    ]
    tasks.append(asyncio.create_task(stats_loop(counters)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
