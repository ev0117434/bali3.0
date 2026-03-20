#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Futures (Linear/USDT) — market data collector.
WS: stream.bybit.com/v5/public/linear  Channel: orderbook.1
Redis key: md:bb:f:{SYMBOL}  Fields: b (bid), a (ask), t (exchange ts ms)

Protocol notes:
- Single connection, subscribe in batches of 200 (futures frame limit)
- Custom ping: {"op": "ping"} every 20s
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from hist_writer import HistWriter

SCRIPT = "bybit_futures"
EX, MKT = "bb", "f"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/bybit/bybit_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://stream.bybit.com/v5/public/linear"

SUB_BATCH       = 200     # max args per subscribe message (futures)
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020
PING_INTERVAL   = 20
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


def redis_key(sym: str) -> bytes:
    return f"md:{EX}:{MKT}:{sym}".encode()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush(r: aioredis.Redis, batch: list, hw: HistWriter, counters: dict) -> None:
    t0 = time.monotonic()
    now_sec = time.time()
    async with r.pipeline(transaction=False) as pipe:
        for key, mapping in batch:
            pipe.hset(key, mapping=mapping)
        hw.add_to_pipe(pipe, batch, now_sec)
        hw.ensure_config(pipe, now_sec)
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
            await ws.send(orjson.dumps({"op": "ping"}))
    except Exception:
        pass


# ── recv loop ─────────────────────────────────────────────────────────────────

async def recv_loop(ws, r: aioredis.Redis, hw: HistWriter, counters: dict, stop: asyncio.Event,
                    t_connected: float, first_logged: list) -> None:
    batch: list = []
    last_flush = time.monotonic()
    try:
        async for raw in ws:
            if stop.is_set():
                break
            now = time.monotonic()
            try:
                msg = orjson.loads(raw)
                topic = msg.get("topic", "")
                if not topic.startswith("orderbook.1."):
                    continue
                data = msg.get("data", {})
                sym = data.get("s")
                if not sym:
                    continue
                ts_ms = str(msg.get("ts", int(time.time() * 1000))).encode()
                fields = {b"t": ts_ms}
                bids = data.get("b", [])
                asks = data.get("a", [])
                if bids:
                    fields[b"b"] = bids[0][0].encode()
                if asks:
                    fields[b"a"] = asks[0][0].encode()
                if len(fields) > 1:
                    batch.append((redis_key(sym), fields))
                    counters["msgs"] += 1
                    if not first_logged[0]:
                        log("INFO", "first_message",
                            ms_since_connected=round((now - t_connected) * 1000, 1),
                            first_sym=sym)
                        first_logged[0] = True
            except Exception:
                pass
            if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= BATCH_TIMEOUT):
                await flush(r, batch, hw, counters)
                batch.clear()
                last_flush = now
    except (ConnectionClosed, asyncio.CancelledError):
        pass
    except Exception as e:
        log("WARN", "recv_error", reason=str(e)[:120])
    finally:
        if batch:
            try:
                await flush(r, batch, hw, counters)
            except Exception:
                pass


# ── main connection loop ──────────────────────────────────────────────────────

async def collect(r: aioredis.Redis, syms: list[str], hw: HistWriter, counters: dict) -> None:
    batches = chunk(syms, SUB_BATCH)
    while True:
        stop = asyncio.Event()
        try:
            log("INFO", "connecting", total_symbols=len(syms))
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_queue=4096,
            ) as ws:
                t_connected = time.monotonic()
                log("INFO", "connected")
                t_sub = time.monotonic()
                for batch in batches:
                    args = [f"orderbook.1.{s}" for s in batch]
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.02)
                log("INFO", "subscribed", sub_batches=len(batches), total_symbols=len(syms),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                first_logged = [False]
                recv_task = asyncio.create_task(recv_loop(ws, r, hw, counters, stop, t_connected, first_logged))
                ping_task = asyncio.create_task(ping_loop(ws, stop))
                done, pending = await asyncio.wait(
                    [recv_task, ping_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                stop.set()
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            stop.set()

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
    hw = HistWriter(EX, MKT, syms)
    await asyncio.gather(
        collect(r, syms, hw, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
