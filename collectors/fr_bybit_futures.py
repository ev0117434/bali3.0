#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Futures (Linear/USDT) — funding rate collector.

WS: stream.bybit.com/v5/public/linear  Channel: tickers
    Push interval: 200 ms — fastest of all four exchanges.

Redis key: fr:bb:{SYMBOL}
  Fields: r (rate), nr (empty — not in Bybit tickers payload),
          ft (next funding time ms), ts (receive ts ms)

Protocol notes:
- First message per symbol: full snapshot (type="snapshot").
- Subsequent messages: delta (type="delta") — only changed fields present.
  Cache last known values per symbol and merge delta on top.
  Cache survives reconnects (snapshot will refresh it on reconnect).
- Custom ping: {"op": "ping"} every 20 s.
- Subscribe in batches of 200 (linear frame limit).
"""

import asyncio
import random
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from fr_hist_writer import FrHistWriter

SCRIPT = "fr_bybit_futures"
EX     = "bb"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/bybit/bybit_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://stream.bybit.com/v5/public/linear"

SUB_BATCH       = 200
BATCH_SIZE      = 100
_BATCH_JITTER   = random.uniform(0, 0.100)
BATCH_TIMEOUT   = 0.200 + _BATCH_JITTER  # 200–300 ms, jittered per-process
MAX_BATCH       = 300
PING_INTERVAL   = 20
STATS_INTERVAL  = 30
RECONNECT_DELAY = 3


# ── logger ───────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── helpers ───────────────────────────────────────────────────────────────────

def load_symbols() -> list[str]:
    return [s for s in SUBSCRIBE_FILE.read_text().strip().splitlines() if s]


def chunk(lst: list, n: int) -> list[list]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def redis_key(sym: str) -> bytes:
    return f"fr:{EX}:{sym}".encode()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush(r: aioredis.Redis, batch: list, hw: FrHistWriter, counters: dict) -> None:
    t0 = time.monotonic()
    now_sec = time.time()
    async with r.pipeline(transaction=False) as pipe:
        for sym, r_b, nr_b, ft_b, ts_b in batch:
            pipe.hset(redis_key(sym), mapping={b"r": r_b, b"nr": nr_b, b"ft": ft_b, b"ts": ts_b})
        hw.add_to_pipe(pipe, batch, now_sec)
        hw.ensure_config(pipe, now_sec)
        await pipe.execute()
    counters["flushes"]   += 1
    counters["lat_total"] += (time.monotonic() - t0) * 1000


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

async def recv_loop(
    ws, r: aioredis.Redis, hw: FrHistWriter, counters: dict,
    stop: asyncio.Event, t_connected: float, first_logged: list,
    cache: dict,
) -> None:
    """
    cache: dict[sym -> {"r": bytes, "ft": bytes}]
    Persists across reconnects; snapshot on reconnect refreshes all fields.
    """
    batch: list = []
    last_flush   = time.monotonic()
    try:
        async for raw in ws:
            if stop.is_set():
                break
            now = time.monotonic()
            try:
                msg      = orjson.loads(raw)
                topic    = msg.get("topic", "")
                msg_type = msg.get("type", "")
                if not topic.startswith("tickers.") or msg_type not in ("snapshot", "delta"):
                    continue

                data = msg.get("data", {})
                sym  = data.get("symbol", "")
                if not sym:
                    continue

                # Merge incoming fields into per-symbol cache
                entry = cache.setdefault(sym, {"r": b"", "ft": b""})
                fr = data.get("fundingRate")
                ft = data.get("nextFundingTime")
                if fr is not None:
                    entry["r"]  = str(fr).encode()
                if ft is not None:
                    entry["ft"] = str(ft).encode()

                # Only write once we have at least a rate
                if not entry["r"]:
                    continue

                ts_b = str(msg.get("ts", int(time.time() * 1000))).encode()
                batch.append((sym, entry["r"], b"", entry["ft"], ts_b))
                counters["msgs"] += 1

                if not first_logged[0]:
                    log("INFO", "first_message",
                        ms_since_connected=round((now - t_connected) * 1000, 1),
                        first_sym=sym)
                    first_logged[0] = True

            except Exception as fe:
                log("WARN", "flush_on_disconnect_failed",
                    batch_size=len(batch), reason=str(fe)[:80])

            if batch and (now - last_flush >= BATCH_TIMEOUT or len(batch) >= MAX_BATCH):
                _bs = len(batch)
                await flush(r, batch, hw, counters)
                batch.clear()
                last_flush = now
                if _bs > 100:
                    await asyncio.sleep(0)

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

async def collect(r: aioredis.Redis, syms: list[str], hw: FrHistWriter, counters: dict) -> None:
    batches = chunk(syms, SUB_BATCH)
    cache: dict = {}   # survives reconnects
    t_disconnect = None
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
                if t_disconnect is not None:
                    log("INFO", "reconnected",
                        downtime_sec=round(time.monotonic() - t_disconnect, 1))
                    t_disconnect = None

                t_sub = time.monotonic()
                for batch in batches:
                    args = [f"tickers.{s}" for s in batch]
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.02)
                log("INFO", "subscribed",
                    sub_batches=len(batches), total_symbols=len(syms),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                first_logged = [False]
                recv_task = asyncio.create_task(
                    recv_loop(ws, r, hw, counters, stop, t_connected, first_logged, cache)
                )
                ping_task = asyncio.create_task(ping_loop(ws, stop))
                done, pending = await asyncio.wait(
                    [recv_task, ping_task], return_when=asyncio.FIRST_COMPLETED
                )
                stop.set()
                for t in pending:
                    t.cancel()
                await asyncio.gather(*pending, return_exceptions=True)

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            t_disconnect = time.monotonic()
            stop.set()

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


# ── stats ─────────────────────────────────────────────────────────────────────

async def stats_loop(counters: dict) -> None:
    prev_msgs    = 0
    prev_flushes = 0
    prev_lat     = 0.0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs = counters["msgs"]
        rate = (msgs - prev_msgs) / STATS_INTERVAL
        int_flushes = counters["flushes"] - prev_flushes
        avg_lat     = (counters["lat_total"] - prev_lat) / int_flushes if int_flushes > 0 else 0.0
        log("INFO", "stats",
            msgs_total=msgs,
            msgs_per_sec=round(rate, 1),
            flushes_total=counters["flushes"],
            avg_pipeline_ms=round(avg_lat, 3))
        prev_msgs    = msgs
        prev_flushes = counters["flushes"]
        prev_lat     = counters["lat_total"]


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    hw = FrHistWriter(EX, syms)
    await asyncio.gather(
        collect(r, syms, hw, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
