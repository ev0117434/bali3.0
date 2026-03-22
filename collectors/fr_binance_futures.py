#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Binance Futures — funding rate collector.

WS: fstream.binance.com/ws/!markPrice@arr@1s
    Single stream → ALL symbols every 1 second. Most efficient of all four.

Redis key: fr:bn:{SYMBOL}
  Fields: r (rate), nr (empty — Binance doesn't provide predicted rate),
          ft (next funding time ms), ts (receive ts ms)

Protocol notes:
- One connection handles every symbol — no per-symbol subscriptions.
- Binance force-closes the WS after 24 h → reconnect after 23 h.
- Default websockets ping/pong keepalive is sufficient.
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

SCRIPT = "fr_binance_futures"
EX     = "bn"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/binance/binance_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://fstream.binance.com/ws/!markPrice@arr@1s"

BATCH_SIZE      = 100
_BATCH_JITTER   = random.uniform(0, 0.100)
BATCH_TIMEOUT   = 0.200 + _BATCH_JITTER  # 200–300 ms, jittered per-process
MAX_BATCH       = 300
MAX_CONN_SEC    = 23 * 3600      # reconnect before forced 24 h close
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


# ── main collect loop ─────────────────────────────────────────────────────────

async def collect(r: aioredis.Redis, sym_set: set, hw: FrHistWriter, counters: dict) -> None:
    t_disconnect = None
    while True:
        batch: list = []
        last_flush   = time.monotonic()
        try:
            log("INFO", "connecting", note="all_symbols_stream_1s")
            async with websockets.connect(
                WS_URL,
                ping_interval=20,
                ping_timeout=20,
                close_timeout=5,
                max_queue=4096,
            ) as ws:
                t_connected      = time.monotonic()
                first_msg_logged = False
                log("INFO", "connected")
                if t_disconnect is not None:
                    log("INFO", "reconnected",
                        downtime_sec=round(time.monotonic() - t_disconnect, 1))
                    t_disconnect = None

                async for raw in ws:
                    # Proactive reconnect before Binance's forced 24 h close
                    if time.monotonic() - t_connected >= MAX_CONN_SEC:
                        log("INFO", "planned_reconnect", reason="24h_limit")
                        break

                    now = time.monotonic()
                    try:
                        data = orjson.loads(raw)
                        if not isinstance(data, list):
                            continue
                        ts_b = str(int(time.time() * 1000)).encode()
                        for item in data:
                            sym   = item.get("s", "")
                            r_val = item.get("r", "")
                            if sym not in sym_set or not r_val:
                                continue
                            # T = next funding time ms; 0 means not set
                            ft_raw = item.get("T", 0)
                            ft_b   = str(ft_raw).encode() if ft_raw else b""
                            batch.append((sym, r_val.encode(), b"", ft_b, ts_b))
                            counters["msgs"] += 1

                        if not first_msg_logged and batch:
                            log("INFO", "first_message",
                                ms_since_connected=round((now - t_connected) * 1000, 1))
                            first_msg_logged = True
                    except Exception:
                        pass

                    if batch and (now - last_flush >= BATCH_TIMEOUT or len(batch) >= MAX_BATCH):
                        _bs = len(batch)
                        await flush(r, batch, hw, counters)
                        batch.clear()
                        last_flush = now
                        if _bs > 100:
                            await asyncio.sleep(0)

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            t_disconnect = time.monotonic()
            if batch:
                try:
                    await flush(r, batch, hw, counters)
                except Exception as fe:
                    log("WARN", "flush_on_disconnect_failed",
                        batch_size=len(batch), reason=str(fe)[:80])

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
        collect(r, set(syms), hw, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
