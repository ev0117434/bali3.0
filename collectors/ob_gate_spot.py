#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gate.io Spot — order book collector (10 levels).
WS: api.gateio.ws/ws/v4/  Channel: spot.order_book
Redis key: ob:gt:s:{SYMBOL}
Fields: b1..b10 (bid prices), bq1..bq10 (bid qty),
        a1..a10 (ask prices), aq1..aq10 (ask qty), t (exchange ts ms)

Protocol notes:
- Subscribe file has BTCUSDT; Gate.io requires BTC_USDT (native format)
- Single connection; one subscribe message per symbol (payload: [sym, "10", "100ms"])
- Custom ping: {"time": ts, "channel": "spot.ping"} every 20s
- spot.order_book sends periodic full snapshots — no local state needed
- Update message: result.bids / result.asks = [["price", "qty"], ...] (10 levels)
- result.s = native symbol (BTC_USDT)
- Subscribe logs progress every 100 symbols (~1s intervals)

Counters logged in stats (every 30s):
  msgs_total      — total snapshots written to Redis
  avg_levels      — average top-10 levels actually present per flush
  shallow_warnings — times received < 10 levels on either side
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

SCRIPT = "ob_gate_spot"
EX, MKT = "gt", "s"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/gate/gate_spot.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://api.gateio.ws/ws/v4/"
CHANNEL         = "spot.order_book"

LEVELS          = 10
SUB_DELAY       = 0.010   # 10ms between subscribe messages (one per symbol)
SUB_LOG_EVERY   = 100     # log subscribe progress every N symbols
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020
PING_INTERVAL   = 20
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


def to_native(sym: str) -> str:
    """BTCUSDT → BTC_USDT"""
    if sym.endswith("USDT"):
        return sym[:-4] + "_USDT"
    if sym.endswith("USDC"):
        return sym[:-4] + "_USDC"
    return sym


def normalize(sym: str) -> str:
    """BTC_USDT → BTCUSDT"""
    return sym.replace("_", "")


def redis_key(sym: str) -> bytes:
    return f"ob:{EX}:{MKT}:{sym}".encode()


def ts_now() -> int:
    return int(time.time())


def build_mapping(bids: list, asks: list, ts_ms: bytes) -> dict:
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


# ── ping loop ─────────────────────────────────────────────────────────────────

async def ping_loop(ws, stop: asyncio.Event) -> None:
    try:
        while not stop.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop.is_set():
                break
            await ws.send(orjson.dumps({"time": ts_now(), "channel": "spot.ping"}))
    except Exception:
        pass


# ── recv loop ─────────────────────────────────────────────────────────────────

async def recv_loop(
    ws,
    r: aioredis.Redis,
    hw: OBHistWriter,
    counters: dict,
    stop: asyncio.Event,
    t_connected: float,
    first_logged: list,
) -> None:
    batch: list = []
    last_flush = time.monotonic()
    try:
        async for raw in ws:
            if stop.is_set():
                break
            now = time.monotonic()
            try:
                msg = orjson.loads(raw)
                if msg.get("channel") != CHANNEL:
                    continue
                if msg.get("event") not in ("update", "all"):
                    continue
                result = msg.get("result", {})
                native_sym = result.get("s", "")
                bids = result.get("bids", [])
                asks = result.get("asks", [])
                if native_sym and (bids or asks):
                    sym     = normalize(native_sym)
                    exch_ts = result.get("t")
                    ts_ms   = str(exch_ts if exch_ts else int(time.time() * 1000)).encode()

                    n_bid = len(bids[:LEVELS])
                    n_ask = len(asks[:LEVELS])
                    counters["levels_total"] += n_bid + n_ask
                    counters["levels_count"] += 2
                    if n_bid < LEVELS or n_ask < LEVELS:
                        counters["shallow"] += 1

                    batch.append((redis_key(sym), build_mapping(bids, asks, ts_ms)))
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

async def collect(r: aioredis.Redis, syms: list[str], hw: OBHistWriter, counters: dict) -> None:
    native = [to_native(s) for s in syms]
    while True:
        stop = asyncio.Event()
        try:
            log("INFO", "connecting", total_symbols=len(syms))
            async with websockets.connect(
                WS_URL,
                ping_interval=None,
                close_timeout=5,
                max_queue=8192,
            ) as ws:
                t_connected = time.monotonic()
                log("INFO", "connected")

                # One subscribe per symbol — log progress every SUB_LOG_EVERY symbols
                t_sub = time.monotonic()
                for i, nat in enumerate(native, 1):
                    await ws.send(orjson.dumps({
                        "time":    ts_now(),
                        "channel": CHANNEL,
                        "event":   "subscribe",
                        "payload": [nat, str(LEVELS), "100ms"],
                    }))
                    await asyncio.sleep(SUB_DELAY)
                    if i % SUB_LOG_EVERY == 0:
                        log("INFO", "subscribing_progress",
                            subscribed=i,
                            total=len(native),
                            elapsed_ms=round((time.monotonic() - t_sub) * 1000, 1))
                log("INFO", "subscribed",
                    total_symbols=len(syms),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                first_logged = [False]
                recv_task = asyncio.create_task(
                    recv_loop(ws, r, hw, counters, stop, t_connected, first_logged))
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
    hw = OBHistWriter(EX, MKT, syms)
    await asyncio.gather(
        collect(r, syms, hw, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
