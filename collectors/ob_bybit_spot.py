#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Bybit Spot — order book collector (10 levels).
WS: stream.bybit.com/v5/public/spot  Channel: orderbook.50
Redis key: ob:bb:s:{SYMBOL}
Fields: b1..b10 (bid prices), bq1..bq10 (bid qty),
        a1..a10 (ask prices), aq1..aq10 (ask qty), t (exchange ts ms)

Protocol notes:
- Single connection, subscribe in batches of 10 (spot frame limit)
- Custom ping: {"op": "ping"} every 20s
- type="snapshot" → full book for symbol (logged per symbol on first init)
- type="delta"    → incremental change; skipped with WARN if no snapshot yet
- Local book reset on every reconnect (fresh snapshots follow automatically)

Counters logged in stats (every 30s):
  books_initialized  — symbols with a live local book
  snapshots_total    — total snapshot messages received
  deltas_total       — total delta messages received
  delta_skipped      — deltas dropped (arrived before snapshot)
  avg_levels         — average top-10 levels actually present per flush
  shallow_warnings   — times received < 10 levels on either side
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

SCRIPT = "ob_bybit_spot"
EX, MKT = "bb", "s"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/bybit/bybit_spot.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://stream.bybit.com/v5/public/spot"

LEVELS          = 10
SUB_BATCH       = 10
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


def chunk(lst: list, n: int) -> list[list]:
    return [lst[i : i + n] for i in range(0, len(lst), n)]


def redis_key(sym: str) -> bytes:
    return f"ob:{EX}:{MKT}:{sym}".encode()


def build_mapping(bids: list, asks: list, ts_ms: bytes) -> dict:
    m = {b"t": ts_ms}
    for i, (p, q) in enumerate(bids, 1):
        m[f"b{i}".encode()]  = str(p).encode()
        m[f"bq{i}".encode()] = str(q).encode()
    for i, (p, q) in enumerate(asks, 1):
        m[f"a{i}".encode()]  = str(p).encode()
        m[f"aq{i}".encode()] = str(q).encode()
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
            await ws.send(orjson.dumps({"op": "ping"}))
    except Exception:
        pass


# ── recv loop ────────────────────────────────────────────────────────────────

async def recv_loop(
    ws,
    r: aioredis.Redis,
    hw: OBHistWriter,
    counters: dict,
    books: dict,          # shared with stats_loop and collect (cleared on reconnect)
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
                topic = msg.get("topic", "")
                if not topic.startswith("orderbook.50."):
                    continue

                sym      = topic[len("orderbook.50."):]
                msg_type = msg.get("type", "")
                data     = msg.get("data", {})
                ts_ms    = str(msg.get("ts", int(time.time() * 1000))).encode()
                raw_bids = data.get("b", [])
                raw_asks = data.get("a", [])

                if msg_type == "snapshot":
                    is_new = sym not in books
                    books[sym] = {
                        "bids": {p: q for p, q in raw_bids},
                        "asks": {p: q for p, q in raw_asks},
                    }
                    counters["snapshots"] += 1
                    if is_new:
                        log("INFO", "book_init",
                            symbol=sym,
                            bid_levels=len(raw_bids),
                            ask_levels=len(raw_asks),
                            books_initialized=len(books))

                elif msg_type == "delta":
                    if sym not in books:
                        counters["delta_skipped"] += 1
                        log("WARN", "delta_before_snapshot",
                            symbol=sym,
                            delta_skipped_total=counters["delta_skipped"])
                        continue
                    for p, q in raw_bids:
                        if q == "0":
                            books[sym]["bids"].pop(p, None)
                        else:
                            books[sym]["bids"][p] = q
                    for p, q in raw_asks:
                        if q == "0":
                            books[sym]["asks"].pop(p, None)
                        else:
                            books[sym]["asks"][p] = q
                    counters["deltas"] += 1
                else:
                    continue

                top_bids = sorted(
                    books[sym]["bids"].items(),
                    key=lambda x: float(x[0]),
                    reverse=True,
                )[:LEVELS]
                top_asks = sorted(
                    books[sym]["asks"].items(),
                    key=lambda x: float(x[0]),
                )[:LEVELS]

                if not top_bids and not top_asks:
                    continue

                # Track level depth
                n_bid = len(top_bids)
                n_ask = len(top_asks)
                counters["levels_total"] += n_bid + n_ask
                counters["levels_count"] += 2
                if n_bid < LEVELS or n_ask < LEVELS:
                    counters["shallow"] += 1

                batch.append((redis_key(sym), build_mapping(top_bids, top_asks, ts_ms)))
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

async def collect(
    r: aioredis.Redis,
    syms: list[str],
    hw: OBHistWriter,
    counters: dict,
    books: dict,
) -> None:
    batches = chunk(syms, SUB_BATCH)
    while True:
        # Reset local book on each reconnect — fresh snapshots will follow
        books.clear()
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
                    args = [f"orderbook.50.{s}" for s in batch]
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.02)
                log("INFO", "subscribed",
                    sub_batches=len(batches),
                    total_symbols=len(syms),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                first_logged = [False]
                recv_task = asyncio.create_task(
                    recv_loop(ws, r, hw, counters, books, stop, t_connected, first_logged))
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

async def stats_loop(counters: dict, books: dict) -> None:
    prev_msgs = 0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs    = counters["msgs"]
        rate    = (msgs - prev_msgs) / STATS_INTERVAL
        avg_lat = counters["lat_total"] / counters["flushes"] if counters["flushes"] else 0.0
        avg_lvl = (counters["levels_total"] / counters["levels_count"]
                   if counters["levels_count"] else 0.0)
        log("INFO", "stats",
            msgs_total         = msgs,
            msgs_per_sec       = round(rate, 1),
            flushes_total      = counters["flushes"],
            avg_pipeline_ms    = round(avg_lat, 3),
            books_initialized  = len(books),
            snapshots_total    = counters["snapshots"],
            deltas_total       = counters["deltas"],
            delta_skipped      = counters["delta_skipped"],
            avg_levels         = round(avg_lvl, 2),
            shallow_warnings   = counters["shallow"],
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
        "msgs":          0,
        "flushes":       0,
        "lat_total":     0.0,
        "snapshots":     0,
        "deltas":        0,
        "delta_skipped": 0,
        "levels_total":  0,
        "levels_count":  0,
        "shallow":       0,
    }
    books: dict = {}   # sym -> {"bids": {...}, "asks": {...}}  shared across coroutines

    hw = OBHistWriter(EX, MKT, syms)
    await asyncio.gather(
        collect(r, syms, hw, counters, books),
        stats_loop(counters, books),
    )


if __name__ == "__main__":
    asyncio.run(main())
