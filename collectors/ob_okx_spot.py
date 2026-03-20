#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX Spot — order book collector (10 levels).
WS: ws.okx.com:8443/ws/v5/public  Channel: books
Redis key: ob:ok:s:{SYMBOL}
Fields: b1..b10 (bid prices), bq1..bq10 (bid qty),
        a1..a10 (ask prices), aq1..aq10 (ask qty), t (exchange ts ms)

Protocol notes:
- Subscribe file has BTCUSDT; OKX requires BTC-USDT (native format)
- Multiple connections, 150 instId per connection, 0.15s stagger between opens
- Custom ping: text "ping", response "pong" every 25s
- OKX requires text frames (str) for subscribe messages
- Channel "books": action="snapshot" → full book (logged per inst on first init),
  action="update" → incremental (WARN if no snapshot yet for that inst)
- Bid/ask entry format: [price, qty, "", num_orders]
- Local book per inst_id; reset on reconnect

Counters logged in stats (every 30s):
  books_initialized  — instIds with a live local book
  snapshots_total    — total snapshot messages received
  updates_total      — total update (incremental) messages received
  update_skipped     — updates dropped (no snapshot yet)
  avg_levels         — average top-10 levels actually present per flush
  shallow_warnings   — times emitted < 10 levels on either side
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

SCRIPT = "ob_okx_spot"
EX, MKT = "ok", "s"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/okx/okx_spot.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://ws.okx.com:8443/ws/v5/public"

LEVELS          = 10
CHUNK_SIZE      = 150
CONNECT_DELAY   = 0.15
SUB_BATCH       = 20
BATCH_SIZE      = 100
BATCH_TIMEOUT   = 0.020
PING_INTERVAL   = 25
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


def to_native(sym: str) -> str:
    if sym.endswith("USDT"):
        return sym[:-4] + "-USDT"
    if sym.endswith("USDC"):
        return sym[:-4] + "-USDC"
    return sym


def normalize(inst_id: str) -> str:
    return inst_id.replace("-", "")


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
            await ws.send("ping")
    except Exception:
        pass


# ── per-chunk connection ──────────────────────────────────────────────────────

async def collect_chunk(
    r: aioredis.Redis,
    inst_ids: list[str],
    hw: OBHistWriter,
    counters: dict,
    books: dict,          # shared across all chunks and stats_loop
    delay: float,
) -> None:
    if delay:
        await asyncio.sleep(delay)

    while True:
        # Clear only the entries belonging to this chunk on reconnect
        for iid in inst_ids:
            books.pop(iid, None)

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
                t_connected = time.monotonic()
                log("INFO", "connected", chunk_symbols=len(inst_ids))

                sub_batches = chunk(inst_ids, SUB_BATCH)
                t_sub = time.monotonic()
                for sb in sub_batches:
                    args = [{"channel": "books", "instId": iid} for iid in sb]
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}).decode())
                    await asyncio.sleep(0.05)
                log("INFO", "subscribed",
                    chunk_symbols=len(inst_ids),
                    sub_batches=len(sub_batches),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                ping_task = asyncio.create_task(ping_loop(ws, stop))
                first_msg_logged = False

                async for raw in ws:
                    if raw == "pong":
                        continue
                    now = time.monotonic()
                    try:
                        msg    = orjson.loads(raw)
                        action = msg.get("action", "")
                        if action not in ("snapshot", "update"):
                            continue
                        arg     = msg.get("arg", {})
                        inst_id = arg.get("instId", "")
                        data_list = msg.get("data", [])
                        if not inst_id or not data_list:
                            continue

                        sym = normalize(inst_id)

                        for item in data_list:
                            raw_bids = item.get("bids", [])
                            raw_asks = item.get("asks", [])
                            ts_str   = item.get("ts", "")
                            ts_ms    = (ts_str.encode() if ts_str
                                        else str(int(time.time() * 1000)).encode())

                            if action == "snapshot":
                                is_new = inst_id not in books
                                books[inst_id] = {
                                    "bids": {row[0]: row[1] for row in raw_bids},
                                    "asks": {row[0]: row[1] for row in raw_asks},
                                }
                                counters["snapshots"] += 1
                                if is_new:
                                    log("INFO", "book_init",
                                        symbol=sym,
                                        bid_levels=len(raw_bids),
                                        ask_levels=len(raw_asks),
                                        books_initialized=len(books))
                            else:  # update
                                if inst_id not in books:
                                    counters["update_skipped"] += 1
                                    log("WARN", "update_before_snapshot",
                                        symbol=sym,
                                        update_skipped_total=counters["update_skipped"])
                                    continue
                                for row in raw_bids:
                                    p, q = row[0], row[1]
                                    if q == "0":
                                        books[inst_id]["bids"].pop(p, None)
                                    else:
                                        books[inst_id]["bids"][p] = q
                                for row in raw_asks:
                                    p, q = row[0], row[1]
                                    if q == "0":
                                        books[inst_id]["asks"].pop(p, None)
                                    else:
                                        books[inst_id]["asks"][p] = q
                                counters["updates"] += 1

                            if inst_id not in books:
                                continue

                            top_bids = sorted(
                                books[inst_id]["bids"].items(),
                                key=lambda x: float(x[0]),
                                reverse=True,
                            )[:LEVELS]
                            top_asks = sorted(
                                books[inst_id]["asks"].items(),
                                key=lambda x: float(x[0]),
                            )[:LEVELS]

                            if not top_bids and not top_asks:
                                continue

                            n_bid = len(top_bids)
                            n_ask = len(top_asks)
                            counters["levels_total"] += n_bid + n_ask
                            counters["levels_count"] += 2
                            if n_bid < LEVELS or n_ask < LEVELS:
                                counters["shallow"] += 1

                            batch.append((redis_key(sym), build_mapping(top_bids, top_asks, ts_ms)))
                            counters["msgs"] += 1
                            if not first_msg_logged:
                                log("INFO", "first_message",
                                    chunk_symbols=len(inst_ids),
                                    ms_since_connected=round((now - t_connected) * 1000, 1),
                                    first_sym=sym)
                                first_msg_logged = True

                    except Exception:
                        pass

                    if len(batch) >= BATCH_SIZE or (batch and now - last_flush >= BATCH_TIMEOUT):
                        await flush(r, batch, hw, counters)
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
                    await flush(r, batch, hw, counters)
                except Exception:
                    pass

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
            updates_total      = counters["updates"],
            update_skipped     = counters["update_skipped"],
            avg_levels         = round(avg_lvl, 2),
            shallow_warnings   = counters["shallow"],
        )
        prev_msgs = msgs


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    native = [to_native(s) for s in syms]
    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {
        "msgs":           0,
        "flushes":        0,
        "lat_total":      0.0,
        "snapshots":      0,
        "updates":        0,
        "update_skipped": 0,
        "levels_total":   0,
        "levels_count":   0,
        "shallow":        0,
    }
    books: dict = {}  # inst_id -> {"bids": {...}, "asks": {...}}  shared across all chunks

    chunks = chunk(native, CHUNK_SIZE)
    log("INFO", "subscribing", total_symbols=len(syms), connections=len(chunks))

    hw = OBHistWriter(EX, MKT, syms)
    tasks = [
        asyncio.create_task(collect_chunk(r, ch, hw, counters, books, i * CONNECT_DELAY))
        for i, ch in enumerate(chunks)
    ]
    tasks.append(asyncio.create_task(stats_loop(counters, books)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
