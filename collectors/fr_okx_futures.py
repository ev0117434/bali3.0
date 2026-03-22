#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OKX Futures (USDT Perpetual Swaps) — funding rate collector.

WS: ws.okx.com:8443/ws/v5/public  Channel: funding-rate
    Pushes on change only (~30 s typical). Silent during quiet markets.

Redis key: fr:ok:{SYMBOL}  (normalized: BTCUSDT)
  Fields: r (rate), nr (next predicted rate), ft (next funding time ms), ts (recv ms)

Protocol notes:
- Subscribe file has BTCUSDT; OKX channel requires BTC-USDT-SWAP (native).
- Multiple connections: 300 instIds per connection, 0.15 s stagger between opens.
- Sub-batch: 20 args per subscribe message (OKX frame limit ~4 KB).
- Custom ping: text "ping" every 25 s; server responds "pong".
- REST fallback every 30 s: GET /api/v5/public/funding-rate?instId={sym}
  Calls all symbols sequentially with 0.15 s delay (stays well within
  OKX's 20 req/2 s limit). Guarantees data freshness during quiet markets.
- [!] OKX April 2025: new formulaType field added — ignored here.
- [!] Do NOT use wsaws.okx.com (deprecated AWS domain).
"""

import asyncio
import random
import ssl
import sys
import time
import urllib.request
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from fr_hist_writer import FrHistWriter

SCRIPT = "fr_okx_futures"
EX     = "ok"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/okx/okx_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://ws.okx.com:8443/ws/v5/public"
REST_BASE       = "https://www.okx.com/api/v5/public/funding-rate"

CHUNK_SIZE      = 300
CONNECT_DELAY   = 0.15
SUB_BATCH       = 20
BATCH_SIZE      = 100
_BATCH_JITTER   = random.uniform(0, 0.100)
BATCH_TIMEOUT   = 0.200 + _BATCH_JITTER  # 200–300 ms, jittered per-process
MAX_BATCH       = 300
PING_INTERVAL   = 25
REST_INTERVAL   = 30
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
    return f"fr:{EX}:{sym}".encode()


# ── HTTP helper (for REST fallback) ──────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()


async def http_get(url: str) -> dict:
    loop = asyncio.get_running_loop()
    try:
        def _fetch():
            req = urllib.request.Request(url, headers={
                "Accept":     "application/json",
                "User-Agent": "Mozilla/5.0 (compatible; bali-collector/3.0)",
            })
            with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as resp:
                return orjson.loads(resp.read())
        return await loop.run_in_executor(None, _fetch)
    except Exception:
        return {}


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
            await ws.send("ping")
    except Exception:
        pass


# ── per-chunk WS connection ───────────────────────────────────────────────────

async def collect_chunk(
    r: aioredis.Redis,
    inst_ids: list[str],    # native OKX format: BTC-USDT-SWAP
    hw: FrHistWriter,
    counters: dict,
    delay: float,
) -> None:
    if delay:
        await asyncio.sleep(delay)

    t_disconnect = None
    while True:
        stop  = asyncio.Event()
        batch: list = []
        last_flush  = time.monotonic()
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
                if t_disconnect is not None:
                    log("INFO", "reconnected",
                        downtime_sec=round(time.monotonic() - t_disconnect, 1))
                    t_disconnect = None

                sub_batches = chunk(inst_ids, SUB_BATCH)
                t_sub = time.monotonic()
                for sb in sub_batches:
                    args = [{"channel": "funding-rate", "instId": iid} for iid in sb]
                    await ws.send(orjson.dumps({"op": "subscribe", "args": args}).decode())
                    await asyncio.sleep(0.05)
                log("INFO", "subscribed",
                    chunk_symbols=len(inst_ids), sub_batches=len(sub_batches),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                ping_task        = asyncio.create_task(ping_loop(ws, stop))
                first_msg_logged = False

                async for raw in ws:
                    if raw == "pong":
                        continue
                    now = time.monotonic()
                    try:
                        msg       = orjson.loads(raw)
                        data_list = msg.get("data")
                        if not data_list:
                            continue
                        for item in data_list:
                            inst_id = item.get("instId", "")
                            r_val   = item.get("fundingRate", "")
                            if not inst_id or not r_val:
                                continue
                            sym   = normalize(inst_id)
                            nr_b  = item.get("nextFundingRate", "").encode()
                            ft_b  = item.get("nextFundingTime", "").encode()
                            ts_b  = str(int(time.time() * 1000)).encode()
                            batch.append((sym, r_val.encode(), nr_b, ft_b, ts_b))
                            counters["msgs"] += 1
                            if not first_msg_logged:
                                log("INFO", "first_message",
                                    chunk_symbols=len(inst_ids),
                                    ms_since_connected=round((now - t_connected) * 1000, 1),
                                    first_sym=sym)
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

                stop.set()
                ping_task.cancel()
                await asyncio.gather(ping_task, return_exceptions=True)

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            t_disconnect = time.monotonic()
            stop.set()
            if batch:
                try:
                    await flush(r, batch, hw, counters)
                except Exception as fe:
                    log("WARN", "flush_on_disconnect_failed",
                        batch_size=len(batch), reason=str(fe)[:80])

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


# ── REST fallback loop ────────────────────────────────────────────────────────

async def rest_fallback_loop(
    r: aioredis.Redis,
    inst_ids: list[str],    # native: BTC-USDT-SWAP
    sym_map: dict,          # native -> normalized
    hw: FrHistWriter,
    counters: dict,
) -> None:
    """
    Every REST_INTERVAL seconds, refresh all symbols via REST.
    Sequential with 0.15 s between calls → ~6.6 calls/s, well within 10/s limit.
    For large symbol sets (>200) the cycle may exceed REST_INTERVAL;
    in that case the next iteration starts immediately after completion.
    """
    await asyncio.sleep(REST_INTERVAL)   # initial delay — let WS seed first
    while True:
        cycle_start = time.time()
        batch: list = []
        ok = fail = 0
        for inst_id in inst_ids:
            try:
                data = await http_get(f"{REST_BASE}?instId={inst_id}")
                items = data.get("data", [])
                if items:
                    item  = items[0]
                    sym   = sym_map[inst_id]
                    r_b   = item.get("fundingRate", "").encode()
                    nr_b  = item.get("nextFundingRate", "").encode()
                    ft_b  = item.get("nextFundingTime", "").encode()
                    ts_b  = str(int(time.time() * 1000)).encode()
                    if r_b:
                        batch.append((sym, r_b, nr_b, ft_b, ts_b))
                        counters["msgs"] += 1
                        ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
            await asyncio.sleep(0.15)

        if batch:
            try:
                await flush(r, batch, hw, counters)
            except Exception:
                pass
        log("INFO", "rest_refresh", refreshed=ok, failed=fail)

        elapsed = time.time() - cycle_start
        wait    = max(0.0, REST_INTERVAL - elapsed)
        if wait:
            await asyncio.sleep(wait)


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

    native  = [to_native(s) for s in syms]
    sym_map = {n: s for n, s in zip(native, syms)}   # BTC-USDT-SWAP -> BTCUSDT

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    chunks   = chunk(native, CHUNK_SIZE)
    log("INFO", "subscribing", total_symbols=len(syms), connections=len(chunks))

    hw = FrHistWriter(EX, syms)
    tasks = [
        asyncio.create_task(collect_chunk(r, ch, hw, counters, i * CONNECT_DELAY))
        for i, ch in enumerate(chunks)
    ]
    tasks.append(asyncio.create_task(rest_fallback_loop(r, native, sym_map, hw, counters)))
    tasks.append(asyncio.create_task(stats_loop(counters)))
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
