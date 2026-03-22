#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Gate.io Futures (USDT Perpetual) — funding rate collector.

WS: fx-ws.gateio.ws/v4/ws/usdt  Channel: futures.tickers
    Event-driven — pushes only on change. Silent during quiet markets.

Redis key: fr:gt:{SYMBOL}  (normalized: BTCUSDT)
  Fields: r (rate), nr (funding_rate_indicative = predicted next rate),
          ft (empty — not available in WS payload), ts (recv ms)

Protocol notes:
- Subscribe file has BTCUSDT; Gate.io requires BTC_USDT (native format).
- Single connection, subscribe in batches of 100.
- Custom ping: {"time": ts, "channel": "futures.ping"} every 20 s.
- REST fallback every 30 s: GET /api/v4/futures/usdt/tickers
  One call returns ALL symbols — very efficient.
- [!] Gate.io auto-compresses funding interval to 1 h (no notice) when
  rate hits ±0.3%. Watch funding_rate_indicative for early warning.
- time_ms field used for millisecond precision (not time field).
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

SCRIPT = "fr_gate_futures"
EX     = "gt"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/gate/gate_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"
WS_URL          = "wss://fx-ws.gateio.ws/v4/ws/usdt"
CHANNEL         = "futures.tickers"
REST_URL        = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
CONTRACTS_URL   = "https://api.gateio.ws/api/v4/futures/usdt/contracts"

SUB_BATCH       = 100
BATCH_SIZE      = 100
_BATCH_JITTER   = random.uniform(0, 0.100)
BATCH_TIMEOUT   = 0.200 + _BATCH_JITTER  # 200–300 ms, jittered per-process
MAX_BATCH       = 300
PING_INTERVAL   = 10
REST_INTERVAL   = 30
CONTRACTS_INTERVAL = 300  # 5 min — funding_next_apply changes rarely
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
    return f"fr:{EX}:{sym}".encode()


def ts_now() -> int:
    return int(time.time())


# ── HTTP helper (for REST fallback) ──────────────────────────────────────────

_ssl_ctx = ssl.create_default_context()


async def http_get_list(url: str) -> list:
    """Fetch JSON list from url (Gate.io tickers returns a raw array)."""
    loop = asyncio.get_running_loop()
    try:
        def _fetch():
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=10, context=_ssl_ctx) as resp:
                return orjson.loads(resp.read())
        result = await loop.run_in_executor(None, _fetch)
        return result if isinstance(result, list) else []
    except Exception:
        return []


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
            await ws.send(orjson.dumps({"time": ts_now(), "channel": "futures.ping"}))
    except Exception:
        pass


# ── recv loop ─────────────────────────────────────────────────────────────────

async def recv_loop(
    ws, r: aioredis.Redis, hw: FrHistWriter, counters: dict,
    stop: asyncio.Event, t_connected: float, first_logged: list,
    sym_set: set,
) -> None:
    batch: list = []
    last_flush   = time.monotonic()
    try:
        async for raw in ws:
            if stop.is_set():
                break
            now = time.monotonic()
            try:
                msg = orjson.loads(raw)
                if msg.get("channel") != CHANNEL or msg.get("event") != "update":
                    continue
                results = msg.get("result", [])
                if not isinstance(results, list):
                    results = [results]
                ts_b = str(msg.get("time_ms", int(time.time() * 1000))).encode()
                for item in results:
                    native = item.get("contract", "")
                    r_val  = item.get("funding_rate", "")
                    if not native or not r_val:
                        continue
                    sym = normalize(native)
                    if sym not in sym_set:
                        continue
                    nr_b = item.get("funding_rate_indicative", "").encode()
                    batch.append((sym, r_val.encode(), nr_b, b"", ts_b))
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

async def collect(
    r: aioredis.Redis, syms: list[str], hw: FrHistWriter,
    counters: dict, sym_set: set,
) -> None:
    native  = [to_native(s) for s in syms]
    batches = chunk(native, SUB_BATCH)
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
                    await ws.send(orjson.dumps({
                        "time":    ts_now(),
                        "channel": CHANNEL,
                        "event":   "subscribe",
                        "payload": batch,
                    }))
                    await asyncio.sleep(0.02)
                log("INFO", "subscribed",
                    sub_batches=len(batches), total_symbols=len(syms),
                    subscribe_ms=round((time.monotonic() - t_sub) * 1000, 1))

                first_logged = [False]
                recv_task = asyncio.create_task(
                    recv_loop(ws, r, hw, counters, stop, t_connected, first_logged, sym_set)
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


# ── REST fallback loop ────────────────────────────────────────────────────────

async def rest_fallback_loop(
    r: aioredis.Redis,
    sym_map: dict,          # native (BTC_USDT) -> normalized (BTCUSDT)
    hw: FrHistWriter,
    counters: dict,
) -> None:
    """
    Gate.io REST GET /futures/usdt/tickers returns ALL symbols in one call.
    Run every REST_INTERVAL seconds to guarantee freshness during quiet markets.
    """
    await asyncio.sleep(REST_INTERVAL)   # let WS seed data first
    while True:
        try:
            items = await http_get_list(REST_URL)
            batch: list = []
            ts_b = str(int(time.time() * 1000)).encode()
            for item in items:
                native = item.get("contract", "")
                r_val  = item.get("funding_rate", "")
                if native not in sym_map or not r_val:
                    continue
                sym  = sym_map[native]
                nr_b = item.get("funding_rate_indicative", "").encode()
                batch.append((sym, r_val.encode(), nr_b, b"", ts_b))
                counters["msgs"] += 1
            if batch:
                await flush(r, batch, hw, counters)
            log("INFO", "rest_refresh", refreshed=len(batch))
        except Exception as e:
            log("WARN", "rest_error", reason=str(e)[:120])

        await asyncio.sleep(REST_INTERVAL)


# ── contracts loop (funding_next_apply → ft) ─────────────────────────────────

async def contracts_loop(
    r: aioredis.Redis,
    sym_map: dict,   # native (BTC_USDT) -> normalized (BTCUSDT)
    counters: dict,
) -> None:
    """
    GET /futures/usdt/contracts every 5 min — one call returns ALL contracts.
    Writes funding_next_apply (unix sec → ms) into fr:gt:{sym} ft field.
    """
    await asyncio.sleep(10)   # let WS + REST seed r/nr first
    while True:
        try:
            items = await http_get_list(CONTRACTS_URL)
            updated = 0
            async with r.pipeline(transaction=False) as pipe:
                for item in items:
                    native = item.get("name", "")
                    if native not in sym_map:
                        continue
                    ft_val = item.get("funding_next_apply")
                    if ft_val is None:
                        continue
                    sym  = sym_map[native]
                    ft_b = str(int(ft_val) * 1000).encode()   # sec → ms
                    pipe.hset(redis_key(sym), mapping={b"ft": ft_b})
                    updated += 1
                await pipe.execute()
            log("INFO", "contracts_refresh", updated=updated)
        except Exception as e:
            log("WARN", "contracts_error", reason=str(e)[:120])

        await asyncio.sleep(CONTRACTS_INTERVAL)


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
    sym_map = {n: s for n, s in zip(native, syms)}   # BTC_USDT -> BTCUSDT
    sym_set = set(syms)

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    hw = FrHistWriter(EX, syms)
    await asyncio.gather(
        collect(r, syms, hw, counters, sym_set),
        rest_fallback_loop(r, sym_map, hw, counters),
        contracts_loop(r, sym_map, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
