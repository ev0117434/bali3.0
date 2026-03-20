#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spread_monitor.py — мониторинг спреда spot→futures по 12 направлениям.

Каждые 0.3с читает Redis, считает (futures_bid - spot_ask) / spot_ask * 100.
Сигнал = спред > MIN_SPREAD_PCT И данные свежее STALE_THRESHOLD секунд.
Кулдаун COOLDOWN_SEC секунд на (направление, символ) после сигнала.

Выход:
    signals/signals.jsonl   — один JSON-объект на строку
    signals/signals.csv     — CSV: spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis

SCRIPT = "spread_monitor"

COMBO_DIR       = Path(__file__).parent.parent / "dictionaries" / "combination"
SIGNALS_DIR     = Path(__file__).parent.parent / "signals"
SIGNAL_JSONL    = SIGNALS_DIR / "signals.jsonl"
SIGNAL_CSV      = SIGNALS_DIR / "signals.csv"

REDIS_SOCK      = "/var/run/redis/redis.sock"

POLL_INTERVAL   = 0.3     # секунд между циклами
MIN_SPREAD_PCT  = 1.0     # минимальный спред для сигнала
STALE_THRESHOLD = 300     # секунд — пропускать если данные старше
COOLDOWN_SEC    = 3500    # секунд кулдаун на (направление, символ)
STATS_INTERVAL  = 30      # секунд между stats-логами
CLEANUP_EVERY   = 100     # циклов между чисткой протухших кулдаунов

EXCHANGE_CODE = {
    "binance": "bn",
    "bybit":   "bb",
    "okx":     "ok",
    "gate":    "gt",
}
# Обратный маппинг для CSV (полное название)
CODE_NAME = {v: k for k, v in EXCHANGE_CODE.items()}

CSV_HEADER = "spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts\n"


# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── load directions ───────────────────────────────────────────────────────────

def load_directions() -> dict:
    """
    Возвращает dict:
        direction_name -> (spot_ex_code, fut_ex_code, [symbols])
    """
    dirs = {}
    for f in sorted(COMBO_DIR.glob("*_spot_*_futures.txt")):
        name = f.stem  # binance_spot_bybit_futures
        # parse spotex and futex from filename
        # format: {spotex}_spot_{futex}_futures
        after_spot = name.split("_spot_", 1)
        if len(after_spot) != 2:
            continue
        spot_name = after_spot[0]                        # binance
        fut_name  = after_spot[1].replace("_futures", "") # bybit
        if spot_name not in EXCHANGE_CODE or fut_name not in EXCHANGE_CODE:
            continue
        spot_ex = EXCHANGE_CODE[spot_name]
        fut_ex  = EXCHANGE_CODE[fut_name]
        symbols = [s.strip() for s in f.read_text().splitlines() if s.strip()]
        dirs[name] = (spot_ex, fut_ex, symbols)
    return dirs


# ── Redis scan cycle ──────────────────────────────────────────────────────────

async def scan_cycle(
    r:           aioredis.Redis,
    directions:  dict,
    cooldowns:   dict,
    f_jsonl,
    f_csv,
    counters:    dict,
) -> None:
    now_ms = int(time.time() * 1000)
    now    = time.time()

    # Collect unique keys across all directions
    key_list: list[bytes] = list({
        key
        for spot_ex, fut_ex, symbols in directions.values()
        for sym in symbols
        for key in (
            f"md:{spot_ex}:s:{sym}".encode(),
            f"md:{fut_ex}:f:{sym}".encode(),
        )
    })

    if not key_list:
        return

    # Pipeline: fetch b, a, t for every key in one round-trip
    async with r.pipeline(transaction=False) as pipe:
        for key in key_list:
            pipe.hmget(key, b"b", b"a", b"t")
        results = await pipe.execute()

    cache: dict[bytes, tuple] = {
        key: (vals[0], vals[1], vals[2])   # (bid, ask, ts_ms_bytes)
        for key, vals in zip(key_list, results)
    }

    for direction, (spot_ex, fut_ex, symbols) in directions.items():
        for sym in symbols:
            counters["scanned"] += 1

            spot_key = f"md:{spot_ex}:s:{sym}".encode()
            fut_key  = f"md:{fut_ex}:f:{sym}".encode()

            _, spot_ask_b, spot_ts_b = cache.get(spot_key, (None, None, None))
            fut_bid_b, _, fut_ts_b   = cache.get(fut_key,  (None, None, None))

            if not spot_ask_b or not fut_bid_b:
                continue

            # Freshness check
            try:
                if spot_ts_b and (now_ms - int(spot_ts_b)) / 1000 > STALE_THRESHOLD:
                    counters["stale_skipped"] += 1
                    continue
                if fut_ts_b and (now_ms - int(fut_ts_b)) / 1000 > STALE_THRESHOLD:
                    counters["stale_skipped"] += 1
                    continue
            except (ValueError, TypeError):
                continue

            # Spread calculation: (futures_bid - spot_ask) / spot_ask * 100
            try:
                spot_ask = float(spot_ask_b)
                fut_bid  = float(fut_bid_b)
            except (ValueError, TypeError):
                continue

            if spot_ask <= 0 or fut_bid <= 0:
                continue

            spread_pct = (fut_bid - spot_ask) / spot_ask * 100

            if spread_pct < MIN_SPREAD_PCT:
                continue

            # Cooldown check
            cd_key    = (direction, sym)
            cd_expiry = cooldowns.get(cd_key, 0.0)
            if now < cd_expiry:
                counters["cooldown_skipped"] += 1
                continue

            # ── fire signal ──
            cooldowns[cd_key] = now + COOLDOWN_SEC
            counters["signals"] += 1

            spot_ask_s = spot_ask_b.decode()
            fut_bid_s  = fut_bid_b.decode()
            spread_r   = round(spread_pct, 4)
            spot_name  = CODE_NAME.get(spot_ex, spot_ex)
            fut_name   = CODE_NAME.get(fut_ex,  fut_ex)

            log("INFO", "signal",
                direction=direction,
                symbol=sym,
                spread_pct=spread_r,
                spot_ask=spot_ask_s,
                fut_bid=fut_bid_s,
                spot_ex=spot_ex,
                fut_ex=fut_ex,
                cooldown_until=round(now + COOLDOWN_SEC),
            )

            # JSON line
            signal = {
                "ts":         now,
                "direction":  direction,
                "symbol":     sym,
                "spread_pct": spread_r,
                "spot_ask":   spot_ask_s,
                "fut_bid":    fut_bid_s,
                "spot_ex":    spot_ex,
                "fut_ex":     fut_ex,
            }
            try:
                f_jsonl.write(orjson.dumps(signal) + b"\n")
                f_jsonl.flush()
            except Exception as e:
                log("WARN", "jsonl_write_error", reason=str(e)[:120])

            # CSV line: spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts_ms
            try:
                csv_line = (
                    f"{spot_name},{fut_name},{sym},"
                    f"{spot_ask_s},{fut_bid_s},{spread_r},"
                    f"{now_ms}\n"
                )
                f_csv.write(csv_line.encode())
                f_csv.flush()
            except Exception as e:
                log("WARN", "csv_write_error", reason=str(e)[:120])


# ── stats loop ────────────────────────────────────────────────────────────────

async def stats_loop(counters: dict, cooldowns: dict) -> None:
    prev_signals = 0
    while True:
        await asyncio.sleep(STATS_INTERVAL)

        # Cleanup expired cooldowns
        now     = time.time()
        expired = [k for k, v in cooldowns.items() if v < now]
        for k in expired:
            del cooldowns[k]

        signals = counters["signals"]
        log("INFO", "stats",
            signals_total         = signals,
            signals_per_interval  = signals - prev_signals,
            cooldowns_active      = len(cooldowns),
            scanned_total         = counters["scanned"],
            stale_skipped         = counters["stale_skipped"],
            cooldown_skipped      = counters["cooldown_skipped"],
            last_cycle_ms         = round(counters["last_cycle_ms"], 2),
        )
        prev_signals = signals


# ── main ──────────────────────────────────────────────────────────────────────

async def main() -> None:
    log("INFO", "startup",
        poll_interval_sec   = POLL_INTERVAL,
        min_spread_pct      = MIN_SPREAD_PCT,
        stale_threshold_sec = STALE_THRESHOLD,
        cooldown_sec        = COOLDOWN_SEC,
    )

    directions = load_directions()
    if not directions:
        log("ERROR", "no_directions", combo_dir=str(COMBO_DIR),
            msg="No combination files found — run dictionaries/main.py first")
        return

    total_symbols = sum(len(v[2]) for v in directions.values())
    log("INFO", "directions_loaded",
        directions=len(directions),
        total_symbols=total_symbols,
    )
    for name, (spot_ex, fut_ex, symbols) in directions.items():
        log("INFO", "direction_loaded",
            direction=name,
            symbols=len(symbols),
            spot_ex=spot_ex,
            fut_ex=fut_ex,
        )

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=2)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    SIGNALS_DIR.mkdir(parents=True, exist_ok=True)

    # Open both output files (append mode — survive restarts)
    f_jsonl = open(SIGNAL_JSONL, "ab")
    f_csv   = open(SIGNAL_CSV,   "ab")

    # Write CSV header only if file is empty
    if SIGNAL_CSV.stat().st_size == 0:
        f_csv.write(CSV_HEADER.encode())
        f_csv.flush()

    log("INFO", "signal_files_opened",
        jsonl=str(SIGNAL_JSONL),
        csv=str(SIGNAL_CSV),
    )

    cooldowns: dict = {}
    counters = {
        "signals":          0,
        "scanned":          0,
        "stale_skipped":    0,
        "cooldown_skipped": 0,
        "last_cycle_ms":    0.0,
    }
    cycle_count = 0

    async def poll_loop() -> None:
        nonlocal cycle_count
        while True:
            t0 = time.monotonic()
            try:
                await scan_cycle(r, directions, cooldowns, f_jsonl, f_csv, counters)
            except Exception as e:
                log("ERROR", "cycle_error", reason=str(e)[:200])
            elapsed = time.monotonic() - t0
            counters["last_cycle_ms"] = elapsed * 1000
            cycle_count += 1
            await asyncio.sleep(max(0.0, POLL_INTERVAL - elapsed))

    await asyncio.gather(
        poll_loop(),
        stats_loop(counters, cooldowns),
    )


if __name__ == "__main__":
    asyncio.run(main())
