#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
staleness_monitor.py — мониторинг свежести данных в Redis.

Каждые 60 секунд проверяет все ключи md:* и логирует stale-данные.
Ключ считается stale если поле 't' не обновлялось более STALE_THRESHOLD секунд.

Запуск:
    python3 collectors/staleness_monitor.py              # только stale лог
    python3 collectors/staleness_monitor.py --buckets    # + распределение по корзинам
"""

import argparse
import asyncio
import sys
import time

import orjson
import redis.asyncio as aioredis

REDIS_SOCK      = "/var/run/redis/redis.sock"
KEY_PATTERN     = "md:??:?:*"   # matches md:bn:s:SYM only, excludes md:hist:*
CHECK_INTERVAL  = 60       # секунд между проверками
STALE_THRESHOLD = 300      # 5 минут = stale
SCAN_COUNT      = 500      # ключей за один SCAN-вызов
SCRIPT          = "staleness_monitor"

# Границы корзин в секундах (левый край включительно, правый исключительно)
BUCKETS = [
    (60,  120,  "1-2min"),
    (120, 180,  "2-3min"),
    (180, 240,  "3-4min"),
    (240, 300,  "4-5min"),
    (300, None, "5+min"),
]


# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── Redis helpers ─────────────────────────────────────────────────────────────

async def scan_all_keys(r: aioredis.Redis) -> list[bytes]:
    """Возвращает все ключи, совпадающие с KEY_PATTERN."""
    keys = []
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor, match=KEY_PATTERN, count=SCAN_COUNT)
        keys.extend(batch)
        if cursor == 0:
            break
    return keys


async def fetch_timestamps(r: aioredis.Redis, keys: list[bytes]) -> dict[bytes, int | None]:
    """Pipeline HGET t для всех ключей. Возвращает {key: timestamp_ms | None}."""
    if not keys:
        return {}
    result = {}
    chunk_size = 500
    for i in range(0, len(keys), chunk_size):
        chunk = keys[i : i + chunk_size]
        async with r.pipeline(transaction=False) as pipe:
            for key in chunk:
                pipe.hget(key, b"t")
            values = await pipe.execute()
        for key, val in zip(chunk, values):
            if val is not None:
                try:
                    result[key] = int(val)
                except (ValueError, TypeError):
                    result[key] = None
            else:
                result[key] = None
    return result


# ── bucket analysis ───────────────────────────────────────────────────────────

def build_buckets(ages_sec: list[float]) -> dict:
    """
    Строит распределение возраста ключей по корзинам.
    ages_sec — список возрастов всех ключей в секундах.
    """
    counts = {label: 0 for _, _, label in BUCKETS}
    counts["fresh"] = 0   # < 1 минуты

    for age in ages_sec:
        if age < 60:
            counts["fresh"] += 1
            continue
        placed = False
        for lo, hi, label in BUCKETS:
            if hi is None:
                if age >= lo:
                    counts[label] += 1
                    placed = True
                    break
            elif lo <= age < hi:
                counts[label] += 1
                placed = True
                break
        if not placed:
            counts["fresh"] += 1

    return counts


# ── main check ────────────────────────────────────────────────────────────────

async def check(r: aioredis.Redis, with_buckets: bool, cycle: int = 0) -> None:
    t_check = time.monotonic()
    now_ms = int(time.time() * 1000)
    log("INFO", "check_start", cycle=cycle, pattern=KEY_PATTERN)

    t_scan = time.monotonic()
    keys = await scan_all_keys(r)
    scan_ms = round((time.monotonic() - t_scan) * 1000, 1)
    log("INFO", "scan_complete", cycle=cycle, keys_found=len(keys), scan_ms=scan_ms)

    if not keys:
        log("WARN", "check", cycle=cycle, total_keys=0, stale_count=0,
            msg="No md:* keys found in Redis")
        return

    t_fetch = time.monotonic()
    ts_map = await fetch_timestamps(r, keys)
    fetch_ms = round((time.monotonic() - t_fetch) * 1000, 1)
    log("INFO", "fetch_complete", cycle=cycle, keys_fetched=len(ts_map), fetch_ms=fetch_ms)

    stale_keys = []
    ages_sec = []
    no_ts = 0

    for key, ts_ms in ts_map.items():
        if ts_ms is None:
            no_ts += 1
            continue
        age_sec = (now_ms - ts_ms) / 1000.0
        ages_sec.append(age_sec)
        if age_sec > STALE_THRESHOLD:
            stale_keys.append({
                "key": key.decode(),
                "age_sec": round(age_sec, 1),
            })

    # Группируем stale по бирже:рынку для компактного лога
    stale_by_source: dict[str, int] = {}
    for entry in stale_keys:
        # key format: md:{ex}:{mkt}:{sym}
        parts = entry["key"].split(":")
        src = f"{parts[1]}:{parts[2]}" if len(parts) >= 3 else "unknown"
        stale_by_source[src] = stale_by_source.get(src, 0) + 1

    # Топ-10 самых устаревших ключей для примера
    stale_keys.sort(key=lambda x: x["age_sec"], reverse=True)
    top_stale = stale_keys[:10]

    log_kw = dict(
        total_keys=len(keys),
        keys_with_ts=len(ages_sec),
        keys_no_ts=no_ts,
        stale_count=len(stale_keys),
        stale_threshold_sec=STALE_THRESHOLD,
        scan_ms=scan_ms,
    )

    if stale_keys:
        log_kw["stale_by_source"] = stale_by_source
        log_kw["top_stale"] = top_stale
        log("WARN", "check", **log_kw)
    else:
        log("INFO", "check", **log_kw)

    if with_buckets and ages_sec:
        buckets = build_buckets(ages_sec)
        avg_age = round(sum(ages_sec) / len(ages_sec), 1)
        max_age = round(max(ages_sec), 1)
        log("INFO", "buckets",
            cycle=cycle,
            total_keys=len(ages_sec),
            avg_age_sec=avg_age,
            max_age_sec=max_age,
            distribution=buckets)

    total_ms = round((time.monotonic() - t_check) * 1000, 1)
    log("INFO", "check_complete", cycle=cycle, total_ms=total_ms)


# ── run loop ──────────────────────────────────────────────────────────────────

async def main(with_buckets: bool) -> None:
    log("INFO", "startup",
        check_interval_sec=CHECK_INTERVAL,
        stale_threshold_sec=STALE_THRESHOLD,
        with_buckets=with_buckets)

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=2)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    cycle = 0
    while True:
        cycle += 1
        try:
            await check(r, with_buckets, cycle)
        except Exception as e:
            log("ERROR", "check_failed", cycle=cycle, reason=str(e)[:200])
        await asyncio.sleep(CHECK_INTERVAL)


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Redis staleness monitor for md:* keys"
    )
    parser.add_argument(
        "--buckets",
        action="store_true",
        help="Show age distribution by 1-minute buckets (1-2min, 2-3min, ..., 5+min)",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.buckets))
    except KeyboardInterrupt:
        log("INFO", "stopped")
