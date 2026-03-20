#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
spread_monitor.py — мониторинг спреда spot→futures по 12 направлениям.

Каждые 0.3с читает Redis, считает (futures_bid - spot_ask) / spot_ask * 100.
Сигнал = спред > MIN_SPREAD_PCT И данные свежее STALE_THRESHOLD секунд.
Кулдаун COOLDOWN_SEC секунд на (направление, символ) после сигнала.

Выход:
    signals/signals.jsonl        — нормальные сигналы (1% .. 100%)
    signals/signals.csv          — нормальные сигналы CSV
    signals/anomalies.jsonl      — аномалии (спред >= 100%)
    signals/anomalies.csv        — аномалии CSV
"""

import asyncio
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import orjson
import redis.asyncio as aioredis

from hist_writer import write_snapshot_history, SNAPSHOT_CSV_HEADER, OB_EMPTY, OB_LEVELS

SCRIPT = "spread_monitor"

COMBO_DIR       = Path(__file__).parent.parent / "dictionaries" / "combination"
SIGNALS_DIR     = Path(__file__).parent.parent / "signals"
SIGNAL_JSONL    = SIGNALS_DIR / "signals.jsonl"
SIGNAL_CSV      = SIGNALS_DIR / "signals.csv"
ANOMALY_JSONL   = SIGNALS_DIR / "anomalies.jsonl"
ANOMALY_CSV     = SIGNALS_DIR / "anomalies.csv"
SNAPSHOT_DIR    = SIGNALS_DIR / "snapshots"

REDIS_SOCK      = "/var/run/redis/redis.sock"

POLL_INTERVAL   = 0.3     # секунд между циклами
MIN_SPREAD_PCT  = 1.0     # минимальный спред для сигнала
STALE_THRESHOLD = 300     # секунд — пропускать если данные старше
COOLDOWN_SEC    = 3500    # секунд кулдаун на (направление, символ)
ANOMALY_THRESHOLD = 100.0 # спред >= этого значения → аномалия
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


def _format_ob(ob_dict: dict) -> str:
    """
    Convert ob HASH (bytes keys/values from HGETALL) to 40 comma-separated CSV values:
    b1,bq1,...,b10,bq10,a1,aq1,...,a10,aq10
    Returns OB_EMPTY if the dict is empty (ob collector not running or pair missing).
    """
    if not ob_dict:
        return OB_EMPTY
    parts = []
    for i in range(1, OB_LEVELS + 1):
        parts.append(ob_dict.get(f"b{i}".encode(),  b"").decode())
        parts.append(ob_dict.get(f"bq{i}".encode(), b"").decode())
    for i in range(1, OB_LEVELS + 1):
        parts.append(ob_dict.get(f"a{i}".encode(),  b"").decode())
        parts.append(ob_dict.get(f"aq{i}".encode(), b"").decode())
    return ",".join(parts)


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
    r:                aioredis.Redis,
    directions:       dict,
    cooldowns:        dict,
    active_snapshots: dict,
    f_jsonl,
    f_csv,
    f_anomaly_jsonl,
    f_anomaly_csv,
    counters:         dict,
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

    # Batch-fetch OB data for all pairs with active snapshots (one pipeline)
    ob_cache: dict[bytes, dict] = {}
    if active_snapshots:
        snap_pairs = list(active_snapshots.keys())
        ob_keys: list[bytes] = []
        for direction, sym in snap_pairs:
            spot_ex_s, fut_ex_s, _ = directions[direction]
            ob_keys.append(f"ob:{spot_ex_s}:s:{sym}".encode())
            ob_keys.append(f"ob:{fut_ex_s}:f:{sym}".encode())
        async with r.pipeline(transaction=False) as pipe:
            for k in ob_keys:
                pipe.hgetall(k)
            ob_results = await pipe.execute()
        ob_cache = dict(zip(ob_keys, ob_results))

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
            spread_r   = round(spread_pct, 4)
            spot_ask_s = spot_ask_b.decode()
            fut_bid_s  = fut_bid_b.decode()
            spot_name  = CODE_NAME.get(spot_ex, spot_ex)
            fut_name   = CODE_NAME.get(fut_ex,  fut_ex)

            # ── write to active snapshot (every cycle, regardless of threshold) ──
            snap_key = (direction, sym)
            snap     = active_snapshots.get(snap_key)
            if snap:
                if now <= snap["expires"]:
                    s_ob = _format_ob(ob_cache.get(f"ob:{spot_ex}:s:{sym}".encode(), {}))
                    f_ob = _format_ob(ob_cache.get(f"ob:{fut_ex}:f:{sym}".encode(), {}))
                    snap_row = (
                        f"{spot_name},{fut_name},{sym},"
                        f"{spot_ask_s},{fut_bid_s},{spread_r},{now_ms},"
                        f"{s_ob},{f_ob}\n"
                    ).encode()
                    try:
                        snap["fh"].write(snap_row)
                        snap["fh"].flush()
                    except Exception:
                        pass
                else:
                    try:
                        snap["fh"].close()
                    except Exception:
                        pass
                    del active_snapshots[snap_key]

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

            # ── open snapshot file ────────────────────────────────────────────
            if snap_key not in active_snapshots:
                _now_utc  = datetime.now(timezone.utc)
                _ts_str   = _now_utc.strftime('%Y%m%d_%H%M%S')
                _snap_dir = SNAPSHOT_DIR / _now_utc.strftime('%Y-%m-%d') / _now_utc.strftime('%H')
                _snap_dir.mkdir(parents=True, exist_ok=True)
                _fname    = _snap_dir / f"{spot_name}_{fut_name}_{sym}_{_ts_str}.csv"
                _fh       = open(_fname, "wb")
                _fh.write(SNAPSHOT_CSV_HEADER.encode())
                _fh.flush()
                # prepend 1-hour history (includes OB history)
                hist_rows = await write_snapshot_history(
                    r, _fh, spot_ex, fut_ex, sym, spot_name, fut_name)
                if hist_rows:
                    log("INFO", "snapshot_history_written",
                        direction=direction, symbol=sym, rows=hist_rows)
                active_snapshots[snap_key] = {"fh": _fh, "expires": now + COOLDOWN_SEC}
                log("INFO", "snapshot_opened", direction=direction, symbol=sym,
                    file=str(_fname), duration_sec=COOLDOWN_SEC)
                # first live row — fetch OB for this new pair
                try:
                    async with r.pipeline(transaction=False) as _pipe:
                        _pipe.hgetall(f"ob:{spot_ex}:s:{sym}".encode())
                        _pipe.hgetall(f"ob:{fut_ex}:f:{sym}".encode())
                        _s_ob_raw, _f_ob_raw = await _pipe.execute()
                    _s_ob = _format_ob(_s_ob_raw)
                    _f_ob = _format_ob(_f_ob_raw)
                    _fh.write((
                        f"{spot_name},{fut_name},{sym},"
                        f"{spot_ask_s},{fut_bid_s},{spread_r},{now_ms},"
                        f"{_s_ob},{_f_ob}\n"
                    ).encode())
                    _fh.flush()
                except Exception:
                    pass

            is_anomaly = spread_pct >= ANOMALY_THRESHOLD

            log("INFO", "anomaly" if is_anomaly else "signal",
                direction=direction,
                symbol=sym,
                spread_pct=spread_r,
                spot_ask=spot_ask_s,
                fut_bid=fut_bid_s,
                spot_ex=spot_ex,
                fut_ex=fut_ex,
                cooldown_until=round(now + COOLDOWN_SEC),
            )

            record = {
                "ts":         now,
                "direction":  direction,
                "symbol":     sym,
                "spread_pct": spread_r,
                "spot_ask":   spot_ask_s,
                "fut_bid":    fut_bid_s,
                "spot_ex":    spot_ex,
                "fut_ex":     fut_ex,
            }
            csv_line = (
                f"{spot_name},{fut_name},{sym},"
                f"{spot_ask_s},{fut_bid_s},{spread_r},"
                f"{now_ms}\n"
            ).encode()

            if is_anomaly:
                counters["anomalies"] += 1
                try:
                    f_anomaly_jsonl.write(orjson.dumps(record) + b"\n")
                    f_anomaly_jsonl.flush()
                except Exception as e:
                    log("WARN", "anomaly_jsonl_write_error", reason=str(e)[:120])
                try:
                    f_anomaly_csv.write(csv_line)
                    f_anomaly_csv.flush()
                except Exception as e:
                    log("WARN", "anomaly_csv_write_error", reason=str(e)[:120])
            else:
                try:
                    f_jsonl.write(orjson.dumps(record) + b"\n")
                    f_jsonl.flush()
                except Exception as e:
                    log("WARN", "jsonl_write_error", reason=str(e)[:120])
                try:
                    f_csv.write(csv_line)
                    f_csv.flush()
                except Exception as e:
                    log("WARN", "csv_write_error", reason=str(e)[:120])


# ── stats loop ────────────────────────────────────────────────────────────────

async def stats_loop(counters: dict, cooldowns: dict, active_snapshots: dict) -> None:
    prev_signals = 0
    while True:
        await asyncio.sleep(STATS_INTERVAL)

        now = time.time()

        # Cleanup expired cooldowns
        expired = [k for k, v in cooldowns.items() if v < now]
        for k in expired:
            del cooldowns[k]

        # Cleanup snapshots that expired while their pair had stale data
        expired_snaps = [k for k, v in active_snapshots.items() if v["expires"] < now]
        for k in expired_snaps:
            try:
                active_snapshots[k]["fh"].close()
            except Exception:
                pass
            del active_snapshots[k]

        signals = counters["signals"]
        log("INFO", "stats",
            signals_total         = signals,
            signals_per_interval  = signals - prev_signals,
            anomalies_total       = counters["anomalies"],
            cooldowns_active      = len(cooldowns),
            snapshots_active      = len(active_snapshots),
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

    # Open all output files (append mode — survive restarts)
    f_jsonl        = open(SIGNAL_JSONL,  "ab")
    f_csv          = open(SIGNAL_CSV,    "ab")
    f_anomaly_jsonl = open(ANOMALY_JSONL, "ab")
    f_anomaly_csv   = open(ANOMALY_CSV,   "ab")

    # Write CSV headers only if files are empty
    if SIGNAL_CSV.stat().st_size == 0:
        f_csv.write(CSV_HEADER.encode())
        f_csv.flush()
    if ANOMALY_CSV.stat().st_size == 0:
        f_anomaly_csv.write(CSV_HEADER.encode())
        f_anomaly_csv.flush()

    log("INFO", "signal_files_opened",
        signals_jsonl=str(SIGNAL_JSONL),
        signals_csv=str(SIGNAL_CSV),
        anomalies_jsonl=str(ANOMALY_JSONL),
        anomalies_csv=str(ANOMALY_CSV),
        anomaly_threshold_pct=ANOMALY_THRESHOLD,
    )

    cooldowns:        dict = {}
    active_snapshots: dict = {}
    counters = {
        "signals":          0,
        "anomalies":        0,
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
                await scan_cycle(r, directions, cooldowns, active_snapshots,
                                 f_jsonl, f_csv, f_anomaly_jsonl, f_anomaly_csv, counters)
            except Exception as e:
                log("ERROR", "cycle_error", reason=str(e)[:200])
            elapsed = time.monotonic() - t0
            counters["last_cycle_ms"] = elapsed * 1000
            cycle_count += 1
            await asyncio.sleep(max(0.0, POLL_INTERVAL - elapsed))

    await asyncio.gather(
        poll_loop(),
        stats_loop(counters, cooldowns, active_snapshots),
    )


if __name__ == "__main__":
    asyncio.run(main())
