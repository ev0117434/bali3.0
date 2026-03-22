#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
enrich_snapshot.py — enrich snapshot CSVs with per-row metrics, and/or build ML dataset.

Modes
-----
File enrichment (default when a file is given):
    python3 collectors/enrich_snapshot.py <file.csv>
    Adds 20 computed columns to every row and writes <file>_enriched.csv.
    Flash-spike filter applies: if signal spread doesn't hold for 3 cycles, skipped.

ML dataset (--ml flag, or no file given):
    python3 collectors/enrich_snapshot.py --ml           # process all new snapshots
    python3 collectors/enrich_snapshot.py --ml --rebuild # drop DB + reprocess all
    Each snapshot → ONE summary row in signals/ml_dataset.csv.

New columns added by enrichment (20 total)
------------------------------------------
OB (per-row, from s_/f_ columns):
    ob_imbalance_spot, ob_imbalance_fut   — (bid_qty - ask_qty) / total, top-5 levels
    ob_vol_spot, ob_vol_fut               — sum of all 10 bid+ask quantities
    ob_skew_spot, ob_skew_fut             — (top-5 qty - bottom-5 qty) / total
    bid_wall_spot, ask_wall_fut           — 1 if max_qty / mean_qty > 3 (top-5)
    spread_l2, spread_l5                  — (fut_bid_L2 - spot_ask_L2) / spot_ask_L2 * 100
    ba_spread_spot, ba_spread_fut         — best_bid - best_ask (absolute)
FR (per-row, from fr_r + fr_ft + ts):
    fr_r_abs                              — abs(fr_r)
    mins_to_funding                       — (fr_ft - ts) / 60_000
    funding_urgency                       — fr_r / mins_to_funding²
    fr_favorable                          — 1 if FR direction favors spread convergence
Rolling spread (per-row, window over past rows):
    spread_mean_5m, spread_mean_15m       — rolling mean of spread_pct
    spread_std_5m                         — rolling std of spread_pct
    spread_vel_5m                         — (spread_now - spread_5min_ago) / 300 (%/s)
"""

import argparse
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import json
import numpy as np
import pandas as pd

# ── paths ─────────────────────────────────────────────────────────────────────

SIGNALS_DIR   = Path(__file__).parent.parent / "signals"
SNAPSHOTS_DIR = SIGNALS_DIR / "snapshots"
ML_DATASET    = SIGNALS_DIR / "ml_dataset.csv"
HISTORY_DB    = SIGNALS_DIR / "signal_history.db"

# ── constants ─────────────────────────────────────────────────────────────────

CONVERGENCE_THRESHOLD = 0.3    # spread_pct <= this → converged
TRAIN_RATIO           = 0.8
IMBALANCE_LEVELS      = 5      # top-N levels for imbalance
WALL_RATIO            = 3.0    # max/mean > this → wall detected
OB_LEVELS             = 10
HOLD_CYCLES           = 3      # spread must hold for N consecutive live cycles
HOLD_TOLERANCE        = 0.3    # allowed drop from signal spread (%), i.e. lower_bound = spread_at_t0 - 0.3

_SOURCE_IDS_PATH = Path(__file__).parent / "source_ids.json"
SOURCE_IDS: dict[str, int] = {
    k: v for k, v in json.load(open(_SOURCE_IDS_PATH)).items()
    if not k.startswith("_")
}
# For filename validation: known exchange names
_EXCHANGES = {"binance", "bybit", "okx", "gate"}


# ── SQLite history DB ─────────────────────────────────────────────────────────

def init_db(db_path: Path) -> sqlite3.Connection:
    con = sqlite3.connect(db_path)
    con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            direction     TEXT    NOT NULL,
            symbol        TEXT    NOT NULL,
            t0_ts         REAL    NOT NULL,
            snapshot_file TEXT    NOT NULL UNIQUE,
            converged     INTEGER NOT NULL,
            processed_at  REAL    NOT NULL
        )
    """)
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_pair "
        "ON signals(direction, symbol, t0_ts)"
    )
    con.commit()
    return con


def db_is_processed(con: sqlite3.Connection, snapshot_file: str) -> bool:
    return con.execute(
        "SELECT 1 FROM signals WHERE snapshot_file = ?", (snapshot_file,)
    ).fetchone() is not None


def db_insert(
    con: sqlite3.Connection,
    direction: str, symbol: str, t0_ts: float,
    snapshot_file: str, converged: int,
) -> None:
    con.execute(
        "INSERT OR IGNORE INTO signals "
        "(direction, symbol, t0_ts, snapshot_file, converged, processed_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (direction, symbol, t0_ts, snapshot_file, converged, time.time()),
    )
    con.commit()


def db_pair_history(
    con: sqlite3.Connection, direction: str, symbol: str, before_ts: float
) -> list[int]:
    """Returns list of converged values for this pair before before_ts, oldest first."""
    rows = con.execute(
        "SELECT converged FROM signals "
        "WHERE direction=? AND symbol=? AND t0_ts < ? "
        "ORDER BY t0_ts",
        (direction, symbol, before_ts),
    ).fetchall()
    return [r[0] for r in rows]


def db_first_ts(
    con: sqlite3.Connection, direction: str, symbol: str, before_ts: float
) -> float | None:
    row = con.execute(
        "SELECT MIN(t0_ts) FROM signals "
        "WHERE direction=? AND symbol=? AND t0_ts < ?",
        (direction, symbol, before_ts),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


# ── filename parsing ───────────────────────────────────────────────────────────

def parse_filename(name: str) -> tuple | None:
    """
    binance_gate_FUNUSDT_20260321_181109.csv
    → (spot_exch, fut_exch, symbol, t0_datetime)
    Format: {spot}_{fut}_{SYMBOL}_{YYYYMMDD}_{HHMMSS}
    """
    stem  = Path(name).stem
    parts = stem.split("_")
    if len(parts) < 5:
        return None
    date_s, time_s = parts[-2], parts[-1]
    if len(date_s) != 8 or len(time_s) != 6:
        return None
    spot_exch = parts[0]
    fut_exch  = parts[1]
    symbol    = "_".join(parts[2:-2])
    if spot_exch not in _EXCHANGES or fut_exch not in _EXCHANGES:
        return None
    try:
        dt = datetime.strptime(
            f"{date_s}_{time_s}", "%Y%m%d_%H%M%S"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None
    return spot_exch, fut_exch, symbol, dt


# ── OB helpers ────────────────────────────────────────────────────────────────

def get_ob_arrays(
    row: pd.Series, prefix: str, levels: int = OB_LEVELS
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Returns (bid_prices, bid_qtys, ask_prices, ask_qtys)."""
    def arr(col_tmpl: str) -> np.ndarray:
        vals = []
        for i in range(1, levels + 1):
            try:
                vals.append(float(row[f"{prefix}{col_tmpl}{i}"]))
            except (KeyError, ValueError, TypeError):
                vals.append(np.nan)
        return np.array(vals, dtype=float)
    return arr("b"), arr("bq"), arr("a"), arr("aq")


def imbalance(bid_qty: np.ndarray, ask_qty: np.ndarray,
              top_n: int = IMBALANCE_LEVELS) -> float:
    b = np.nansum(bid_qty[:top_n])
    a = np.nansum(ask_qty[:top_n])
    total = b + a
    return float((b - a) / total) if total > 0 else np.nan


def ob_volume(bid_qty: np.ndarray, ask_qty: np.ndarray) -> float:
    return float(np.nansum(bid_qty) + np.nansum(ask_qty))


def ob_skew(bid_qty: np.ndarray, ask_qty: np.ndarray, top_n: int = 5) -> float:
    """Asymmetry: top N levels vs bottom N levels (bid + ask combined)."""
    top = np.nansum(bid_qty[:top_n]) + np.nansum(ask_qty[:top_n])
    bot = np.nansum(bid_qty[top_n:]) + np.nansum(ask_qty[top_n:])
    total = top + bot
    return float((top - bot) / total) if total > 0 else np.nan


def has_wall(qty: np.ndarray, top_n: int = 5, ratio: float = WALL_RATIO) -> int:
    q = qty[:top_n]
    valid = q[~np.isnan(q)]
    if len(valid) < 2:
        return 0
    mean_q = np.mean(valid)
    return int(mean_q > 0 and np.max(valid) / mean_q > ratio)


def row_imbalance(row: pd.Series, prefix: str) -> float:
    _, bq, _, aq = get_ob_arrays(row, prefix)
    return imbalance(bq, aq)


def row_volume(row: pd.Series, prefix: str) -> float:
    _, bq, _, aq = get_ob_arrays(row, prefix)
    return ob_volume(bq, aq)


# ── spread helpers ────────────────────────────────────────────────────────────

def spread_at_offset(
    hist: pd.DataFrame, t0_sec: float, offset_sec: float,
    max_gap: float = 120.0,
) -> float:
    """Spread value closest to (t0_sec + offset_sec), within max_gap seconds."""
    if hist.empty:
        return np.nan
    target = t0_sec + offset_sec
    idx = (hist["ts_sec"] - target).abs().idxmin()
    row = hist.loc[idx]
    if abs(row["ts_sec"] - target) > max_gap:
        return np.nan
    return float(row["spread_pct"])


def trend_slope(
    spread: pd.Series, ts_sec: pd.Series, min_points: int = 5
) -> float:
    """Linear regression slope of spread vs time (units: %/second)."""
    if len(spread) < min_points:
        return np.nan
    t = (ts_sec - ts_sec.iloc[0]).values.astype(float)
    s = spread.values.astype(float)
    mask = ~np.isnan(s)
    if mask.sum() < min_points:
        return np.nan
    return float(np.polyfit(t[mask], s[mask], 1)[0])


# ── FR helpers ────────────────────────────────────────────────────────────────

def compute_fr_features(row: pd.Series, spread_at_t0: float, t0_ms: float) -> dict:
    fr_r = fr_ft = np.nan
    try:
        fr_r = float(row.get("fr_r", ""))
    except (ValueError, TypeError):
        pass
    try:
        fr_ft = float(row.get("fr_ft", ""))
    except (ValueError, TypeError):
        pass

    fr_r_abs        = abs(fr_r)        if not np.isnan(fr_r)  else np.nan
    mins_to_funding = np.nan
    funding_urgency = np.nan
    fr_favorable    = np.nan

    if not np.isnan(fr_ft) and fr_ft > 0:
        mins_to_funding = (fr_ft - t0_ms) / 60_000
        if mins_to_funding > 0 and not np.isnan(fr_r):
            funding_urgency = fr_r / (mins_to_funding ** 2)

    if not np.isnan(fr_r) and not np.isnan(spread_at_t0):
        # spread > 0: fut > spot → fr < 0 favors convergence (shorts get paid)
        # spread < 0: fut < spot → fr > 0 favors convergence
        fr_favorable = int(fr_r < 0) if spread_at_t0 > 0 else int(fr_r > 0)

    return {
        "fr_r":             fr_r,
        "fr_ft":            fr_ft,
        "fr_r_abs":         fr_r_abs,
        "mins_to_funding":  mins_to_funding,
        "funding_urgency":  funding_urgency,
        "fr_favorable":     fr_favorable,
    }


# ── main snapshot processor ───────────────────────────────────────────────────

def process_snapshot(csv_path: Path, con: sqlite3.Connection) -> dict | None:
    parsed = parse_filename(csv_path.name)
    if parsed is None:
        print(f"  SKIP (bad filename): {csv_path.name}")
        return None

    spot_exch, fut_exch, symbol, t0_dt = parsed
    direction = f"{spot_exch}_spot_{fut_exch}_futures"
    t0_ms     = t0_dt.timestamp() * 1000
    t0_sec    = t0_dt.timestamp()

    try:
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  SKIP (read error): {csv_path.name}: {e}")
        return None

    required = {"ts", "spread_pct", "ask_spot", "bid_futures"}
    if not required.issubset(df.columns):
        print(f"  SKIP (missing columns): {csv_path.name}")
        return None

    df["ts"]         = pd.to_numeric(df["ts"],         errors="coerce")
    df["spread_pct"] = pd.to_numeric(df["spread_pct"], errors="coerce")
    df["ask_spot"]   = pd.to_numeric(df["ask_spot"],   errors="coerce")
    df["ts_sec"]     = df["ts"] / 1000

    # convert all OB columns
    ob_cols = [c for c in df.columns if c[:2] in ("s_", "f_")]
    df[ob_cols] = df[ob_cols].apply(pd.to_numeric, errors="coerce")

    # The filename timestamp is second-precision and can differ by up to ~1s from
    # the actual signal row ts (spread_monitor uses ms-precision time.time()).
    # Find the real signal row: first row with spread >= 1% within ±2s of t0_ms.
    _near = df[
        (df["ts"] >= t0_ms - 2000) & (df["ts"] <= t0_ms + 2000) &
        (df["spread_pct"] >= 1.0)
    ]
    _split_ts = float(_near.iloc[0]["ts"]) if not _near.empty else t0_ms - 1000
    hist = df[df["ts"] < _split_ts].copy()
    live = df[df["ts"] >= _split_ts].copy()

    if live.empty:
        print(f"  SKIP (no live rows): {csv_path.name}")
        return None

    t0_row       = live.iloc[0]
    spread_at_t0 = float(t0_row["spread_pct"]) if not pd.isna(t0_row["spread_pct"]) else np.nan

    # ── flash signal filter ───────────────────────────────────────────────────
    # Spread must hold within [spread_at_t0 - HOLD_TOLERANCE, +inf) for the
    # first HOLD_CYCLES live rows. Drops one-tick spikes that vanish in < 0.9s.
    if not np.isnan(spread_at_t0):
        lower_bound = spread_at_t0 - HOLD_TOLERANCE
        check_rows  = live.head(HOLD_CYCLES)["spread_pct"].dropna()
        if len(check_rows) == 0 or (check_rows < lower_bound).any():
            print(f"  SKIP (flash spike): spread dropped > {HOLD_TOLERANCE}% "
                  f"within {HOLD_CYCLES} cycles (signal={spread_at_t0:.4f}%)")
            return None

    # ── spread dynamics ───────────────────────────────────────────────────────
    hist_60m = hist[hist["ts_sec"] >= t0_sec - 3600]
    hist_30m = hist[hist["ts_sec"] >= t0_sec - 1800]
    hist_15m = hist[hist["ts_sec"] >= t0_sec - 900]
    hist_5m  = hist[hist["ts_sec"] >= t0_sec - 300]

    def safe_mean(s): return float(s.mean()) if not s.empty else np.nan
    def safe_std(s):  return float(s.std())  if len(s) > 1  else np.nan
    def safe_min(s):  return float(s.min())  if not s.empty else np.nan
    def safe_max(s):  return float(s.max())  if not s.empty else np.nan

    sp = "spread_pct"
    spread_mean_60m = safe_mean(hist_60m[sp])
    spread_mean_30m = safe_mean(hist_30m[sp])
    spread_mean_15m = safe_mean(hist_15m[sp])
    spread_mean_5m  = safe_mean(hist_5m[sp])
    spread_std_15m  = safe_std(hist_15m[sp])
    spread_std_5m   = safe_std(hist_5m[sp])
    spread_min_60m  = safe_min(hist_60m[sp])
    spread_max_60m  = safe_max(hist_60m[sp])

    spread_at_m5m  = spread_at_offset(hist, t0_sec, -300)
    spread_at_m15m = spread_at_offset(hist, t0_sec, -900)
    spread_at_m30m = spread_at_offset(hist, t0_sec, -1800)

    slope_30m = trend_slope(hist_30m[sp], hist_30m["ts_sec"])

    vel_5m = np.nan
    if not np.isnan(spread_at_m5m) and not np.isnan(spread_at_t0):
        vel_5m = (spread_at_t0 - spread_at_m5m) / 300.0

    pct_rank = np.nan
    if not hist_60m.empty and not np.isnan(spread_at_t0):
        pct_rank = float((hist_60m[sp] <= spread_at_t0).mean())

    time_above = np.nan
    if not hist_60m.empty and not np.isnan(spread_at_t0):
        time_above = float((hist_60m[sp] > spread_at_t0).mean())

    # ── rolling OB features (last 5m of history) ──────────────────────────────
    imb_spot_series = hist_5m.apply(row_imbalance, axis=1, prefix="s_") if not hist_5m.empty else pd.Series(dtype=float)
    imb_fut_series  = hist_5m.apply(row_imbalance, axis=1, prefix="f_") if not hist_5m.empty else pd.Series(dtype=float)
    vol_spot_series = hist_5m.apply(row_volume,    axis=1, prefix="s_") if not hist_5m.empty else pd.Series(dtype=float)
    vol_fut_series  = hist_5m.apply(row_volume,    axis=1, prefix="f_") if not hist_5m.empty else pd.Series(dtype=float)

    imb_spot_mean_5m  = safe_mean(imb_spot_series)
    imb_spot_std_5m   = safe_std(imb_spot_series)
    imb_fut_mean_5m   = safe_mean(imb_fut_series)
    imb_fut_std_5m    = safe_std(imb_fut_series)
    imb_spot_trend    = trend_slope(imb_spot_series, hist_5m["ts_sec"]) if not hist_5m.empty else np.nan
    imb_fut_trend     = trend_slope(imb_fut_series,  hist_5m["ts_sec"]) if not hist_5m.empty else np.nan
    cross_imb_mean_5m = safe_mean(imb_spot_series * imb_fut_series)

    vol_spot_mean_5m  = safe_mean(vol_spot_series)
    vol_fut_mean_5m   = safe_mean(vol_fut_series)
    vol_ratio_series  = vol_spot_series / vol_fut_series.replace(0, np.nan)
    vol_ratio_trend   = trend_slope(vol_ratio_series, hist_5m["ts_sec"]) if not hist_5m.empty else np.nan

    # ── OB features at t=0 ───────────────────────────────────────────────────
    s_bp, s_bq, s_ap, s_aq = get_ob_arrays(t0_row, "s_")
    f_bp, f_bq, f_ap, f_aq = get_ob_arrays(t0_row, "f_")

    imb_spot_t0  = imbalance(s_bq, s_aq)
    imb_fut_t0   = imbalance(f_bq, f_aq)
    vol_spot_t0  = ob_volume(s_bq, s_aq)
    vol_fut_t0   = ob_volume(f_bq, f_aq)
    skew_spot    = ob_skew(s_bq, s_aq)
    skew_fut     = ob_skew(f_bq, f_aq)
    wall_bid_spot = has_wall(s_bq)
    wall_ask_fut  = has_wall(f_aq)

    spread_level2 = spread_level5 = np.nan
    if not np.isnan(f_bp[1]) and not np.isnan(s_ap[1]) and s_ap[1] > 0:
        spread_level2 = (f_bp[1] - s_ap[1]) / s_ap[1] * 100
    if not np.isnan(f_bp[4]) and not np.isnan(s_ap[4]) and s_ap[4] > 0:
        spread_level5 = (f_bp[4] - s_ap[4]) / s_ap[4] * 100

    best_spread_spot = float(s_bp[0] - s_ap[0]) if not (np.isnan(s_bp[0]) or np.isnan(s_ap[0])) else np.nan
    best_spread_fut  = float(f_bp[0] - f_ap[0]) if not (np.isnan(f_bp[0]) or np.isnan(f_ap[0])) else np.nan

    price_level = np.nan
    mean_ask_60m = safe_mean(hist_60m["ask_spot"])
    cur_ask = float(t0_row["ask_spot"]) if not pd.isna(t0_row.get("ask_spot")) else np.nan
    if not np.isnan(cur_ask) and not np.isnan(mean_ask_60m) and mean_ask_60m > 0:
        price_level = cur_ask / mean_ask_60m

    # ── FR features ───────────────────────────────────────────────────────────
    fr_feats = compute_fr_features(t0_row, spread_at_t0, t0_ms)

    # ── context ───────────────────────────────────────────────────────────────
    hour_of_day    = t0_dt.hour
    day_of_week    = t0_dt.weekday()
    is_weekend     = int(day_of_week >= 5)
    asia_session   = int(0  <= hour_of_day < 8)
    europe_session = int(8  <= hour_of_day < 16)
    us_session     = int(16 <= hour_of_day < 24)
    is_gate        = int("gate" in (spot_exch, fut_exch))

    # ── pair history ──────────────────────────────────────────────────────────
    history             = db_pair_history(con, direction, symbol, t0_sec)
    signal_repeat_count = len(history)
    first_ts            = db_first_ts(con, direction, symbol, t0_sec)
    hours_since_first   = (t0_sec - first_ts) / 3600 if first_ts else 0.0
    prev_converged      = history[-1] if history else np.nan

    # ── target ────────────────────────────────────────────────────────────────
    converged = int(live["spread_pct"].min() <= CONVERGENCE_THRESHOLD)

    return {
        "file":                    csv_path.name,
        "direction":               direction,
        "symbol":                  symbol,
        "t0_ts":                   t0_sec,
        # spread dynamics
        "spread_at_t0":            spread_at_t0,
        "spread_at_minus_5m":      spread_at_m5m,
        "spread_at_minus_15m":     spread_at_m15m,
        "spread_at_minus_30m":     spread_at_m30m,
        "spread_mean_5m":          spread_mean_5m,
        "spread_mean_15m":         spread_mean_15m,
        "spread_mean_30m":         spread_mean_30m,
        "spread_mean_60m":         spread_mean_60m,
        "spread_std_5m":           spread_std_5m,
        "spread_std_15m":          spread_std_15m,
        "spread_min_60m":          spread_min_60m,
        "spread_max_60m":          spread_max_60m,
        "spread_trend_slope":      slope_30m,
        "spread_velocity_last5m":  vel_5m,
        "spread_percentile_rank":  pct_rank,
        "time_above_threshold":    time_above,
        # rolling OB (last 5m)
        "imbalance_spot_mean_5m":  imb_spot_mean_5m,
        "imbalance_spot_std_5m":   imb_spot_std_5m,
        "imbalance_fut_mean_5m":   imb_fut_mean_5m,
        "imbalance_fut_std_5m":    imb_fut_std_5m,
        "imbalance_spot_trend":    imb_spot_trend,
        "imbalance_fut_trend":     imb_fut_trend,
        "cross_imbalance_mean_5m": cross_imb_mean_5m,
        "vol_spot_mean_5m":        vol_spot_mean_5m,
        "vol_fut_mean_5m":         vol_fut_mean_5m,
        "vol_ratio_trend":         vol_ratio_trend,
        # OB at t=0
        "imbalance_spot":          imb_spot_t0,
        "imbalance_fut":           imb_fut_t0,
        "vol_spot":                vol_spot_t0,
        "vol_fut":                 vol_fut_t0,
        "ob_skew_spot":            skew_spot,
        "ob_skew_fut":             skew_fut,
        "bid_wall_spot":           wall_bid_spot,
        "ask_wall_fut":            wall_ask_fut,
        "spread_level2":           spread_level2,
        "spread_level5":           spread_level5,
        "best_spread_spot":        best_spread_spot,
        "best_spread_fut":         best_spread_fut,
        "price_level":             price_level,
        # FR
        **fr_feats,
        # context
        "spot_source_id":          SOURCE_IDS.get(f"{spot_exch}_spot",     -1),
        "fut_source_id":           SOURCE_IDS.get(f"{fut_exch}_futures",   -1),
        "direction_id":            -1,   # filled in second pass
        "is_gate_involved":        is_gate,
        "hour_of_day":             hour_of_day,
        "day_of_week":             day_of_week,
        "is_weekend":              is_weekend,
        "asia_session":            asia_session,
        "europe_session":          europe_session,
        "us_session":              us_session,
        # pair history
        "signal_repeat_count":     signal_repeat_count,
        "hours_since_first_signal": hours_since_first,
        "prev_signal_converged":   prev_converged,
        "pair_success_rate":       np.nan,  # filled in second pass
        # target
        "converged":               converged,
        "split":                   "",       # filled in second pass
    }


# ── second pass: split + pair_success_rate + direction_id ────────────────────

def second_pass(rows: list[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    df.sort_values("t0_ts", inplace=True)
    df.reset_index(drop=True, inplace=True)

    # train/test split by time
    cutoff = int(len(df) * TRAIN_RATIO)
    df["split"] = "test"
    df.iloc[:cutoff, df.columns.get_loc("split")] = "train"

    # direction_id: assign integer label to each unique direction
    directions = {d: i for i, d in enumerate(sorted(df["direction"].unique()))}
    df["direction_id"] = df["direction"].map(directions)

    # pair_success_rate: computed from train only, joined as lookup
    train = df[df["split"] == "train"]
    pair_rates = (
        train.groupby(["direction", "symbol"])["converged"]
        .mean()
        .rename("pair_success_rate")
    )
    global_prior = float(train["converged"].mean()) if len(train) > 0 else 0.5

    df = df.drop(columns=["pair_success_rate"])
    df = df.merge(pair_rates, on=["direction", "symbol"], how="left")
    df["pair_success_rate"] = df["pair_success_rate"].fillna(global_prior)

    return df


# ── per-row file enrichment ───────────────────────────────────────────────────

def enrich_file(csv_path: Path) -> Path | None:
    """
    Read snapshot CSV, apply flash-spike filter, add 20 per-row metric columns,
    write <original_stem>_enriched.csv next to the source file.
    Returns output path, or None if skipped/failed.
    """
    parsed = parse_filename(csv_path.name)
    if parsed is None:
        print(f"  SKIP (bad filename): {csv_path.name}")
        return None

    spot_exch, fut_exch, symbol, t0_dt = parsed
    t0_ms  = t0_dt.timestamp() * 1000

    try:
        df = pd.read_csv(csv_path, dtype=str, low_memory=False)
    except Exception as e:
        print(f"  ERROR (read): {e}")
        return None

    required = {"ts", "spread_pct", "ask_spot", "bid_futures"}
    if not required.issubset(df.columns):
        print(f"  ERROR (missing columns): {required - set(df.columns)}")
        return None

    df["ts"]         = pd.to_numeric(df["ts"],         errors="coerce")
    df["spread_pct"] = pd.to_numeric(df["spread_pct"], errors="coerce")

    ob_cols = [c for c in df.columns if c[:2] in ("s_", "f_")]
    df[ob_cols] = df[ob_cols].apply(pd.to_numeric, errors="coerce")

    for col in ("fr_r", "fr_ft"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # ── flash-spike filter (same logic as ML mode) ────────────────────────────
    _near = df[
        (df["ts"] >= t0_ms - 2000) & (df["ts"] <= t0_ms + 2000) &
        (df["spread_pct"] >= 1.0)
    ]
    _split_ts    = float(_near.iloc[0]["ts"]) if not _near.empty else t0_ms - 1000
    live_check   = df[df["ts"] >= _split_ts].head(HOLD_CYCLES)
    signal_spread = float(_near.iloc[0]["spread_pct"]) if not _near.empty else np.nan

    if not np.isnan(signal_spread):
        lower_bound = signal_spread - HOLD_TOLERANCE
        check_vals  = live_check["spread_pct"].dropna()
        if len(check_vals) == 0 or (check_vals < lower_bound).any():
            print(f"  SKIP (flash spike): spread dropped > {HOLD_TOLERANCE}% "
                  f"within {HOLD_CYCLES} cycles (signal={signal_spread:.4f}%)")
            return None

    df = df.sort_values("ts").reset_index(drop=True)

    # ── OB metrics (per-row, vectorised where possible) ───────────────────────
    def _ob_metrics(row: pd.Series) -> pd.Series:
        s_bp, s_bq, s_ap, s_aq = get_ob_arrays(row, "s_")
        f_bp, f_bq, f_ap, f_aq = get_ob_arrays(row, "f_")

        imb_s = imbalance(s_bq, s_aq)
        imb_f = imbalance(f_bq, f_aq)
        vol_s = ob_volume(s_bq, s_aq)
        vol_f = ob_volume(f_bq, f_aq)
        skw_s = ob_skew(s_bq, s_aq)
        skw_f = ob_skew(f_bq, f_aq)
        wl_bs = has_wall(s_bq)
        wl_af = has_wall(f_aq)

        spr_l2 = spr_l5 = np.nan
        if not np.isnan(f_bp[1]) and not np.isnan(s_ap[1]) and s_ap[1] > 0:
            spr_l2 = (f_bp[1] - s_ap[1]) / s_ap[1] * 100
        if not np.isnan(f_bp[4]) and not np.isnan(s_ap[4]) and s_ap[4] > 0:
            spr_l5 = (f_bp[4] - s_ap[4]) / s_ap[4] * 100

        ba_s = float(s_bp[0] - s_ap[0]) if not (np.isnan(s_bp[0]) or np.isnan(s_ap[0])) else np.nan
        ba_f = float(f_bp[0] - f_ap[0]) if not (np.isnan(f_bp[0]) or np.isnan(f_ap[0])) else np.nan

        return pd.Series([imb_s, imb_f, vol_s, vol_f, skw_s, skw_f,
                          wl_bs, wl_af, spr_l2, spr_l5, ba_s, ba_f])

    ob_cols_new = [
        "ob_imbalance_spot", "ob_imbalance_fut",
        "ob_vol_spot",       "ob_vol_fut",
        "ob_skew_spot",      "ob_skew_fut",
        "bid_wall_spot",     "ask_wall_fut",
        "spread_l2",         "spread_l5",
        "ba_spread_spot",    "ba_spread_fut",
    ]
    df[ob_cols_new] = df.apply(_ob_metrics, axis=1).values

    # ── FR derived metrics ────────────────────────────────────────────────────
    if "fr_r" in df.columns and "fr_ft" in df.columns:
        df["fr_r_abs"]        = df["fr_r"].abs()
        mins                  = (df["fr_ft"] - df["ts"]) / 60_000
        df["mins_to_funding"] = mins.where(mins > 0, np.nan)
        valid_urgency         = df["mins_to_funding"].notna() & df["fr_r"].notna()
        df["funding_urgency"] = np.where(
            valid_urgency, df["fr_r"] / (df["mins_to_funding"] ** 2), np.nan
        )
        valid_fav = df["fr_r"].notna() & df["spread_pct"].notna()
        df["fr_favorable"] = np.where(
            valid_fav,
            np.where(df["spread_pct"] > 0,
                     (df["fr_r"] < 0).astype(float),
                     (df["fr_r"] > 0).astype(float)),
            np.nan,
        )
    else:
        for col in ("fr_r_abs", "mins_to_funding", "funding_urgency", "fr_favorable"):
            df[col] = np.nan

    # ── rolling spread metrics (time-indexed) ─────────────────────────────────
    # Use DatetimeIndex so pandas rolling() respects actual elapsed time,
    # not row count (rows vary: ~1/s in history, ~3.3/s in live window).
    dt_idx  = pd.to_datetime(df["ts"], unit="ms")
    sp_ts   = pd.Series(df["spread_pct"].values, index=dt_idx, dtype=float)

    r5      = sp_ts.rolling("5min",  min_periods=1)
    r15     = sp_ts.rolling("15min", min_periods=1)

    df["spread_mean_5m"]  = r5.mean().values
    df["spread_mean_15m"] = r15.mean().values
    df["spread_std_5m"]   = r5.std(ddof=1).values   # NaN when only 1 point

    # velocity: (spread_now - spread_at_oldest_point_in_5m_window) / window_sec
    spread_5m_ago = r5.apply(lambda x: x.iloc[0], raw=False)
    elapsed_5m    = r5.apply(lambda x: (x.index[-1] - x.index[0]).total_seconds()
                             if len(x) > 1 else np.nan, raw=False)
    df["spread_vel_5m"] = np.where(
        elapsed_5m.values > 0,
        (sp_ts.values - spread_5m_ago.values) / elapsed_5m.values,
        np.nan,
    )

    # ── write enriched file ───────────────────────────────────────────────────
    out_path = csv_path.parent / (csv_path.stem + "_enriched.csv")
    df.to_csv(out_path, index=False, float_format="%.6g")
    return out_path


# ── entry point ───────────────────────────────────────────────────────────────

def find_all_snapshots() -> list[Path]:
    return sorted(SNAPSHOTS_DIR.rglob("*.csv"), key=lambda p: p.name)


def run(files: list[Path], rebuild: bool = False) -> None:
    if rebuild and HISTORY_DB.exists():
        HISTORY_DB.unlink()
        print("Rebuilt: dropped signal_history.db")

    con = init_db(HISTORY_DB)

    rows: list[dict] = []
    total  = len(files)
    done   = skipped = failed = 0

    for i, csv_path in enumerate(files, 1):
        print(f"[{i}/{total}] {csv_path.name}", end=" ... ")
        sys.stdout.flush()

        if not rebuild and db_is_processed(con, csv_path.name):
            print("already processed")
            skipped += 1
            continue

        row = process_snapshot(csv_path, con)
        if row is None:
            failed += 1
            continue

        db_insert(con, row["direction"], row["symbol"],
                  row["t0_ts"], csv_path.name, row["converged"])
        rows.append(row)
        done += 1
        print(f"OK  converged={row['converged']}  "
              f"spread={row['spread_at_t0']:.3f}%")

    con.close()

    if not rows:
        print(f"\nNo new snapshots to process. "
              f"(skipped={skipped}, failed={failed})")
        return

    print(f"\nProcessed {done} snapshots "
          f"(skipped={skipped}, failed={failed})")
    print("Running second pass (split / pair_success_rate / direction_id) ...")

    df = second_pass(rows)

    # append to existing dataset or write fresh
    write_header = rebuild or not ML_DATASET.exists()
    df.to_csv(ML_DATASET, mode="w" if write_header else "a",
              index=False, header=write_header)

    train = df[df["split"] == "train"]
    test  = df[df["split"] == "test"]
    conv_rate = df["converged"].mean() * 100

    print(f"\nDataset: {len(df)} rows  "
          f"(train={len(train)}, test={len(test)})")
    print(f"Convergence rate: {conv_rate:.1f}%")
    print(f"Global prior (train): "
          f"{train['converged'].mean()*100:.1f}%" if len(train) > 0 else "")
    print(f"Output: {ML_DATASET}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Enrich Bali 3.0 snapshot CSVs with per-row metrics, "
                    "or build ML dataset summary."
    )
    parser.add_argument(
        "file", nargs="?",
        help="Snapshot CSV to enrich (adds per-row metrics, writes <name>_enriched.csv)"
    )
    parser.add_argument(
        "--ml", action="store_true",
        help="ML dataset mode: summarise snapshots into signals/ml_dataset.csv"
    )
    parser.add_argument(
        "--rebuild", action="store_true",
        help="(--ml only) drop DB and reprocess all snapshots from scratch"
    )
    args = parser.parse_args()

    # ── file enrichment mode ──────────────────────────────────────────────────
    if args.file and not args.ml:
        csv_path = Path(args.file)
        if not csv_path.exists():
            print(f"File not found: {csv_path}")
            return
        print(f"Enriching: {csv_path.name} ...")
        out = enrich_file(csv_path)
        if out:
            n_new = 20  # new columns added
            rows  = sum(1 for _ in open(out)) - 1
            print(f"OK  →  {out.name}  ({rows} rows, +{n_new} columns)")
        return

    # ── ML dataset mode ───────────────────────────────────────────────────────
    if args.file:
        files = [Path(args.file)]
    else:
        files = find_all_snapshots()
        if not files:
            print(f"No snapshots found in {SNAPSHOTS_DIR}")
            return
        print(f"Found {len(files)} snapshot(s)")

    run(files, rebuild=args.rebuild)


if __name__ == "__main__":
    main()
