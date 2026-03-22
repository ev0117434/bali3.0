# Bali 3.0

Real-time market data collection and spread monitoring system for cryptocurrency arbitrage across 4 exchanges. Generates cross-exchange trading pair dictionaries, streams live best-bid/ask prices, full 10-level order books, and funding rates to Redis, monitors data freshness, and detects spread opportunities across 12 spot→futures directions.

---

## Overview

| Subsystem | Scripts | Purpose | Runtime |
|-----------|---------|---------|---------|
| **Dictionary Generator** | `dictionaries/main.py` | Discovers active trading pairs via REST + WebSocket validation | ~70s, run on demand |
| **md collectors** (×8) | `binance/bybit/okx/gate_spot/futures.py` | Streams real-time best-bid/ask into Redis `md:*` keys | Continuous |
| **ob collectors** (×8) | `ob_binance/bybit/okx/gate_spot/futures.py` | Streams real-time 10-level order books into Redis `ob:*` keys | Continuous |
| **fr collectors** (×4) | `fr_binance/bybit/okx/gate_futures.py` | Streams funding rates into Redis `fr:*` keys | Continuous |
| **Staleness Monitor** | `staleness_monitor.py` | Checks `md:*` key freshness every 60s, alerts on stale data | Continuous |
| **Spread Monitor** | `spread_monitor.py` | Scans 12 spot→futures directions every 0.3s, writes signals + snapshots | Continuous |
| **Redis Monitor** | `redis_monitor.py` | Monitors Redis latency, memory, OOM, slowlog every 5s | Continuous |
| **Telegram Alerts** | `telegram_alert.py` | Sends Telegram notifications for signals, anomalies, crashes, Redis health | Continuous, optional |

**Exchanges:** Binance · Bybit · OKX · Gate.io
**Markets:** Spot + Futures (USDT/USDC perpetuals) for each exchange
**Total data collectors:** 20 (8 md + 8 ob + 4 fr)

---

## Requirements

- Python 3.10+
- Redis (configured via `setup_redis.sh`, recommended maxmemory ≥ 40 GB)

```bash
pip install websockets orjson redis[hiredis] rich
```

---

## Quick Start

### 1. Set up Redis

```bash
sudo bash setup_redis.sh           # install, configure, start
sudo bash setup_redis.sh --check   # verify health
```

### 2. Generate trading pair dictionaries

```bash
cd dictionaries && python3 main.py && cd ..
```

~70 seconds. Generates 8 subscription files for collectors and 12 combination files for spread monitor.

### 3. Start everything

```bash
python3 run_all.py                  # live dashboard (default)
python3 run_all.py --no-dash        # JSON logs only, no TUI
python3 run_all.py --buckets        # + staleness age-bucket distribution
python3 run_all.py --delay 60       # wait 60s before signals/snapshots start
```

On startup:
- Archives previous `logs/` and `signals/` into `old/YYYY-MM-DD_HH-MM/`
- Flushes Redis (SCAN + DEL all keys)
- Starts all 20 collectors + 3 monitors simultaneously
- Renders live dashboard in terminal (updates every 1 second)

Press `Ctrl+C` to stop all processes cleanly.

---

## Architecture

```
┌────────────────────────────────────────────────────────────┐
│                    Dictionary Generator                     │
│                    dictionaries/main.py                     │
│                                                            │
│  Phase 1: REST API fetch from all 4 exchanges (parallel)  │
│  Phase 2: WebSocket validation, 60s window (parallel)      │
│  Phase 3: Build 12 intersection files (combination/)       │
│  Phase 4: Build 8 subscription files (subscribe/)          │
│  Phase 5: Print report                                     │
└────────────────────────────────────────────────────────────┘
          ↓ subscribe/*.txt                ↓ combination/*.txt
┌──────────────────────────┐   ┌──────────────────────────────┐
│   md collectors (×8)     │   │       Spread Monitor         │
│   best-bid/ask → md:*    │   │   collectors/spread_monitor  │
│                          │   │                              │
│   ob collectors (×8)  →Redis  12 directions, 0.3s poll     │
│   10-level OB → ob:*  md:*:*  signals/signals.jsonl+csv    │
│                       ob:*:*  signals/anomalies.jsonl+csv  │
│   fr collectors (×4)  fr:*:*  signals/snapshots/*.csv      │
│   funding rate → fr:* │   └──────────────────────────────────┘
└──────────────────────────┘
                              ┌──────────────────────────────┐
                              │      Staleness Monitor       │
                              │  checks md:* keys every 60s  │
                              ├──────────────────────────────┤
                              │      Redis Monitor           │
                              │  latency + memory every 5s   │
                              └──────────────────────────────┘
```

### Data flow

```
REST API → pairs list → WS validation → active pairs
    → intersections (12 files) → subscribe files (8 files)
        → md collectors  → Redis HASH md:*  + ZSET md:hist:*
        → ob collectors  → Redis HASH ob:*  + ZSET ob:hist:*
        → fr collectors  → Redis HASH fr:*  + ZSET fr:hist:*
            → spread_monitor reads md:*, ob:* → signals + snapshots
            → staleness_monitor scans md:* for stale keys
            → redis_monitor checks Redis health
```

---

## Directory Structure

```
bali3.0/
├── run_all.py                           # Orchestrator: 20 collectors + monitors + dashboard
├── setup_redis.sh                       # Redis install & configuration
├── CLAUDE.md                            # Project notes for AI assistant
│
├── logs/
│   └── collectors_YYYY-MM-DD_HH-MM.log # Auto-rotating, 12h chunks, keeps last 2
│
├── signals/
│   ├── signals.jsonl                    # Normal spread signals (1%–99%)
│   ├── signals.csv                      # Same, CSV format
│   ├── anomalies.jsonl                  # Anomalies (spread ≥100%)
│   ├── anomalies.csv                    # Same, CSV format
│   ├── snapshots/YYYY-MM-DD/HH/        # Per-signal spread evolution (3500s window)
│   │   └── {spot}_{fut}_{sym}_{YYYYMMDD_HHMMSS}.csv  # 89 columns: base + spot OB + fut OB + fr
│   ├── ml_dataset.csv                  # ML features (output of enrich_snapshot.py)
│   └── signal_history.db               # SQLite: per-pair signal history (enrich_snapshot.py)
│
├── old/
│   └── YYYY-MM-DD_HH-MM/               # Archived from previous run
│       ├── logs/
│       └── signals/
│
├── dictionaries/
│   ├── main.py                          # Dictionary generator (5 phases)
│   ├── binance/ bybit/ okx/ gate/       # pairs.py + ws.py + data/
│   ├── combination/                     # 12 intersection files: spot_A ∩ futures_B
│   └── subscribe/                       # 8 subscription files: input for collectors
│
└── collectors/
    ├── hist_writer.py                   # md:hist + snapshot history + OB/FR history reader
    ├── ob_hist_writer.py                # ob:hist history module
    ├── fr_hist_writer.py                # fr:hist history module + read_fr_history()
    ├── source_ids.json                  # Fixed integer IDs for 8 exchange+market sources
    ├── enrich_snapshot.py               # ML dataset generator from snapshot CSVs
    │
    ├── binance_spot.py                  # md: Binance Spot best-bid/ask
    ├── binance_futures.py               # md: Binance Futures best-bid/ask
    ├── bybit_spot.py                    # md: Bybit Spot best-bid/ask
    ├── bybit_futures.py                 # md: Bybit Futures best-bid/ask
    ├── okx_spot.py                      # md: OKX Spot best-bid/ask
    ├── okx_futures.py                   # md: OKX Futures best-bid/ask
    ├── gate_spot.py                     # md: Gate.io Spot best-bid/ask
    ├── gate_futures.py                  # md: Gate.io Futures best-bid/ask
    │
    ├── ob_binance_spot.py               # ob: Binance Spot 10-level order book
    ├── ob_binance_futures.py            # ob: Binance Futures 10-level order book
    ├── ob_bybit_spot.py                 # ob: Bybit Spot 10-level order book
    ├── ob_bybit_futures.py              # ob: Bybit Futures 10-level order book
    ├── ob_okx_spot.py                   # ob: OKX Spot 10-level order book
    ├── ob_okx_futures.py                # ob: OKX Futures 10-level order book
    ├── ob_gate_spot.py                  # ob: Gate.io Spot 10-level order book
    ├── ob_gate_futures.py               # ob: Gate.io Futures 10-level order book
    │
    ├── fr_binance_futures.py            # fr: Binance funding rates (stream !markPrice@arr@1s)
    ├── fr_bybit_futures.py              # fr: Bybit funding rates (tickers channel, delta cache)
    ├── fr_okx_futures.py                # fr: OKX funding rates (WS + REST fallback every 30s)
    ├── fr_gate_futures.py               # fr: Gate.io funding rates (WS + REST fallback + contracts loop every 5m)
    │
    ├── staleness_monitor.py             # Monitors md:* key freshness (threshold: 300s)
    ├── spread_monitor.py                # Detects cross-exchange spread opportunities
    ├── redis_monitor.py                 # Monitors Redis health (latency, memory, OOM)
    ├── telegram_alert.py                # Telegram notifications (requires alert_config.json)
    └── alert_config.json                # Alert config: bot_token, chat_id, thresholds
```

---

## Redis Key Schema

### md — best bid/ask (current)

```
md:{ex}:{mkt}:{symbol}                  →  HASH    b=bid, a=ask, t=ts_ms
md:hist:{ex}:{mkt}:{symbol}:{chunk_id}  →  ZSET    member: bid|ask|ts_ms  score: ts_ms
md:hist:config                          →  STRING   JSON chunk metadata
```

### ob — order book 10 levels

```
ob:{ex}:{mkt}:{symbol}                  →  HASH    41 fields: b1–b10, bq1–bq10, a1–a10, aq1–aq10, t
ob:hist:{ex}:{mkt}:{symbol}:{chunk_id}  →  ZSET    member: b1|bq1|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
ob:hist:config                          →  STRING   JSON chunk metadata
```

### fr — funding rates

```
fr:{ex}:{symbol}                        →  HASH    r=rate, nr=next predicted, ft=next funding time ms, ts=recv ms
fr:hist:{ex}:{symbol}:{chunk_id}        →  ZSET    member: r|nr|ft|ts_ms  score: ts_ms
fr:hist:config                          →  STRING   JSON chunk metadata
```

**Exchange codes:** `bn` = Binance · `bb` = Bybit · `ok` = OKX · `gt` = Gate.io
**Market codes:** `s` = spot · `f` = futures
**History:** 20-min chunks, 4 chunks kept (80-min window, always covers last hour), throttle 1 write/sec/symbol

---

## Signals

### Signal files

| File | Content |
|------|---------|
| `signals/signals.jsonl` | Normal signals (spread 1%–99%) |
| `signals/signals.csv` | Same, CSV |
| `signals/anomalies.jsonl` | Anomalies (spread ≥100%) |
| `signals/anomalies.csv` | Same, CSV |

### Signal JSONL format

```json
{
  "ts": 1774090954.48,
  "direction": "gate_spot_bybit_futures",
  "symbol": "VANRYUSDT",
  "spread_pct": 7.8494,
  "spot_ask": "0.005631",
  "fut_bid": "0.006073",
  "spot_ex": "gt",
  "fut_ex": "bb"
}
```

### Snapshot CSV (89 columns)

Each signal opens a snapshot file written every 0.3s for 3500s (≈2500 rows), capturing spread evolution.
At the moment of signal, 1 hour of historical data is prepended (also 89 columns).

```
spot_source_id, fut_source_id, symbol, ask_spot, bid_futures, spread_pct, ts,  ← 7 base columns
s_b1..s_b10, s_bq1..s_bq10, s_a1..s_a10, s_aq1..s_aq10,                      ← 40 spot OB columns
f_b1..f_b10, f_bq1..f_bq10, f_a1..f_a10, f_aq1..f_aq10,                      ← 40 futures OB columns
fr_r, fr_ft                                                                     ← 2 FR columns
```

Total: **89 columns** = 7 base + 40 spot OB + 40 futures OB + 2 FR.

`spot_source_id`/`fut_source_id` are integer IDs (from `collectors/source_ids.json`):
`binance_spot=0, binance_futures=1, bybit_spot=2, bybit_futures=3, okx_spot=4, okx_futures=5, gate_spot=6, gate_futures=7`

---

## Funding Rate Collectors

| Collector | Method | Update frequency | Notes |
|-----------|--------|-----------------|-------|
| `fr_binance_futures` | WS stream `!markPrice@arr@1s` | Every 1s | Single stream for all symbols, reconnects before 23h forced close |
| `fr_bybit_futures` | WS `tickers` channel | On change | Delta cache pattern, persists across reconnects |
| `fr_okx_futures` | WS `funding-rate` + REST fallback | On change + every 30s | WS pushes ~every 30 min; REST guarantees freshness |
| `fr_gate_futures` | WS `futures.tickers` + REST fallback + contracts loop | On change + every 30s (rates) / every 5m (ft) | REST fetches all symbols in one call; contracts loop fills `ft` field |

---

## WebSocket Protocol Summary

### md collectors

| Collector | Endpoint | Channel | Ping |
|-----------|----------|---------|------|
| binance_spot/futures | `stream.binance.com:9443` / `fstream.binance.com` | `{sym}@bookTicker` | Built-in 20s |
| bybit_spot/futures | `stream.bybit.com/v5/public/spot\|linear` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s |
| okx_spot/futures | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | `"ping"` 25s |
| gate_spot/futures | `api.gateio.ws/ws/v4/` / `fx-ws.gateio.ws` | `spot\|futures.book_ticker` | timestamped JSON 20s |

### ob collectors

| Collector | Channel | Update model |
|-----------|---------|-------------|
| ob_binance | `{sym}@depth20@100ms` | Full snapshot every 100ms |
| ob_bybit | `orderbook.50.{SYM}` | Snapshot + incremental delta |
| ob_okx | `books` | Snapshot + incremental (up to 400 levels), top-10 extracted |
| ob_gate | `spot\|futures.order_book_update` | Full 20-level snapshot |

---

## ML Dataset

`collectors/enrich_snapshot.py` converts raw snapshot CSVs into a machine-learning dataset.

```bash
# Process all snapshots in signals/snapshots/ → signals/ml_dataset.csv
python3 collectors/enrich_snapshot.py

# Rebuild from scratch (resets signal_history.db)
python3 collectors/enrich_snapshot.py --rebuild

# Process a single file
python3 collectors/enrich_snapshot.py signals/snapshots/2026-03-21/11/gate_binance_BTCUSDT_20260321_110000.csv
```

Output: `signals/ml_dataset.csv` — one row per snapshot file, **65 features** + `converged` (target) + `split` (train/test).

| Feature group | Features |
|---------------|---------|
| Spread dynamics | mean/std/min/max/slope/velocity/percentile over 5m/15m/30m/60m windows |
| Order book | imbalance, volume, skew, walls, level-2/5 spreads, best spreads (at t=0) |
| Funding rate | `fr_r_abs`, `mins_to_funding`, `funding_urgency` (fr_r × 1/mins²), `fr_favorable` |
| Context | source IDs, direction_id, is_gate, hour/weekday/is_weekend |
| Pair history | `repeat_count`, `hours_since_first`, `prev_converged`, `pair_success_rate` |

`converged = 1` if spread drops below 0.3% at any point during the 3500s window.
`pair_success_rate` is computed only from the training split (80% by time) to avoid data leakage.

---

## Logging

All scripts emit JSON lines to stdout:

```json
{"ts": 1711234567.12, "lvl": "INFO", "script": "binance_spot", "event": "stats",
 "msgs_total": 98808, "msgs_per_sec": 823.4, "flushes_total": 988, "avg_pipeline_ms": 0.412}
```

Common events: `startup`, `connecting`, `connected`, `subscribed`, `first_message`, `stats`, `disconnected`, `reconnecting`, `rest_refresh`, `collector_crashed`.

Logs auto-written to `logs/collectors_YYYY-MM-DD_HH-MM.log` (12-hour rotation, 2 files kept).

```bash
tail -f logs/collectors_*.log | jq .
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log
jq 'select(.event == "stats" and .script != "spread_monitor")' logs/collectors_*.log
```

---

## Redis Health

```bash
redis-cli -s /var/run/redis/redis.sock PING
redis-cli -s /var/run/redis/redis.sock INFO memory
redis-cli -s /var/run/redis/redis.sock DBSIZE
```

Thresholds (enforced by `redis_monitor.py`):

| Metric | WARN | CRIT |
|--------|------|------|
| PING latency | > 6ms | > 30ms |
| Write latency | > 15ms | > 60ms |
| Memory usage | > 70% | > 85% |
| Fragmentation ratio | > 1.5 | > 2.0 |

Recommended maxmemory: **40 GB** (typical usage: ~500 MB with history).

**Important:** `volatile-ttl` eviction + no TTL on keys = OOM blocks ALL writes. Keep maxmemory ≥ 40 GB.
