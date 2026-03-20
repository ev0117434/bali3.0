# Bali 3.0

Real-time market data collection and spread monitoring system for cryptocurrency arbitrage across 4 exchanges. Generates cross-exchange trading pair dictionaries, streams live order book data to Redis, monitors data freshness, and detects spread opportunities across 12 spot→futures directions.

---

## Overview

| Subsystem | Purpose | Runtime |
|-----------|---------|---------|
| **Dictionary Generator** | Discovers active trading pairs via REST + WebSocket validation | ~70 seconds, run on demand |
| **Collectors** (×8) | Streams real-time bid/ask from WebSocket feeds into Redis | Continuous |
| **Staleness Monitor** | Checks Redis key freshness every 60s, alerts on stale data | Continuous |
| **Spread Monitor** | Scans 12 spot→futures directions every 0.3s, writes signals | Continuous |

**Exchanges:** Binance · Bybit · OKX · Gate.io
**Markets:** Spot + Futures (USDT/USDC perpetuals) for each exchange
**Redis keys:** `md:{exchange}:{market}:{symbol}` → `{b: bid, a: ask, t: timestamp_ms}`

---

## Requirements

- Python 3.10+
- Redis (configured via `setup_redis.sh`)

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

~70 seconds. Generates subscription files for collectors and 12 combination files for spread monitor.

### 3. Start everything

```bash
python3 run_all.py
```

- Dashboard renders in terminal (stderr), updates every 2 seconds
- JSON logs auto-written to `logs/collectors_YYYY-MM-DD_HH-MM.log` (12-hour chunks, keeps last 2)
- Redis is flushed automatically on startup
- Press `Ctrl+C` to stop

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Dictionary Generator                       │
│                    dictionaries/main.py                      │
│                                                             │
│  Phase 1: REST fetch (parallel, ~3-5s)                     │
│  Phase 2: WebSocket validation (parallel, 60s window)       │
│  Phase 3: Build 12 intersection files (set ∩)               │
│  Phase 4: Build 8 subscription files (set ∪)                │
│  Phase 5: Print report                                      │
└─────────────────────────────────────────────────────────────┘
              ↓ subscribe/*.txt          ↓ combination/*.txt
┌─────────────────────────┐   ┌────────────────────────────────┐
│       Collectors        │   │        Spread Monitor          │
│                         │   │    collectors/spread_monitor.py│
│  binance_spot           │   │                                │
│  binance_futures        │   │  12 directions, 0.3s poll      │
│  bybit_spot         →Redis  │  (futures_bid-spot_ask)/ask    │
│  bybit_futures      md:*:*:*│  signals/signals.jsonl+csv     │
│  okx_spot               │   │  signals/anomalies.jsonl+csv   │
│  okx_futures            │   └────────────────────────────────┘
│  gate_spot              │
│  gate_futures           │   ┌────────────────────────────────┐
│                         │   │      Staleness Monitor         │
│  staleness_monitor      │   │  checks md:* keys every 60s    │
└─────────────────────────┘   └────────────────────────────────┘
```

### Data Flow

```
REST API → pairs list → WS validation → active pairs
    → intersections (12 files) → subscribe files (8 files)
        → collectors → Redis HASH pipeline
            → spread_monitor reads Redis → signals files
            → staleness_monitor scans for stale keys
```

---

## Directory Structure

```
bali3.0/
├── run_all.py                       # Orchestrator: all tasks + live dashboard
├── setup_redis.sh                   # Redis install & configuration
├── logs/
│   └── collectors_YYYY-MM-DD_HH-MM.log  # Auto-rotating, 12h chunks, keeps 2
├── signals/
│   ├── signals.jsonl                # Spread signals 1%–99%
│   ├── signals.csv                  # Same, CSV format
│   ├── anomalies.jsonl              # Anomalies ≥100% spread
│   └── anomalies.csv                # Same, CSV format
│
├── dictionaries/
│   ├── main.py                      # Dictionary generator (5 phases)
│   ├── binance/
│   │   ├── binance_pairs.py         # REST: api.binance.com / fapi.binance.com
│   │   ├── binance_ws.py            # WS: stream.binance.com:9443, bookTicker
│   │   └── data/
│   ├── bybit/
│   │   ├── bybit_pairs.py           # REST: api.bybit.com/v5/market/instruments-info
│   │   ├── bybit_ws.py              # WS: stream.bybit.com/v5/public, orderbook.1
│   │   └── data/
│   ├── okx/
│   │   ├── okx_pairs.py             # REST: okx.com/api/v5/public/instruments
│   │   ├── okx_ws.py                # WS: ws.okx.com:8443/ws/v5/public, tickers
│   │   └── data/                    # Includes *_native.txt (BTC-USDT[-SWAP])
│   ├── gate/
│   │   ├── gate_pairs.py            # REST: api.gateio.ws/api/v4
│   │   ├── gate_ws.py               # WS: api.gateio.ws / fx-ws.gateio.ws
│   │   └── data/                    # Includes *_native.txt (BTC_USDT)
│   ├── combination/                 # 12 files: spot_A ∩ futures_B
│   └── subscribe/                   # 8 files: input for collectors
│       ├── binance/{spot,futures}.txt
│       ├── bybit/{spot,futures}.txt
│       ├── okx/{spot,futures}.txt
│       └── gate/{spot,futures}.txt
│
└── collectors/
    ├── binance_spot.py
    ├── binance_futures.py
    ├── bybit_spot.py
    ├── bybit_futures.py
    ├── okx_spot.py
    ├── okx_futures.py
    ├── gate_spot.py
    ├── gate_futures.py
    ├── staleness_monitor.py
    └── spread_monitor.py
```

---

## run_all.py

Single entry point. On startup:
1. Flushes Redis (SCAN + DEL all keys)
2. Opens rotating log file in `logs/`
3. Starts all 8 collectors + staleness_monitor + spread_monitor as async tasks
4. Renders live dashboard in terminal

```bash
python3 run_all.py                  # dashboard + auto log file
python3 run_all.py --buckets        # + staleness age-bucket distribution
python3 run_all.py --no-dash        # JSON logs only, no TUI
```

### Log rotation

Logs are written automatically — no shell redirect needed:

```
logs/collectors_2026-03-20_14-00.log   ← current chunk
logs/collectors_2026-03-21_02-00.log   ← next chunk (after 12h)
```

When a 3rd chunk would be created, the oldest is deleted. Always 2 files maximum = last 24 hours.

---

## Collectors

### Redis key schema

```
md:{exchange}:{market}:{symbol}   →   HASH
```

| Field | Type | Description |
|-------|------|-------------|
| `b` | string | Best bid price |
| `a` | string | Best ask price |
| `t` | string | Exchange timestamp (ms) |

**Exchange codes:** `bn` = Binance · `bb` = Bybit · `ok` = OKX · `gt` = Gate.io
**Market codes:** `s` = spot · `f` = futures

**Examples:**
```
md:bn:s:BTCUSDT    → Binance Spot BTC/USDT
md:ok:f:BTCUSDT    → OKX Futures BTC/USDT (SWAP)
md:gt:s:ETHUSDT    → Gate.io Spot ETH/USDT
```

### WebSocket protocol details

| Collector | Endpoint | Channel | Ping | Chunk |
|-----------|----------|---------|------|-------|
| binance_spot | `stream.binance.com:9443/stream` | `{sym}@bookTicker` | Built-in 20s | 300 |
| binance_futures | `fstream.binance.com/stream` | `{sym}@bookTicker` | Built-in 20s | 300 |
| bybit_spot | `stream.bybit.com/v5/public/spot` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | single conn |
| bybit_futures | `stream.bybit.com/v5/public/linear` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | single conn |
| okx_spot | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | `"ping"` 25s | 300 |
| okx_futures | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | `"ping"` 25s | 300 |
| gate_spot | `api.gateio.ws/ws/v4/` | `spot.book_ticker` | JSON timestamped 20s | single conn |
| gate_futures | `fx-ws.gateio.ws/v4/ws/usdt` | `futures.book_ticker` | JSON timestamped 20s | single conn |

### Redis write pipeline

`pipeline(transaction=False)` with two flush triggers: batch reaches 100 commands **or** 20ms elapsed. Keeps write latency under 2ms on Unix socket.

### Collector log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | — |
| `symbols_loaded` | INFO | `count`, `file` |
| `redis_connected` | INFO | `socket` |
| `connecting` | INFO | `chunk_symbols` |
| `connected` | INFO | `chunk_symbols`, `subscribe_ms` |
| `subscribed` | INFO | `total_symbols`, `subscribe_ms` |
| `first_message` | INFO | `ms_since_connected`, `first_sym` |
| `stats` | INFO | `msgs_total`, `msgs_per_sec`, `flushes_total`, `avg_pipeline_ms` |
| `disconnected` | WARN | `reason` |
| `reconnecting` | INFO | `delay_sec` |

---

## Staleness Monitor

Scans all `md:*` keys in Redis every 60 seconds. A key is stale if its `t` field hasn't been updated in more than 300 seconds (5 minutes).

```bash
# Standalone (optional — already included in run_all.py)
python3 collectors/staleness_monitor.py
python3 collectors/staleness_monitor.py --buckets   # + 1-min age distribution
```

### Staleness monitor log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `check_start` | INFO | `cycle`, `pattern` |
| `scan_complete` | INFO | `cycle`, `keys_found`, `scan_ms` |
| `fetch_complete` | INFO | `cycle`, `keys_fetched`, `fetch_ms` |
| `check` | INFO/WARN | `total_keys`, `stale_count`, `stale_by_source`, `top_stale` |
| `buckets` | INFO | `distribution` (fresh, 1-2min … 5+min) |
| `check_complete` | INFO | `cycle`, `total_ms` |

---

## Spread Monitor

Reads Redis every 0.3 seconds across all 12 spot→futures directions. Generates a signal when spread exceeds the threshold and no cooldown is active.

### Spread formula

```
spread = (futures_bid - spot_ask) / spot_ask × 100
```

Spot is always assumed cheaper than futures (positive spread only).

### 12 directions

Each file in `dictionaries/combination/` defines one direction:

```
binance_spot  × bybit_futures,  okx_futures,  gate_futures
bybit_spot    × binance_futures, okx_futures,  gate_futures
okx_spot      × binance_futures, bybit_futures, gate_futures
gate_spot     × binance_futures, bybit_futures, okx_futures
```

### Signal thresholds

| Range | Output |
|-------|--------|
| 1% – 99.99% | `signals/signals.jsonl` + `signals/signals.csv` |
| ≥ 100% | `signals/anomalies.jsonl` + `signals/anomalies.csv` |

Anomalies (≥100%) are separated because they typically indicate stale or bad data from one exchange and should be investigated rather than acted on.

### Signal file formats

**JSONL** (`signals.jsonl`, `anomalies.jsonl`) — one object per line:
```json
{"ts": 1711234567.1, "direction": "binance_spot_bybit_futures", "symbol": "BTCUSDT",
 "spread_pct": 1.42, "spot_ask": "67234.10", "fut_bid": "68189.20",
 "spot_ex": "bn", "fut_ex": "bb"}
```

**CSV** (`signals.csv`, `anomalies.csv`):
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,67234.10,68189.20,1.4200,1711234567123
```

Both files open in append mode — signals accumulate across restarts. The CSV header is written only if the file is empty.

### Cooldown

After a signal fires for `(direction, symbol)`, no new signal is written for that pair for **3500 seconds**. Cooldowns are stored in memory and reset on restart.

### Data freshness

If either the spot or futures price hasn't been updated in the last **300 seconds**, the pair is skipped entirely (not treated as a signal).

### Spread monitor log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | `poll_interval_sec`, `min_spread_pct`, `cooldown_sec` |
| `directions_loaded` | INFO | `directions`, `total_symbols` |
| `direction_loaded` | INFO | `direction`, `symbols`, `spot_ex`, `fut_ex` |
| `redis_connected` | INFO | `socket` |
| `signal_files_opened` | INFO | paths to all 4 output files |
| `signal` | INFO | `direction`, `symbol`, `spread_pct`, `spot_ask`, `fut_bid` |
| `anomaly` | INFO | same fields as `signal` |
| `stats` | INFO | `signals_total`, `anomalies_total`, `cooldowns_active`, `last_cycle_ms` |
| `cycle_error` | ERROR | `reason` |

---

## Dashboard

Updates every 2 seconds in terminal (stderr). JSON logs go to rotating files simultaneously.

```
Bali 3.0  uptime 120s  log collectors_2026-03-20_14-00.log
┌──────────────────────┬───────────┬────────┬───────────┬─────────┬─────────┬────────┬──────────┐
│ Script               │ Status    │ msgs/s │ total     │ flushes │ pipe ms │ errors │ last seen│
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ binance_spot         │ streaming │  823.4 │ 98,808    │   988   │  0.412  │   0    │     1s   │
│ ...                  │           │        │           │         │         │        │          │
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ staleness_monitor    │ ok        │   —    │ 4,821     │ stale:0 │    —    │   0    │    12s   │
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ spread_monitor       │ scanning  │   3    │ 47        │  cd:12  │  18.4   │   0    │     1s   │
└──────────────────────┴───────────┴────────┴───────────┴─────────┴─────────┴─────────┴─────────┘
```

**spread_monitor columns:**
- `msgs/s` → signals fired in last 30s interval
- `total` → total signals since start
- `flushes` → `cd:N` — active cooldowns
- `pipe ms` → last cycle duration in ms

**Status colors:** `streaming/scanning/ok` = green · `connecting/reconnecting` = yellow · `disconnected/STALE:N` = red · `CRASHED` = bold red

---

## Redis

### Configuration

```
Socket:      /var/run/redis/redis.sock
Eviction:    volatile-ttl
Persistence: disabled
FLUSHDB:     disabled (security) — run_all.py uses SCAN+DEL on startup
```

### Health check

```bash
sudo bash setup_redis.sh --check
redis-cli -s /var/run/redis/redis.sock PING
redis-cli -s /var/run/redis/redis.sock INFO memory
redis-cli -s /var/run/redis/redis.sock SLOWLOG GET 10
```

### Thresholds

| Metric | OK | WARN | CRIT |
|--------|-----|------|------|
| PING latency | < 1ms | > 1ms | timeout |
| Memory usage | < 60% | > 60% | > 95% |
| Fragmentation ratio | < 1.3 | > 1.3 | — |
| Blocked clients | 0 | — | > 0 |

---

## Updating Pair Lists

```bash
pkill -f run_all.py
cd dictionaries && python3 main.py && cd ..
python3 run_all.py
```

---

## Logs

All logs are JSON, one object per line. Fields present in every line: `ts`, `lvl`, `script`, `event`.

```bash
# Follow current log chunk
tail -f logs/collectors_*.log | jq .

# All signals
jq 'select(.event == "signal")' logs/collectors_*.log

# Anomalies only
jq 'select(.event == "anomaly")' logs/collectors_*.log

# Errors and warnings
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log

# Spread monitor stats
jq 'select(.script == "spread_monitor" and .event == "stats")' logs/collectors_*.log

# Stale alerts
jq 'select(.event == "check" and .stale_count > 0)' logs/collectors_*.log

# Per-collector stats
jq 'select(.script == "binance_spot" and .event == "stats")' logs/collectors_*.log
```
