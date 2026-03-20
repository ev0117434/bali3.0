# Bali 3.0

Real-time market data collection and spread monitoring system for cryptocurrency arbitrage across 4 exchanges. Generates cross-exchange trading pair dictionaries, streams live best-bid/ask and full 10-level order books to Redis, monitors data freshness, and detects spread opportunities across 12 spot→futures directions.

---

## Overview

| Subsystem | Scripts | Purpose | Runtime |
|-----------|---------|---------|---------|
| **Dictionary Generator** | `dictionaries/main.py` | Discovers active trading pairs via REST + WebSocket validation | ~70s, run on demand |
| **md collectors** (×8) | `binance/bybit/okx/gate_spot/futures.py` | Streams real-time best-bid/ask into Redis `md:*` keys | Continuous |
| **ob collectors** (×8) | `ob_binance/bybit/okx/gate_spot/futures.py` | Streams real-time 10-level order books into Redis `ob:*` keys | Continuous |
| **Staleness Monitor** | `staleness_monitor.py` | Checks `md:*` key freshness every 60s, alerts on stale data | Continuous |
| **Spread Monitor** | `spread_monitor.py` | Scans 12 spot→futures directions every 0.3s, writes signals + snapshots | Continuous |

**Exchanges:** Binance · Bybit · OKX · Gate.io
**Markets:** Spot + Futures (USDT/USDC perpetuals) for each exchange
**Total collectors:** 16 (8 best-bid/ask + 8 order book)

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

~70 seconds. Generates 8 subscription files for collectors and 12 combination files for spread monitor.

### 3. Start everything

```bash
python3 run_all.py                  # live dashboard (default)
python3 run_all.py --no-dash        # JSON logs only, no TUI
python3 run_all.py --buckets        # + staleness age-bucket distribution
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
              ↓ subscribe/*.txt              ↓ combination/*.txt
┌──────────────────────────────┐   ┌────────────────────────────────┐
│   md collectors (×8)         │   │        Spread Monitor          │
│   best-bid/ask → md:* keys   │   │    collectors/spread_monitor.py│
│                              │   │                                │
│   binance_spot               │   │  12 directions, 0.3s poll      │
│   binance_futures            │   │  (futures_bid-spot_ask)/ask    │
│   bybit_spot             →Redis  │  signals/signals.jsonl+csv     │
│   bybit_futures          md:*:*  │  signals/anomalies.jsonl+csv   │
│   okx_spot                   │   └────────────────────────────────┘
│   okx_futures                │
│   gate_spot                  │   ┌────────────────────────────────┐
│   gate_futures               │   │      Staleness Monitor         │
│                              │   │  checks md:* keys every 60s    │
│   ob collectors (×8)         │   └────────────────────────────────┘
│   10-level book → ob:* keys  │
│                          →Redis
│   ob_binance_spot        ob:*:*
│   ob_binance_futures         │
│   ob_bybit_spot              │
│   ob_bybit_futures           │
│   ob_okx_spot                │
│   ob_okx_futures             │
│   ob_gate_spot               │
│   ob_gate_futures            │
└──────────────────────────────┘
```

### Data flow

```
REST API → pairs list → WS validation → active pairs
    → intersections (12 files) → subscribe files (8 files)
        → md collectors  → Redis HASH md:*  + ZSET md:hist:*
        → ob collectors  → Redis HASH ob:*  + ZSET ob:hist:*
            → spread_monitor reads md:* → signals files
            → staleness_monitor scans md:* for stale keys
```

---

## Directory Structure

```
bali3.0/
├── run_all.py                           # Orchestrator: all 16 collectors + monitors + dashboard
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
│   └── snapshots/YYYY-MM-DD/HH/        # Per-signal spread evolution (3500s window)
│       └── {spot}_{fut}_{sym}_{ts}.csv
│
├── dictionaries/
│   ├── main.py                          # Dictionary generator (5 phases)
│   ├── binance/
│   │   ├── binance_pairs.py             # REST: api.binance.com / fapi.binance.com
│   │   ├── binance_ws.py                # WS: stream.binance.com:9443, bookTicker
│   │   └── data/                        # Raw and active pair lists
│   ├── bybit/
│   │   ├── bybit_pairs.py               # REST: api.bybit.com/v5/market/instruments-info
│   │   ├── bybit_ws.py                  # WS: stream.bybit.com/v5/public, orderbook.1
│   │   └── data/
│   ├── okx/
│   │   ├── okx_pairs.py                 # REST: okx.com/api/v5/public/instruments
│   │   ├── okx_ws.py                    # WS: ws.okx.com:8443/ws/v5/public, tickers
│   │   └── data/                        # Includes *_native.txt (BTC-USDT[-SWAP])
│   ├── gate/
│   │   ├── gate_pairs.py                # REST: api.gateio.ws/api/v4
│   │   ├── gate_ws.py                   # WS: api.gateio.ws / fx-ws.gateio.ws
│   │   └── data/                        # Includes *_native.txt (BTC_USDT)
│   ├── combination/                     # 12 intersection files: spot_A ∩ futures_B
│   └── subscribe/                       # 8 subscription files: input for collectors
│       ├── binance/{spot,futures}.txt
│       ├── bybit/{spot,futures}.txt
│       ├── okx/{spot,futures}.txt
│       └── gate/{spot,futures}.txt
│
└── collectors/
    ├── hist_writer.py                   # md:hist history module (20-min ZSET chunks)
    ├── ob_hist_writer.py                # ob:hist history module (same logic, 41-field member)
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
    ├── ob_bybit_spot.py                 # ob: Bybit Spot 10-level order book (local book)
    ├── ob_bybit_futures.py              # ob: Bybit Futures 10-level order book (local book)
    ├── ob_okx_spot.py                   # ob: OKX Spot 10-level order book (local book)
    ├── ob_okx_futures.py                # ob: OKX Futures 10-level order book (local book)
    ├── ob_gate_spot.py                  # ob: Gate.io Spot 10-level order book
    ├── ob_gate_futures.py               # ob: Gate.io Futures 10-level order book
    │
    ├── staleness_monitor.py             # Monitors md:* key freshness
    └── spread_monitor.py                # Detects cross-exchange spread opportunities
```

---

## run_all.py

Single entry point for all components. On startup:
1. Flushes Redis (SCAN + DEL all keys)
2. Opens rotating log file in `logs/`
3. Starts all 16 collectors + staleness_monitor + spread_monitor as async tasks
4. Renders live dashboard in terminal (stderr)

```bash
python3 run_all.py                  # dashboard + auto log file
python3 run_all.py --buckets        # + staleness age-bucket distribution in logs
python3 run_all.py --no-dash        # JSON logs only, no TUI
```

### Log rotation

Files rotate every 12 hours automatically. Only last 2 files are kept (last 24 hours):

```
logs/collectors_2026-03-20_14-00.log   ← previous chunk
logs/collectors_2026-03-21_02-00.log   ← current chunk
```

---

## md Collectors — best-bid/ask

8 collectors that stream real-time best bid and best ask prices from exchange WebSocket feeds.

### Redis key schema (md)

```
md:{ex}:{mkt}:{symbol}                  →  HASH   (current best bid/ask)
md:hist:{ex}:{mkt}:{symbol}:{chunk_id}  →  ZSET   (1-hour price history, 20-min chunks)
md:hist:config                          →  STRING  (JSON, active chunk metadata)
```

**HASH fields:** `b` = best bid · `a` = best ask · `t` = timestamp ms

**ZSET member format:** `{bid}|{ask}|{ts_ms}` · **score:** `ts_ms`

**Exchange codes:** `bn` = Binance · `bb` = Bybit · `ok` = OKX · `gt` = Gate.io
**Market codes:** `s` = spot · `f` = futures

**Examples:**
```
md:bn:s:BTCUSDT              → Binance Spot BTC/USDT (current)
md:hist:bn:s:BTCUSDT:1450782 → Binance Spot BTC/USDT history chunk 1450782
md:ok:f:BTCUSDT              → OKX Futures BTC/USDT (current)
```

### WebSocket protocol details (md collectors)

| Collector | Endpoint | Channel | Ping | Chunk |
|-----------|----------|---------|------|-------|
| binance_spot | `stream.binance.com:9443/stream` | `{sym}@bookTicker` | Built-in 20s | 300 syms/conn |
| binance_futures | `fstream.binance.com/stream` | `{sym}@bookTicker` | Built-in 20s | 300 syms/conn |
| bybit_spot | `stream.bybit.com/v5/public/spot` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | single conn, 10/batch |
| bybit_futures | `stream.bybit.com/v5/public/linear` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | single conn, 200/batch |
| okx_spot | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | `"ping"` 25s | 300 syms/conn, 20/batch |
| okx_futures | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | `"ping"` 25s | 300 syms/conn, 20/batch |
| gate_spot | `api.gateio.ws/ws/v4/` | `spot.book_ticker` | timestamped JSON 20s | single conn, 1 sub/sym |
| gate_futures | `fx-ws.gateio.ws/v4/ws/usdt` | `futures.book_ticker` | timestamped JSON 20s | single conn, 1 sub/sym |

### md collector log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | — |
| `symbols_loaded` | INFO | `count`, `file` |
| `redis_connected` | INFO | `socket` |
| `subscribing` | INFO | `total_symbols`, `connections` |
| `connecting` | INFO | `chunk_symbols` |
| `connected` | INFO | `chunk_symbols` |
| `subscribed` | INFO | `total_symbols` or `chunk_symbols`, `subscribe_ms` |
| `first_message` | INFO | `ms_since_connected`, `first_sym` |
| `stats` | INFO | `msgs_total`, `msgs_per_sec`, `flushes_total`, `avg_pipeline_ms` |
| `disconnected` | WARN | `reason` |
| `reconnecting` | INFO | `delay_sec` |

---

## ob Collectors — 10-level order books

8 collectors that maintain real-time 10-level order books (10 bid levels + 10 ask levels, each with price and quantity).

### Local book management

Three of the four exchanges send **incremental updates** after an initial snapshot. These collectors maintain a local order book dictionary in memory:

- **Bybit** (`snapshot` → `delta`): `books[sym] = {"bids": {price: qty}, "asks": {price: qty}}`. Cleared on reconnect. Logs `book_init` on first snapshot per symbol, `delta_before_snapshot` WARN if delta arrives out of order.
- **OKX** (`snapshot` → `update`): Same pattern. Multiple parallel connections (chunks of 150 instIds). On reconnect, only that chunk's books are cleared. Logs `book_init` and `update_before_snapshot` WARN.
- **Binance** and **Gate.io**: Full snapshot every 100ms — no local book needed.

### Redis key schema (ob)

```
ob:{ex}:{mkt}:{symbol}                  →  HASH   (current 10-level order book)
ob:hist:{ex}:{mkt}:{symbol}:{chunk_id}  →  ZSET   (order book history, 20-min chunks)
ob:hist:config                          →  STRING  (JSON, active chunk metadata)
```

**HASH fields (41 total):**

| Field | Description |
|-------|-------------|
| `b1`..`b10` | Bid prices, level 1 (best) to 10 (worst) |
| `bq1`..`bq10` | Bid quantities for each level |
| `a1`..`a10` | Ask prices, level 1 (best) to 10 (worst) |
| `aq1`..`aq10` | Ask quantities for each level |
| `t` | Timestamp ms (exchange timestamp where available, else local) |

**ZSET member format (pipe-separated, 41 values):**
```
b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
```
**score:** `ts_ms`

**Examples:**
```
ob:bn:s:BTCUSDT               → Binance Spot BTC/USDT order book (current)
ob:hist:bb:f:ETHUSDT:1478370  → Bybit Futures ETH/USDT OB history chunk 1478370
ob:ok:f:BTCUSDT               → OKX Futures BTC/USDT order book (current)
```

### Query order book

```bash
# Get full order book for a symbol
redis-cli -s /var/run/redis/redis.sock HGETALL ob:bn:s:BTCUSDT

# Get just top 3 bids and asks
redis-cli -s /var/run/redis/redis.sock HMGET ob:bn:s:BTCUSDT b1 bq1 b2 bq2 b3 bq3 a1 aq1 a2 aq2 a3 aq3

# Check data age
T=$(redis-cli -s /var/run/redis/redis.sock HGET ob:bn:s:BTCUSDT t)
echo "Age: $(( ($(date +%s%3N) - T) / 1000 ))ms"
```

### WebSocket protocol details (ob collectors)

| Collector | Endpoint | Channel | Depth | Book type | Ping |
|-----------|----------|---------|-------|-----------|------|
| ob_binance_spot | `stream.binance.com:9443/stream` | `{sym}@depth10@100ms` | Full snap 100ms | No local book | Built-in 20s |
| ob_binance_futures | `fstream.binance.com/stream` | `{sym}@depth10@100ms` | Full snap 100ms | No local book | Built-in 20s |
| ob_bybit_spot | `stream.bybit.com/v5/public/spot` | `orderbook.50.{SYM}` | snap+delta, top-10 | Local book | `{"op":"ping"}` 20s |
| ob_bybit_futures | `stream.bybit.com/v5/public/linear` | `orderbook.50.{SYM}` | snap+delta, top-10 | Local book | `{"op":"ping"}` 20s |
| ob_okx_spot | `ws.okx.com:8443/ws/v5/public` | `books` instId | snap+update, top-10 | Local book | `"ping"` 25s |
| ob_okx_futures | `ws.okx.com:8443/ws/v5/public` | `books` instId | snap+update, top-10 | Local book | `"ping"` 25s |
| ob_gate_spot | `api.gateio.ws/ws/v4/` | `spot.order_book` | Full snap 100ms | No local book | timestamped JSON 20s |
| ob_gate_futures | `fx-ws.gateio.ws/v4/ws/usdt` | `futures.order_book` | Full snap realtime | No local book | timestamped JSON 20s |

**Important protocol notes:**
- **Binance Futures** depth messages use fields `b`/`a` (NOT `bids`/`asks` like spot)
- **Bybit** supports depths 1, 50, 200 (spot) and 1, 50, 200, 500 (futures). `orderbook.10` does not exist
- **OKX** `books` channel sends full-depth book (400 levels), top 10 extracted locally
- **Gate.io Futures** interval must be `"0"` (realtime); `"100ms"` is rejected by the endpoint

### ob collector log events

| Event | Level | Key fields | When |
|-------|-------|-----------|------|
| `startup` | INFO | — | Script start |
| `symbols_loaded` | INFO | `count`, `file` | After reading subscribe file |
| `redis_connected` | INFO | `socket` | After Redis connect |
| `subscribing` | INFO | `total_symbols`, `connections` | Before spawning chunk tasks |
| `connecting` | INFO | `chunk_symbols` | Each WS connect attempt |
| `connected` | INFO | `chunk_symbols` | WS handshake complete |
| `subscribing_progress` | INFO | `subscribed`, `total`, `elapsed_ms` | Gate only: every 100 symbols |
| `subscribed` | INFO | `total_symbols`, `subscribe_ms` | All subs sent |
| `book_init` | INFO | `symbol`, `bid_levels`, `ask_levels`, `books_initialized` | Bybit/OKX: first snapshot per symbol |
| `first_message` | INFO | `ms_since_connected`, `first_sym` | First data written to Redis |
| `delta_before_snapshot` | WARN | `symbol`, `delta_skipped_total` | Bybit: delta arrived before snapshot |
| `update_before_snapshot` | WARN | `symbol`, `update_skipped_total` | OKX: update arrived before snapshot |
| `stats` | INFO | see below | Every 30s |
| `disconnected` | WARN | `reason` | Connection lost |
| `reconnecting` | INFO | `delay_sec` | Before reconnect sleep |

**stats event fields (ob collectors):**

| Field | Description | Bybit/OKX only |
|-------|-------------|----------------|
| `msgs_total` | Total order book updates written to Redis | |
| `msgs_per_sec` | Rate in last 30s interval | |
| `flushes_total` | Redis pipeline batches executed | |
| `avg_pipeline_ms` | Average pipeline latency | |
| `avg_levels` | Average number of levels actually present (expect ≈10.0) | |
| `shallow_warnings` | Times a flush had < 10 levels on either side | |
| `books_initialized` | Current number of symbols with a live local book | ✓ |
| `snapshots_total` | Total initial snapshot messages received | ✓ |
| `deltas_total` / `updates_total` | Total incremental messages received | ✓ |
| `delta_skipped` / `update_skipped` | Incremental messages dropped (no snapshot yet) | ✓ |

---

## History Writers

Both `hist_writer.py` (md) and `ob_hist_writer.py` (ob) share the same chunking design:

- **Chunk size:** 20 minutes (`chunk_id = int(unix_sec // 1200)`)
- **Chunks kept:** 4 (80-minute window, always covers last 60 minutes)
- **Throttle:** max 1 sample per second per symbol
- **Rotation:** when chunk 5 starts, chunk 1 is deleted in the same pipeline batch — no extra round-trips
- **Config key:** written once on startup and once per rotation, only when dirty

### md history member format
```
{bid}|{ask}|{ts_ms}
```

### ob history member format (41 pipe-separated values)
```
b1|bq1|b2|bq2|b3|bq3|b4|bq4|b5|bq5|b6|bq6|b7|bq7|b8|bq8|b9|bq9|b10|bq10|a1|aq1|a2|aq2|a3|aq3|a4|aq4|a5|aq5|a6|aq6|a7|aq7|a8|aq8|a9|aq9|a10|aq10|ts_ms
```

### Query history

```bash
# Get current chunk_id
CID=$(python3 -c "import time; print(int(time.time()//1200))")

# Read md history for last hour (all 4 chunks)
for i in 3 2 1 0; do
  redis-cli -s /var/run/redis/redis.sock \
    ZRANGEBYSCORE "md:hist:bn:s:BTCUSDT:$((CID - i))" \
    "$(($(date +%s%3N) - 3600000))" "+inf"
done

# Read ob history for last hour
for i in 3 2 1 0; do
  redis-cli -s /var/run/redis/redis.sock \
    ZRANGEBYSCORE "ob:hist:bn:s:BTCUSDT:$((CID - i))" \
    "$(($(date +%s%3N) - 3600000))" "+inf"
done

# Check config
redis-cli -s /var/run/redis/redis.sock GET md:hist:config | python3 -m json.tool
redis-cli -s /var/run/redis/redis.sock GET ob:hist:config | python3 -m json.tool
```

---

## Staleness Monitor

Scans all `md:*` keys in Redis every 60 seconds. A key is stale if its `t` field hasn't been updated in more than 300 seconds.

```bash
python3 collectors/staleness_monitor.py              # stale alerts only
python3 collectors/staleness_monitor.py --buckets    # + 1-min age distribution
```

### Log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | `check_interval_sec`, `stale_threshold_sec`, `with_buckets` |
| `redis_connected` | INFO | `socket` |
| `check_start` | INFO | `cycle`, `pattern` |
| `scan_complete` | INFO | `cycle`, `keys_found`, `scan_ms` |
| `fetch_complete` | INFO | `cycle`, `keys_fetched`, `fetch_ms` |
| `check` | INFO/WARN | `total_keys`, `keys_with_ts`, `stale_count`, `stale_by_source`, `top_stale` |
| `buckets` | INFO | `distribution` (fresh, 1-2min, 2-3min, 3-4min, 4-5min, 5+min) |
| `check_complete` | INFO | `cycle`, `total_ms` |
| `check_failed` | ERROR | `cycle`, `reason` |
| `stopped` | INFO | — |

---

## Spread Monitor

Reads Redis every 0.3 seconds across all 12 spot→futures directions. Generates a signal when spread exceeds the threshold and no cooldown is active.

### Spread formula

```
spread = (futures_bid - spot_ask) / spot_ask × 100
```

### 12 directions

```
binance_spot  × bybit_futures,  okx_futures,  gate_futures
bybit_spot    × binance_futures, okx_futures,  gate_futures
okx_spot      × binance_futures, bybit_futures, gate_futures
gate_spot     × binance_futures, bybit_futures, okx_futures
```

### Signal thresholds

| Range | Output file |
|-------|-------------|
| 1.0% – 99.99% | `signals/signals.jsonl` + `signals/signals.csv` |
| ≥ 100% | `signals/anomalies.jsonl` + `signals/anomalies.csv` |

### Signal file formats

**JSONL:**
```json
{"ts": 1711234567.1, "direction": "binance_spot_bybit_futures", "symbol": "BTCUSDT",
 "spread_pct": 1.42, "spot_ask": "67234.10", "fut_bid": "68189.20",
 "spot_ex": "bn", "fut_ex": "bb", "cooldown_until": 1711238067}
```

**CSV:**
```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,67234.10,68189.20,1.4200,1711234567123
```

Both files append across restarts. CSV header is written only on first creation.

### Snapshots

On every signal, a snapshot CSV opens at `signals/snapshots/YYYY-MM-DD/HH/{spot}_{fut}_{sym}_{ts}.csv`. It starts with up to 3600 historical rows (last 1 hour from `md:hist:*`), then appends live rows at 0.3s for the full 3500s cooldown window.

### Cooldown

After a signal fires for `(direction, symbol)`, no new signal is written for that pair for **3500 seconds**. Cooldowns are in-memory and reset on restart.

### Data freshness guard

If either spot or futures price is older than 300 seconds, the pair is skipped entirely.

### Log events

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | `poll_interval_sec`, `min_spread_pct`, `cooldown_sec` |
| `directions_loaded` | INFO | `directions`, `total_symbols` |
| `direction_loaded` | INFO | `direction`, `symbols`, `spot_ex`, `fut_ex` |
| `redis_connected` | INFO | `socket` |
| `signal_files_opened` | INFO | file paths, `anomaly_threshold_pct` |
| `signal` | INFO | `direction`, `symbol`, `spread_pct`, `spot_ask`, `fut_bid`, `cooldown_until` |
| `anomaly` | INFO | same as `signal` |
| `snapshot_opened` | INFO | `direction`, `symbol`, `file`, `duration_sec` |
| `snapshot_history_written` | INFO | `direction`, `symbol`, `rows` |
| `stats` | INFO | `signals_total`, `anomalies_total`, `cooldowns_active`, `snapshots_active`, `last_cycle_ms` |
| `cycle_error` | ERROR | `reason` |

---

## Dashboard

Updates every 2 seconds in terminal (stderr). JSON logs go to rotating files simultaneously.

```
Bali 3.0  uptime 120s  log collectors_2026-03-20_14-00.log
┌──────────────────────────┬───────────┬────────┬───────────┬─────────┬─────────┬────────┬──────────┐
│ Script                   │ Status    │ msgs/s │ total     │ flushes │ pipe ms │ errors │ last seen│
├──────────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ binance_spot             │ streaming │  963.1 │ 605,069   │  6,030  │  7.777  │   0    │     1s   │
│ ...                      │           │        │           │         │         │        │          │
│ ob_binance_spot          │ streaming │  560.9 │ 361,157   │  3,611  │  7.418  │   0    │     1s   │
│ ob_bybit_futures         │ streaming │ 3028.6 │1,776,870  │ 17,769  │  7.844  │   0    │     1s   │
│ ...                      │           │        │           │         │         │        │          │
├──────────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ staleness_monitor        │ ok        │   —    │ 2,933     │ stale:0 │    —    │   0    │    12s   │
├──────────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ spread_monitor           │ scanning  │   3    │ 47        │  cd:0   │  18.4   │   0    │     1s   │
└──────────────────────────┴───────────┴────────┴───────────┴─────────┴─────────┴─────────┴─────────┘
```

**Status colors:** `streaming/scanning/ok` = green · `connecting/reconnecting` = yellow · `disconnected/STALE:N` = red · `CRASHED` = bold red

**spread_monitor columns:** `msgs/s` = signals/30s · `total` = total signals · `flushes` = `cd:N` active cooldowns · `pipe ms` = last cycle ms

---

## Redis

### Configuration

```
Socket:      /var/run/redis/redis.sock
Eviction:    volatile-ttl
Persistence: disabled
FLUSHDB:     disabled (security) — run_all.py uses SCAN+DEL on startup
```

### Key summary

```
md:{ex}:{mkt}:{sym}                  →  HASH 3 fields (b, a, t)
md:hist:{ex}:{mkt}:{sym}:{chunk_id}  →  ZSET (bid|ask|ts_ms)
md:hist:config                       →  STRING JSON
ob:{ex}:{mkt}:{sym}                  →  HASH 41 fields (b1..b10, bq1..bq10, a1..a10, aq1..aq10, t)
ob:hist:{ex}:{mkt}:{sym}:{chunk_id}  →  ZSET (b1|bq1|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms)
ob:hist:config                       →  STRING JSON
```

### Health check

```bash
sudo bash setup_redis.sh --check
redis-cli -s /var/run/redis/redis.sock PING
redis-cli -s /var/run/redis/redis.sock INFO memory
redis-cli -s /var/run/redis/redis.sock DBSIZE
```

### Thresholds

| Metric | OK | WARN | CRIT |
|--------|-----|------|------|
| PING latency | < 1ms | > 1ms | timeout |
| Memory usage | < 60% | > 60% | > 95% |
| Fragmentation ratio | < 1.3 | > 1.3 | — |
| Blocked clients | 0 | — | > 0 |

---

## Redis write pipeline

All collectors use `pipeline(transaction=False)` with two flush triggers:
- Batch reaches **100 commands**, or
- **20ms** elapsed since last flush

Inside each pipeline batch: `HSET` for current price/book + `ZADD` for history + `SET` for config (if dirty). No extra round-trips for history or config.

---

## Updating Pair Lists

```bash
pkill -f run_all.py
cd dictionaries && python3 main.py && cd ..
python3 run_all.py
```

---

## Logs

All logs are JSON, one object per line. Fields in every line: `ts`, `lvl`, `script`, `event`.

```bash
# Follow live
tail -f logs/collectors_*.log | jq .

# All errors and warnings
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log

# Collector stats (msgs/s, pipeline latency)
jq 'select(.event == "stats" and .script != "spread_monitor")' logs/collectors_*.log

# OB-specific: book initializations
jq 'select(.event == "book_init")' logs/collectors_*.log

# OB-specific: shallow depth warnings
jq 'select(.event == "stats" and .shallow_warnings > 0)' logs/collectors_*.log

# Spread signals
jq 'select(.event == "signal" or .event == "anomaly")' logs/collectors_*.log

# Stale key alerts
jq 'select(.event == "check" and .stale_count > 0)' logs/collectors_*.log

# Subscription timing
jq 'select(.event == "subscribed") | {script, subscribe_ms}' logs/collectors_*.log

# Time to first message per collector
jq 'select(.event == "first_message") | {script, ms_since_connected}' logs/collectors_*.log
```
