# Bali 3.0

Real-time market data collection system for cryptocurrency spread monitoring across 4 exchanges. Generates cross-exchange trading pair dictionaries, streams live order book data to Redis, and monitors data freshness.

---

## Overview

Bali 3.0 consists of two independent subsystems:

| Subsystem | Purpose | Runtime |
|-----------|---------|---------|
| **Dictionary Generator** | Discovers active trading pairs across exchanges via REST + WebSocket | ~70 seconds, run on demand |
| **Collectors** | Streams real-time bid/ask data from 8 WebSocket feeds into Redis | Continuous, always running |

**Exchanges:** Binance · Bybit · OKX · Gate.io
**Markets:** Spot + Futures (USDT/USDC perpetuals) for each exchange
**Redis keys:** `md:{exchange}:{market}:{symbol}` → `{b: bid, a: ask, t: timestamp_ms}`

---

## Requirements

- Python 3.10+
- Redis (configured via `setup_redis.sh`)

```bash
pip install websockets orjson redis[hiredis] rich aiohttp
```

---

## Quick Start

### 1. Set up Redis

```bash
sudo bash setup_redis.sh           # install, configure, start Redis
sudo bash setup_redis.sh --check   # verify Redis health
```

### 2. Generate trading pair dictionaries

```bash
cd dictionaries
python3 main.py
```

This takes ~70 seconds. Outputs:
- `dictionaries/subscribe/{exchange}/*.txt` — subscription files for collectors
- `dictionaries/combination/*.txt` — cross-exchange pair intersections

### 3. Start collectors

```bash
# Dashboard in terminal, JSON logs saved to file
python3 run_all.py > logs/collectors.log

# With bucket distribution in staleness monitor
python3 run_all.py --buckets > logs/collectors.log

# JSON logs only, no dashboard
python3 run_all.py --no-dash 2>&1 | tee logs/collectors.log
```

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   Dictionary Generator                       │
│                    dictionaries/main.py                      │
│                                                             │
│  Phase 1: REST fetch (parallel, 4 workers, ~3-5s)          │
│    binance_pairs.py  bybit_pairs.py  okx_pairs.py  gate_pairs.py
│           ↓                                                 │
│  Phase 2: WebSocket validation (parallel, 60s window)       │
│    binance_ws.py     bybit_ws.py     okx_ws.py     gate_ws.py
│           ↓                                                 │
│  Phase 3: Build 12 intersection files (set ∩)               │
│  Phase 4: Build 8 subscription files (set ∪)                │
│  Phase 5: Print statistics report                           │
└─────────────────────────────────────────────────────────────┘
                           ↓ subscribe files
┌─────────────────────────────────────────────────────────────┐
│                      Collectors                             │
│                       run_all.py                            │
│                                                             │
│  binance_spot.py    binance_futures.py                      │
│  bybit_spot.py      bybit_futures.py      → Redis           │
│  okx_spot.py        okx_futures.py           md:*:*:*       │
│  gate_spot.py       gate_futures.py                         │
│  staleness_monitor.py  (checks Redis every 60s)             │
└─────────────────────────────────────────────────────────────┘
```

### Data Flow

```
REST API → pairs list → WS validation → active pairs
    → intersections (12 files) → subscribe files (8 files)
        → collectors read subscribe files at startup
            → WS stream → Redis HASH pipeline
                → staleness_monitor scans for stale keys
```

---

## Directory Structure

```
bali3.0/
├── run_all.py                      # Collector orchestrator + live dashboard
├── setup_redis.sh                  # Redis install & configuration
├── logs/
│   └── collectors.log
│
├── dictionaries/
│   ├── main.py                     # Dictionary generator (5 phases)
│   ├── binance/
│   │   ├── binance_pairs.py        # REST: api.binance.com / fapi.binance.com
│   │   ├── binance_ws.py           # WS: stream.binance.com:9443, bookTicker
│   │   └── data/                   # Generated pair files
│   ├── bybit/
│   │   ├── bybit_pairs.py          # REST: api.bybit.com/v5/market/instruments-info
│   │   ├── bybit_ws.py             # WS: stream.bybit.com/v5/public, orderbook.1
│   │   └── data/
│   ├── okx/
│   │   ├── okx_pairs.py            # REST: okx.com/api/v5/public/instruments
│   │   ├── okx_ws.py               # WS: ws.okx.com:8443/ws/v5/public, tickers
│   │   └── data/                   # Includes *_native.txt (BTC-USDT[-SWAP] format)
│   ├── gate/
│   │   ├── gate_pairs.py           # REST: api.gateio.ws/api/v4
│   │   ├── gate_ws.py              # WS: api.gateio.ws / fx-ws.gateio.ws, book_ticker
│   │   └── data/                   # Includes *_native.txt (BTC_USDT format)
│   ├── combination/                # 12 files: spot_A ∩ futures_B
│   └── subscribe/                  # 8 files: input for collectors
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
    └── staleness_monitor.py
```

---

## Dictionary Generator

Run when exchange pair lists need to be refreshed (new listings, delistings).

```bash
cd dictionaries && python3 main.py
```

**Expected output:**
```
Phase 1 — REST fetch complete in 3.2s
  Binance:  383 spot,  223 futures
  Bybit:    304 spot,  196 futures
  OKX:      312 spot,  198 futures
  Gate.io:  580 spot,  241 futures

Phase 2 — WebSocket validation (60s window)...
  Binance:  381 active spot,  221 active futures
  ...

Phase 3 — Building 12 combination files
Phase 4 — Building 8 subscribe files
Phase 5 — Report
  subscribe/binance/binance_spot.txt:  418 symbols
  ...
```

### Exchange-specific notes

| Exchange | Symbol format (REST) | WS subscription format |
|----------|---------------------|----------------------|
| Binance | `BTCUSDT` | `btcusdt@bookTicker` (in URL) |
| Bybit | `BTCUSDT` | `orderbook.1.BTCUSDT` |
| OKX | `BTCUSDT` | `BTC-USDT` (spot) / `BTC-USDT-SWAP` (futures) |
| Gate.io | `BTCUSDT` | `BTC_USDT` |

OKX and Gate.io require separate native-format files for WS because their API uses different symbol notation than the normalized cross-exchange format.

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

### Collector internals

Each collector follows the same pattern:

```
startup → load symbols → connect Redis → [per connection chunk]:
    connecting → connected → subscribe (batched) → subscribed
        → first_message (logged once) → streaming loop
            → flush to Redis (batch 100 cmds or every 20ms)
        → stats log every 30s
    on disconnect → reconnecting → retry after 3s
```

### WebSocket protocol details

| Collector | Endpoint | Channel | Ping | Chunk |
|-----------|----------|---------|------|-------|
| binance_spot | `stream.binance.com:9443/stream` | `{sym}@bookTicker` | Built-in 20s | 300 |
| binance_futures | `fstream.binance.com/stream` | `{sym}@bookTicker` | Built-in 20s | 300 |
| bybit_spot | `stream.bybit.com/v5/public/spot` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | — (single conn) |
| bybit_futures | `stream.bybit.com/v5/public/linear` | `orderbook.1.{SYM}` | `{"op":"ping"}` 20s | — (single conn) |
| okx_spot | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | String `"ping"` 25s | 300 |
| okx_futures | `ws.okx.com:8443/ws/v5/public` | `tickers` instId | String `"ping"` 25s | 300 |
| gate_spot | `api.gateio.ws/ws/v4/` | `spot.book_ticker` | JSON `{time,channel}` 20s | — (single conn) |
| gate_futures | `fx-ws.gateio.ws/v4/ws/usdt` | `futures.book_ticker` | JSON `{time,channel}` 20s | — (single conn) |

### Redis write pipeline

All collectors use `pipeline(transaction=False)` with two flush triggers:

- **Size trigger:** batch reaches 100 commands
- **Time trigger:** 20ms since last flush (whichever comes first)

This keeps Redis write latency under 2ms on a Unix socket.

### Log events (JSON, stdout)

Every log line is a JSON object with `ts`, `lvl`, `script`, `event` fields plus event-specific data.

| Event | Level | When |
|-------|-------|------|
| `startup` | INFO | Script starts |
| `symbols_loaded` | INFO | Subscribe file read |
| `redis_connected` | INFO | Redis socket opened |
| `connecting` | INFO | WS TCP connect attempt |
| `connected` | INFO | WS handshake complete + `subscribe_ms` |
| `subscribed` | INFO | All subscriptions sent + `subscribe_ms` |
| `first_message` | INFO | First valid tick received + `ms_since_connected` |
| `stats` | INFO | Every 30s: `msgs_total`, `msgs_per_sec`, `avg_pipeline_ms` |
| `disconnected` | WARN | WS closed or error + `reason` |
| `reconnecting` | INFO | Before reconnect sleep |

### Staleness monitor log events

| Event | Level | Fields |
|-------|-------|--------|
| `check_start` | INFO | `cycle`, `pattern` |
| `scan_complete` | INFO | `cycle`, `keys_found`, `scan_ms` |
| `fetch_complete` | INFO | `cycle`, `keys_fetched`, `fetch_ms` |
| `check` | INFO/WARN | `total_keys`, `stale_count`, `stale_by_source`, `top_stale` |
| `buckets` | INFO | `distribution` (1-2min, 2-3min, ..., 5+min) |
| `check_complete` | INFO | `cycle`, `total_ms` |

---

## Dashboard

The live dashboard renders to stderr every 2 seconds and shows per-script metrics:

```
Bali 3.0  uptime 120s
┌──────────────────────┬───────────┬────────┬───────────┬─────────┬─────────┬────────┬──────────┐
│ Script               │ Status    │ msgs/s │ total     │ flushes │ pipe ms │ errors │ last seen│
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ binance_spot         │ streaming │  823.4 │ 98,808    │   988   │  0.412  │   0    │     1s   │
│ binance_futures      │ streaming │  411.2 │ 49,344    │   493   │  0.398  │   0    │     2s   │
│ bybit_spot           │ streaming │  198.3 │ 23,796    │   238   │  0.501  │   0    │     1s   │
│ bybit_futures        │ streaming │  145.7 │ 17,484    │   175   │  0.489  │   0    │     2s   │
│ okx_spot             │ streaming │  312.1 │ 37,452    │   375   │  0.621  │   0    │     1s   │
│ okx_futures          │ streaming │  278.4 │ 33,408    │   334   │  0.598  │   0    │     2s   │
│ gate_spot            │ streaming │  201.5 │ 24,180    │   242   │  0.445  │   0    │     1s   │
│ gate_futures         │ streaming │  189.2 │ 22,704    │   227   │  0.431  │   0    │     2s   │
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ staleness_monitor    │ ok        │   —    │ 4,821     │ stale:0 │    —    │   0    │    12s   │
└──────────────────────┴───────────┴────────┴───────────┴─────────┴─────────┴────────┴──────────┘
```

**Status colors:** `streaming` = green · `connecting/reconnecting` = yellow · `disconnected` = red · `CRASHED` = bold red

**Note:** `msgs/s` and `total` update every 30 seconds (from `stats` events). Values show 0 for the first 30 seconds after startup — this is normal.

---

## Redis

### Configuration

Redis runs on a Unix socket for maximum throughput:

```
Socket:     /var/run/redis/redis.sock
Eviction:   volatile-ttl
Persistence: disabled
```

### Health check

```bash
sudo bash setup_redis.sh --check
redis-cli -s /var/run/redis/redis.sock PING        # should return PONG in <1ms
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

Run the dictionary generator whenever you need to refresh which pairs are tracked:

```bash
cd dictionaries && python3 main.py
```

Then restart collectors to pick up the new subscribe files:

```bash
# Kill existing collectors
pkill -f run_all.py

# Restart
python3 run_all.py > logs/collectors.log
```

---

## Logs

JSON log format — every line is a valid JSON object:

```json
{"ts": 1711234567.123, "lvl": "INFO", "script": "binance_spot", "event": "stats",
 "msgs_total": 98808, "msgs_per_sec": 823.4, "flushes_total": 988, "avg_pipeline_ms": 0.412}
```

Parse logs with `jq`:

```bash
# Follow live
tail -f logs/collectors.log | jq .

# Stats events only
cat logs/collectors.log | jq 'select(.event == "stats")'

# Errors and warnings
cat logs/collectors.log | jq 'select(.lvl == "WARN" or .lvl == "ERROR")'

# Specific script
cat logs/collectors.log | jq 'select(.script == "binance_spot")'

# Stale alerts
cat logs/collectors.log | jq 'select(.event == "check" and .stale_count > 0)'
```
