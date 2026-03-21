# User Guide — Bali 3.0

Step-by-step instructions for setting up, running, and maintaining the system from scratch.

---

## Table of Contents

1. [First-time setup](#1-first-time-setup)
2. [Generate trading pair dictionaries](#2-generate-trading-pair-dictionaries)
3. [Start the system](#3-start-the-system)
4. [Reading the dashboard](#4-reading-the-dashboard)
5. [Reading logs](#5-reading-logs)
6. [Reading signals](#6-reading-signals)
7. [Snapshots](#7-snapshots)
8. [Querying price history (md collectors)](#8-querying-price-history-md-collectors)
9. [Querying order book data (ob collectors)](#9-querying-order-book-data-ob-collectors)
10. [Updating pair lists](#10-updating-pair-lists)
11. [Stopping the system](#11-stopping-the-system)
12. [Querying Redis directly](#12-querying-redis-directly)
13. [Troubleshooting](#13-troubleshooting)
14. [System health checks](#14-system-health-checks)
15. [Reference](#15-reference)

---

## 1. First-time setup

### Install Redis

```bash
sudo bash setup_redis.sh
```

This installs Redis, configures it for low-latency operation on a Unix socket, and starts the service. Takes about 30–60 seconds.

Verify:

```bash
sudo bash setup_redis.sh --check
```

You should see `PING: X.XXXms` in green. If you see `FAIL` in red — re-run the setup script.

### Install Python dependencies

```bash
pip install websockets orjson redis[hiredis] rich
```

### Enter the project directory

```bash
cd /root/bali3.0
```

All commands below assume this working directory.

---

## 2. Generate trading pair dictionaries

**Run once before first start, and again whenever you want to refresh the pair list.**

```bash
cd dictionaries && python3 main.py && cd ..
```

Takes ~70 seconds:

```
[0s]   Phase 1 — REST API fetch from all 4 exchanges (parallel)
[~5s]  Phase 2 — WebSocket validation, 60 second window (parallel)
[~65s] Phase 3 — Build 12 intersection files (combination/)
[~65s] Phase 4 — Build 8 subscription files (subscribe/)
[~65s] Phase 5 — Print report
```

**Expected output at the end:**
```
subscribe/binance/binance_spot.txt:    418 symbols
subscribe/binance/binance_futures.txt: 203 symbols
subscribe/bybit/bybit_spot.txt:        312 symbols
...
```

If any exchange shows 0 symbols — re-run (temporary connection issue).

### What gets generated

**8 subscription files** — read by collectors at startup:
```
dictionaries/subscribe/binance/binance_spot.txt
dictionaries/subscribe/binance/binance_futures.txt
dictionaries/subscribe/bybit/bybit_spot.txt
dictionaries/subscribe/bybit/bybit_futures.txt
dictionaries/subscribe/okx/okx_spot.txt
dictionaries/subscribe/okx/okx_futures.txt
dictionaries/subscribe/gate/gate_spot.txt
dictionaries/subscribe/gate/gate_futures.txt
```

**12 combination files** — read by spread_monitor at startup:
```
dictionaries/combination/binance_spot_bybit_futures.txt
dictionaries/combination/binance_spot_okx_futures.txt
... (12 total: all spot×futures exchange pairs)
```

---

## 3. Start the system

### Normal start

```bash
python3 run_all.py
```

What happens on startup:
1. Previous `logs/` and `signals/` are moved to `old/YYYY-MM-DD_HH-MM/` (if non-empty)
2. Redis is flushed (all old keys removed)
3. You are prompted: `"Delay before signals/snapshots [seconds, Enter = 0]:"`
   - Enter a number (e.g. `300`) to wait N seconds before `spread_monitor` starts
   - Press Enter or type `0` to start everything immediately
   - In non-interactive mode (piped/background), prompt is skipped, delay = 0
4. All 8 best-price collectors (`binance_spot`, `binance_futures`, `bybit_spot`, `bybit_futures`, `okx_spot`, `okx_futures`, `gate_spot`, `gate_futures`) start connecting
5. All 8 order book collectors (`ob_binance_spot`, `ob_binance_futures`, `ob_bybit_spot`, `ob_bybit_futures`, `ob_okx_spot`, `ob_okx_futures`, `ob_gate_spot`, `ob_gate_futures`) start connecting
6. `staleness_monitor` starts
7. `spread_monitor` starts after the specified delay
8. Live dashboard appears in terminal

Previous runs accumulate in `old/` with timestamped folders. Each contains `logs/` and `signals/` subdirectories.

### Start with staleness age distribution

```bash
python3 run_all.py --buckets
```

Adds a log line every 60s with key age distribution: `fresh (<1min), 1-2min, 2-3min, 3-4min, 4-5min, 5+min`.

### Start without dashboard

```bash
python3 run_all.py --no-dash
```

Useful in non-interactive environments. JSON logs still go to the rotating log file.

### Run in background

```bash
nohup python3 run_all.py &
echo $! > run_all.pid
```

To stop:
```bash
kill $(cat run_all.pid)
```

---

## 4. Reading the dashboard

The dashboard updates every 2 seconds. It shows 3 sections:

```
Bali 3.0  uptime 120s  log collectors_2026-03-20_14-00.log
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
├──────────────────────┼───────────┼────────┼───────────┼─────────┼─────────┼────────┼──────────┤
│ spread_monitor       │ scanning  │   3    │ 47        │  cd:12  │  18.4   │   0    │     1s   │
└──────────────────────┴───────────┴────────┴───────────┴─────────┴─────────┴─────────┴─────────┘
```

### Collectors columns (top 8 rows)

| Column | Description |
|--------|-------------|
| **msgs/s** | Market data messages per second (updates every 30s) |
| **total** | Total messages since start |
| **flushes** | Redis pipeline batches executed |
| **pipe ms** | Average Redis pipeline latency |
| **errors** | Disconnection count |
| **last seen** | Seconds since last log event |

### staleness_monitor row

| Column | Description |
|--------|-------------|
| **total** | Total Redis keys being tracked |
| **flushes** | `stale:N` — how many keys are currently stale |

### spread_monitor row

| Column | Description |
|--------|-------------|
| **msgs/s** | Signals fired in the last 30s interval |
| **total** | Total signals since start (normal + anomaly) |
| **flushes** | `cd:N` — active cooldowns |
| **pipe ms** | Last poll cycle duration in ms (should be <300) |

### Status values

| Status | Color | Meaning |
|--------|-------|---------|
| `starting` | dim | Initializing |
| `connecting` | yellow | TCP connection in progress |
| `connected` | cyan | WS handshake done, sending subscriptions |
| `subscribed` | cyan | Subscriptions sent, waiting for data |
| `streaming` | **green** | Receiving market data — normal |
| `scanning` | **green** | (spread_monitor) Actively polling Redis |
| `ok` | **green** | (staleness_monitor) No stale keys |
| `disconnected` | red | Connection lost, will reconnect in 3s |
| `reconnecting` | yellow | Waiting before reconnect |
| `STALE:N` | **red** | N keys stale > 5 minutes |
| `CRASHED` | **bold red** | Unhandled exception — check logs |

### Normal startup timeline

```
0s   → all collectors show "connecting"
2s   → "connected" → "subscribed"
5s   → "streaming" (first market data received)
5s   → spread_monitor shows "scanning"
30s  → msgs/s and total columns fill in (first stats event)
60s  → staleness_monitor first check → "ok"
```

**`msgs/s = 0` and `total = 0` for the first 30 seconds is normal.**

---

## 5. Reading logs

Logs are written automatically to rotating files. No redirect needed.

```bash
# Show current log file name
ls -t logs/collectors_*.log | head -1

# Follow live
tail -f logs/collectors_*.log

# Follow with pretty print
tail -f logs/collectors_*.log | jq .
```

### Useful filters

```bash
# All errors and warnings
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log

# Collector stats (msgs/s, pipeline latency)
jq 'select(.event == "stats" and .script != "spread_monitor")' logs/collectors_*.log

# Spread monitor stats
jq 'select(.script == "spread_monitor" and .event == "stats")' logs/collectors_*.log

# All disconnections
jq 'select(.event == "disconnected")' logs/collectors_*.log

# Stale alerts
jq 'select(.event == "check" and .stale_count > 0)' logs/collectors_*.log

# All signals from logs
jq 'select(.event == "signal" or .event == "anomaly")' logs/collectors_*.log

# Time from connect to first message per collector
jq 'select(.event == "first_message") | {script, ms_since_connected}' logs/collectors_*.log

# Subscription timing
jq 'select(.event == "subscribed") | {script, subscribe_ms}' logs/collectors_*.log
```

### Log line structure

```json
{
  "ts":     1711234567.123,
  "lvl":    "INFO",
  "script": "spread_monitor",
  "event":  "signal",
  "direction": "binance_spot_bybit_futures",
  "symbol": "BTCUSDT",
  "spread_pct": 1.42
}
```

---

## 6. Reading signals

### Signal files

| File | Content |
|------|---------|
| `signals/signals.jsonl` | Normal signals (spread 1% – 99%) |
| `signals/signals.csv` | Same, CSV format |
| `signals/anomalies.jsonl` | Anomalies (spread ≥ 100%) |
| `signals/anomalies.csv` | Same, CSV format |

Files accumulate across restarts (append mode). The CSV header is written only when the file is first created.

### CSV format

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
binance,bybit,BTCUSDT,67234.10,68189.20,1.4200,1711234567123
gate,binance,ETHUSDT,3120.50,3152.80,1.0341,1711234601456
```

- `ts` is Unix time in **milliseconds**
- `spot_exch` / `fut_exch` are full exchange names (binance, bybit, okx, gate)

### Follow signals live

```bash
tail -f signals/signals.csv
tail -f signals/anomalies.csv
```

### Analyze signals with jq

```bash
# All signals for a specific symbol
jq 'select(.symbol == "BTCUSDT")' signals/signals.jsonl

# Signals for a specific direction
jq 'select(.direction == "binance_spot_bybit_futures")' signals/signals.jsonl

# Top spreads
jq -s 'sort_by(-.spread_pct) | .[0:10]' signals/signals.jsonl

# Count signals per direction
jq -r '.direction' signals/signals.jsonl | sort | uniq -c | sort -rn
```

### What anomalies mean

A spread ≥ 100% almost always means one of:
- A collector was disconnected and the price in Redis is from hours ago
- The exchange listed a new token with very different pricing conventions
- A data quality issue on one side

Anomalies should be **investigated, not traded**. They are separated from normal signals exactly for this reason.

---

## 7. Snapshots

When a signal fires, `spread_monitor` automatically opens a **snapshot CSV** that records the spread between that spot and futures pair every 0.3 seconds for the full 3500-second cooldown window. This captures the complete evolution of the spread after the signal.

### File location

```
signals/snapshots/
  YYYY-MM-DD/
    HH/
      {spot}_{fut}_{symbol}_{YYYYMMDD_HHMMSS}.csv
```

Directory is bucketed by **UTC day and hour of the signal**. Example:

```
signals/snapshots/2026-03-20/14/binance_bybit_BTCUSDT_20260320_143022.csv
```

### Snapshot CSV format

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts,
s_b1,s_bq1,...,s_b10,s_bq10,s_a1,s_aq1,...,s_a10,s_aq10,   ← 40 spot OB columns
f_b1,f_bq1,...,f_b10,f_bq10,f_a1,f_aq1,...,f_a10,f_aq10    ← 40 futures OB columns
```

Total: **87 columns**. The spot and futures OB columns contain the full 10-level order book at each moment in time.

The file starts with **up to 3600 historical rows** (the last 1 hour from `md:hist:*` joined with `ob:hist:*` for order book depth at each second). Live rows every 0.3s include real-time OB data fetched in the same cycle. If `ob:hist:*` data is missing for a historical second, those 40 fields are empty.

- `ts` is Unix time in **milliseconds**
- `spread_pct` = `(futures_bid - spot_ask) / spot_ask × 100`, rounded to 4 decimal places
- Rows are written even when spread drops below `MIN_SPREAD_PCT` — the full evolution is captured
- File is appended to; never re-opened after initial write
- File is closed automatically when the 3500s window expires

### Follow a snapshot live

```bash
# List recent snapshot files
find signals/snapshots -name "*.csv" -newer signals/signals.csv | sort

# Follow a specific snapshot
tail -f signals/snapshots/2026-03-20/14/binance_bybit_BTCUSDT_20260320_143022.csv
```

### Spread monitor log events for snapshots

| Event | Level | Key fields |
|-------|-------|------------|
| `snapshot_opened` | INFO | `direction`, `symbol`, `file`, `duration_sec` |
| `snapshot_history_written` | INFO | `direction`, `symbol`, `rows` |

---

## 8. Querying price history (md collectors)

All collectors write 1 sample per second per symbol to Redis ZSETs in 20-minute chunks. This provides up to ~80 minutes of history at any time (always covers the last hour).

### Key scheme

```
md:hist:{ex}:{mkt}:{symbol}:{chunk_id}   →   ZSET
  score:  ts_ms (integer milliseconds)
  member: {bid}|{ask}|{ts_ms}
```

`chunk_id = int(unix_seconds // 1200)` — changes every 20 minutes. Four chunks are kept at any time. The oldest chunk is deleted when the fifth starts.

### Config key

```bash
redis-cli -s /var/run/redis/redis.sock GET md:hist:config | python3 -m json.tool
```

Output:
```json
{
  "chunk_sec": 1200,
  "max_chunks": 4,
  "sample_sec": 1,
  "active_chunk_id": 1450782,
  "chunks": [
    {"id": 1450779, "start_ms": 1741234800000, "end_ms": 1741236000000, "active": false},
    {"id": 1450780, "start_ms": 1741236000000, "end_ms": 1741237200000, "active": false},
    {"id": 1450781, "start_ms": 1741237200000, "end_ms": 1741238400000, "active": false},
    {"id": 1450782, "start_ms": 1741238400000, "end_ms": 1741239600000, "active": true}
  ]
}
```

### Query last hour for a symbol

```bash
# Get current chunk_id
CID=$(python3 -c "import time; print(int(time.time()//1200))")

# Read last hour from all 4 chunks, pipe format: bid|ask|ts_ms
for i in 3 2 1 0; do
  redis-cli -s /var/run/redis/redis.sock \
    ZRANGEBYSCORE "md:hist:bn:s:BTCUSDT:$((CID - i))" \
    "$(($(date +%s%3N) - 3600000))" "+inf"
done
```

### Memory estimate

~1 sample/sec × 3600 sec × 4 chunks × 4000 source×symbol pairs × ~50 bytes ≈ **700 MB** peak.

---

## 9. Querying order book data (ob collectors)

Eight separate OB collectors (`ob_binance_spot`, `ob_binance_futures`, `ob_bybit_spot`, `ob_bybit_futures`, `ob_okx_spot`, `ob_okx_futures`, `ob_gate_spot`, `ob_gate_futures`) maintain full 10-level order books (bids + asks). Each collector writes to two Redis structures: a current-state HASH and a history ZSET.

### Current order book (HASH)

```
ob:{ex}:{mkt}:{symbol}    →   HASH (41 fields)
```

Fields:

| Field | Description |
|-------|-------------|
| `b1`..`b10` | Bid prices, level 1 (best) to level 10 |
| `bq1`..`bq10` | Bid quantities, level 1 to level 10 |
| `a1`..`a10` | Ask prices, level 1 (best) to level 10 |
| `aq1`..`aq10` | Ask quantities, level 1 to level 10 |
| `t` | Exchange timestamp (milliseconds) or local time if exchange doesn't provide it |

Example:

```bash
redis-cli -s /var/run/redis/redis.sock HGETALL ob:bn:f:BTCUSDT
# b1  67234.10    bq1  12.500
# b2  67233.00    bq2   8.200
# ...
# a1  67234.20    aq1   9.100
# ...
# t   1711234567123
```

Key naming (`{ex}:{mkt}`):

```
ob:bn:s  — Binance spot
ob:bn:f  — Binance futures
ob:bb:s  — Bybit spot
ob:bb:f  — Bybit futures
ob:ok:s  — OKX spot
ob:ok:f  — OKX futures
ob:gt:s  — Gate.io spot
ob:gt:f  — Gate.io futures
```

### Order book history (ZSET)

```
ob:hist:{ex}:{mkt}:{sym}:{chunk_id}   →   ZSET
  score:  ts_ms (integer milliseconds)
  member: b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
```

`chunk_id = int(unix_seconds // 1200)` — changes every 20 minutes. Four chunks are kept (80-minute window, always covers the last hour). Writes are throttled to 1 sample per second per symbol.

The member is a pipe-delimited string of 41 values: 10 bid price+qty pairs, then 10 ask price+qty pairs, then the timestamp.

### Config key

```bash
redis-cli -s /var/run/redis/redis.sock GET ob:hist:config | python3 -m json.tool
```

Output:
```json
{
  "chunk_sec": 1200,
  "max_chunks": 4,
  "sample_sec": 1,
  "levels": 10,
  "active_chunk_id": 1450782,
  "key_pattern": "ob:hist:{ex}:{mkt}:{sym}:{chunk_id}",
  "member_format": "b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms",
  "sources": ["bn:s","bn:f","bb:s","bb:f","ok:s","ok:f","gt:s","gt:f"],
  "chunks": [...]
}
```

### Query the last hour for a symbol

```bash
# Get current chunk_id
CID=$(python3 -c "import time; print(int(time.time()//1200))")
NOW_MS=$(date +%s%3N)
SINCE_MS=$((NOW_MS - 3600000))

# Read all 4 chunks and parse in Python
python3 - <<'EOF'
import redis, time

r = redis.Redis(unix_socket_path="/var/run/redis/redis.sock")
cid = int(time.time() // 1200)
now_ms = int(time.time() * 1000)
since_ms = now_ms - 3600000

rows = []
for i in range(3, -1, -1):
    key = f"ob:hist:bn:f:BTCUSDT:{cid - i}"
    for val, score in r.zrangebyscore(key, since_ms, "+inf", withscores=True):
        parts = val.decode().split("|")
        # parts[0]=b1, parts[1]=bq1, ..., parts[39]=aq10, parts[40]=ts_ms
        rows.append({
            "b1": parts[0], "a1": parts[20],
            "ts_ms": int(parts[40])
        })

print(f"Rows in last hour: {len(rows)}")
if rows:
    print(f"Best bid: {rows[-1]['b1']},  Best ask: {rows[-1]['a1']}")
EOF
```

### Scan current OB keys

```bash
# All ob keys (current state)
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:bn:*" | grep -v hist | wc -l

# Check a specific exchange+market
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:bb:f:*" | grep -v hist

# Count all ob: history chunks
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:hist:*" | wc -l

# Get best bid/ask from current OB for a symbol
redis-cli -s /var/run/redis/redis.sock HMGET ob:bn:f:BTCUSDT b1 bq1 a1 aq1 t
```

### Check OB data freshness

```bash
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:bn:f:*" | grep -v hist | head -5 | while read key; do
  T=$(redis-cli -s /var/run/redis/redis.sock HGET "$key" t 2>/dev/null)
  [ -n "$T" ] && echo "$key  $(( ($(date +%s%3N) - T) / 1000 ))s old"
done
```

### Memory estimate

~1 sample/sec × 3600 sec × 4 chunks × 4000 ob×symbol pairs × ~200 bytes ≈ **2–3 GB** peak (10-level OB entries are ~4× larger than single best-price entries).

---

## 10. Updating pair lists

New coins get listed and delisted regularly. Refresh weekly or after major exchange announcements.

```bash
pkill -f run_all.py
cd dictionaries && python3 main.py && cd ..
python3 run_all.py
```

---

## 11. Stopping the system

### Foreground (Ctrl+C)

The system gracefully:
1. Cancels all async tasks
2. Flushes remaining Redis batches
3. Closes log and signal files
4. Logs `stopped` event

### Background

```bash
pkill -f run_all.py
# or
kill $(cat run_all.pid)
```

---

## 12. Querying Redis directly

```bash
redis-cli -s /var/run/redis/redis.sock
```

### Get a price

```bash
redis-cli -s /var/run/redis/redis.sock HGETALL md:bn:s:BTCUSDT
# Output:
# b  67234.10    ← best bid
# a  67234.20    ← best ask
# t  1711234567123  ← exchange timestamp ms
```

### Key naming

```
md:{ex}:{mkt}:{symbol}       — best bid/ask HASH (3 fields: b, a, t)
ob:{ex}:{mkt}:{symbol}       — 10-level order book HASH (41 fields: b1..b10, bq1..bq10, a1..a10, aq1..aq10, t)
md:hist:{ex}:{mkt}:{sym}:{chunk_id}   — price history ZSET
ob:hist:{ex}:{mkt}:{sym}:{chunk_id}   — order book history ZSET

{ex}:  bn=Binance  bb=Bybit  ok=OKX  gt=Gate.io
{mkt}: s=spot      f=futures
```

### Useful commands

```bash
# Total keys (all types)
redis-cli -s /var/run/redis/redis.sock DBSIZE

# Best-price keys for Binance spot
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:bn:s:*" | grep -v hist

# Order book keys for Binance spot
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:bn:s:*" | grep -v hist

# Get best bid/ask (md collector)
redis-cli -s /var/run/redis/redis.sock HGETALL md:bn:s:BTCUSDT

# Get full 10-level order book (ob collector)
redis-cli -s /var/run/redis/redis.sock HGETALL ob:bn:s:BTCUSDT

# Get just the top-of-book from OB key
redis-cli -s /var/run/redis/redis.sock HMGET ob:bn:f:BTCUSDT b1 bq1 a1 aq1 t

# Check data age for a key
T=$(redis-cli -s /var/run/redis/redis.sock HGET md:bn:s:BTCUSDT t)
echo "Age: $(( ($(date +%s%3N) - T) / 1000 ))s"

# Check OB data age
T=$(redis-cli -s /var/run/redis/redis.sock HGET ob:bn:f:BTCUSDT t)
echo "OB Age: $(( ($(date +%s%3N) - T) / 1000 ))s"
```

---

## 13. Troubleshooting

### Collector shows `CRASHED`

```bash
jq 'select(.event == "collector_crashed")' logs/collectors_*.log
```

Common causes:
- Subscribe file missing → re-run `dictionaries/main.py`
- Redis not running → `sudo bash setup_redis.sh --check`
- Network unreachable → check connectivity

### All collectors stuck in `connecting`

```bash
python3 -c "
import websockets, asyncio
asyncio.run(websockets.connect('wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker').__aenter__())
print('OK')
"
```

### `streaming` but msgs/s = 0

Normal for the first 30 seconds. Stats fire every 30 seconds.

### STALE keys at startup

Normal — they're from the previous run. Collectors will overwrite them within seconds.

If stale count grows while collectors show `streaming`:
```bash
jq 'select(.event == "check" and .stale_count > 0) | .stale_by_source' logs/collectors_*.log | tail -5
```

### spread_monitor shows `CRASHED` or no signals

```bash
# Check for errors
jq 'select(.script == "spread_monitor" and .lvl == "ERROR")' logs/collectors_*.log

# Verify combination files exist
ls dictionaries/combination/*.txt | wc -l   # should be 12
```

### Many anomalies (spread ≥ 100%)

Check if a specific collector is disconnected — its stale prices will produce false spreads:
```bash
jq 'select(.event == "anomaly") | {direction, symbol, spread_pct}' logs/collectors_*.log | head -20
```

If anomalies always involve the same exchange (e.g., `gt` in `fut_ex`), that collector is likely having issues.

### Redis connection refused

```bash
sudo bash setup_redis.sh --check
sudo systemctl start redis   # if down
```

### OB collector shows `subscribed` but 0 msgs after 60s

This means subscriptions were sent but no data is coming back. Common causes:

1. **Wrong channel name** — verify the exchange accepts the channel. All channels were verified in testing:
   - Binance: `@depth10@100ms` combined streams ✓
   - Bybit: `orderbook.50.{SYM}` (NOT `orderbook.10` — does not exist) ✓
   - OKX: `books` channel ✓
   - Gate spot: `spot.order_book` with interval `"100ms"` ✓
   - Gate futures: `futures.order_book` with interval `"0"` (NOT `"100ms"`) ✓

2. **Exchange rejected subscription** — check logs for error frames:
```bash
jq 'select(.script | startswith("ob_")) | select(.lvl == "WARN")' logs/collectors_*.log
```

### OB `avg_levels` is consistently below 10.0

Means some symbols consistently have fewer than 10 levels in the book (thin markets). This is normal for low-liquidity pairs. Check which symbols trigger shallow warnings:

```bash
jq 'select(.script | startswith("ob_")) | select(.event == "stats") | {script, avg_levels, shallow_warnings}' logs/collectors_*.log | tail -20
```

### OB `books_initialized` count stopped growing (Bybit/OKX)

Means some symbols never received their initial snapshot. Usually resolves on reconnect. If persistent:
```bash
jq 'select(.script | startswith("ob_bb") or startswith("ob_ok")) | select(.event == "delta_before_snapshot" or .event == "update_before_snapshot")' logs/collectors_*.log | tail -10
```

---

## 14. System health checks

### Quick check

```bash
redis-cli -s /var/run/redis/redis.sock PING       # should return PONG
redis-cli -s /var/run/redis/redis.sock DBSIZE     # number of price keys
pgrep -f run_all.py && echo "Running" || echo "Stopped"
ls -lh signals/signals.csv signals/anomalies.csv  # signal file sizes
```

### Full Redis diagnostics

```bash
sudo bash setup_redis.sh --check
```

### Check data freshness

```bash
# md keys (best bid/ask)
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:bn:s:*" | grep -v hist | head -5 | while read key; do
  T=$(redis-cli -s /var/run/redis/redis.sock HGET "$key" t 2>/dev/null)
  [ -n "$T" ] && echo "md $key  $(( ($(date +%s%3N) - T) / 1000 ))s old"
done

# ob keys (10-level order book)
redis-cli -s /var/run/redis/redis.sock --scan --pattern "ob:bn:f:*" | grep -v hist | head -5 | while read key; do
  T=$(redis-cli -s /var/run/redis/redis.sock HGET "$key" t 2>/dev/null)
  [ -n "$T" ] && echo "ob $key  $(( ($(date +%s%3N) - T) / 1000 ))s old"
done
```

### Check OB collector stats

```bash
# Latest stats from each ob collector
jq 'select(.script | startswith("ob_") and (.event == "stats"))' logs/collectors_*.log | \
  jq -s 'group_by(.script) | map(last)[] | {script, msgs_per_sec, avg_levels, shallow_warnings}'
```

### Errors in the last hour

```bash
awk -v cutoff="$(date -d '1 hour ago' +%s)" '
  {split($0, a, "\"ts\":"); split(a[2], b, ","); if (b[1]+0 > cutoff) print}
' logs/collectors_*.log | jq 'select(.lvl == "WARN" or .lvl == "ERROR")'
```

---

## 15. Reference

### run_all.py flags

```
python3 run_all.py [OPTIONS]

--buckets    Enable 1-min age buckets in staleness_monitor logs
--no-dash    No live dashboard, JSON logs only
```

At startup (interactive mode), prompts for delay before signals/snapshots. Skipped with delay=0 in non-interactive mode (stdin not tty).

### md Collector constants (best bid/ask — binance_spot, etc.)

| Constant | Value | Description |
|----------|-------|-------------|
| `BATCH_SIZE` | 100 | Redis pipeline flush threshold |
| `BATCH_TIMEOUT` | 20ms | Redis pipeline flush timeout |
| `STATS_INTERVAL` | 30s | Stats log frequency |
| `RECONNECT_DELAY` | 3s | Wait after disconnect |
| `PING_INTERVAL` | 20–25s | Exchange keepalive |

### md Collector log events (full list)

| Event | Level | Key fields |
|-------|-------|------------|
| `startup` | INFO | — |
| `symbols_loaded` | INFO | `count`, `file` |
| `redis_connected` | INFO | `socket` |
| `connecting` | INFO | `chunk_symbols` or `total_symbols` |
| `connected` | INFO | — |
| `subscribed` | INFO | `total_symbols`, `subscribe_ms` |
| `subscribing_progress` | INFO | `subscribed`, `total`, `elapsed_ms` |
| `book_init` | INFO | `symbol`, `bid_levels`, `ask_levels`, `books_initialized` *(Bybit/OKX)* |
| `first_message` | INFO | `ms_since_connected`, `first_sym` |
| `delta_before_snapshot` | WARN | `symbol`, `delta_skipped_total` *(Bybit/OKX)* |
| `update_before_snapshot` | WARN | `symbol`, `update_skipped_total` *(OKX)* |
| `stats` | INFO | `msgs_total`, `msgs_per_sec`, `flushes_total`, `avg_pipeline_ms` |
| `disconnected` | WARN | `reason` |
| `reconnecting` | INFO | `delay_sec` |
| `recv_error` | WARN | `reason` |

### Staleness monitor constants

| Constant | Value | Description |
|----------|-------|-------------|
| `CHECK_INTERVAL` | 60s | Scan frequency |
| `STALE_THRESHOLD` | 300s | Age threshold for stale |
| `SCAN_COUNT` | 500 | Keys per SCAN iteration |

### Spread monitor constants

| Constant | Value | Description |
|----------|-------|-------------|
| `POLL_INTERVAL` | 0.3s | Redis scan frequency |
| `MIN_SPREAD_PCT` | 1.0% | Minimum spread to signal |
| `ANOMALY_THRESHOLD` | 100.0% | Threshold for anomaly file |
| `STALE_THRESHOLD` | 300s | Skip pair if data older than this |
| `COOLDOWN_SEC` | 3500s | Silence per (direction, symbol) after signal; also = snapshot window duration |
| `STATS_INTERVAL` | 30s | Stats log frequency |

### Spread monitor log events (full list)

| Event | Level | Key fields |
|-------|-------|-----------|
| `startup` | INFO | `poll_interval_sec`, `min_spread_pct`, `cooldown_sec` |
| `directions_loaded` | INFO | `directions`, `total_symbols` |
| `redis_connected` | INFO | `socket` |
| `signal_files_opened` | INFO | paths, `anomaly_threshold_pct` |
| `signal` | INFO | `direction`, `symbol`, `spread_pct`, `spot_ask`, `fut_bid`, `cooldown_until` |
| `anomaly` | INFO | same as `signal` |
| `snapshot_opened` | INFO | `direction`, `symbol`, `file`, `duration_sec` |
| `snapshot_history_written` | INFO | `direction`, `symbol`, `rows` |
| `stats` | INFO | `signals_total`, `anomalies_total`, `cooldowns_active`, `snapshots_active`, `last_cycle_ms` |
| `cycle_error` | ERROR | `reason` |

### ob Collector constants (10-level order book — ob_binance_spot, etc.)

Each OB snapshot contributes 40 columns (20 bid + 20 ask) to snapshot CSVs, for a total of **87 columns** per snapshot row (7 base fields + 40 spot OB + 40 futures OB).

| Constant | Value | Description |
|----------|-------|-------------|
| `LEVELS` | 10 | Depth levels written per side |
| `BATCH_SIZE` | 100 | Redis pipeline flush threshold |
| `BATCH_TIMEOUT` | 20ms | Redis pipeline flush timeout |
| `STATS_INTERVAL` | 30s | Stats log frequency |
| `RECONNECT_DELAY` | 3s | Wait after disconnect |
| `PING_INTERVAL` | 20s | Exchange keepalive |
| `SUB_DELAY` | 10ms | Delay between subscribe messages *(Gate)* |
| `SUB_LOG_EVERY` | 100 | Log progress every N subscriptions *(Gate)* |
| `SUB_BATCH` | 10 (spot) / 200 (futures) | Symbols per subscribe batch *(Bybit)* |
| `CHUNK_SIZE` | 200 | Symbols per WS connection *(Binance)* |

### ob Collector WS channel details

| Exchange | WS Endpoint | Channel | Update Model |
|----------|-------------|---------|--------------|
| Binance spot | `stream.binance.com` | `{sym}@depth10@100ms` combined stream | Periodic snapshots (no local book) |
| Binance futures | `fstream.binance.com` | `{sym}@depth10@100ms` combined stream | Periodic snapshots; uses `b`/`a` fields (not `bids`/`asks`) |
| Bybit spot | `stream.bybit.com/v5/public/spot` | `orderbook.50.{SYM}` | Snapshot + delta → local book (top 10 extracted) |
| Bybit futures | `stream.bybit.com/v5/public/linear` | `orderbook.50.{SYM}` | Snapshot + delta → local book (top 10 extracted) |
| OKX spot | `ws.okx.com:8443/ws/v5/public` | `books` channel | Snapshot + update → local book (up to 400 levels, top 10 extracted) |
| OKX futures | `ws.okx.com:8443/ws/v5/public` | `books` channel | Snapshot + update → local book (up to 400 levels, top 10 extracted) |
| Gate.io spot | `api.gateio.ws/ws/v4/` | `spot.order_book`, interval `100ms` | Periodic full snapshots |
| Gate.io futures | `fx-ws.gateio.ws/v4/ws/usdt` | `futures.order_book`, interval `0` | Periodic full snapshots |

### ob Collector log events (full list)

| Event | Level | Key fields |
|-------|-------|------------|
| `startup` | INFO | — |
| `symbols_loaded` | INFO | `count`, `file` |
| `redis_connected` | INFO | `socket` |
| `subscribing` | INFO | `total_symbols`, `connections` *(Binance)* |
| `connecting` | INFO | `total_symbols` or `chunk_symbols` |
| `connected` | INFO | — |
| `subscribed` | INFO | `total_symbols`, `subscribe_ms` |
| `subscribing_progress` | INFO | `subscribed`, `total`, `elapsed_ms` *(Gate)* |
| `book_init` | INFO | `symbol`, `bid_levels`, `ask_levels`, `books_initialized` *(Bybit/OKX)* |
| `first_message` | INFO | `ms_since_connected`, `first_sym` |
| `delta_before_snapshot` | WARN | `symbol`, `delta_skipped_total` *(Bybit)* |
| `update_before_snapshot` | WARN | `symbol`, `update_skipped_total` *(OKX)* |
| `stats` | INFO | `msgs_total`, `msgs_per_sec`, `flushes_total`, `avg_pipeline_ms`, `avg_levels`, `shallow_warnings`, *(Bybit/OKX add)* `books_initialized`, `snapshots_total`, `deltas_total`, `delta_skipped` |
| `disconnected` | WARN | `reason` |
| `reconnecting` | INFO | `delay_sec` |
| `recv_error` | WARN | `reason` |

### ob Collector stats fields

| Field | Description |
|-------|-------------|
| `msgs_total` | Total OB snapshots written to Redis |
| `msgs_per_sec` | Rate over last 30s interval |
| `flushes_total` | Redis pipeline batches executed |
| `avg_pipeline_ms` | Average Redis pipeline flush latency |
| `avg_levels` | Average levels present per flush (max 10.0) — lower = shallower book |
| `shallow_warnings` | Times a symbol had < 10 levels on either bid or ask side |
| `books_initialized` | Symbols with a live local book *(Bybit/OKX only)* |
| `snapshots_total` | Snapshot messages received *(Bybit/OKX only)* |
| `deltas_total` | Delta/update messages received *(Bybit/OKX only)* |
| `delta_skipped` | Deltas dropped (arrived before snapshot) *(Bybit/OKX only)* |

### History writer constants (hist_writer.py / ob_hist_writer.py)

Both `hist_writer.py` (md) and `ob_hist_writer.py` (ob) use identical chunk parameters:

| Constant | Value | Description |
|----------|-------|-------------|
| `CHUNK_SEC` | 1200s | 20-minute chunk duration |
| `MAX_CHUNKS` | 4 | Chunks kept at once (80 min window) |
| `SAMPLE_SEC` | 1s | Max write rate per symbol |
| `CONFIG_KEY` | `md:hist:config` (md) / `ob:hist:config` (ob) | Global config key in Redis |
| `LEVELS` | 10 | Depth levels per side stored per sample *(ob only)* |

Member format in Redis ZSET:
- **md** (`md:hist:*`): `{bid}|{ask}|{ts_ms}`
- **ob** (`ob:hist:*`): `b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms` (41 pipe-delimited values)

### Log rotation

| Setting | Value |
|---------|-------|
| Chunk size | 12 hours |
| Max chunks kept | 2 (last 24 hours) |
| File pattern | `logs/collectors_YYYY-MM-DD_HH-MM.log` |
| Rotation trigger | Elapsed time (checked each write) |
