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
7. [Updating pair lists](#7-updating-pair-lists)
8. [Stopping the system](#8-stopping-the-system)
9. [Querying Redis directly](#9-querying-redis-directly)
10. [Troubleshooting](#10-troubleshooting)
11. [System health checks](#11-system-health-checks)
12. [Reference](#12-reference)

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
1. Redis is flushed (all old keys removed)
2. A new log file is opened: `logs/collectors_YYYY-MM-DD_HH-MM.log`
3. All 8 collectors start connecting to their exchanges
4. `staleness_monitor` and `spread_monitor` start
5. Live dashboard appears in terminal

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

## 7. Updating pair lists

New coins get listed and delisted regularly. Refresh weekly or after major exchange announcements.

```bash
pkill -f run_all.py
cd dictionaries && python3 main.py && cd ..
python3 run_all.py
```

---

## 8. Stopping the system

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

## 9. Querying Redis directly

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
md:{ex}:{mkt}:{symbol}

{ex}:  bn=Binance  bb=Bybit  ok=OKX  gt=Gate.io
{mkt}: s=spot      f=futures
```

### Useful commands

```bash
# Total keys
redis-cli -s /var/run/redis/redis.sock DBSIZE

# All Binance keys
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:bn:*"

# Check data age for a key
T=$(redis-cli -s /var/run/redis/redis.sock HGET md:bn:s:BTCUSDT t)
echo "Age: $(( ($(date +%s%3N) - T) / 1000 ))s"
```

---

## 10. Troubleshooting

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

---

## 11. System health checks

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
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:*" | head -10 | while read key; do
  T=$(redis-cli -s /var/run/redis/redis.sock HGET "$key" t 2>/dev/null)
  [ -n "$T" ] && echo "$key  $(( ($(date +%s%3N) - T) / 1000 ))s old"
done
```

### Errors in the last hour

```bash
awk -v cutoff="$(date -d '1 hour ago' +%s)" '
  {split($0, a, "\"ts\":"); split(a[2], b, ","); if (b[1]+0 > cutoff) print}
' logs/collectors_*.log | jq 'select(.lvl == "WARN" or .lvl == "ERROR")'
```

---

## 12. Reference

### run_all.py flags

```
python3 run_all.py [OPTIONS]

--buckets    Enable 1-min age buckets in staleness_monitor logs
--no-dash    No live dashboard, JSON logs only
```

### Collector constants

| Constant | Value | Description |
|----------|-------|-------------|
| `BATCH_SIZE` | 100 | Redis pipeline flush threshold |
| `BATCH_TIMEOUT` | 20ms | Redis pipeline flush timeout |
| `STATS_INTERVAL` | 30s | Stats log frequency |
| `RECONNECT_DELAY` | 3s | Wait after disconnect |
| `PING_INTERVAL` | 20–25s | Exchange keepalive |

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
| `COOLDOWN_SEC` | 3500s | Silence per (direction, symbol) after signal |
| `STATS_INTERVAL` | 30s | Stats log frequency |

### Log rotation

| Setting | Value |
|---------|-------|
| Chunk size | 12 hours |
| Max chunks kept | 2 (last 24 hours) |
| File pattern | `logs/collectors_YYYY-MM-DD_HH-MM.log` |
| Rotation trigger | Elapsed time (checked each write) |
