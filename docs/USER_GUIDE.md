# User Guide — Bali 3.0

Step-by-step instructions for setting up, running, and maintaining the system from scratch.

---

## Table of Contents

1. [First-time setup](#1-first-time-setup)
2. [Generate trading pair dictionaries](#2-generate-trading-pair-dictionaries)
3. [Start the collectors](#3-start-the-collectors)
4. [Reading the dashboard](#4-reading-the-dashboard)
5. [Reading logs](#5-reading-logs)
6. [Updating pair lists](#6-updating-pair-lists)
7. [Stopping the system](#7-stopping-the-system)
8. [Querying Redis directly](#8-querying-redis-directly)
9. [Troubleshooting](#9-troubleshooting)
10. [System health checks](#10-system-health-checks)

---

## 1. First-time setup

### Install Redis

```bash
sudo bash setup_redis.sh
```

This installs Redis, configures it for low-latency operation on a Unix socket, and starts the service. Takes about 30–60 seconds.

Verify it worked:

```bash
sudo bash setup_redis.sh --check
```

You should see `PING: X.XXXms` in green. If you see `FAIL` in red, Redis is not running — re-run the setup script.

### Install Python dependencies

```bash
pip install websockets orjson redis[hiredis] rich aiohttp
```

### Clone and enter the project

```bash
cd /root/bali3.0
```

All commands below are run from this directory unless specified otherwise.

---

## 2. Generate trading pair dictionaries

**Run this once before starting collectors, and again whenever you want to refresh the pair list.**

```bash
cd dictionaries && python3 main.py && cd ..
```

What happens during the ~70 seconds:

```
[0s]   Phase 1 — Connects to REST APIs for all 4 exchanges simultaneously
[~5s]  Phase 2 — Opens WebSocket connections to all exchanges, listens for 60 seconds
[~65s] Phase 3 — Builds 12 intersection files (which pairs are active on both exchanges)
[~65s] Phase 4 — Builds 8 subscription files (which symbols each collector should subscribe to)
[~65s] Phase 5 — Prints the statistics report
```

**Expected result at the end:**

```
subscribe/binance/binance_spot.txt:    418 symbols
subscribe/binance/binance_futures.txt: 203 symbols
subscribe/bybit/bybit_spot.txt:        312 symbols
...
```

If any exchange shows 0 symbols, that exchange likely had a connection issue — re-run.

### What gets generated

```
dictionaries/subscribe/binance/binance_spot.txt    ← symbols for binance_spot collector
dictionaries/subscribe/binance/binance_futures.txt ← symbols for binance_futures collector
dictionaries/subscribe/bybit/bybit_spot.txt
dictionaries/subscribe/bybit/bybit_futures.txt
dictionaries/subscribe/okx/okx_spot.txt
dictionaries/subscribe/okx/okx_futures.txt
dictionaries/subscribe/gate/gate_spot.txt
dictionaries/subscribe/gate/gate_futures.txt
```

These 8 files are read by collectors at startup.

---

## 3. Start the collectors

### Normal start (dashboard + logs to file)

```bash
python3 run_all.py > logs/collectors.log
```

- Dashboard renders in your terminal (updates every 2 seconds)
- Clean JSON logs saved to `logs/collectors.log`
- Press `Ctrl+C` to stop

### Start with staleness bucket distribution

```bash
python3 run_all.py --buckets > logs/collectors.log
```

Adds a log line every 60s showing how old the data is by 1-minute buckets:
`fresh (<1min), 1-2min, 2-3min, 3-4min, 4-5min, 5+min (stale)`

### Start without dashboard (logs only)

```bash
python3 run_all.py --no-dash 2>&1 | tee logs/collectors.log
```

Useful if you're running in a non-interactive environment or want raw JSON only.

### Run in background

```bash
nohup python3 run_all.py > logs/collectors.log 2>&1 &
echo $! > run_all.pid
```

To stop background process:

```bash
kill $(cat run_all.pid)
```

---

## 4. Reading the dashboard

The dashboard shows live metrics per collector, updating every 2 seconds.

```
Bali 3.0  uptime 120s
┌──────────────────────┬───────────┬────────┬───────────┬─────────┬─────────┬────────┬──────────┐
│ Script               │ Status    │ msgs/s │ total     │ flushes │ pipe ms │ errors │ last seen│
```

### Column meanings

| Column | Description |
|--------|-------------|
| **Script** | Collector name |
| **Status** | Current lifecycle state |
| **msgs/s** | Messages received per second (updated every 30s) |
| **total** | Total messages received since start |
| **flushes** | Number of Redis pipeline batches executed |
| **pipe ms** | Average Redis pipeline latency in milliseconds |
| **errors** | Number of disconnection events |
| **last seen** | Seconds since last log event from this script |

### Status values

| Status | Color | Meaning |
|--------|-------|---------|
| `starting` | dim | Script initialized, not yet connecting |
| `connecting` | yellow | TCP connection in progress |
| `connected` | cyan | WebSocket handshake complete, sending subscriptions |
| `subscribed` | cyan | Subscriptions sent, waiting for first message |
| `streaming` | **green** | Receiving market data — normal operating state |
| `disconnected` | red | Connection lost, will auto-reconnect |
| `reconnecting` | yellow | Waiting 3s before reconnect attempt |
| `CRASHED` | **bold red** | Unhandled exception — check logs |
| `ok` | green | (staleness_monitor) Last check: no stale keys |
| `STALE:N` | red | (staleness_monitor) N keys haven't updated in 5+ minutes |
| `checking` | cyan | (staleness_monitor) Scan in progress |

### Normal behavior timeline

```
0s   → all scripts show "connecting"
2s   → "connected" then "subscribed"
5s   → "streaming" (first_message event received)
30s  → msgs/s and total columns populate with first stats
60s  → staleness_monitor runs first check, shows "ok"
```

**Important:** `msgs/s = 0` and `total = 0` for the first 30 seconds is normal. Stats are reported every 30 seconds.

---

## 5. Reading logs

Logs are written to `logs/collectors.log` as newline-delimited JSON.

### Follow live

```bash
tail -f logs/collectors.log
```

### Follow live with pretty print

```bash
tail -f logs/collectors.log | jq .
```

### Useful filters

```bash
# Only errors and warnings
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors.log

# Stats from all collectors
jq 'select(.event == "stats")' logs/collectors.log

# Stats from one collector
jq 'select(.script == "binance_spot" and .event == "stats")' logs/collectors.log

# All disconnection events
jq 'select(.event == "disconnected")' logs/collectors.log

# Stale key alerts (only when there are stale keys)
jq 'select(.event == "check" and .stale_count > 0)' logs/collectors.log

# Staleness check timing
jq 'select(.event == "check_complete")' logs/collectors.log

# How long did subscriptions take per collector?
jq 'select(.event == "subscribed") | {script, subscribe_ms}' logs/collectors.log

# How long from connect to first message?
jq 'select(.event == "first_message") | {script, ms_since_connected}' logs/collectors.log
```

### Log line structure

Every line is a JSON object:

```json
{
  "ts":     1711234567.123,   // Unix timestamp (float)
  "lvl":    "INFO",           // INFO | WARN | ERROR
  "script": "binance_spot",   // which script emitted this
  "event":  "stats",          // event name
  // ... event-specific fields
}
```

---

## 6. Updating pair lists

New coins get listed and delisted regularly. Re-run the dictionary generator to refresh:

```bash
# Stop collectors
pkill -f run_all.py

# Regenerate dictionaries (~70 seconds)
cd dictionaries && python3 main.py && cd ..

# Restart collectors
python3 run_all.py > logs/collectors.log
```

**How often to refresh:** Weekly is sufficient for most use cases. After major exchange announcements (large batches of new listings) re-run immediately.

---

## 7. Stopping the system

### If running in foreground

Press `Ctrl+C`. The system will:
1. Send cancel to all tasks
2. Flush remaining Redis batches
3. Log `stopped` event

### If running in background

```bash
pkill -f run_all.py
```

Or if you saved the PID:

```bash
kill $(cat run_all.pid)
```

---

## 8. Querying Redis directly

Connect to Redis via Unix socket:

```bash
redis-cli -s /var/run/redis/redis.sock
```

### Get a specific price

```bash
# Binance Spot BTC/USDT
redis-cli -s /var/run/redis/redis.sock HGETALL md:bn:s:BTCUSDT

# OKX Futures ETH/USDT
redis-cli -s /var/run/redis/redis.sock HGETALL md:ok:f:ETHUSDT
```

Output:
```
1) "b"
2) "67234.10"
3) "a"
4) "67234.20"
5) "t"
6) "1711234567123"
```

### Count all market data keys

```bash
redis-cli -s /var/run/redis/redis.sock DBSIZE
```

### Scan all keys for a specific exchange

```bash
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:bn:*"
```

### Check age of a key

```bash
# Get timestamp from key
T=$(redis-cli -s /var/run/redis/redis.sock HGET md:bn:s:BTCUSDT t)
# Compare to now (in seconds)
echo "Age: $(($(date +%s%3N) - T)) ms"
```

### Key naming reference

```
md:{ex}:{mkt}:{symbol}

{ex}:  bn = Binance
       bb = Bybit
       ok = OKX
       gt = Gate.io

{mkt}: s = spot
       f = futures
```

---

## 9. Troubleshooting

### Collector shows `CRASHED`

Check the log for the specific error:

```bash
jq 'select(.event == "collector_crashed")' logs/collectors.log
```

Common causes:
- Subscribe file missing — re-run `dictionaries/main.py`
- Redis not running — run `sudo bash setup_redis.sh --check`
- Network issue — check internet connectivity

### All collectors stuck in `connecting`

Network or DNS issue. Test manually:

```bash
python3 -c "import websockets, asyncio; asyncio.run(websockets.connect('wss://stream.binance.com:9443/stream?streams=btcusdt@bookTicker').__aenter__())" && echo "OK"
```

### `streaming` status but msgs/s = 0

This is normal for the first 30 seconds. `msgs/s` is populated from `stats` events which fire every 30 seconds. Wait for the first stats cycle.

### STALE keys in staleness monitor

Some keys being stale at startup is normal — they're from the previous run before collectors restarted. They'll become fresh within seconds as collectors begin writing.

If stale count grows over time while collectors show `streaming`, it means a specific exchange is not sending data for those symbols. Check which exchange:

```bash
jq 'select(.event == "check" and .stale_count > 0) | .stale_by_source' logs/collectors.log | tail -5
```

### Redis connection refused

```bash
sudo bash setup_redis.sh --check
# If Redis is down:
sudo systemctl start redis
```

### OKX shows 0 messages

OKX requires subscriptions to be sent as **text** WebSocket frames (not binary). The collector handles this with `.decode()` on the JSON payload. If you see 0 messages from OKX after subscribing:
1. Check that the subscribe file exists: `ls dictionaries/subscribe/okx/`
2. Check for `pong` messages as the first received (indicates binary frames being sent)

### Gate.io futures shows errors

Gate.io futures use a separate endpoint (`fx-ws.gateio.ws`) that requires ping messages with the channel name `futures.ping`. If the regular ping is sent, the connection will be closed.

### `AttributeError: readonly attribute` on startup

Old version of `run_all.py`. Pull the latest version from the repository.

---

## 10. System health checks

### Quick health check

```bash
# Redis responding
redis-cli -s /var/run/redis/redis.sock PING

# Number of keys in Redis
redis-cli -s /var/run/redis/redis.sock DBSIZE

# Collectors running
pgrep -f run_all.py && echo "Running" || echo "Not running"
```

### Full Redis diagnostics

```bash
sudo bash setup_redis.sh --check
```

Reports:
- Ping latency
- Memory usage and fragmentation
- Connected clients
- Recent slow commands
- Eviction statistics

### Check data freshness manually

```bash
# Sample 10 random keys and check their age
redis-cli -s /var/run/redis/redis.sock --scan --pattern "md:*" | head -10 | while read key; do
  T=$(redis-cli -s /var/run/redis/redis.sock HGET "$key" t 2>/dev/null)
  if [ -n "$T" ]; then
    AGE=$(( ($(date +%s%3N) - T) / 1000 ))
    echo "$key  age: ${AGE}s"
  fi
done
```

### Check for errors in the last hour

```bash
awk -v cutoff="$(date -d '1 hour ago' +%s)" '
  {split($0, a, "\"ts\":"); split(a[2], b, ","); if (b[1]+0 > cutoff) print}
' logs/collectors.log | jq 'select(.lvl == "WARN" or .lvl == "ERROR")'
```

---

## Reference

### All run_all.py flags

```
python3 run_all.py [OPTIONS]

Options:
  --buckets    Enable age-bucket distribution in staleness_monitor logs
  --no-dash    Disable live dashboard, output JSON logs only
```

### Timing constants (collectors)

| Constant | Value | Description |
|----------|-------|-------------|
| `BATCH_SIZE` | 100 | Redis pipeline flush threshold (messages) |
| `BATCH_TIMEOUT` | 20ms | Redis pipeline flush threshold (time) |
| `STATS_INTERVAL` | 30s | How often `stats` event is logged |
| `RECONNECT_DELAY` | 3s | Wait before reconnect after disconnect |
| `PING_INTERVAL` | 20–25s | Exchange keepalive ping interval |

### Staleness monitor constants

| Constant | Value | Description |
|----------|-------|-------------|
| `CHECK_INTERVAL` | 60s | How often Redis keys are scanned |
| `STALE_THRESHOLD` | 300s (5 min) | Age at which a key is considered stale |
| `SCAN_COUNT` | 500 | Keys per SCAN iteration |
