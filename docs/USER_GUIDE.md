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
8. [Funding rates](#8-funding-rates)
9. [Querying price history (md)](#9-querying-price-history-md)
10. [Querying order book data (ob)](#10-querying-order-book-data-ob)
11. [Querying funding rate history (fr)](#11-querying-funding-rate-history-fr)
12. [ML dataset generation](#12-ml-dataset-generation)
13. [Snapshot CSV structure reference](#snapshot-csv-structure-89-columns)
14. [Updating pair lists](#14-updating-pair-lists)
15. [Stopping the system](#15-stopping-the-system)
16. [Querying Redis directly](#16-querying-redis-directly)
17. [Troubleshooting](#17-troubleshooting)
18. [Reference](#18-reference)

---

## 1. First-time setup

### Install Redis

```bash
sudo bash setup_redis.sh
```

Installs Redis, configures it for low-latency operation on a Unix socket (`/var/run/redis/redis.sock`), and starts the service.

Verify:

```bash
sudo bash setup_redis.sh --check
```

You should see `PING: X.XXXms` in green. Recommended maxmemory: **40 GB**.

```bash
# Check or set memory limit
redis-cli -s /var/run/redis/redis.sock CONFIG GET maxmemory
redis-cli -s /var/run/redis/redis.sock CONFIG SET maxmemory 40gb
redis-cli -s /var/run/redis/redis.sock CONFIG REWRITE
```

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

Expected output at the end shows symbol counts per subscribe file. If any exchange shows 0 — re-run (temporary connection issue).

**What gets generated:**

- **8 subscription files** in `dictionaries/subscribe/` — read by collectors at startup
- **12 combination files** in `dictionaries/combination/` — read by spread_monitor at startup

---

## 3. Start the system

### Normal start

```bash
python3 run_all.py
```

What happens:
1. Previous `logs/` and `signals/` are moved to `old/YYYY-MM-DD_HH-MM/` (if non-empty)
2. Redis is flushed (SCAN + DEL all keys)
3. You are prompted: `"Delay before signals/snapshots [seconds, Enter = 0]:"` — enter a number to let collectors warm up before signals start
4. All **20 data collectors** start connecting simultaneously:
   - 8 md collectors (best-bid/ask)
   - 8 ob collectors (10-level order book)
   - 4 fr collectors (funding rates)
5. `staleness_monitor` starts
6. `redis_monitor` starts (health checks every 5s)
7. `spread_monitor` starts after the specified delay
8. Live dashboard appears in terminal

### With --delay flag (skip interactive prompt)

```bash
python3 run_all.py --delay 60    # wait 60s before signals/snapshots start
python3 run_all.py --delay 0     # start everything immediately
```

### Other flags

```bash
python3 run_all.py --no-dash     # JSON logs only, no TUI (useful in background)
python3 run_all.py --buckets     # + staleness age-bucket distribution in logs
```

### Run in background

```bash
nohup python3 run_all.py --no-dash --delay 0 > /dev/null &
echo $! > run_all.pid
```

To stop:
```bash
kill $(cat run_all.pid)
```

---

## 4. Reading the dashboard

The dashboard updates every 1 second. It is a single panel with three sections.

### Layout

```
╭── BALI 3.0  2h 15m 30s  14:23:45  collectors_2026-03-21_12-00.log ─────╮
│ MD   ● bn-s  ● bn-f  ● bb-s  ● bb-f  ● ok-s  ● ok-f  ● gt-s  ● gt-f  │
│ OB   ● bn-s  ● bn-f  ● bb-s  ● bb-f  ○ ok-s  ○ ok-f  ● gt-s  ● gt-f  │
│ FR   ● bn-f  ● bb-f  ● ok-f  ● gt-f                                    │
│ ──────────────────────────────────────────────────────────────────────  │
│ signals:3  anom:0  cd:2  snaps:2  redis:431MB  keys:2,919  ping:0.4ms  │
│ ──────────────────────────────────────────────────────────────────────  │
│ > 11:42:33  gt→bb  VANRYUSDT  +7.85%  fr:-0.00030378                   │
│ > 11:38:12  bn→bb  BTCUSDT    +1.23%                                    │
╰─────────────────────────────────────────────────────────────────────────╯
```

### Collector groups (top section)

Three rows show all 20 collectors grouped by type. Each collector is a colored symbol + short name:

| Symbol | Color | Meaning |
|--------|-------|---------|
| `●` | **green** | streaming / ok / scanning |
| `○` | yellow | init in progress (ob collectors building books) |
| `!` | **red bold** | crashed / disconnected / stale |
| `~` | yellow | reconnecting |
| ` ` | dim | starting / connecting / subscribed |

Short name format: `bn-s` = Binance Spot, `bn-f` = Binance Futures, `bb-s` = Bybit Spot, etc.

Disconnection count shown as `(N)` suffix in red if > 0, e.g. `bb-f(2)`.

**OB init progress:** During startup, ob collectors for Bybit and OKX show `○` (yellow) while building local order books. Switches to `●` (green) once all books are initialized.

### Stats line (middle section)

```
signals:3  anom:0  cd:2  snaps:2  redis:431MB  keys:2,919  ping:0.4ms  wr:1.2ms  stale:0
```

| Field | Description |
|-------|-------------|
| `signals:N` | Total signals fired since startup |
| `anom:N` | Anomalies (spread ≥ 100%) — shown in red if > 0 |
| `cd:N` | Active cooldowns (direction+symbol pairs in 3500s cooldown) |
| `snaps:N` | Snapshot CSV files currently open and being written |
| `redis:XMB` | Redis used memory — yellow if WARN, red if CRIT |
| `keys:N` | Total Redis key count |
| `ping:Xms` | Redis PING RTT — yellow if > 6ms, red if > 30ms |
| `wr:Xms` | Redis write latency — yellow if > 15ms, red if > 60ms |
| `blocked:N` | Blocked Redis clients — red, indicates OOM |
| `stale:N` | md:* keys older than 5 minutes — red if > 0 |

Redis health is updated every 5 seconds by `redis_stats_loop` and `redis_monitor`.

### Signal feed (bottom section)

Last 5 signals, newest first:

```
> 11:42:33  gt→bb  VANRYUSDT  +7.85%  fr:-0.00030378
! 11:42:09  bn→gt  FUNUSDT  +2130.93%
```

| Symbol | Meaning |
|--------|---------|
| `>` (green) | Normal signal (spread 1%–99%) |
| `!` (red) | Anomaly (spread ≥ 100%) |

`gt→bb` = Gate spot → Bybit futures (short exchange codes).
`fr:` = current funding rate of the futures exchange at signal time (if available).

### Normal startup timeline

```
0s   → all collectors show connecting (dim)
2s   → connected → subscribed (dim)
5s   → md/fr collectors show ● streaming (green)
5s   → ob collectors show ○ (yellow, building books)
10s  → ob collectors finish init → ● streaming (green)
30s  → stats line fills in (first stats event from collectors)
60s  → staleness_monitor first check → stale:0
```

---

## 5. Reading logs

Logs are auto-written to rotating files. No redirect needed.

```bash
# Show current log file
ls -t logs/collectors_*.log | head -1

# Follow live
tail -f logs/collectors_*.log

# Pretty print with jq
tail -f logs/collectors_*.log | jq .
```

### Useful filters

```bash
# All errors and warnings
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log

# Collector stats (msg/s, pipeline latency)
jq 'select(.event == "stats" and .script != "spread_monitor")' logs/collectors_*.log

# Spread monitor stats
jq 'select(.script == "spread_monitor" and .event == "stats")' logs/collectors_*.log

# Funding rate REST refreshes
jq 'select(.event == "rest_refresh")' logs/collectors_*.log

# All disconnections
jq 'select(.event == "disconnected")' logs/collectors_*.log

# Stale keys alerts
jq 'select(.event == "check" and .stale_count > 0)' logs/collectors_*.log

# All signals and anomalies
jq 'select(.event == "signal" or .event == "anomaly")' logs/collectors_*.log

# Time from connect to first data
jq 'select(.event == "first_message") | {script, ms_since_connected}' logs/collectors_*.log

# Redis health warnings
jq 'select(.script == "redis_monitor" and .event == "degraded")' logs/collectors_*.log

# Redis slowlog entries
jq 'select(.script == "redis_monitor" and .event == "slowlog")' logs/collectors_*.log
```

### Log line structure

```json
{
  "ts":     1711234567.123,
  "lvl":    "INFO",
  "script": "spread_monitor",
  "event":  "signal",
  "direction": "gate_spot_bybit_futures",
  "symbol": "VANRYUSDT",
  "spread_pct": 7.8494,
  "spot_ask": "0.005631",
  "fut_bid": "0.006073",
  "spot_ex": "gt",
  "fut_ex": "bb",
  "cooldown_until": 1774094454
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

Files accumulate across restarts (append mode). CSV header written only when the file is first created.

### Signal JSONL fields

| Field | Type | Description |
|-------|------|-------------|
| `ts` | float | Unix timestamp |
| `direction` | string | e.g. `gate_spot_bybit_futures` |
| `symbol` | string | e.g. `VANRYUSDT` |
| `spread_pct` | float | `(fut_bid - spot_ask) / spot_ask × 100` |
| `spot_ask` | string | Best ask on spot exchange |
| `fut_bid` | string | Best bid on futures exchange |
| `spot_ex` | string | Spot exchange code: `bn`, `bb`, `ok`, `gt` |
| `fut_ex` | string | Futures exchange code: `bn`, `bb`, `ok`, `gt` |

### Signal CSV format (7 columns)

```
spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts
```

### Cooldown

After each signal fires, a 3500-second (≈58 minute) cooldown applies per (direction, symbol) pair. Prevents duplicate signals for the same opportunity.

### Anomaly detection

Spreads ≥ 100% are written to the anomaly files instead of signal files. Usually indicate a data quality issue (stale price, thin order book, delisted pair).

---

## 7. Snapshots

When a signal fires, a CSV snapshot file opens automatically:

```
signals/snapshots/YYYY-MM-DD/HH/{spot}_{fut}_{sym}_{YYYYMMDD_HHMMSS}.csv
```

The file is written every 0.3s for the full 3500-second cooldown window (≈2500 rows), then closed automatically. At the moment of signal, **1 hour of historical data is prepended** (from md:hist and ob:hist Redis ZSETs).

### Snapshot CSV structure (89 columns)

```
# Columns 1–7: Base
spot_source_id, fut_source_id, symbol, ask_spot, bid_futures, spread_pct, ts

# Columns 8–47: Spot order book (40 columns)
s_b1..s_b10    ← bid prices, level 1 = best bid
s_bq1..s_bq10  ← bid quantities
s_a1..s_a10    ← ask prices, level 1 = best ask
s_aq1..s_aq10  ← ask quantities

# Columns 48–87: Futures order book (40 columns)
f_b1..f_b10, f_bq1..f_bq10, f_a1..f_a10, f_aq1..f_aq10

# Columns 88–89: Funding rate of futures exchange (2 columns)
fr_r   ← current funding rate
fr_ft  ← next funding settlement time (unix ms)
```

Total: **89 columns** = 7 base + 40 spot OB + 40 futures OB + 2 FR.

`spot_source_id` and `fut_source_id` are fixed integer IDs (defined in `collectors/source_ids.json`):

| ID | Source |
|----|--------|
| 0 | binance_spot |
| 1 | binance_futures |
| 2 | bybit_spot |
| 3 | bybit_futures |
| 4 | okx_spot |
| 5 | okx_futures |
| 6 | gate_spot |
| 7 | gate_futures |

OB fields are empty strings when the ob collector for that exchange/pair is not running or the pair is missing from its book. FR fields are empty when no funding rate data is available for the futures exchange/symbol.

### Snapshot timing

| Event | Time |
|-------|------|
| Signal fires | t=0 |
| 1h history prepended (from Redis) | t=0 |
| First live row written | t=0 |
| Live rows written every 0.3s | t=0 to t=3500s |
| File closed | t=3500s |
| Approximate live rows | ~2500 |

---

## 8. Funding rates

Four fr collectors stream funding rates continuously.

### Redis key: `fr:{ex}:{symbol}`

```bash
redis-cli -s /var/run/redis/redis.sock HGETALL fr:bb:BTCUSDT
# r  = -0.0001234  (current funding rate)
# nr = -0.0001100  (predicted next rate)
# ft = 1774094400000  (next funding settlement, unix ms)
# ts = 1774090954000  (when we received this, unix ms)
```

### Coverage check

```bash
# Count fr keys for each exchange
redis-cli -s /var/run/redis/redis.sock KEYS "fr:bn:*" | wc -l  # Binance
redis-cli -s /var/run/redis/redis.sock KEYS "fr:bb:*" | wc -l  # Bybit
redis-cli -s /var/run/redis/redis.sock KEYS "fr:ok:*" | wc -l  # OKX
redis-cli -s /var/run/redis/redis.sock KEYS "fr:gt:*" | wc -l  # Gate.io
```

After ~2 minutes of warmup, all futures symbols should have fr data (fr_okx and fr_gate rely on REST fallback for first fill).

### FR collector specifics

| Collector | Update source | Notes |
|-----------|--------------|-------|
| `fr_binance_futures` | WS `!markPrice@arr@1s` | Single stream, all symbols at once, reconnects every ~23h |
| `fr_bybit_futures` | WS `tickers` channel | Delta pattern: caches snapshot, merges deltas, persists across reconnects |
| `fr_okx_futures` | WS `funding-rate` + REST | WS pushes only on change (~every 30 min); REST fills gaps every 30s |
| `fr_gate_futures` | WS `futures.tickers` + REST + contracts loop | REST fetches rates every 30s; contracts loop fills `ft` (next funding time) every 5 min |

---

## 9. Querying price history (md)

Price history is stored as ZSET chunks (20 min per chunk, 4 chunks kept = 80 min window).

```python
import redis, time

r = redis.Redis(unix_socket_path='/var/run/redis/redis.sock', decode_responses=False)

CHUNK_SEC = 1200
now_ms    = int(time.time() * 1000)
since_ms  = now_ms - 3600_000   # last 1 hour
cid       = int(time.time() // CHUNK_SEC)

rows = []
for i in range(4):
    key     = f"md:hist:bn:s:BTCUSDT:{cid - i}".encode()
    entries = r.zrangebyscore(key, since_ms, '+inf')
    for e in entries:
        bid, ask, ts = e.split(b'|')
        rows.append((int(ts), float(bid), float(ask)))

rows.sort()  # chronological order
for ts_ms, bid, ask in rows[-5:]:
    print(ts_ms, bid, ask)
```

Or use the built-in helper:

```python
# From within collectors/ directory
from hist_writer import read_history
import asyncio, redis.asyncio as aioredis

async def main():
    r = aioredis.Redis(unix_socket_path='/var/run/redis/redis.sock')
    rows = await read_history(r, ex='bn', mkt='s', sym='BTCUSDT', since_sec=3600.0)
    # rows: list of (ts_ms, bid_str, ask_str), sorted chronologically
    print(rows[-3:])

asyncio.run(main())
```

---

## 10. Querying order book data (ob)

### Current order book

```bash
redis-cli -s /var/run/redis/redis.sock HGETALL ob:bn:s:BTCUSDT
# Returns 41 fields: b1..b10, bq1..bq10, a1..a10, aq1..aq10, t
```

```python
r = redis.Redis(unix_socket_path='/var/run/redis/redis.sock', decode_responses=True)
ob = r.hgetall('ob:bn:s:BTCUSDT')
print('Best bid:', ob['b1'], 'qty:', ob['bq1'])
print('Best ask:', ob['a1'], 'qty:', ob['aq1'])
print('Timestamp:', ob['t'])
```

### OB history

```python
from hist_writer import read_ob_history
import asyncio, redis.asyncio as aioredis

async def main():
    r = aioredis.Redis(unix_socket_path='/var/run/redis/redis.sock')
    ob_hist = await read_ob_history(r, ex='bn', mkt='s', sym='BTCUSDT', since_sec=3600.0)
    # ob_hist: dict ts_sec → comma-separated 40 OB values
    # format: b1,bq1,...,b10,bq10,a1,aq1,...,a10,aq10
    for ts_sec in sorted(ob_hist)[-3:]:
        print(ts_sec, ob_hist[ts_sec][:60])

asyncio.run(main())
```

---

## 11. Querying funding rate history (fr)

```python
from fr_hist_writer import read_fr_history
import asyncio, redis.asyncio as aioredis

async def main():
    r = aioredis.Redis(unix_socket_path='/var/run/redis/redis.sock')
    fr_hist = await read_fr_history(r, ex='bb', sym='BTCUSDT', since_sec=3600.0)
    # fr_hist: dict ts_sec → (r_str, nr_str, ft_str)
    for ts_sec in sorted(fr_hist)[-3:]:
        r_val, nr_val, ft_val = fr_hist[ts_sec]
        print(f"{ts_sec}: rate={r_val}  next={nr_val}  ft={ft_val}")

asyncio.run(main())
```

---

## 12. ML dataset generation

`enrich_snapshot.py` processes all snapshot CSVs into a single ML dataset.

```bash
# Process all new snapshots (incremental — skips already-processed files)
python3 collectors/enrich_snapshot.py

# Rebuild everything from scratch
python3 collectors/enrich_snapshot.py --rebuild

# Process a single snapshot file
python3 collectors/enrich_snapshot.py signals/snapshots/2026-03-21/11/gate_binance_BTCUSDT_20260321_110000.csv
```

Output files:

| File | Description |
|------|-------------|
| `signals/ml_dataset.csv` | One row per snapshot. 65 features + `converged` + `split` |
| `signals/signal_history.db` | SQLite database tracking per-pair signal history |

### Feature groups

| Group | Description |
|-------|-------------|
| Spread dynamics | mean, std, min, max, slope, velocity, percentile over 5m/15m/30m/60m windows |
| OB at t=0 | imbalance (top-5), volume, skew, wall detection, level-2/5 spreads, best bid-ask spread, price level |
| Funding rate | `fr_r_abs`, `mins_to_funding`, `funding_urgency` (fr_r × 1/mins²), `fr_favorable` |
| Context | `spot_source_id`, `fut_source_id`, `direction_id`, `is_gate`, hour, weekday, `is_weekend` |
| Pair history | `repeat_count`, `hours_since_first`, `prev_converged`, `pair_success_rate` |

### Convergence target

`converged = 1` if the spread drops below **0.3%** at any point during the 3500s window; `0` otherwise.

### Train/test split

Files are sorted by date. 80% oldest → `split=train`, 20% newest → `split=test`.
`pair_success_rate` is computed only from training rows (per pair) and backfilled into test rows to avoid data leakage.

---

## 14. Updating pair lists

Re-run dictionaries whenever you want to pick up newly listed tokens or drop delisted ones:

```bash
cd dictionaries && python3 main.py && cd ..
```

**You must restart `run_all.py` after regenerating dictionaries.** Collectors load the subscribe files only at startup. The restart will archive old data and flush Redis automatically.

---

## 15. Stopping the system

```bash
Ctrl+C     # graceful shutdown (from the terminal running run_all.py)

# Or if running in background:
kill $(cat run_all.pid)

# Verify all stopped:
ps aux | grep "python3.*collectors\|run_all" | grep -v grep
```

---

## 16. Querying Redis directly

```bash
alias rcli="redis-cli -s /var/run/redis/redis.sock"

# Health
rcli PING

# Memory
rcli INFO memory | grep -E "used_memory_human|maxmemory_human|mem_fragmentation"

# Key counts
rcli DBSIZE                     # total keys
rcli KEYS "md:bn:s:*" | wc -l  # Binance spot md keys
rcli KEYS "fr:*" | wc -l       # all funding rate keys

# Current price
rcli HGETALL md:bn:s:BTCUSDT

# Current funding rate
rcli HGETALL fr:bb:BTCUSDT

# Current order book top-3
rcli HMGET ob:bb:f:BTCUSDT b1 bq1 a1 aq1 b2 bq2 a2 aq2 b3 bq3 a3 aq3

# Slow query log
rcli SLOWLOG GET 10
```

---

## 17. Troubleshooting

### All collectors disconnect repeatedly

**Cause:** Redis OOM — maxmemory too low with `volatile-ttl` policy blocks ALL writes, causing all collectors to fail simultaneously.
**Fix:** `redis-cli -s /var/run/redis/redis.sock CONFIG SET maxmemory 40gb && CONFIG REWRITE`
**Indicator:** `redis_monitor` logs `degraded` event with `flag=oom_rejected` or `flag=blocked_clients`. Dashboard shows `blocked:N` in red.

### `disc` counter increasing for one collector

Check the log for that collector:
```bash
jq 'select(.script == "ob_okx_futures" and .event == "disconnected")' logs/collectors_*.log
```

If `reason` contains `ConnectionClosed` → usually Redis OOM or queue overflow. If network error → check exchange status.

### spread_monitor shows `wait:Xs` for a long time

Normal — this is the configured delay before signals start (`--delay N`). Check your startup command.

### Snapshot files not being created

1. Check `spread_monitor` status in dashboard — should be `● scanning`
2. Check `MIN_SPREAD_PCT` threshold (default: 1.0%) — spread may not be reaching threshold
3. Check cooldown — same (direction, symbol) pair has 3500s cooldown

### Gate.io / OKX funding rate shows 0 msgs/s in stats

Normal for OKX. The `funding-rate` WS channel pushes only on rate change (~every 30 min). The REST fallback every 30s provides continuous coverage. Gate also uses REST fallback. Check `rest_refresh` log events to confirm they are running.

### No keys in Redis after startup

Usually means collectors haven't connected yet (first 5–10 seconds) or subscribe files are empty. Check:
```bash
wc -l dictionaries/subscribe/binance/binance_spot.txt
redis-cli -s /var/run/redis/redis.sock DBSIZE
```

If subscribe files are empty — re-run `dictionaries/main.py`.

### redis_monitor shows WARN or CRIT

```bash
jq 'select(.script == "redis_monitor" and .event == "degraded")' logs/collectors_*.log
```

| Flag | Meaning | Action |
|------|---------|--------|
| `ping_warn` / `ping_crit` | Redis PING > 6ms / > 30ms | Check system load |
| `write_warn` / `write_crit` | Pipeline write > 15ms / > 60ms | Check system load, disk |
| `mem_warn` / `mem_crit` | Memory > 70% / > 85% | Increase maxmemory |
| `blocked_clients` | Clients blocked waiting | OOM — increase maxmemory immediately |
| `oom_rejected` | Connections rejected | OOM — increase maxmemory immediately |
| `frag_warn` / `frag_crit` | Fragmentation > 1.5 / > 2.0 | Run `MEMORY PURGE` |

---

## 18. Reference

### Exchange codes

| Code | Exchange |
|------|----------|
| `bn` | Binance |
| `bb` | Bybit |
| `ok` | OKX |
| `gt` | Gate.io |

### Market codes

| Code | Market |
|------|--------|
| `s` | Spot |
| `f` | Futures (USDT perpetual) |

### Key patterns

| Key | Type | Fields / Member format |
|-----|------|----------------------|
| `md:{ex}:{mkt}:{sym}` | HASH | `b`, `a`, `t` |
| `md:hist:{ex}:{mkt}:{sym}:{cid}` | ZSET | `bid\|ask\|ts_ms` |
| `ob:{ex}:{mkt}:{sym}` | HASH | `b1`–`b10`, `bq1`–`bq10`, `a1`–`a10`, `aq1`–`aq10`, `t` |
| `ob:hist:{ex}:{mkt}:{sym}:{cid}` | ZSET | `b1\|bq1\|…\|b10\|bq10\|a1\|aq1\|…\|a10\|aq10\|ts_ms` |
| `fr:{ex}:{sym}` | HASH | `r`, `nr`, `ft`, `ts` |
| `fr:hist:{ex}:{sym}:{cid}` | ZSET | `r\|nr\|ft\|ts_ms` |
| `md:hist:config` | STRING | JSON chunk metadata |
| `ob:hist:config` | STRING | JSON chunk metadata |
| `fr:hist:config` | STRING | JSON chunk metadata |

**History:** `chunk_id = int(unix_sec // 1200)` · 4 chunks kept · 1 write/sec/symbol throttle

### Signal thresholds

| Setting | Value |
|---------|-------|
| `MIN_SPREAD_PCT` | 1.0% |
| `ANOMALY_THRESHOLD` | 100.0% |
| `COOLDOWN_SEC` | 3500s (≈58 min) |
| `STALE_THRESHOLD` | 300s (5 min) |
| `POLL_INTERVAL` | 0.3s |

### Snapshot timing

| Event | Time |
|-------|------|
| Signal fires | t=0 |
| 1h history prepended | t=0 |
| Live rows written | t=0 to t=3500s |
| File closed | t=3500s |
| Approximate rows | ~2500 (every 0.3s × 3500s) |

### Redis health thresholds (redis_monitor.py)

| Metric | WARN | CRIT |
|--------|------|------|
| PING latency | > 6ms | > 30ms |
| Write latency | > 15ms | > 60ms |
| Memory usage | > 70% | > 85% |
| Fragmentation ratio | > 1.5 | > 2.0 |

Slowlog check: commands exceeding 10ms every 60s.
