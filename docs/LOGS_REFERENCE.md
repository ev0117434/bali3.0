# Logs Reference

All scripts emit JSON lines to `stdout`. `run_all.py` captures them, writes to
`logs/collectors_YYYY-MM-DD_HH-MM.log` (12-hour rotation, last 2 files kept),
and parses them for the live dashboard.

---

## Log line format

```json
{"ts": 1711234567.12, "lvl": "INFO", "script": "binance_spot", "event": "stats",
 "msgs_total": 98808, "msgs_per_sec": 823.4, "flushes_total": 988, "avg_pipeline_ms": 0.412}
```

| Field | Type | Description |
|-------|------|-------------|
| `ts` | float | Unix timestamp (seconds) |
| `lvl` | string | `INFO` / `WARN` / `ERROR` |
| `script` | string | Script name (e.g. `binance_spot`, `spread_monitor`) |
| `event` | string | Event identifier (see tables below) |
| `...` | — | Event-specific fields |

---

## Common events (all scripts)

| event | lvl | Description |
|-------|-----|-------------|
| `startup` | INFO | Process started, config loaded |
| `redis_connected` | INFO | Redis connection established |
| `connecting` | INFO | Opening WebSocket (`url` field) |
| `connected` | INFO | WebSocket handshake complete |
| `subscribed` | INFO | All channels subscribed (`symbols_count`, `chunks`) |
| `first_message` | INFO | First data message received |
| `disconnected` | WARN | WebSocket closed (`reason` field) |
| `reconnecting` | INFO | Reconnect attempt (`attempt`, `sleep_sec`) |
| `collector_crashed` | ERROR | Unhandled exception (`error`, `traceback`) |

---

## md collectors — best bid/ask (×8)

Scripts: `binance_spot`, `binance_futures`, `bybit_spot`, `bybit_futures`,
`okx_spot`, `okx_futures`, `gate_spot`, `gate_futures`

Redis keys written: `md:{ex}:{mkt}:{sym}` HASH · `md:hist:{ex}:{mkt}:{sym}:{chunk_id}` ZSET

### Events

| event | lvl | Fields |
|-------|-----|--------|
| `startup` | INFO | `symbols_count` |
| `first_message` | INFO | `symbol`, `bid`, `ask` |
| `stats` | INFO | `msgs_total`, `msgs_per_sec`, `flushes_total`, `avg_pipeline_ms` |

### stats fields

| Field | Description |
|-------|-------------|
| `msgs_total` | Total WebSocket messages received since startup |
| `msgs_per_sec` | Message rate over last interval |
| `flushes_total` | Total Redis pipeline flushes |
| `avg_pipeline_ms` | Average `pipeline.execute()` latency (ms) |

`stats` is emitted every 30 seconds.

---

## ob collectors — 10-level order book (×8)

Scripts: `ob_binance_spot`, `ob_binance_futures`, `ob_bybit_spot`, `ob_bybit_futures`,
`ob_okx_spot`, `ob_okx_futures`, `ob_gate_spot`, `ob_gate_futures`

Redis keys written: `ob:{ex}:{mkt}:{sym}` HASH (41 fields) · `ob:hist:{ex}:{mkt}:{sym}:{chunk_id}` ZSET

All common events plus:

### Additional events

| event | lvl | Fields | Description |
|-------|-----|--------|-------------|
| `symbols_loaded` | INFO | `count` | Bybit/OKX: total symbols to initialize (used for init progress in dashboard) |
| `book_init` | INFO | `symbol`, `levels`, `books_initialized` | Complete snapshot received for a symbol; `books_initialized` = running total |
| `subscribing_progress` | INFO | `done`, `total` | Bybit: logged every 50 symbols during subscription |
| `delta_before_snapshot` | WARN | `symbol` | Bybit/OKX: delta arrived before initial snapshot — ignored |
| `update_before_snapshot` | WARN | `symbol` | Gate: update arrived before initial snapshot — ignored |
| `shallow_warning` | WARN | `symbol`, `levels` | Order book has fewer than 10 levels |

### stats fields

| Field | Description |
|-------|-------------|
| `msgs_total`, `msgs_per_sec` | Same as md |
| `flushes_total`, `avg_pipeline_ms` | Same as md |
| `books_initialized` | Symbols with a fully initialised order book |
| `snapshots_total` | Total full snapshots received (Binance: every 100ms) |
| `deltas_total` / `updates_total` | Incremental delta updates applied (Bybit/OKX / Gate) |
| `delta_skipped` / `update_skipped` | Deltas dropped before first snapshot |
| `avg_levels` | Average number of levels in the order book |

### Order book update model by exchange

| Exchange | Channel | Model |
|----------|---------|-------|
| Binance | `{sym}@depth20@100ms` | Full snapshot every 100 ms |
| Bybit | `orderbook.50.{SYM}` | Snapshot then incremental deltas |
| OKX | `books` | Snapshot then incremental deltas (up to 400 levels, top-10 extracted) |
| Gate | `spot/futures.order_book_update` | Full 20-level snapshot on each update |

**Note:** Only Bybit and OKX ob collectors emit `symbols_loaded`, so only they show `○ init X/Y` progress in the dashboard. Binance and Gate ob collectors initialize silently.

---

## fr collectors — funding rates (×4)

Scripts: `fr_binance_futures`, `fr_bybit_futures`, `fr_okx_futures`, `fr_gate_futures`

Redis keys written: `fr:{ex}:{sym}` HASH · `fr:hist:{ex}:{sym}:{chunk_id}` ZSET

All common events plus exchange-specific ones below.

### fr_binance_futures

| event | lvl | Fields | Description |
|-------|-----|--------|-------------|
| `first_message` | INFO | `symbols_count` | Number of symbols in first batch |
| `stats` | INFO | `symbols_total`, `msgs_total`, `msgs_per_sec`, `avg_pipeline_ms` | |
| `planned_reconnect` | INFO | — | Scheduled reconnect every ~23 h (Binance closes stream after 24 h) |

Stream: `!markPrice@arr@1s` — single stream for all symbols, updates every 1 second.

### fr_bybit_futures

| event | lvl | Fields | Description |
|-------|-----|--------|-------------|
| `stats` | INFO | `symbols_total`, `msgs_total`, `msgs_per_sec`, `cache_size`, `avg_pipeline_ms` | |
| `recv_error` | WARN | `error` | Message decode error |

`cache_size` = number of symbols with cached delta state (persists across reconnects).
Channel: `tickers` — pushes on rate change only.

### fr_okx_futures

| event | lvl | Fields | Description |
|-------|-----|--------|-------------|
| `stats` | INFO | `symbols_total`, `ws_updates`, `rest_updates`, `msgs_per_sec`, `avg_pipeline_ms` | |
| `rest_refresh` | INFO | `refreshed`, `failed` | REST poll: `refreshed`=symbols updated, `failed`=errors (every 30 s) |

WS channel (`funding-rate`) pushes approximately every 30 minutes. REST fallback
guarantees freshness between WS pushes.

### fr_gate_futures

| event | lvl | Fields | Description |
|-------|-----|--------|-------------|
| `stats` | INFO | `symbols_total`, `ws_updates`, `rest_updates`, `msgs_per_sec`, `avg_pipeline_ms` | |
| `rest_refresh` | INFO | `refreshed` | REST poll: `refreshed`=symbols updated in this call (every 30 s) |
| `contracts_refresh` | INFO | `updated` | Contracts poll: `updated`=symbols whose `ft` field was set (every 5 min) |
| `rest_error` | WARN | `error` | REST tickers request failed |
| `contracts_error` | WARN | `error` | REST contracts request failed |
| `recv_error` | WARN | `error` | WS message decode error |

`contracts_refresh` fires every 5 minutes after a GET to `/api/v4/futures/usdt/contracts`.
This is the only way Gate.io populates the `ft` (next funding time) field, as it is not
available in the WS ticker payload.

---

## staleness_monitor

Scans all `md:*` keys every 60 seconds, checks field `t` age.
Stale threshold: **300 seconds**.

| event | lvl | Fields |
|-------|-----|--------|
| `startup` | INFO | |
| `redis_connected` | INFO | |
| `check_start` | INFO | |
| `scan_complete` | INFO | `total_keys`, `scan_ms` |
| `fetch_complete` | INFO | `keys_with_ts`, `keys_no_ts` |
| `check` (ok) | INFO | `total_keys`, `keys_with_ts`, `keys_no_ts`, `stale_count`, `stale_threshold_sec`, `scan_ms` |
| `check` (stale) | WARN | same + `stale_by_source` `{ex:mkt: count}`, `top_stale` `[{key, age_sec}]` |
| `buckets` | INFO | `bucket_0_60s`, `bucket_60_120s`, … *(only with `--buckets` flag)* |
| `check_complete` | INFO | |
| `check_failed` | ERROR | `error` |

`stale_by_source` example: `{"bn:s": 2, "gt:f": 1}` — stale keys grouped by exchange+market.
`top_stale` — up to 10 oldest keys with their age in seconds.

---

## spread_monitor

Scans 12 spot→futures directions every 0.3 seconds.
Signal cooldown per (direction, symbol): **3500 seconds**.

| event | lvl | Fields |
|-------|-----|--------|
| `startup` | INFO | `poll_interval_sec`, `min_spread_pct`, `stale_threshold_sec`, `cooldown_sec` |
| `directions_loaded` | INFO | `directions`, `total_symbols` |
| `direction_loaded` | INFO | `direction`, `symbols`, `spot_ex`, `fut_ex` |
| `redis_connected` | INFO | |
| `signal_files_opened` | INFO | `signals_jsonl`, `signals_csv`, `anomalies_jsonl`, `anomalies_csv`, `anomaly_threshold_pct` |
| `snapshot_history_written` | INFO | `direction`, `symbol`, `rows` — historical rows prepended at signal fire |
| `snapshot_opened` | INFO | `direction`, `symbol`, `file`, `duration_sec` |
| `signal` | INFO | `direction`, `symbol`, `spread_pct`, `spot_ask`, `fut_bid`, `spot_ex`, `fut_ex`, `cooldown_until` |
| `anomaly` | WARN | same as `signal` — fired when `spread_pct ≥ 100%` |
| `stats` | INFO | see table below |
| `cycle_error` | ERROR | `error` |

**Note:** The `spread_monitor_waiting` event is emitted by `run_all.py` (not spread_monitor itself) when a delay is configured: `{"event": "spread_monitor_waiting", "delay_sec": N}`.

### stats fields

| Field | Description |
|-------|-------------|
| `signals_total` | Signals fired since startup |
| `signals_per_interval` | Signals in last 30 s window |
| `anomalies_total` | Anomalies fired since startup |
| `cooldowns_active` | (direction, symbol) pairs currently in cooldown |
| `snapshots_active` | Snapshot CSV files currently open and being written |
| `scanned_total` | Total (direction, symbol) pairs evaluated since startup |
| `stale_skipped` | Pairs skipped because bid/ask data was stale |
| `cooldown_skipped` | Pairs skipped because in cooldown |
| `last_cycle_ms` | Duration of the last 0.3 s scan cycle (ms) |

`stats` is emitted every 30 seconds.

---

## redis_monitor

Monitors Redis health continuously. Runs as part of `run_all.py`.
Can also be run standalone: `python3 collectors/redis_monitor.py`

Fast check every **5 seconds**: PING latency, write benchmark, memory, clients, OOM counter.
Slow check every **60 seconds**: SLOWLOG entries (commands > 10ms).

| event | lvl | Fields |
|-------|-----|--------|
| `startup` | INFO | `fast_interval_sec`, `slow_interval_sec`, `thresholds` |
| `redis_connected` | INFO | `socket` |
| `stats` | INFO/WARN/ERROR | see table below |
| `degraded` | WARN | `flag`, `ping_ms`, `write_ms`, `mem_pct`, `mem_mb`, `frag`, `blocked`, `new_rejected` |
| `recovered` | INFO | `flag` |
| `slowlog` | WARN | `cmd`, `duration_ms`, `ts_cmd` |

`degraded` fires when a metric first crosses a threshold. `recovered` fires when it drops back below. One event per flag per crossing.

### stats fields

| Field | Description |
|-------|-------------|
| `ping_ms` | Redis PING round-trip time (ms) |
| `write_ms` | Pipeline SET benchmark latency (ms) |
| `mem_mb` | Used memory (MB) |
| `max_mb` | Configured maxmemory (MB) |
| `mem_pct` | Memory usage percentage |
| `frag` | `mem_fragmentation_ratio` from Redis INFO |
| `clients` | Connected clients count |
| `blocked` | Blocked clients count (> 0 = OOM condition) |
| `ops_sec` | Redis operations per second |
| `rejected_new` | New connection rejections since last check |
| `rejected_total` | Total rejected connections since Redis start |
| `flags` | List of active flag names (empty = healthy) |

`lvl` = `INFO` (no flags), `WARN` (warn flags only), `ERROR` (any crit flag).

### Degradation flags

| Flag | Condition | Severity |
|------|-----------|----------|
| `ping_warn` | PING > 6ms | WARN |
| `ping_crit` | PING > 30ms | CRIT |
| `write_warn` | Write > 15ms | WARN |
| `write_crit` | Write > 60ms | CRIT |
| `mem_warn` | Memory > 70% | WARN |
| `mem_crit` | Memory > 85% | CRIT |
| `frag_warn` | Fragmentation > 1.5 | WARN |
| `frag_crit` | Fragmentation > 2.0 | CRIT |
| `blocked_clients` | blocked_clients > 0 | CRIT |
| `oom_rejected` | New rejected connections > 0 | CRIT |

CRIT flags cause dashboard to show `redis_monitor` status as `CRIT` (red). WARN-only flags show `warn` (yellow).

---

## telegram_alert

Not a log emitter itself — reads logs from other scripts and forwards matching events to Telegram.
Configured via `collectors/alert_config.json`.

### Alert events forwarded

| Source event | Alert type | Message |
|-------------|-----------|---------|
| `signal` | 🟡 Signal | direction, symbol, spread_pct |
| `anomaly` | 🔴 Anomaly | direction, symbol, spread_pct |
| `disconnected` | 🔴 Disconnect | script name |
| `reconnected` | 🟢 Reconnect | script name, symbol count |
| `degraded` (redis) | 🔴 Redis CRIT | flag name |
| `recovered` (redis) | 🟢 Redis OK | flag name |
| `collector_crashed` | 🔴 Crash | script, error |

Multiple Telegram chat IDs are supported (configured as a list in `alert_config.json`).

---

## Log query recipes

```bash
# Stream all logs in real time
tail -f logs/collectors_*.log | jq .

# Errors and warnings only
jq 'select(.lvl == "WARN" or .lvl == "ERROR")' logs/collectors_*.log

# Stats for all collectors (excluding spread_monitor)
jq 'select(.event == "stats" and .script != "spread_monitor")' logs/collectors_*.log

# Signals and anomalies
jq 'select(.event == "signal" or .event == "anomaly")' logs/collectors_*.log

# All disconnects
jq 'select(.event == "disconnected")' logs/collectors_*.log

# Order book init completions
jq 'select(.event == "book_init")' logs/collectors_*.log

# FR REST refreshes
jq 'select(.event == "rest_refresh")' logs/collectors_*.log

# Gate.io contracts loop (ft field updates)
jq 'select(.event == "contracts_refresh")' logs/collectors_*.log

# Staleness warnings
jq 'select(.event == "check" and .lvl == "WARN")' logs/collectors_*.log

# Spread monitor cycle time spikes (> 50ms)
jq 'select(.event == "stats" and .script == "spread_monitor" and .last_cycle_ms > 50)' logs/collectors_*.log

# Per-script message rate
jq 'select(.event == "stats") | {script, msgs_per_sec}' logs/collectors_*.log

# Snapshot opens today
jq 'select(.event == "snapshot_opened")' logs/collectors_*.log

# Redis degradation events
jq 'select(.script == "redis_monitor" and .event == "degraded")' logs/collectors_*.log

# Redis slowlog entries
jq 'select(.script == "redis_monitor" and .event == "slowlog")' logs/collectors_*.log

# Redis stats with flags
jq 'select(.script == "redis_monitor" and .event == "stats" and (.flags | length > 0))' logs/collectors_*.log
```
