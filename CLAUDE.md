# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Bali 3.0** — система генерации словарей торговых пар для мониторинга крипто-спредов. Собирает активные торговые пары с 4 бирж (Binance, Bybit, OKX, Gate.io) через REST API + WebSocket-валидацию, затем строит пересечения для арбитражных стратегий.

## Commands

```bash
# 1. Generate symbol dictionaries (run once before collectors)
cd /root/bali3.0/dictionaries && python3 main.py
# Expected: ~65-70s total (Phase 1: ~3-5s REST, Phase 2: ~60s WebSocket, Phases 3-5: <1s)

# 2. Run all collectors + monitors (production entry point)
python3 /root/bali3.0/run_all.py              # live dashboard (default)
python3 /root/bali3.0/run_all.py --no-dash    # JSON-only logs
python3 /root/bali3.0/run_all.py --buckets    # staleness with age-bucket report

# Run individual collector/monitor standalone
python3 collectors/binance_spot.py
python3 collectors/staleness_monitor.py [--buckets]
python3 collectors/spread_monitor.py

# Install dependencies
pip install websockets redis[hiredis] orjson rich

# Redis setup and health check
sudo bash /root/bali3.0/setup_redis.sh          # Install & configure
sudo bash /root/bali3.0/setup_redis.sh --check  # Health check only
```

## Architecture

### Execution Phases (dictionaries/main.py)

1. **Phase 1 — Parallel REST fetch** via `ThreadPoolExecutor(4 workers)`: each exchange module fetches raw pair lists and saves to `<exchange>/data/`.
2. **Phase 2 — Parallel WebSocket validation** via `asyncio.gather`: all 4 exchanges validate which pairs emit live market data over 60s. Active pairs saved to `<exchange>/data/<exchange>_*_active.txt`.
3. **Phase 3 — Build combinations**: 12 intersection files (set ∩) in `combination/`, each representing pairs active on both a spot market of exchange A and futures market of exchange B.
4. **Phase 4 — Build subscribe files**: 8 aggregate files in `subscribe/{exchange}/` (set ∪ over relevant combinations), consumed by downstream market-data collectors at startup.
5. **Phase 5 — Report**: prints statistics.

### Exchange Module Pattern

Each exchange has two files:
- `<exchange>_pairs.py` — REST API fetch, filters active USDT/USDC pairs, saves to `data/`
- `<exchange>_ws.py` — WebSocket validation, marks pairs as active if any data received in 60s

**OKX and Gate.io** use two symbol formats:
- **Native** (e.g. `BTC-USDT`, `BTC_USDT`) — used for WebSocket subscriptions (protocol-required)
- **Normalized** (e.g. `BTCUSDT`) — used for cross-exchange set intersections

Normalization: remove hyphens/underscores and `-SWAP` suffix.

### Exchange-Specific WS Details

| Exchange | WS Endpoint | Channel | Ping Format | Chunk Size |
|----------|-------------|---------|-------------|------------|
| Binance | `stream.binance.com:9443/stream` | `{sym}@bookTicker` | Default | 300 |
| Bybit | `stream.bybit.com/v5/public/spot\|linear` | `orderbook.1.{SYM}` | `{"op":"ping"}` | 10 (spot) / 200 (futures) |
| OKX | `ws.okx.com:8443/ws/v5/public` | `tickers` with `instId` | String `"ping"` | 300 |
| Gate.io | `api.gateio.ws/ws/v4/` (spot) / `fx-ws.gateio.ws` (futures) | `spot/futures.book_ticker` | Timestamped JSON | 100 |

### Output Files

- `combination/*.txt` — 12 files (4 exchange pairs × 3 spot→futures combinations per spot exchange)
- `subscribe/{exchange}/*.txt` — 8 files read by downstream collectors for symbol subscriptions

## Collectors (collectors/)

**`run_all.py`** — orchestrator. On startup: SCAN+DEL all Redis keys, then launches all 8 WS collectors + `staleness_monitor` + `spread_monitor` as asyncio tasks. Intercepts `sys.stdout` to parse JSON log lines for the live Rich dashboard. Log files rotate every 12h, keeps last 2 chunks (`logs/collectors_YYYY-MM-DD_HH-MM.log`).

**8 WS collectors** (`binance_spot`, `binance_futures`, `bybit_spot`, `bybit_futures`, `okx_spot`, `okx_futures`, `gate_spot`, `gate_futures`): each reads its subscribe file at startup, connects to the exchange WebSocket, batches incoming ticks (up to 100 messages or 20ms), and writes to Redis via pipeline.

**`staleness_monitor`** — every 60s scans all `md:*` keys, checks field `t` age against STALE_THRESHOLD (300s). Logs stale keys grouped by exchange:market.

**`spread_monitor`** — every 0.3s reads Redis for all 12 directions, computes `(futures_bid - spot_ask) / spot_ask * 100`. Fires signals with 3500s cooldown per (direction, symbol). Writes to `signals/signals.jsonl`, `signals/signals.csv`; anomalies (≥100%) to `signals/anomalies.jsonl`, `signals/anomalies.csv`.

On signal fire, opens a **snapshot CSV** under `signals/snapshots/YYYY-MM-DD/HH/{spot}_{fut}_{sym}_{ts}.csv` and writes a row every 0.3s for the full 3500s cooldown window, capturing the spread evolution after the signal. Directory is bucketed by UTC day and hour of the signal. Snapshot rows are written regardless of whether the spread stays above `MIN_SPREAD_PCT`. Files are closed automatically when the window expires.

### Collector Logging Pattern

All scripts emit JSON lines to `sys.stdout.buffer`:
```python
{"ts": float, "lvl": "INFO", "script": "binance_spot", "event": "stats", ...}
```
Common events: `connecting`, `connected`, `subscribed`, `first_message`, `stats`, `disconnected`, `reconnecting`, `collector_crashed`.

### Redis

Configured via `setup_redis.sh`: Unix socket only (`/var/run/redis/redis.sock`), no persistence (`volatile-ttl` eviction), latency-optimized. Used by downstream collectors, not by `dictionaries/main.py` itself.

#### Правила написания кода с Redis

**Зависимости:** `pip install redis[hiredis] orjson`

**Подключение:**
```python
import redis.asyncio as aioredis
r = aioredis.Redis(
    unix_socket_path='/var/run/redis/redis.sock',
    decode_responses=False,  # всегда False, работаем с bytes
    max_connections=20,
)
```

**Pipeline — обязательно всегда**, никогда одиночные команды:
```python
async with r.pipeline(transaction=False) as pipe:  # transaction=False всегда
    pipe.hset('key', mapping={...})
    pipe.zadd('hist_key', {payload: now_ms})
    await pipe.execute()
```

**Батчинг в коллекторе:** накапливать до 100 команд или сбрасывать каждые 20ms.

**Pub/Sub — отдельный клиент** (подписка блокирует соединение).

**Обработка ошибок:** оборачивать в `try/except (ConnectionError, TimeoutError)` с retry (3 попытки, `sleep(0.1 * attempt)`).

#### Схема ключей (может меняться)

> Текущие имена ключей — рабочий вариант, будут уточнены. При изменении — обновить здесь.

| Ключ | Тип | Описание |
|------|-----|----------|
| `md:bn:s:BTCUSDT` | HASH | Текущая цена (market data, binance, spot) |
| `md:hist:bn:s:BTCUSDT:{chunk_id}` | ZSET | История тиков (score=ts_ms, member=`bid\|ask\|ts_ms`) |
| `md:hist:config` | STRING | JSON-конфиг активных чанков истории |
| `sig:bn_s:bb_f:BTCUSDT` | STRING | Последний сигнал спреда |

**Сокращения:** `bn`=binance, `bb`=bybit, `ok`=okx, `gt`=gate · `s`=spot, `f`=futures

**Поля HASH:** `b`=bid, `a`=ask, `t`=local_ts_ms

#### История (hist_writer.py)

**Схема**: чанки по 20 минут, `chunk_id = int(unix_sec // 1200)`. Хранится 4 чанка = 80 минут (гарантирует покрытие последнего часа). При старте 5-го чанка удаляется 1-й, при старте 6-го — 2-й и т.д.

**Дросселирование**: не более 1 записи/сек на символ → ~3600 строк/символ/час. Удаление старых чанков делается через тот же pipeline при ротации.

**`md:hist:config`**: обновляется автоматически при старте и ротации каждым коллектором (один раз на событие, флаг `_dirty`).

#### Мониторинг

```bash
redis-cli -s /var/run/redis/redis.sock PING        # < 1ms = OK
redis-cli -s /var/run/redis/redis.sock INFO memory  # fragmentation < 1.3
redis-cli -s /var/run/redis/redis.sock SLOWLOG GET 10
```

Пороги: OK `PING < 1ms, memory < 60%` · WARN `> 1ms / > 60%` · CRIT `timeout / > 95% / blocked_clients > 0`
