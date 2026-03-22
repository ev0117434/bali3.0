# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Bali 3.0** — система мониторинга крипто-спредов с полным стеком рыночных данных. Собирает активные торговые пары с 5 бирж (Binance, Bybit, OKX, Gate.io, Bitget) через REST API + WebSocket-валидацию, строит пересечения для арбитражных стратегий, затем запускает **25 коллекторов**: 10 md-коллекторов (лучшая цена bid/ask), 10 ob-коллекторов (10-уровневый стакан заявок), 5 fr-коллекторов (funding rates), плюс 4 монитора: `staleness_monitor`, `spread_monitor`, `redis_monitor`, `telegram_alert`.

## Commands

```bash
# 1. Generate symbol dictionaries (run once before collectors)
cd /root/bali3.0/dictionaries && python3 main.py
# Expected: ~65-70s total (Phase 1: ~3-5s REST, Phase 2: ~60s WebSocket, Phases 3-5: <1s)

# 2. Run all collectors + monitors (production entry point)
python3 /root/bali3.0/run_all.py              # live dashboard (default)
python3 /root/bali3.0/run_all.py --no-dash    # JSON-only logs
python3 /root/bali3.0/run_all.py --buckets    # staleness with age-bucket report
python3 /root/bali3.0/run_all.py --delay 60   # skip prompt, 60s delay before signals
# On startup without --delay, asks interactively: "Delay before signals/snapshots [seconds, Enter = 0]:"
# Archives old logs/ and signals/ into old/{YYYY-MM-DD_HH-MM}/ before starting

# Run individual collector/monitor standalone
python3 collectors/binance_spot.py
python3 collectors/fr_okx_futures.py
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

1. **Phase 1 — Parallel REST fetch** via `ThreadPoolExecutor(5 workers)`: each exchange module fetches raw pair lists and saves to `<exchange>/data/`.
2. **Phase 2 — Parallel WebSocket validation** via `asyncio.gather`: all 5 exchanges validate which pairs emit live market data over 60s. Active pairs saved to `<exchange>/data/<exchange>_*_active.txt`.
3. **Phase 3 — Build combinations**: 20 intersection files (set ∩) in `combination/`, each representing pairs active on both a spot market of exchange A and futures market of exchange B.
4. **Phase 4 — Build subscribe files**: 10 aggregate files in `subscribe/{exchange}/` (set ∪ over relevant combinations), consumed by downstream market-data collectors at startup.
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
| Bitget | `ws.bitget.com/v2/ws/public` | `books1` (instType: SPOT/USDT-FUTURES) | String `"ping"` | 100 |

### Output Files

- `combination/*.txt` — 20 files (5 exchanges × 4 spot→futures combinations per spot exchange)
- `subscribe/{exchange}/*.txt` — 10 files read by downstream collectors for symbol subscriptions

## Collectors (collectors/)

**`run_all.py`** — оркестратор. При запуске:
1. Архивирует `logs/` и `signals/` в `old/YYYY-MM-DD_HH-MM/`.
2. SCAN+DEL все Redis-ключи.
3. Определяет spread_delay: флаг `--delay N` > интерактивный промпт > 0.
4. Устанавливает CPU-affinity для себя и Redis (`os.sched_setaffinity`).
5. Запускает **4 subprocess-группы** коллекторов, каждая со своим asyncio event loop:
   - `runner_ob.py` — 10 OB-коллекторов → cores 12-19
   - `runner_md.py` — 10 MD-коллекторов → cores 20-23
   - `runner_fr.py` — 5 FR-коллекторов  → cores 24-25
   - `runner_monitors.py` — staleness, redis_monitor, telegram, spread → cores 26-27
6. Читает stdout каждого subprocess через asyncio pipe → пишет в лог-файл → обновляет state дашборда.
7. `redis_stats_loop` — опрашивает Redis INFO memory + dbsize каждые 5s.

Лог-файлы ротируются каждые 12h, хранится 2 чанка (`logs/collectors_YYYY-MM-DD_HH-MM.log`).

**CPU affinity (32 ядра, i9-13900):**
```
Cores  0-3:   системный резерв (не назначается никому)
Cores  4-11:  Redis (8 ядер = io-threads 8)
Cores 12-19:  OB-коллекторы (8 ядер, тяжёлые dict-сортировки)
Cores 20-23:  MD-коллекторы (4 ядра, I/O-bound)
Cores 24-25:  FR-коллекторы (2 ядра, лёгкие)
Cores 26-27:  Мониторы (2 ядра)
Cores 28-31:  run_all.py + дашборд (4 ядра)
```
Константы в `run_all.py`: `CPU_REDIS`, `CPU_OB`, `CPU_MD`, `CPU_FR`, `CPU_MONITORS`, `CPU_RUNALL`.

**Почему subprocess, а не asyncio tasks в одном процессе:**
Старая архитектура запускала все 25 коллекторов в одном event loop = одно Python-ядро = 86.7% CPU. OB-коллекторы особенно тяжёлые (сортировка словарей bid/ask на каждый тик). Subprocess-группы позволяют параллельно использовать 4 независимых ядра Python (обход GIL).

**Dashboard layout (run_all.py) — single Rich panel:**
- Header: `BALI 3.0 | uptime | clock | log file name`
- Row 1: `MD  ● bn-s  ● bn-f  ● bb-s  ...` — 10 md collectors as colored dots
- Row 2: `OB  ● bn-s  ● bn-f  ○ ok-s  ...` — 10 ob collectors (○ yellow = init in progress)
- Row 3: `FR  ● bn-f  ● bb-f  ● ok-f  ● gt-f  ● bg-f` — 5 fr collectors
- Separator
- Stats line: `signals:N  anom:N  cd:N  snaps:N  redis:XMB  keys:N  ping:Xms  wr:Xms  stale:N`
- Separator
- Signal feed: last 5 signals with `>` (normal) / `!` (anomaly) icons, direction (gt→bb), symbol, spread%

Collector dot colors: green = streaming/ok/scanning · yellow = connecting/init · red = crashed/disconnected/stale
Redis stats colored by threshold: `redis:` yellow/red if mem WARN/CRIT, `ping:` yellow > 6ms / red > 30ms, `wr:` yellow > 15ms / red > 60ms, `stale:` red if > 0.

`redis_monitor` provides `degraded`/`recovered` events and periodic `stats` events (with `flags` list). Dashboard shows redis_monitor status as `CRIT` (red) or `warn` (yellow) based on flags.

**10 md WS collectors** (`binance_spot`, `binance_futures`, `bybit_spot`, `bybit_futures`, `okx_spot`, `okx_futures`, `gate_spot`, `gate_futures`, `bitget_spot`, `bitget_futures`): each reads its subscribe file at startup, connects to the exchange WebSocket, batches incoming ticks, and writes best bid/ask to `md:*` HASH keys and tick history to `md:hist:*` ZSET keys via pipeline. Flush triggered on timeout **or** batch size cap (`MAX_BATCH=400`).

**10 ob WS collectors** (`ob_binance_spot`, `ob_binance_futures`, `ob_bybit_spot`, `ob_bybit_futures`, `ob_okx_spot`, `ob_okx_futures`, `ob_gate_spot`, `ob_gate_futures`, `ob_bitget_spot`, `ob_bitget_futures`): each reads its subscribe file at startup, connects to the exchange WebSocket, maintains a 10-level order book, and writes to `ob:*` HASH keys (41 fields: `b1`–`b10`, `bq1`–`bq10`, `a1`–`a10`, `aq1`–`aq10`, `t`) and order book history to `ob:hist:*` ZSET keys via `ob_hist_writer`. Bitget OB collectors use `books15` channel with incremental delta updates (local book maintained in memory, snapshot on reconnect). Flush triggered on timeout **or** batch size cap (`MAX_BATCH=150` — smaller cap than MD due to 41-field HSET pipelines).

**5 fr WS collectors** (`fr_binance_futures`, `fr_bybit_futures`, `fr_okx_futures`, `fr_gate_futures`, `fr_bitget_futures`): stream funding rates to `fr:{ex}:{sym}` HASH keys and `fr:hist:*` ZSET keys via `fr_hist_writer`. OKX, Gate, and Bitget also run a REST fallback loop (every 30s) to guarantee freshness since their WS channels push only on rate change. `fr_gate_futures` additionally runs a `contracts_loop` every 5 min (GET `/api/v4/futures/usdt/contracts`) to populate the `ft` (next funding time) field, which is not available in the Gate WS ticker payload. Flush triggered on timeout **or** batch size cap (`MAX_BATCH=300`).

**`staleness_monitor`** — every 60s scans all `md:*` keys, checks field `t` age against STALE_THRESHOLD (300s). Logs stale keys grouped by exchange:market.

**`redis_monitor`** — health check every 5s: PING latency, pipeline write benchmark, `INFO memory/stats/clients`. Flags: `ping_warn/crit` (6/30ms), `write_warn/crit` (15/60ms), `mem_warn/crit` (70/85%), `frag_warn/crit` (1.5/2.0), `blocked_clients`, `oom_rejected`. Emits `degraded` on threshold cross, `recovered` on recovery, `stats` every 5s. Slowlog check every 60s (threshold 10ms). Dashboard shows its status as `CRIT`/`warn`/`ok`.

**`spread_monitor`** — every 0.3s reads Redis for all 20 directions, computes `(futures_bid - spot_ask) / spot_ask * 100`. Fires signals with 3500s cooldown per (direction, symbol). Writes to `signals/signals.jsonl`, `signals/signals.csv`; anomalies (≥100%) to `signals/anomalies.jsonl`, `signals/anomalies.csv`.

On signal fire, opens a **snapshot CSV** under `signals/snapshots/YYYY-MM-DD/HH/{spot}_{fut}_{sym}_{YYYYMMDD_HHMMSS}.csv` and writes a row every 0.3s for the full 3500s cooldown window. Snapshot rows: **89 columns** = 7 base (`spot_source_id`, `fut_source_id`, `symbol`, `ask_spot`, `bid_futures`, `spread_pct`, `ts`) + 40 spot OB + 40 futures OB + 2 FR (`fr_r`, `fr_ft` — funding rate of the futures exchange only). `spot_source_id`/`fut_source_id` are integer IDs from `source_ids.json` (binance_spot=0 … gate_futures=7, bitget_spot=8, bitget_futures=9). History prepend at signal fire reads `md:hist:*`, `ob:hist:*`, and `fr:hist:*` and joins rows by `ts_sec`.

Signal JSONL fields: `ts`, `direction`, `symbol`, `spread_pct`, `spot_ask`, `fut_bid`, `spot_ex`, `fut_ex`. No `fr` field — funding rate data is NOT included in signal JSONL records.

### Collector Logging Pattern

All scripts emit JSON lines to `sys.stdout.buffer`:
```python
{"ts": float, "lvl": "INFO", "script": "binance_spot", "event": "stats", ...}
```
Common events: `connecting`, `connected`, `subscribed`, `first_message`, `stats`, `disconnected`, `reconnecting`, `rest_refresh`, `collector_crashed`.

### Redis

Configured via `setup_redis.sh`: Unix socket only (`/var/run/redis/redis.sock`), no persistence (`volatile-ttl` eviction), latency-optimized. **Recommended maxmemory: 40 GB** (typical usage ~500 MB; lower limits cause OOM under `volatile-ttl` policy which blocks all writes). Used by downstream collectors, not by `dictionaries/main.py` itself.

**Актуальная конфигурация Redis (после оптимизации 2026-03-22):**
- `io-threads 8` — повышено с 2 (32 ядра CPU; улучшает параллелизм при 25+ одновременных pipeline)
- `activedefrag yes` + `active-defrag-ignore-bytes 10mb` — активная дефрагментация стартует раньше (10MB вместо 100MB), устраняет fragmentation ratio 4-5x после холодного старта
- `maxmemory 40gb` — зафиксировано в `setup_redis.sh` (была ошибка: скрипт генерировал 2gb)

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

**Батчинг в коллекторе:** два условия флаша — по времени **или** по количеству:
```python
_BATCH_JITTER = random.uniform(0, 0.100)
BATCH_TIMEOUT = 0.200 + _BATCH_JITTER  # 200–300 ms, jittered per-process
MAX_BATCH     = 150   # 400 для MD, 150 для OB, 300 для FR

if batch and (now - last_flush >= BATCH_TIMEOUT or len(batch) >= MAX_BATCH):
    _bs = len(batch)
    await flush(r, batch, hw, counters)
    batch.clear()
    last_flush = now
    if _bs > 100:
        await asyncio.sleep(0)  # уступаем event loop после тяжёлого pipeline
```
Jitter предотвращает синхронизацию таймеров 25 коллекторов (thundering herd). MAX_BATCH ограничивает размер pipeline при высоком потоке сообщений (ob_bybit_futures флашит каждые ~63ms по батчу, а не по таймеру).

**Pub/Sub — отдельный клиент** (подписка блокирует соединение).

**Обработка ошибок:** оборачивать в `try/except (ConnectionError, TimeoutError)` с retry (3 попытки, `sleep(0.1 * attempt)`).

#### Схема ключей

| Ключ | Тип | Описание |
|------|-----|----------|
| `md:{ex}:{mkt}:{sym}` | HASH | Текущая лучшая цена. Поля: `b`=bid, `a`=ask, `t`=ts_ms |
| `md:hist:{ex}:{mkt}:{sym}:{chunk_id}` | ZSET | История тиков (score=ts_ms, member=`bid\|ask\|ts_ms`) |
| `md:hist:config` | STRING | JSON-конфиг активных md-чанков |
| `ob:{ex}:{mkt}:{sym}` | HASH | Текущий стакан 10 уровней. 41 поле: `b1`–`b10`, `bq1`–`bq10`, `a1`–`a10`, `aq1`–`aq10`, `t` |
| `ob:hist:{ex}:{mkt}:{sym}:{chunk_id}` | ZSET | История стакана (41 значение через pipe) |
| `ob:hist:config` | STRING | JSON-конфиг активных ob-чанков |
| `fr:{ex}:{sym}` | HASH | Текущий funding rate. Поля: `r`=rate, `nr`=next predicted, `ft`=next funding time ms, `ts`=recv ms |
| `fr:hist:{ex}:{sym}:{chunk_id}` | ZSET | История FR (score=ts_ms, member=`r\|nr\|ft\|ts_ms`) |
| `fr:hist:config` | STRING | JSON-конфиг активных fr-чанков |

**Сокращения:** `bn`=binance, `bb`=bybit, `ok`=okx, `gt`=gate, `bg`=bitget · `s`=spot, `f`=futures

#### История (hist_writer.py, ob_hist_writer.py, fr_hist_writer.py)

**Общая схема**: чанки по 20 минут, `chunk_id = int(unix_sec // 1200)`. Хранится 4 чанка = 80 минут (гарантирует покрытие последнего часа). При ротации удаляется самый старый чанк через тот же pipeline.

**Дросселирование**: не более 1 записи/сек на символ → ~3600 строк/символ/час.

- **`hist_writer.py`** (md + snapshot): member `bid|ask|ts_ms`. Конфиг `md:hist:config`. Содержит также `write_snapshot_history()` для записи исторических строк в снапшот при открытии файла (включает ob-историю и fr-историю), `read_ob_history()` для чтения ob:hist, и константы `SNAPSHOT_CSV_HEADER` (89 колонок), `OB_EMPTY`, `FR_EMPTY`, `OB_LEVELS`. Загружает `SOURCE_IDS` из `source_ids.json`.
- **`ob_hist_writer.py`** (ob): member `b1|bq1|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms` — 41 значение. Конфиг `ob:hist:config`.
- **`fr_hist_writer.py`** (fr): member `r|nr|ft|ts_ms`. Конфиг `fr:hist:config`. Содержит `read_fr_history(r, ex, sym, since_sec)` → `dict[ts_sec → (r_str, nr_str, ft_str)]`.

#### Snapshot CSV (89 колонок)

```
spot_source_id, fut_source_id, symbol, ask_spot, bid_futures, spread_pct, ts,  ← 7 base
s_b1..s_b10, s_bq1..s_bq10, s_a1..s_a10, s_aq1..s_aq10,                      ← 40 spot OB
f_b1..f_b10, f_bq1..f_bq10, f_a1..f_a10, f_aq1..f_aq10,                      ← 40 futures OB
fr_r, fr_ft                                                                     ← 2 FR (futures exchange only)
```

Source ID mapping (from `collectors/source_ids.json`):
`binance_spot=0, binance_futures=1, bybit_spot=2, bybit_futures=3, okx_spot=4, okx_futures=5, gate_spot=6, gate_futures=7`

#### Мониторинг

```bash
redis-cli -s /var/run/redis/redis.sock PING        # < 1ms = OK
redis-cli -s /var/run/redis/redis.sock INFO memory  # fragmentation < 1.3
redis-cli -s /var/run/redis/redis.sock SLOWLOG GET 10
```

Пороги: OK `PING < 6ms, Write < 15ms, memory < 70%` · WARN `PING > 6ms / Write > 15ms / mem > 70%` · CRIT `PING > 30ms / Write > 60ms / mem > 85% / blocked_clients > 0`

**ВАЖНО**: `volatile-ttl` eviction policy + нет TTL на ключах = при OOM Redis блокирует ВСЕ записи ("command not allowed when used memory > maxmemory"). Это вызывает массовые реконнекты ВСЕХ коллекторов. Решение: maxmemory ≥ 40 GB.

#### Характеристики задержек (после оптимизации 2026-03-22)

Путь данных: **Exchange WS → collector batch → Redis HSET → spread_monitor read**

| Тип данных | Ключ Redis | Задержка (норма) | Задержка (пик) | Триггер флаша |
|---|---|---|---|---|
| MD bid/ask | `md:*` | 100–315ms | ~345ms | MAX_BATCH=400 или TIMEOUT |
| OB стакан | `ob:*` | 63–315ms | ~345ms | MAX_BATCH=150 или TIMEOUT |
| FR funding | `fr:*` | 200–315ms | ~345ms | TIMEOUT (низкий mps) |

Коллекторы с высоким потоком (флаш по MAX_BATCH, не по таймеру):
- `binance_futures` 2697 msg/s → флаш каждые ~148ms
- `ob_bybit_futures` 2360 msg/s → флаш каждые ~63ms
- `ob_binance_futures` 1360 msg/s → флаш каждые ~110ms

Время от тика биржи до проверки `spread_monitor`: **406–640ms** (WS 1-5ms + batch 100-315ms + pipeline 5-20ms + цикл 300ms).

До оптимизации: 291–855ms со спайками до 470ms в цикле spread_monitor и 7395ms в staleness_monitor scan.
