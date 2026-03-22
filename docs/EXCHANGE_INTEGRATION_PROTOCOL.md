# Протокол интеграции новой биржи в Bali 3.0

> **Версия:** 1.0
> **Охват:** dictionary → MD → OB → FR → run_all → spread_monitor → dashboard
> **Референс:** интеграция Binance / Bybit / OKX / Gate.io / Bitget

---

## Содержание

1. [Обзор архитектуры](#1-обзор-архитектуры)
2. [Глоссарий и соглашения](#2-глоссарий-и-соглашения)
3. [Шаг 1 — Dictionary: получение пар](#3-шаг-1--dictionary-получение-пар)
4. [Шаг 2 — Dictionary: WS-валидация](#4-шаг-2--dictionary-ws-валидация)
5. [Шаг 3 — Обновление main.py (dictionary)](#5-шаг-3--обновление-mainpy-dictionary)
6. [Шаг 4 — source_ids.json](#6-шаг-4--source_idsjson)
7. [Шаг 5 — MD-коллекторы (md:* ключи)](#7-шаг-5--md-коллекторы-md-ключи)
8. [Шаг 6 — OB-коллекторы (ob:* ключи)](#8-шаг-6--ob-коллекторы-ob-ключи)
9. [Шаг 7 — FR-коллектор (fr:* ключи)](#9-шаг-7--fr-коллектор-fr-ключи)
10. [Шаг 8 — run_all.py](#10-шаг-8--run_allpy)
11. [Шаг 9 — spread_monitor.py](#11-шаг-9--spread_monitorpy)
12. [Шаг 10 — hist_writer конфиги (sources)](#12-шаг-10--hist_writer-конфиги-sources)
13. [Шаг 11 — Генерация словарей](#13-шаг-11--генерация-словарей)
14. [Итерация 1: Синтаксис и импорты](#14-итерация-1-синтаксис-и-импорты)
15. [Итерация 2: Логика и Redis-схема](#15-итерация-2-логика-и-redis-схема)
16. [Итерация 3: End-to-end запуск](#16-итерация-3-end-to-end-запуск)
17. [Справочник протоколов бирж](#17-справочник-протоколов-бирж)
18. [Полный чеклист интеграции](#18-полный-чеклист-интеграции)

---

## 1. Обзор архитектуры

```
dictionaries/
  {ex}/{ex}_pairs.py     → REST API: список пар → data/*.txt
  {ex}/{ex}_ws.py        → WS-валидация: активные пары → data/*_active.txt
  main.py                → оркестратор 5 фаз; генерирует combination/ subscribe/ symbol_ids.json

collectors/
  {ex}_spot.py           → MD collector spot  → md:bg:s:{sym}  HASH
  {ex}_futures.py        → MD collector fut   → md:bg:f:{sym}  HASH
  ob_{ex}_spot.py        → OB collector spot  → ob:bg:s:{sym}  HASH (41 поле)
  ob_{ex}_futures.py     → OB collector fut   → ob:bg:f:{sym}  HASH (41 поле)
  fr_{ex}_futures.py     → FR collector fut   → fr:bg:{sym}    HASH (4 поля)
  hist_writer.py         → shared: MD history writer (md:hist:*)
  ob_hist_writer.py      → shared: OB history writer (ob:hist:*)
  fr_hist_writer.py      → shared: FR history writer (fr:hist:*)
  spread_monitor.py      → читает md:* / ob:* / fr:*, генерирует сигналы + снапшоты
  staleness_monitor.py   → проверяет свежесть md:* ключей каждые 60 с
  run_all.py             → запускает всё; Rich TUI дашборд

Redis key space:
  md:{ex}:{mkt}:{sym}              HASH  b,a,t
  md:hist:{ex}:{mkt}:{sym}:{cid}  ZSET  bid|ask|ts_ms
  ob:{ex}:{mkt}:{sym}              HASH  b1..b10, bq1..bq10, a1..a10, aq1..aq10, t
  ob:hist:{ex}:{mkt}:{sym}:{cid}  ZSET  b1|bq1|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
  fr:{ex}:{sym}                   HASH  r, nr, ft, ts
  fr:hist:{ex}:{sym}:{cid}        ZSET  r|nr|ft|ts_ms
```

**Поток данных при сигнале:**
```
md:*  →  spread_monitor → сигнал → снапшот CSV
ob:*  →  spread_monitor → строки снапшота (40 полей OB)
fr:*  →  spread_monitor → строки снапшота (fr_r, fr_ft)
md:hist:* + ob:hist:* + fr:hist:*  →  write_snapshot_history() → prepend 1h истории
```

---

## 2. Глоссарий и соглашения

| Переменная | Значение | Пример |
|-----------|---------|--------|
| `{EX}` | Код биржи в Redis (2-3 символа) | `bg` |
| `{MKT}` | Рынок: `s` = spot, `f` = futures | `s` |
| `{ex_name}` | Полное имя биржи (snake_case) | `bitget` |
| `{ex_dir}` | Название папки в subscribe/ | `bitget` |
| `SCRIPT` | Имя скрипта в JSON-логах | `"bitget_spot"` |

**Правило именования:**
```
MD:  {ex_name}_spot.py          SCRIPT = "{ex_name}_spot"    EX="{EX}" MKT="s"
MD:  {ex_name}_futures.py       SCRIPT = "{ex_name}_futures" EX="{EX}" MKT="f"
OB:  ob_{ex_name}_spot.py       SCRIPT = "ob_{ex_name}_spot"
OB:  ob_{ex_name}_futures.py    SCRIPT = "ob_{ex_name}_futures"
FR:  fr_{ex_name}_futures.py    SCRIPT = "fr_{ex_name}_futures"
```

**Обязательный Redis-паттерн — всегда pipeline:**
```python
async with r.pipeline(transaction=False) as pipe:
    pipe.hset(key, mapping=...)
    hw.add_to_pipe(pipe, batch, now_sec)
    hw.ensure_config(pipe, now_sec)
    await pipe.execute()
```

**Обязательный паттерн логирования:**
```python
def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()
```

**Обязательные события для дашборда** (run_all.py их читает):
| Event | Когда логировать | Влияние на статус |
|-------|-----------------|-------------------|
| `connecting` | Начало TCP-соединения | 🟡 "connecting" |
| `connected` | WS-хендшейк завершён | 🟡 "connected" |
| `subscribed` | Отправлены все subscribe | 🟡 "subscribed" |
| `first_message` | Первое рыночное сообщение | 🟢 "streaming" |
| `disconnected` | Обрыв соединения | 🔴 "disconnected" |
| `reconnecting` | Пауза перед повтором | 🟡 "reconnecting" |
| `stats` | Каждые 30 с; поля: `msgs_total`, `msgs_per_sec`, `avg_pipeline_ms` | 🟢 "streaming" |
| `collector_crashed` | Необработанное исключение | 🔴 "CRASHED" |

---

## 3. Шаг 1 — Dictionary: получение пар

**Создать:** `dictionaries/{ex_name}/{ex_name}_pairs.py`

**Обязательный публичный интерфейс:**
```python
def fetch_pairs() -> tuple[list[str], list[str]]:
    """Возвращает (spot_normalized, futures_normalized) — формат BTCUSDT."""
```

**Дополнительно для бирж с нативным форматом** (OKX, Gate):
```python
def load_native() -> tuple[list[str], list[str]]:
    """Возвращает (spot_native, futures_native) из ранее сохранённых файлов."""
```

**Шаблон файла (`{ex_name}_pairs.py`):**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
{ex_name}_pairs.py - Получение торговых пар {ExchangeName} через REST API.
Spot:    {spot_api_url}
Futures: {futures_api_url}
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL    = "{SPOT_REST_URL}"
FUTURES_URL = "{FUTURES_REST_URL}"

SPOT_FILE    = DATA_DIR / "{ex_name}_spot.txt"
FUTURES_FILE = DATA_DIR / "{ex_name}_futures.txt"

# Для бирж с нативным форматом — дополнительно:
# SPOT_NATIVE_FILE    = DATA_DIR / "{ex_name}_spot_native.txt"
# FUTURES_NATIVE_FILE = DATA_DIR / "{ex_name}_futures_native.txt"

QUOTE_ASSETS = ("USDT", "USDC")


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _normalize(sym: str) -> str:
    """Привести нативный формат к BTCUSDT. Если уже нормализован — вернуть как есть."""
    # Binance/Bybit/Bitget: без изменений
    # OKX: s.replace("-","").replace("-SWAP","")  →  до замены -SWAP
    # Gate: s.replace("_","")
    return sym  # заменить под конкретную биржу


def _extract_spot(data: list) -> List[str]:
    pairs = []
    for s in data:
        if s.get("{STATUS_FIELD}") != "{STATUS_VALUE}":   # e.g. "status"/"online"
            continue
        if s.get("{QUOTE_FIELD}") not in QUOTE_ASSETS:    # e.g. "quoteCoin"
            continue
        pairs.append(_normalize(s["{SYMBOL_FIELD}"]))     # e.g. "symbol"
    return sorted(pairs)


def _extract_futures(data: list) -> List[str]:
    pairs = []
    for s in data:
        if s.get("{STATUS_FIELD}") != "{STATUS_VALUE}":
            continue
        if s.get("{QUOTE_FIELD}") not in QUOTE_ASSETS:
            continue
        pairs.append(_normalize(s["{SYMBOL_FIELD}"]))
    return sorted(pairs)


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    spot_data    = _fetch_json(SPOT_URL)
    spot         = _extract_spot(spot_data.get("data", []))   # путь к массиву зависит от биржи

    futures_data = _fetch_json(FUTURES_URL)
    futures      = _extract_futures(futures_data.get("data", []))

    _save(SPOT_FILE,    spot)
    _save(FUTURES_FILE, futures)
    return spot, futures


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"{'{ex_name}'} Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"{'{ex_name}'} Futures: {len(futures)} пар  -> {FUTURES_FILE}")
```

**Чеклист `_pairs.py`:**
- [ ] Оба URL доступны без авторизации (публичный API)
- [ ] Фильтр `quoteCoin / quoteAsset / quoteCcy` по `{USDT, USDC}`
- [ ] Фильтр `status == "active" / "online" / "Trading" / "live"` — уточнить по документации
- [ ] Результат — нормализованный `BTCUSDT` без символов-разделителей
- [ ] Для нативного формата — сохраняются оба файла (native + normalized)
- [ ] Функция `load_native()` добавлена если нужна нативная форма для WS
- [ ] `__main__` блок для standalone запуска

---

## 4. Шаг 2 — Dictionary: WS-валидация

**Создать:** `dictionaries/{ex_name}/{ex_name}_ws.py`

**Обязательный публичный интерфейс:**
```python
# Биржи без нативного формата (Binance-подобные):
async def _run(spot: list, futures: list, duration: int) -> tuple[list, list]:

# Биржи с нативным форматом (OKX/Gate-подобные):
async def _run(spot_native, futures_native, spot_norm, futures_norm, duration) -> tuple[list, list]:
```

**Шаблон (`{ex_name}_ws.py`):**
```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
{ex_name}_ws.py - WS-валидация пар {ExchangeName}.
Канал: {WS_CHANNEL}
Пинг: {PING_FORMAT}
"""

import asyncio, json, time
from pathlib import Path
from typing import List, Set, Tuple
import websockets

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

WS_URL              = "{WS_ENDPOINT}"
INST_TYPE_SPOT      = "{SPOT_INST_TYPE}"     # если используется instType
INST_TYPE_FUTURES   = "{FUTURES_INST_TYPE}"
BATCH_SIZE          = 100    # подобрать под лимиты биржи
DURATION_SECONDS    = 60
PING_INTERVAL       = 25
SUBSCRIBE_DELAY     = 0.1    # сек между батчами (соблюдаем rate limit)

SPOT_ACTIVE_FILE    = DATA_DIR / "{ex_name}_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "{ex_name}_futures_active.txt"


def _chunk(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


async def _ping_loop(ws, stop_evt: asyncio.Event) -> None:
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            await ws.send("{PING_PAYLOAD}")   # "ping" или json.dumps({"op":"ping"})
    except Exception:
        pass


async def _validate_market(inst_type: str, symbols: List[str], duration: int) -> Set[str]:
    responded: Set[str] = set()
    stop_evt = asyncio.Event()

    async def _recv_loop(ws):
        try:
            async for raw in ws:
                if raw == "pong":          # для строкового пинга
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # Извлечь sym из msg — зависит от биржи:
                # Bitget:  msg["arg"]["instId"]  при  msg["action"] in ("snapshot","update")
                # OKX:     msg["data"][0]["instId"]
                # Gate:    msg["result"]["s"]
                # Bybit:   msg["topic"].split(".")[-1]
                sym = _extract_sym(msg)    # реализовать под конкретную биржу
                if sym:
                    responded.add(sym.upper())
        except (asyncio.CancelledError, Exception):
            pass

    try:
        async with websockets.connect(WS_URL, ping_interval=None, max_queue=2048, close_timeout=3) as ws:
            for batch in _chunk(symbols, BATCH_SIZE):
                args = _build_subscribe(inst_type, batch)   # реализовать под биржу
                await ws.send(json.dumps(args))
                await asyncio.sleep(SUBSCRIBE_DELAY)

            recv_task = asyncio.create_task(_recv_loop(ws))
            ping_task = asyncio.create_task(_ping_loop(ws, stop_evt))
            try:
                await asyncio.sleep(duration)
            finally:
                stop_evt.set()
                recv_task.cancel()
                ping_task.cancel()
                try:
                    await ws.close()
                except Exception:
                    pass
                await asyncio.gather(recv_task, ping_task, return_exceptions=True)
    except Exception:
        pass

    return responded


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


async def _run(spot: List[str], futures: List[str], duration: int) -> Tuple[List[str], List[str]]:
    spot_task    = asyncio.create_task(_validate_market(INST_TYPE_SPOT,    spot,    duration))
    futures_task = asyncio.create_task(_validate_market(INST_TYPE_FUTURES, futures, duration))
    spot_resp, fut_resp = await asyncio.gather(spot_task, futures_task)

    spot_active    = [s for s in spot    if s in spot_resp]
    futures_active = [s for s in futures if s in fut_resp]

    _save(SPOT_ACTIVE_FILE,    spot_active)
    _save(FUTURES_ACTIVE_FILE, futures_active)
    return spot_active, futures_active


if __name__ == "__main__":
    import sys
    sys.path.insert(0, str(BASE_DIR.parent))
    from {ex_name}.{ex_name}_pairs import fetch_pairs
    spot, futures = fetch_pairs()
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = asyncio.run(_run(spot, futures, DURATION_SECONDS))
    print(f"Spot:    {len(active_spot)}/{len(spot)}")
    print(f"Futures: {len(active_futures)}/{len(futures)}")
```

**Чеклист `_ws.py`:**
- [ ] `_run()` возвращает `(spot_active, futures_active)` — нормализованный формат
- [ ] Сохраняет `*_active.txt` файлы
- [ ] Пинг-корутина не вызывает исключений при `stop_evt`
- [ ] `recv_loop` + `ping_loop` оба отменяются через `asyncio.gather(return_exceptions=True)`
- [ ] `asyncio.sleep(SUBSCRIBE_DELAY)` между батчами (rate limit)
- [ ] Нет блокирующих вызовов внутри async функций
- [ ] `__main__` для standalone теста

---

## 5. Шаг 3 — Обновление main.py (dictionary)

Файл: `dictionaries/main.py`

### 5.1 Добавить в COMBINATIONS (8 новых записей)

```python
# В конце списка COMBINATIONS добавить:
("{ex_name}_spot",  "binance_futures", "{ex_name}_spot_binance_futures.txt"),
("binance_spot",    "{ex_name}_futures","binance_spot_{ex_name}_futures.txt"),
("{ex_name}_spot",  "bybit_futures",   "{ex_name}_spot_bybit_futures.txt"),
("bybit_spot",      "{ex_name}_futures","bybit_spot_{ex_name}_futures.txt"),
("{ex_name}_spot",  "okx_futures",     "{ex_name}_spot_okx_futures.txt"),
("okx_spot",        "{ex_name}_futures","okx_spot_{ex_name}_futures.txt"),
("{ex_name}_spot",  "gate_futures",    "{ex_name}_spot_gate_futures.txt"),
("gate_spot",       "{ex_name}_futures","gate_spot_{ex_name}_futures.txt"),
```

> При наличии 5 бирж пересечения только между разными биржами (не `{ex}_spot × {ex}_futures`).

### 5.2 Добавить в SUBSCRIBE_MAP (2 новых записи)

```python
"{ex_name}_spot":     SUBSCRIBE_DIR / "{ex_dir}" / "{ex_name}_spot.txt",
"{ex_name}_futures":  SUBSCRIBE_DIR / "{ex_dir}" / "{ex_name}_futures.txt",
```

### 5.3 Обновить `_fetch_all_exchanges()`

```python
from {ex_name}.{ex_name}_pairs import fetch_pairs as {EX}_fetch

tasks = {
    ...,                        # существующие
    "{ex_name}": {EX}_fetch,
}
```

Распаковка в `main()`:
```python
{EX}_spot, {EX}_fut = rest_results["{ex_name}"]
r["{EX}_spot_total"]  = len({EX}_spot)
r["{EX}_fut_total"]   = len({EX}_fut)
print(f"       {ExchangeName} — Spot: {len({EX}_spot)}, Futures: {len({EX}_fut)}")
```

### 5.4 Обновить `_validate_all_exchanges()` — добавить параметры

```python
# Добавить параметры в сигнатуру:
{EX}_spot: list,
{EX}_fut: list,

# Добавить в asyncio.gather:
from {ex_name}.{ex_name}_ws import _run as {EX}_run
({EX}_spot_active, {EX}_fut_active) = await {EX}_run(...)  # через asyncio.gather

# Добавить в return:
return (..., {EX}_spot_active, {EX}_fut_active)
```

### 5.5 Обновить вызов в `main()`

```python
# Распаковка результатов WS:
..., a{EX}_spot, a{EX}_fut = asyncio.run(
    _validate_all_exchanges(
        ...,
        {EX}_spot, {EX}_fut,      # новые аргументы
        DURATION_SECONDS,
    )
)
r["{EX}_spot_active"] = len(a{EX}_spot)
r["{EX}_fut_active"]  = len(a{EX}_fut)

# Добавить в словарь active:
active = {
    ...,
    "{ex_name}_spot":    a{EX}_spot,
    "{ex_name}_futures": a{EX}_fut,
}

# Print WS results:
print(f"       {ExchangeName} — Spot: {len(a{EX}_spot)}/{len({EX}_spot)}, ...")
```

### 5.6 Добавить в `_print_report()`

```python
for exch, label in [..., ("{EX}", "{EXCHANGE_LABEL}")]:
```

---

## 6. Шаг 4 — source_ids.json

Файл: `collectors/source_ids.json`

**Правило:** ID никогда не меняются и не удаляются. Новые биржи получают следующие свободные числа.

```json
{
  "_comment": "Fixed source IDs. DO NOT change existing IDs.",
  "binance_spot":    0,
  "binance_futures": 1,
  "bybit_spot":      2,
  "bybit_futures":   3,
  "okx_spot":        4,
  "okx_futures":     5,
  "gate_spot":       6,
  "gate_futures":    7,
  "bitget_spot":     8,
  "bitget_futures":  9,
  "{ex_name}_spot":      N,     ← следующий свободный чётный
  "{ex_name}_futures":   N+1
}
```

**Чеклист:**
- [ ] ID добавлены только в конец
- [ ] Существующие ID не затронуты
- [ ] `collectors/hist_writer.py` подхватит автоматически (читает весь файл при импорте)

---

## 7. Шаг 5 — MD-коллекторы (md:* ключи)

Создать **два файла**: `collectors/{ex_name}_spot.py` и `collectors/{ex_name}_futures.py`

Единственная разница между ними: `EX`, `MKT`, `SCRIPT`, `SUBSCRIBE_FILE`, `WS_*` константы.

### Шаблон MD-коллектора

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
{ExchangeName} {Market} — market data collector.
WS: {WS_ENDPOINT}  Channel: {WS_CHANNEL}
Redis key: md:{EX}:{MKT}:{SYMBOL}  Fields: b (bid), a (ask), t (local ms)
"""

import asyncio
import sys
import time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from hist_writer import HistWriter

SCRIPT = "{ex_name}_{market}"          # "bitget_spot" / "bitget_futures"
EX, MKT = "{EX}", "{s_or_f}"           # "bg", "s"  |  "bg", "f"

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/{ex_dir}/{ex_name}_{market}.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"

# ── WS-константы (специфичны для биржи) ──────────────────────────────────────
WS_URL          = "{WS_ENDPOINT}"
INST_TYPE       = "{SPOT_OR_FUTURES_INST_TYPE}"   # для бирж с instType
BATCH_SIZE      = 100          # символов на одно subscribe-сообщение
CHUNK_SIZE      = 300          # символов на одно WS-соединение (если нужно несколько)
PING_INTERVAL   = 25           # секунд

# ── общие константы ───────────────────────────────────────────────────────────
BATCH_TIMEOUT   = 0.050        # 50 ms — flush каждые 50ms
STATS_INTERVAL  = 30
RECONNECT_DELAY = 3


def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


def load_symbols() -> list[str]:
    return [s for s in SUBSCRIBE_FILE.read_text().strip().splitlines() if s]


def redis_key(sym: str) -> bytes:
    return f"md:{EX}:{MKT}:{sym}".encode()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush(r: aioredis.Redis, batch: list, hw: HistWriter, counters: dict) -> None:
    t0 = time.monotonic()
    now_sec = time.time()
    async with r.pipeline(transaction=False) as pipe:
        for key, mapping in batch:
            pipe.hset(key, mapping=mapping)
        hw.add_to_pipe(pipe, batch, now_sec)
        hw.ensure_config(pipe, now_sec)
        await pipe.execute()
    counters["flushes"]   += 1
    counters["lat_total"] += (time.monotonic() - t0) * 1000


# ── WS connect + collect ──────────────────────────────────────────────────────

async def _ping_loop(ws, stop_evt: asyncio.Event) -> None:
    """Кастомный пинг — адаптировать под биржу."""
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            # Binance-style: встроен в websockets (ping_interval=20), не нужен отдельный
            # Bybit:  await ws.send(json.dumps({"op": "ping"}))
            # OKX:    await ws.send("ping")
            # Gate:   await ws.send(json.dumps({"time": int(time.time()), "channel": "spot.ping"}))
            # Bitget: await ws.send("ping")
            await ws.send("{PING_PAYLOAD}")
    except Exception:
        pass


async def collect(r: aioredis.Redis, syms: list[str], hw: HistWriter, counters: dict) -> None:
    """
    Основной цикл сбора. Паттерн зависит от биржи:
    - Binance-style: URL содержит стримы, одно соединение на chunk
    - Bybit/OKX/Gate/Bitget-style: одно соединение + subscribe-сообщения
    """
    stop_evt = asyncio.Event()
    t_disconnect = None

    while True:
        batch: list = []
        last_flush  = time.monotonic()
        stop_evt.clear()

        try:
            log("INFO", "connecting", symbols=len(syms))
            async with websockets.connect(
                WS_URL,
                ping_interval=None,    # кастомный пинг (или 20 для Binance)
                max_queue=4096,
                close_timeout=5,
            ) as ws:
                t_connected = time.monotonic()
                if t_disconnect is not None:
                    log("INFO", "reconnected",
                        downtime_sec=round(time.monotonic() - t_disconnect, 1))
                    t_disconnect = None

                # ── Подписка (специфично для биржи) ──────────────────────────
                # Batched subscribe:
                import json as _json
                for i in range(0, len(syms), BATCH_SIZE):
                    batch_syms = syms[i : i + BATCH_SIZE]
                    # Bitget:
                    args = [{"instType": INST_TYPE, "channel": "ticker", "instId": s}
                            for s in batch_syms]
                    await ws.send(_json.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.1)
                # ──────────────────────────────────────────────────────────────

                log("INFO", "subscribed", symbols=len(syms))
                ping_task        = asyncio.create_task(_ping_loop(ws, stop_evt))
                first_msg_logged = False

                try:
                    async for raw in ws:
                        now = time.monotonic()
                        if raw == "pong":
                            continue
                        try:
                            # ── Парсинг ответа (специфично для биржи) ────────
                            obj = orjson.loads(raw)
                            # Bitget ticker example:
                            # {"action":"snapshot","arg":{"channel":"ticker","instId":"BTCUSDT"},
                            #  "data":[{"instId":"BTCUSDT","bidPr":"...","askPr":"..."}]}
                            action = obj.get("action")
                            if action not in ("snapshot", "update"):
                                continue
                            arg = obj.get("arg", {})
                            if arg.get("channel") != "ticker":
                                continue
                            for item in obj.get("data", []):
                                sym = item.get("instId") or arg.get("instId")
                                bid = item.get("{BID_FIELD}")  # "bidPr" for Bitget
                                ask = item.get("{ASK_FIELD}")  # "askPr" for Bitget
                                if sym and bid and ask:
                                    ts_ms = str(int(time.time() * 1000)).encode()
                                    batch.append((
                                        redis_key(sym),
                                        {b"b": str(bid).encode(),
                                         b"a": str(ask).encode(),
                                         b"t": ts_ms},
                                    ))
                                    counters["msgs"] += 1
                            # ──────────────────────────────────────────────────
                            if not first_msg_logged and batch:
                                log("INFO", "first_message",
                                    ms_since_connected=round((now - t_connected) * 1000, 1))
                                first_msg_logged = True
                        except Exception:
                            pass

                        if batch and now - last_flush >= BATCH_TIMEOUT:
                            await flush(r, batch, hw, counters)
                            batch.clear()
                            last_flush = now

                finally:
                    stop_evt.set()
                    ping_task.cancel()
                    try:
                        await ping_task
                    except asyncio.CancelledError:
                        pass

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            t_disconnect = time.monotonic()
            if batch:
                try:
                    await flush(r, batch, hw, counters)
                except Exception as fe:
                    log("WARN", "flush_on_disconnect_failed",
                        batch_size=len(batch), reason=str(fe)[:80])

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


async def stats_loop(counters: dict) -> None:
    prev_msgs = prev_flushes = 0
    prev_lat  = 0.0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs        = counters["msgs"]
        rate        = (msgs - prev_msgs) / STATS_INTERVAL
        int_flushes = counters["flushes"] - prev_flushes
        avg_lat     = (counters["lat_total"] - prev_lat) / int_flushes if int_flushes else 0.0
        log("INFO", "stats",
            msgs_total=msgs,
            msgs_per_sec=round(rate, 1),
            flushes_total=counters["flushes"],
            avg_pipeline_ms=round(avg_lat, 3))
        prev_msgs    = msgs
        prev_flushes = counters["flushes"]
        prev_lat     = counters["lat_total"]


async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    hw       = HistWriter(EX, MKT, syms)

    await asyncio.gather(
        collect(r, syms, hw, counters),
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
```

### Ключевые поля MD ticker по биржам

| Биржа | Канал | Bid field | Ask field | Sym field |
|-------|-------|-----------|-----------|-----------|
| Binance | `{sym}@bookTicker` | `b` | `a` | `s` (в `data`) |
| Bybit | `orderbook.1.{SYM}` | `data.b[0][0]` | `data.a[0][0]` | `topic.split(".")[-1]` |
| OKX | `tickers` + `instId` | `bidPx` | `askPx` | `instId` (нативный → normalize) |
| Gate | `spot/futures.book_ticker` | `b` | `a` | `result.s` (нативный → normalize) |
| Bitget | `ticker` + `instId` | `bidPr` | `askPr` | `arg.instId` |

**Чеклист MD-коллектора:**
- [ ] `SCRIPT` совпадает с именем файла без `.py`
- [ ] `EX` и `MKT` установлены корректно (spot → `"s"`, futures → `"f"`)
- [ ] `redis_key()` возвращает `md:{EX}:{MKT}:{sym}`
- [ ] Batch items: `(redis_key(sym), {b"b": bid_bytes, b"a": ask_bytes, b"t": ts_ms_bytes})`
- [ ] `flush()` вызывает `hw.add_to_pipe()` и `hw.ensure_config()`
- [ ] `BATCH_TIMEOUT = 0.050` (50 ms)
- [ ] Все обязательные события залогированы
- [ ] `HistWriter(EX, MKT, syms)` создан с полным списком symbols
- [ ] При дисконнекте: `flush()` остаток буфера

---

## 8. Шаг 6 — OB-коллекторы (ob:* ключи)

Создать: `collectors/ob_{ex_name}_spot.py` и `collectors/ob_{ex_name}_futures.py`

### Ключевые отличия OB от MD

```python
# Импорт
from ob_hist_writer import OBHistWriter

# HistWriter
hw = OBHistWriter(EX, MKT, syms)

# Redis key
def redis_key(sym): return f"ob:{EX}:{MKT}:{sym}".encode()

# Маппинг — 41 поле:
def build_mapping(bids: list, asks: list, ts_ms: bytes) -> dict:
    LEVELS = 10
    m = {b"t": ts_ms}
    for i, row in enumerate(bids[:LEVELS], 1):
        m[f"b{i}".encode()]  = str(row[0]).encode()   # цена
        m[f"bq{i}".encode()] = str(row[1]).encode()   # объём
    for i, row in enumerate(asks[:LEVELS], 1):
        m[f"a{i}".encode()]  = str(row[0]).encode()
        m[f"aq{i}".encode()] = str(row[1]).encode()
    return m

# Структура batch item:
batch.append((redis_key(sym), build_mapping(bids, asks, ts_ms)))
```

### Протоколы OB по биржам

**Binance (`@depth10@100ms`):**
```python
# URL содержит стримы: {sym}@depth10@100ms
# Ответ: {"stream": "btcusdt@depth10@100ms", "data": {"bids": [[price,qty],...], "asks": [...]}}
# Чистый snapshot каждые 100ms, нет delta
data = obj.get("data") or obj
bids = data.get("bids", [])
asks = data.get("asks", [])
sym  = obj.get("stream", "").split("@")[0].upper()
```

**Bybit / OKX (snapshot + delta):**
```python
# Нужно хранить локальный стакан в памяти
# При action="snapshot": перезаписать весь стакан
# При action="delta": добавить/удалить/обновить уровни (qty=0 → удалить)
# При дисконнекте: сбросить локальный стакан, получить snapshot заново

local_books: dict[str, dict] = {}  # sym -> {"bids": {price: qty}, "asks": {price: qty}}

def apply_snapshot(sym, bids, asks):
    local_books[sym] = {
        "bids": {row[0]: row[1] for row in bids},
        "asks": {row[0]: row[1] for row in asks},
    }

def apply_delta(sym, bids_delta, asks_delta):
    book = local_books.get(sym, {"bids": {}, "asks": {}})
    for price, qty in bids_delta:
        if float(qty) == 0:
            book["bids"].pop(price, None)
        else:
            book["bids"][price] = qty
    for price, qty in asks_delta:
        if float(qty) == 0:
            book["asks"].pop(price, None)
        else:
            book["asks"][price] = qty

def get_sorted_levels(sym) -> tuple[list, list]:
    book = local_books.get(sym, {"bids": {}, "asks": {}})
    bids = sorted(book["bids"].items(), key=lambda x: float(x[0]), reverse=True)[:10]
    asks = sorted(book["asks"].items(), key=lambda x: float(x[0]))[:10]
    return [[p, q] for p, q in bids], [[p, q] for p, q in asks]
```

**Bitget (`books15` — рекомендуется для 10-уровневого OB):**
```python
# Канал: books15 (15 уровней, 20ms updates) или books (full depth, 200ms)
# Используем books15 — достаточно для 10 уровней
# {"instType":"SPOT","channel":"books15","instId":"BTCUSDT"}
# Ответ action="snapshot": полный стакан
# Ответ action="update": delta

# ВАЖНО: books1 даёт только 1 уровень — НЕ использовать для OB-коллектора!
# Для WS-валидации (dictionaries) — books1 достаточно.
# Для OB-коллектора — books15 или books.
```

**Проверка достаточности уровней:**
```python
if n_bid < LEVELS or n_ask < LEVELS:
    counters["shallow"] += 1
    log("WARN", "shallow_book", sym=sym, bid_levels=n_bid, ask_levels=n_ask)
```

**Дополнительные события для OB дашборда:**
```python
# Для Bybit/OKX (snapshot+delta) — трекаем инициализацию:
log("INFO", "book_init", books_initialized=len(initialized_syms))
log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))
```

**Counters OB:**
```python
counters = {
    "msgs":          0,
    "flushes":       0,
    "lat_total":     0.0,
    "levels_total":  0,
    "levels_count":  0,
    "shallow":       0,
}
```

**Stats OB:**
```python
log("INFO", "stats",
    msgs_total       = msgs,
    msgs_per_sec     = round(rate, 1),
    flushes_total    = counters["flushes"],
    avg_pipeline_ms  = round(avg_lat, 3),
    avg_levels       = round(avg_lvl, 2),
    shallow_warnings = counters["shallow"],
)
```

**Чеклист OB-коллектора:**
- [ ] `redis_key()` → `ob:{EX}:{MKT}:{sym}`
- [ ] `build_mapping()` — ровно 41 поле: `b1..b10`, `bq1..bq10`, `a1..a10`, `aq1..aq10`, `t`
- [ ] `OBHistWriter(EX, MKT, syms)` создан
- [ ] Для snapshot-only бирж (Binance): локальный стакан не нужен
- [ ] Для delta-бирж (Bybit/OKX/Bitget books15): хранить local_books, сбросить при реконнекте
- [ ] Канал **books15** / **books** (не `books1`!) для 10-уровневого стакана
- [ ] `counters["shallow"]` при неполном стакане
- [ ] `book_init` event для snapshot+delta-бирж

---

## 9. Шаг 7 — FR-коллектор (fr:* ключи)

Создать: `collectors/fr_{ex_name}_futures.py`

**FR работает только для futures.** Spot FR не существует.

### Шаблон FR-коллектора

```python
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
{ExchangeName} Futures — funding rate collector.
WS: {WS_ENDPOINT}  Channel: {WS_CHANNEL}
Redis key: fr:{EX}:{SYMBOL}
Fields: r (rate), nr (next predicted rate), ft (next funding time ms), ts (recv ms)
"""

import asyncio, sys, time
from pathlib import Path

import orjson
import redis.asyncio as aioredis
import websockets
from websockets.exceptions import ConnectionClosed

from fr_hist_writer import FrHistWriter

SCRIPT = "fr_{ex_name}_futures"
EX     = "{EX}"     # без MKT — FR всегда futures

SUBSCRIBE_FILE  = Path(__file__).parent.parent / "dictionaries/subscribe/{ex_dir}/{ex_name}_futures.txt"
REDIS_SOCK      = "/var/run/redis/redis.sock"

BATCH_TIMEOUT   = 0.050
STATS_INTERVAL  = 30
RECONNECT_DELAY = 3

# REST fallback (для бирж где WS обновляет только при изменении):
REST_REFRESH_SEC = 30   # секунд между REST-опросами


def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


def load_symbols() -> list[str]:
    return [s for s in SUBSCRIBE_FILE.read_text().strip().splitlines() if s]


def redis_key(sym: str) -> bytes:
    return f"fr:{EX}:{sym}".encode()


async def flush(r: aioredis.Redis, batch: list, hw: FrHistWriter, counters: dict) -> None:
    t0 = time.monotonic()
    now_sec = time.time()
    async with r.pipeline(transaction=False) as pipe:
        for sym, r_b, nr_b, ft_b, ts_b in batch:
            pipe.hset(redis_key(sym),
                      mapping={b"r": r_b, b"nr": nr_b, b"ft": ft_b, b"ts": ts_b})
        hw.add_to_pipe(pipe, batch, now_sec)
        hw.ensure_config(pipe, now_sec)
        await pipe.execute()
    counters["flushes"]   += 1
    counters["lat_total"] += (time.monotonic() - t0) * 1000


async def collect_ws(r: aioredis.Redis, syms: list[str], hw: FrHistWriter, counters: dict) -> None:
    sym_set = set(syms)
    t_disconnect = None

    while True:
        batch: list = []
        last_flush  = time.monotonic()
        try:
            log("INFO", "connecting")
            async with websockets.connect(
                "{WS_URL}", ping_interval=None, max_queue=4096, close_timeout=5
            ) as ws:
                t_connected = time.monotonic()
                if t_disconnect is not None:
                    log("INFO", "reconnected",
                        downtime_sec=round(time.monotonic() - t_disconnect, 1))
                    t_disconnect = None

                # Подписка на funding rate (специфично для биржи)
                # Bitget: {"op":"subscribe","args":[{"instType":"USDT-FUTURES","channel":"funding-rate","instId":"BTCUSDT"}]}
                import json as _json
                for i in range(0, len(syms), 100):
                    batch_syms = syms[i : i + 100]
                    args = [{"instType": "USDT-FUTURES", "channel": "funding-rate",
                             "instId": s} for s in batch_syms]
                    await ws.send(_json.dumps({"op": "subscribe", "args": args}))
                    await asyncio.sleep(0.1)

                log("INFO", "subscribed", symbols=len(syms))
                first_msg_logged = False

                async for raw in ws:
                    if raw == "pong":
                        continue
                    now = time.monotonic()
                    try:
                        obj = orjson.loads(raw)
                        # Разобрать ответ FR (специфично для биржи)
                        # Bitget funding-rate:
                        # {"action":"snapshot","arg":{"channel":"funding-rate","instId":"BTCUSDT"},
                        #  "data":[{"fundingRate":"0.0001","nextFundingTime":"..."}]}
                        ts_b = str(int(time.time() * 1000)).encode()
                        for item in obj.get("data", []):
                            sym = (obj.get("arg") or {}).get("instId", "")
                            if sym not in sym_set:
                                continue
                            r_val  = str(item.get("{RATE_FIELD}", "")).encode()     # "fundingRate"
                            nr_val = str(item.get("{NEXT_RATE_FIELD}", "")).encode()# если есть
                            ft_val = str(item.get("{NEXT_TIME_FIELD}", "")).encode()# "nextFundingTime"
                            if r_val:
                                batch.append((sym, r_val, nr_val, ft_val, ts_b))
                                counters["msgs"] += 1

                        if not first_msg_logged and batch:
                            log("INFO", "first_message",
                                ms_since_connected=round((now - t_connected) * 1000, 1))
                            first_msg_logged = True
                    except Exception:
                        pass

                    if batch and now - last_flush >= BATCH_TIMEOUT:
                        await flush(r, batch, hw, counters)
                        batch.clear()
                        last_flush = now

        except (ConnectionClosed, OSError, Exception) as e:
            log("WARN", "disconnected", reason=str(e)[:120])
            t_disconnect = time.monotonic()
            if batch:
                try:
                    await flush(r, batch, hw, counters)
                except Exception:
                    pass

        log("INFO", "reconnecting", delay_sec=RECONNECT_DELAY)
        await asyncio.sleep(RECONNECT_DELAY)


async def rest_refresh_loop(r: aioredis.Redis, syms: list[str],
                             hw: FrHistWriter, counters: dict) -> None:
    """
    REST-фолбэк. Нужен если WS отправляет FR только при изменении (OKX, Gate, возможно Bitget).
    Гарантирует свежесть данных даже при редких WS-обновлениях.
    """
    import json as _json
    from urllib.request import Request, urlopen

    REST_URL = "{FR_REST_URL}"   # e.g. "https://api.bitget.com/api/v2/mix/market/funding-time?productType=USDT-FUTURES"
    while True:
        await asyncio.sleep(REST_REFRESH_SEC)
        try:
            req  = Request(REST_URL, headers={"User-Agent": "Mozilla/5.0"})
            with urlopen(req, timeout=10) as resp:
                data = _json.loads(resp.read())
            batch: list = []
            ts_b = str(int(time.time() * 1000)).encode()
            for item in data.get("data", []):
                sym = item.get("{SYMBOL_FIELD}", "")
                r_val = str(item.get("{RATE_FIELD}", "")).encode()
                ft_val = str(item.get("{TIME_FIELD}", "")).encode()
                if sym and r_val:
                    batch.append((sym, r_val, b"", ft_val, ts_b))
            if batch:
                await flush(r, batch, hw, counters)
            log("INFO", "rest_refresh", symbols=len(batch))
        except Exception as e:
            log("WARN", "rest_refresh_failed", reason=str(e)[:120])


async def stats_loop(counters: dict) -> None:
    prev_msgs = prev_flushes = 0
    prev_lat  = 0.0
    while True:
        await asyncio.sleep(STATS_INTERVAL)
        msgs        = counters["msgs"]
        int_flushes = counters["flushes"] - prev_flushes
        avg_lat     = (counters["lat_total"] - prev_lat) / int_flushes if int_flushes else 0.0
        log("INFO", "stats",
            msgs_total=msgs,
            msgs_per_sec=round((msgs - prev_msgs) / STATS_INTERVAL, 1),
            flushes_total=counters["flushes"],
            avg_pipeline_ms=round(avg_lat, 3))
        prev_msgs    = msgs
        prev_flushes = counters["flushes"]
        prev_lat     = counters["lat_total"]


async def main() -> None:
    log("INFO", "startup")
    syms = load_symbols()
    log("INFO", "symbols_loaded", count=len(syms), file=str(SUBSCRIBE_FILE))

    r  = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False, max_connections=4)
    hw = FrHistWriter(EX, syms)

    counters = {"msgs": 0, "flushes": 0, "lat_total": 0.0}
    await asyncio.gather(
        collect_ws(r, syms, hw, counters),
        # rest_refresh_loop(r, syms, hw, counters),  # раскомментировать если WS event-driven
        stats_loop(counters),
    )


if __name__ == "__main__":
    asyncio.run(main())
```

### FR поля по биржам

| Биржа | Канал WS | `r` (rate) | `nr` (next rate) | `ft` (next time ms) | REST fallback |
|-------|----------|-----------|-----------------|---------------------|---------------|
| Binance | `!markPrice@arr@1s` — все символы | `r` | — | `T` | нет (1s stream) |
| Bybit | `tickers.{SYM}` | `fundingRate` | `nextFundingRate` | `nextFundingTime` | нет (200ms) |
| OKX | `funding-rate.{instId}` | `fundingRate` | `nextFundingRate` | `fundingTime` | да (event-driven) |
| Gate | `futures.tickers` | `funding_rate` | — | `funding_next_apply` | да (event-driven) + contracts_loop |
| Bitget | `ticker` (instType: USDT-FUTURES) | `fundingRate` | — | `nextFundingTime` | да (event-driven); REST: `/api/v2/mix/market/tickers?productType=USDT-FUTURES` |

**Чеклист FR-коллектора:**
- [ ] `redis_key()` → `fr:{EX}:{sym}` (без `:{MKT}`)
- [ ] Batch item: `(sym, r_bytes, nr_bytes, ft_bytes, ts_bytes)` — строго этот порядок
- [ ] `ft` поле — unix ms (не секунды). Конвертировать если нужно
- [ ] `nr_b = b""` если биржа не предоставляет predicted rate
- [ ] `FrHistWriter(EX, syms)` — только EX, без MKT
- [ ] REST fallback добавить если WS обновляет только при изменении ставки
- [ ] `rest_refresh` event залогирован

---

## 10. Шаг 8 — run_all.py

Файл: `run_all.py`

### 10.1 Добавить в COLLECTORS

```python
COLLECTORS = [
    # ── MD collectors ─────────────────────────────────────────────────────────
    "binance_spot", "binance_futures",
    "bybit_spot",   "bybit_futures",
    "okx_spot",     "okx_futures",
    "gate_spot",    "gate_futures",
    "{ex_name}_spot", "{ex_name}_futures",    # ← добавить
    # ── OB collectors ─────────────────────────────────────────────────────────
    "ob_binance_spot", "ob_binance_futures",
    "ob_bybit_spot",   "ob_bybit_futures",
    "ob_okx_spot",     "ob_okx_futures",
    "ob_gate_spot",    "ob_gate_futures",
    "ob_{ex_name}_spot", "ob_{ex_name}_futures",  # ← добавить
    # ── FR collectors ─────────────────────────────────────────────────────────
    "fr_binance_futures",
    "fr_bybit_futures",
    "fr_okx_futures",
    "fr_gate_futures",
    "fr_{ex_name}_futures",    # ← добавить
]
```

### 10.2 Обновить счётчики групп

```python
_N_MD = 10   # было 8, стало 10 (+ 2 Bitget)
_N_OB = 10   # было 8, стало 10 (+ 2 Bitget)
_N_FR = 5    # было 4, стало 5  (+ 1 Bitget)
```

> `_N_MD`, `_N_OB`, `_N_FR` — это срезы `COLLECTORS[:_N_MD]`, `COLLECTORS[_N_MD:_N_MD+_N_OB]` и т.д.
> Используются в `build_layout()` для разбивки на строки дашборда.

### 10.3 Добавить в EX_SHORT

```python
EX_SHORT = {
    "binance": "bn",
    "bybit":   "bb",
    "okx":     "ok",
    "gate":    "gt",
    "{ex_name}": "{EX}",    # ← добавить
}
```

`EX_SHORT` используется в:
- `_dir_short()` — отображение направления сигнала (`"bitget→bn"`)
- `_script_short()` — сокращённое имя скрипта в дашборде (`"bg-s"`, `"bg-f"`)

**Чеклист run_all.py:**
- [ ] Все 3 новых коллектора в `COLLECTORS` (2 MD + 2 OB + 1 FR = 5 записей)
- [ ] `_N_MD`, `_N_OB`, `_N_FR` обновлены
- [ ] `EX_SHORT` дополнен
- [ ] Порядок в `COLLECTORS` строго: сначала все MD, потом все OB, потом все FR

---

## 11. Шаг 9 — spread_monitor.py

Файл: `collectors/spread_monitor.py`

### 11.1 Добавить в EXCHANGE_CODE

```python
EXCHANGE_CODE = {
    "binance": "bn",
    "bybit":   "bb",
    "okx":     "ok",
    "gate":    "gt",
    "{ex_name}": "{EX}",    # ← добавить
}
```

`EXCHANGE_CODE` используется в `load_directions()` для разбора имён combination-файлов.
Без этой записи combination-файлы с новой биржей будут пропущены (`continue`).

**Проверка после изменения:**
```python
# В spread_monitor.py, load_directions():
#   if spot_name not in EXCHANGE_CODE or fut_name not in EXCHANGE_CODE:
#       continue   ← без записи — все Bitget-направления пропускаются
```

**Чеклист spread_monitor.py:**
- [ ] `EXCHANGE_CODE["{ex_name}"] = "{EX}"` добавлен
- [ ] Больше никаких изменений не нужно — всё остальное работает через combination-файлы

---

## 12. Шаг 10 — hist_writer конфиги (sources)

Обновить метаданные `sources` в трёх hist_writer файлах.
**Функционально некритично** (эти поля — metadata в Redis `*.hist:config` ключах),
но важно для корректной документации и диагностики.

### `collectors/hist_writer.py`

```python
config = {
    ...
    "sources": ["bn:s","bn:f","bb:s","bb:f","ok:s","ok:f","gt:s","gt:f",
                "{EX}:s","{EX}:f"],    # ← добавить
}
```

### `collectors/ob_hist_writer.py`

```python
config = {
    ...
    "sources": ["bn:s","bn:f","bb:s","bb:f","ok:s","ok:f","gt:s","gt:f",
                "{EX}:s","{EX}:f"],    # ← добавить
}
```

### `collectors/fr_hist_writer.py`

```python
pipe.set(CONFIG_KEY, orjson.dumps({
    ...
    "sources": ["bn", "bb", "ok", "gt", "{EX}"],    # ← добавить
}))
```

---

## 13. Шаг 11 — Генерация словарей

После реализации всех шагов выполнить:

```bash
cd /root/bali3.0/dictionaries
python3 main.py
```

**Ожидаемый вывод:**
```
[Фаза 1/4] REST API: получение пар со всех бирж параллельно...
       Binance   — Spot: N, Futures: N
       Bybit     — Spot: N, Futures: N
       OKX       — Spot: N, Futures: N
       Gate.io   — Spot: N, Futures: N
       {ExchangeName} — Spot: N, Futures: N   ← должна появиться
       Время REST: ~3-5 сек

[Фаза 2/4] WS-валидация: все биржи параллельно (60 сек)...
       {ExchangeName} — Spot: N/N, Futures: N/N   ← должны быть ненулевые

[Фаза 3/4] Создание пересечений (combination/)...
       N  пар  {ex_name}_spot_binance_futures.txt   ← 8 новых файлов
       N  пар  binance_spot_{ex_name}_futures.txt
       ...

[Фаза 4/4] Создание файлов подписки (subscribe/)...
       N  пар  {ex_name}_spot
       N  пар  {ex_name}_futures

[Symbol IDs] M уникальных символов → collectors/symbol_ids.json
```

**Проверка результатов:**
```bash
# Проверить что subscribe-файлы не пустые
wc -l dictionaries/subscribe/{ex_dir}/*.txt

# Проверить что combination-файлы созданы
ls dictionaries/combination/ | grep {ex_name}

# Проверить symbol_ids.json обновлён
python3 -c "import json; d=json.load(open('collectors/symbol_ids.json')); print(len(d))"
```

---

## 14. Итерация 1: Синтаксис и импорты

**Цель:** убедиться что все файлы парсятся Python без ошибок.

```bash
cd /root/bali3.0

# 1. Проверить синтаксис всех новых файлов
python3 -c "
import py_compile, glob
files = [
    'dictionaries/{ex_name}/{ex_name}_pairs.py',
    'dictionaries/{ex_name}/{ex_name}_ws.py',
    'collectors/{ex_name}_spot.py',
    'collectors/{ex_name}_futures.py',
    'collectors/ob_{ex_name}_spot.py',
    'collectors/ob_{ex_name}_futures.py',
    'collectors/fr_{ex_name}_futures.py',
    'collectors/spread_monitor.py',
    'run_all.py',
]
for f in files:
    try:
        py_compile.compile(f, doraise=True)
        print(f'OK  {f}')
    except py_compile.PyCompileError as e:
        print(f'ERR {f}: {e}')
"

# 2. Проверить импорты (без запуска main())
python3 -c "
import sys; sys.path.insert(0,'collectors')
from hist_writer import HistWriter, SYMBOL_IDS, SOURCE_IDS
from ob_hist_writer import OBHistWriter
from fr_hist_writer import FrHistWriter
print('hist_writers: OK')
print(f'  SOURCE_IDS: {SOURCE_IDS}')
print(f'  SYMBOL_IDS: {len(SYMBOL_IDS)} symbols')
"

# 3. Проверить dictionary модули
python3 -c "
import sys; sys.path.insert(0,'dictionaries')
from {ex_name}.{ex_name}_pairs import fetch_pairs
print('{ex_name} pairs: importable OK')
from {ex_name}.{ex_name}_ws import _run
print('{ex_name} ws: importable OK')
"

# 4. Проверить EXCHANGE_CODE в spread_monitor
python3 -c "
import sys; sys.path.insert(0,'collectors')
# Не импортировать напрямую (нужен Redis), читаем файл
with open('collectors/spread_monitor.py') as f:
    src = f.read()
assert \"'{EX}'\" in src or '\"{EX}\"' in src, 'EX_CODE MISSING in EXCHANGE_CODE'
print('spread_monitor EXCHANGE_CODE: OK')
"

# 5. Проверить source_ids.json
python3 -c "
import json
d = json.load(open('collectors/source_ids.json'))
assert '{ex_name}_spot'    in d, 'MISSING {ex_name}_spot'
assert '{ex_name}_futures' in d, 'MISSING {ex_name}_futures'
print(f'source_ids: OK  ({d[\"{ex_name}_spot\"]} / {d[\"{ex_name}_futures\"]})')
"

# 6. Проверить run_all.py COLLECTORS
python3 -c "
import sys; sys.path.insert(0,'.')
# Читаем без запуска asyncio
with open('run_all.py') as f: src = f.read()
for name in ['{ex_name}_spot', '{ex_name}_futures',
             'ob_{ex_name}_spot', 'ob_{ex_name}_futures',
             'fr_{ex_name}_futures']:
    assert name in src, f'MISSING {name} in run_all.py COLLECTORS'
    print(f'run_all COLLECTORS: {name} OK')
"
```

**Критерий прохождения Итерации 1:**
- [ ] Все файлы компилируются без ошибок
- [ ] `hist_writer`, `ob_hist_writer`, `fr_hist_writer` импортируются
- [ ] `source_ids.json` содержит новые ключи
- [ ] `spread_monitor.py` содержит `EXCHANGE_CODE["{ex_name}"]`
- [ ] `run_all.py` содержит все 5 новых имён коллекторов

---

## 15. Итерация 2: Логика и Redis-схема

**Цель:** проверить корректность Redis-ключей, структуры данных и взаимодействия между компонентами.

```bash
# 1. Запустить REST-fetch для новой биржи
python3 dictionaries/{ex_name}/{ex_name}_pairs.py
# Ожидаемо: "Spot: N пар, Futures: N пар"
# Ненулевые значения — признак корректного API

# 2. Запустить WS-валидацию для новой биржи (60 сек)
python3 dictionaries/{ex_name}/{ex_name}_ws.py
# Ожидаемо: "Spot active: N/M  Futures active: N/M"
# N > 0 — признак корректного WS-протокола

# 3. Полная генерация словарей
python3 dictionaries/main.py
# Проверить что все 8 новых combination-файлов созданы и не пустые:
ls -la dictionaries/combination/*{ex_name}*
ls -la dictionaries/subscribe/{ex_dir}/

# 4. Тест MD-коллектора standalone (30 сек, Ctrl+C)
python3 collectors/{ex_name}_spot.py
# Ожидаемые события в JSON: startup → symbols_loaded → connecting →
#                           connected → subscribed → first_message → stats
# Проверить Redis через:
redis-cli -s /var/run/redis/redis.sock HGETALL "md:{EX}:s:BTCUSDT"
# Ожидаемо: b, a, t поля с ненулевыми значениями

# 5. Тест OB-коллектора standalone (30 сек)
python3 collectors/ob_{ex_name}_spot.py
redis-cli -s /var/run/redis/redis.sock HGETALL "ob:{EX}:s:BTCUSDT"
# Ожидаемо: 41 поле (b1..b10, bq1..bq10, a1..a10, aq1..aq10, t)
redis-cli -s /var/run/redis/redis.sock HLEN "ob:{EX}:s:BTCUSDT"
# Ожидаемо: 41

# 6. Тест FR-коллектора standalone (30 сек)
python3 collectors/fr_{ex_name}_futures.py
redis-cli -s /var/run/redis/redis.sock HGETALL "fr:{EX}:BTCUSDT"
# Ожидаемо: r, nr, ft, ts поля

# 7. Проверить hist-ключи
redis-cli -s /var/run/redis/redis.sock KEYS "md:hist:{EX}:s:BTCUSDT:*"
redis-cli -s /var/run/redis/redis.sock KEYS "ob:hist:{EX}:s:BTCUSDT:*"
redis-cli -s /var/run/redis/redis.sock KEYS "fr:hist:{EX}:BTCUSDT:*"

# 8. Проверить структуру OB hist entry
redis-cli -s /var/run/redis/redis.sock ZRANGE "ob:hist:{EX}:s:BTCUSDT:{CHUNK_ID}" 0 0
# Ожидаемо: строка из 41 значения через |

# 9. Проверить что spread_monitor видит новые направления
grep '{ex_name}' dictionaries/combination/*.txt | head -5
# Должны быть непустые строки

# 10. Тест spread_monitor с новыми направлениями (нужен Redis с данными)
python3 -c "
import sys; sys.path.insert(0,'collectors')
# Временный тест load_directions
import asyncio
# Мокаем COMBO_DIR
from pathlib import Path
import spread_monitor
dirs = spread_monitor.load_directions()
bitget_dirs = {k:v for k,v in dirs.items() if '{ex_name}' in k}
print(f'Bitget-направления: {len(bitget_dirs)}')
for name, (s,f,syms) in bitget_dirs.items():
    print(f'  {name}: spot={s} fut={f} symbols={len(syms)}')
"
```

**Критерий прохождения Итерации 2:**
- [ ] REST возвращает ненулевое количество пар (spot и futures)
- [ ] WS-валидация: хотя бы 50% пар активны
- [ ] Все 8 combination-файлов созданы, непустые
- [ ] MD коллектор: `md:{EX}:s:BTCUSDT` HASH с полями `b`, `a`, `t`
- [ ] OB коллектор: `ob:{EX}:s:BTCUSDT` HASH с ровно 41 полем
- [ ] FR коллектор: `fr:{EX}:BTCUSDT` HASH с полями `r`, `ft`, `ts`
- [ ] hist-ключи создаются для всех трёх типов
- [ ] spread_monitor: `len(bitget_dirs) == 8` (4 spot + 4 futures направления)

---

## 16. Итерация 3: End-to-end запуск

**Цель:** запустить полную систему и проверить корректность интеграции в дашборде.

```bash
# 1. Полный запуск на 5 минут
python3 run_all.py --no-dash --delay 30

# 2. Ожидаемые события в логах (grep по {ex_name}):
grep '{ex_name}' logs/collectors_*.log | head -30
# Должны быть: startup, symbols_loaded, connecting, connected,
#              subscribed/first_message, stats

# 3. Проверить что все 5 коллекторов появились в дашборде
# Строка MD должна содержать "{EX}-s" и "{EX}-f"
# Строка OB должна содержать "{EX}-s" и "{EX}-f"
# Строка FR должна содержать "{EX}-f"

# 4. Проверить сигналы spread_monitor
grep 'direction.*{ex_name}' logs/collectors_*.log | head -10

# 5. Количество Redis-ключей с новой биржей:
redis-cli -s /var/run/redis/redis.sock SCAN 0 MATCH "md:{EX}:*" COUNT 1000
redis-cli -s /var/run/redis/redis.sock SCAN 0 MATCH "ob:{EX}:*" COUNT 1000
redis-cli -s /var/run/redis/redis.sock SCAN 0 MATCH "fr:{EX}:*" COUNT 1000

# 6. Staleness monitor: новые ключи должны быть свежими
grep 'stale' logs/collectors_*.log | tail -5
# Ожидаемо: stale_count = 0 или минимальный

# 7. Snapshot CSV — проверить что при сигнале открывается файл
# с корректными source_ids для новой биржи
ls signals/snapshots/ 2>/dev/null | head -5

# 8. Проверить отсутствие ошибок импорта / CRASHED-статуса
grep 'collector_crashed\|CRASHED\|ERROR' logs/collectors_*.log | grep '{ex_name}'
# Ожидаемо: пустой вывод
```

**Критерий прохождения Итерации 3:**
- [ ] Все 5 коллекторов логируют `first_message` в течение 30 секунд
- [ ] Дашборд отображает новые коллекторы зелёным цветом
- [ ] `staleness_monitor` не сообщает о stale ключах новой биржи
- [ ] `spread_monitor` загружает все 8 новых направлений
- [ ] Нет `collector_crashed` для новых коллекторов
- [ ] Снапшоты (если генерируются) содержат корректные `source_id` (N и N+1)

---

## 17. Справочник протоколов бирж

### WS Ping-форматы

| Биржа | Формат пинга | Ответ | Интервал |
|-------|-------------|-------|----------|
| Binance | Встроен в websockets (`ping_interval=20`) | авто | 20 с |
| Bybit | `json.dumps({"op": "ping"})` | `{"op":"pong"}` | 20 с |
| OKX | `"ping"` (строка) | `"pong"` | 25 с |
| Gate | `json.dumps({"time":ts,"channel":"spot.ping"})` | JSON | 20 с |
| Bitget | `"ping"` (строка) | `"pong"` | 25 с |

### Формат подписки

| Биржа | Subscribe payload | Способ |
|-------|------------------|--------|
| Binance | URL: `?streams=btcusdt@bookTicker/...` | Encoded в URL |
| Bybit | `{"op":"subscribe","args":["orderbook.1.BTCUSDT",...]}` | JSON |
| OKX | `{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"},...]}`| JSON |
| Gate | `{"time":ts,"channel":"spot.book_ticker","event":"subscribe","payload":[...]}` | JSON |
| Bitget | `{"op":"subscribe","args":[{"instType":"SPOT","channel":"ticker","instId":"BTCUSDT"},...]}` | JSON |

### Символ в ответах WS

| Биржа | MD-путь к символу | OB-путь к символу | FR-путь |
|-------|------------------|------------------|---------|
| Binance | `data.s` | `stream.split("@")[0].upper()` | `item.s` |
| Bybit | `topic.split(".")[-1]` | `data.s` | `topic.split(".")[-1]` |
| OKX | `data[0].instId` (нативный) | `arg.instId` | `arg.instId` |
| Gate | `result.s` (нативный) | `result.s` | `result.contract` |
| Bitget | `arg.instId` | `arg.instId` | `arg.instId` |

### Нормализация символов

| Биржа | Нативный | Нормализованный | Функция |
|-------|---------|----------------|---------|
| Binance | `BTCUSDT` | `BTCUSDT` | — |
| Bybit | `BTCUSDT` | `BTCUSDT` | — |
| OKX | `BTC-USDT`, `BTC-USDT-SWAP` | `BTCUSDT` | `.replace("-","")[:-4]` если SWAP |
| Gate | `BTC_USDT` | `BTCUSDT` | `.replace("_","")` |
| Bitget | `BTCUSDT` | `BTCUSDT` | — |

### Rate limits WS (подписка)

| Биржа | Макс. подписок/соединение | Макс. msg/с | Рекомендуемый батч |
|-------|--------------------------|------------|-------------------|
| Binance | 1024 стримов в URL | — | 300/URL |
| Bybit spot | — | — | 10 аргументов/msg |
| Bybit futures | — | — | 200 аргументов/msg |
| OKX | 300 instId/соединение | — | 300/соединение |
| Gate | неограничено | — | 100/msg |
| Bitget | 1000 подписок/соединение | 10 msg/с | 100/msg |

---

## 18. Полный чеклист интеграции

### A. Файлы для создания (5 + 2 = 7)

- [ ] `dictionaries/{ex_name}/__init__.py`
- [ ] `dictionaries/{ex_name}/data/` (директория)
- [ ] `dictionaries/{ex_name}/{ex_name}_pairs.py`
- [ ] `dictionaries/{ex_name}/{ex_name}_ws.py`
- [ ] `collectors/{ex_name}_spot.py`
- [ ] `collectors/{ex_name}_futures.py`
- [ ] `collectors/ob_{ex_name}_spot.py`
- [ ] `collectors/ob_{ex_name}_futures.py`
- [ ] `collectors/fr_{ex_name}_futures.py`

### B. Файлы для обновления (7)

- [ ] `dictionaries/main.py` — COMBINATIONS (+8), SUBSCRIBE_MAP (+2), fetch/validate/report
- [ ] `collectors/source_ids.json` — +2 записи
- [ ] `collectors/spread_monitor.py` — EXCHANGE_CODE +1
- [ ] `run_all.py` — COLLECTORS +5, EX_SHORT +1, _N_MD/_N_OB/_N_FR +1/+1/+1
- [ ] `collectors/hist_writer.py` — sources[] +2
- [ ] `collectors/ob_hist_writer.py` — sources[] +2
- [ ] `collectors/fr_hist_writer.py` — sources[] +1

### C. Генерация данных

- [ ] `python3 dictionaries/main.py` → combination/ + subscribe/ + symbol_ids.json

### D. Итерация 1 (синтаксис)

- [ ] Все 9 новых файлов: `py_compile` без ошибок
- [ ] Импорты hist_writers: OK
- [ ] source_ids: оба ключа присутствуют
- [ ] spread_monitor EXCHANGE_CODE: присутствует
- [ ] run_all COLLECTORS: все 5 имён присутствуют

### E. Итерация 2 (логика и Redis)

- [ ] REST fetch: ненулевые Spot + Futures
- [ ] WS validate: >50% пар активны
- [ ] combination-файлы: 8 новых, непустые
- [ ] MD standalone: `md:{EX}:s:BTCUSDT` создан с b/a/t
- [ ] OB standalone: `ob:{EX}:s:BTCUSDT` с ровно 41 полем
- [ ] FR standalone: `fr:{EX}:BTCUSDT` с r/ft/ts
- [ ] hist-ключи: созданы для md/ob/fr
- [ ] spread_monitor load_directions: 8 новых направлений

### F. Итерация 3 (end-to-end)

- [ ] Все 5 коллекторов: `first_message` < 30 сек
- [ ] Дашборд: новые коллекторы зелёные
- [ ] staleness_monitor: 0 stale для новых ключей
- [ ] spread_monitor: 8 направлений загружены
- [ ] Нет `collector_crashed`
- [ ] Снапшоты (если есть): корректные source_id

### G. Документация

- [ ] `dictionaries/README.md` — добавить раздел биржи
- [ ] `docs/EXCHANGE_INTEGRATION_PROTOCOL.md` — обновить таблицы справочника
- [ ] CLAUDE.md (если нужно) — обновить список коллекторов

---

*Документ отражает архитектуру Bali 3.0 на момент интеграции Bitget (биржа №5).*
*При добавлении каждой следующей биржи следовать этому же протоколу.*
