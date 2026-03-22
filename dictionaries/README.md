# dictionaries

Модуль сбора и генерации списков торговых пар для системы Bali 3.0.
Запускается один раз перед стартом коллекторов — результат записывается в `combination/`, `subscribe/` и `collectors/symbol_ids.json`.

## Содержание

- [Быстрый старт](#быстрый-старт)
- [Архитектура выполнения](#архитектура-выполнения)
- [Структура файлов](#структура-файлов)
- [Нормализация символов](#нормализация-символов)
- [Биржевые модули](#биржевые-модули)
  - [Binance](#binance)
  - [Bybit](#bybit)
  - [OKX](#okx)
  - [Gate.io](#gateio)
  - [Bitget](#bitget)
- [Выходные данные](#выходные-данные)
  - [Пересечения — combination/](#пересечения--combination)
  - [Файлы подписки — subscribe/](#файлы-подписки--subscribe)
  - [Словарь символов — symbol_ids.json](#словарь-символов--symbol_idsjson)
- [Публичный API модулей](#публичный-api-модулей)
- [Промежуточные данные](#промежуточные-данные)
- [Зависимости](#зависимости)
- [Диагностика](#диагностика)

---

## Быстрый старт

```bash
cd /root/bali3.0/dictionaries
python3 main.py
```

**Ожидаемое время:** ~65–70 секунд
- Фаза 1 (REST): ~3–5 с
- Фаза 2 (WS-валидация): ~60 с
- Фазы 3–5: <1 с

**Запускать перед** каждым стартом коллекторов (`run_all.py`), чтобы списки символов отражали текущее состояние бирж.

---

## Архитектура выполнения

```
main.py
  │
  ├── Фаза 1 — Параллельный REST (ThreadPoolExecutor, 5 воркеров)
  │     ├── binance_pairs.fetch_pairs()   → binance/data/
  │     ├── bybit_pairs.fetch_pairs()     → bybit/data/
  │     ├── okx_pairs.fetch_pairs()       → okx/data/
  │     ├── gate_pairs.fetch_pairs()      → gate/data/
  │     └── bitget_pairs.fetch_pairs()    → bitget/data/
  │
  ├── Фаза 2 — Параллельная WS-валидация (asyncio.gather, ~60 сек)
  │     ├── binance_ws._run()   → binance/data/*_active.txt
  │     ├── bybit_ws._run()     → bybit/data/*_active.txt
  │     ├── okx_ws._run()       → okx/data/*_active.txt
  │     ├── gate_ws._run()      → gate/data/*_active.txt
  │     └── bitget_ws._run()    → bitget/data/*_active.txt
  │
  ├── Фаза 3 — Пересечения (combination/)
  │     └── 20 файлов: {spot_A} ∩ {futures_B}
  │
  ├── Фаза 4 — Файлы подписки (subscribe/)
  │     └── 10 файлов: объединение из combination по рынку
  │
  ├── Symbol IDs
  │     └── collectors/symbol_ids.json: все уникальные символы → int ID
  │
  └── Фаза 5 — Отчёт в stdout
```

### Почему параллельно

**Фаза 1** использует `ThreadPoolExecutor`, а не `asyncio`, потому что `urllib.request` — блокирующий I/O. 5 воркеров опрашивают биржи одновременно вместо последовательных ~5×1с = ~5с.

**Фаза 2** использует `asyncio.gather` — все 5 бирж стартуют одновременно в одном event loop. Суммарное время ~60 с (одно окно наблюдения) вместо ~5×60 с = ~300 с при последовательном запуске.

### Критерий «активная пара»

Пара считается активной, если по ней пришёл **хотя бы один** ответ WebSocket за 60 секунд наблюдения. Пары без ответа — неликвидные или неактивные — исключаются из всех выходных файлов.

---

## Структура файлов

```
dictionaries/
│
├── main.py                          # Оркестратор — запускает все фазы
│
├── binance/
│   ├── binance_pairs.py             # REST: GET exchangeInfo, фильтр USDT/USDC
│   ├── binance_ws.py                # WS: bookTicker, чанки по 300
│   ├── __init__.py
│   └── data/
│       ├── binance_spot.txt         # Все спот-пары от REST
│       ├── binance_spot_active.txt  # Прошедшие WS-валидацию
│       ├── binance_futures.txt
│       └── binance_futures_active.txt
│
├── bybit/
│   ├── bybit_pairs.py               # REST: cursor-пагинация, category=spot/linear
│   ├── bybit_ws.py                  # WS: orderbook.1, батчи 10/200, {"op":"ping"}
│   ├── __init__.py
│   └── data/
│       ├── bybit_spot.txt
│       ├── bybit_spot_active.txt
│       ├── bybit_futures.txt
│       └── bybit_futures_active.txt
│
├── okx/
│   ├── okx_pairs.py                 # REST: SPOT + SWAP, нормализация BTC-USDT→BTCUSDT
│   ├── okx_ws.py                    # WS: tickers, 300 instId/соединение, пинг "ping"
│   ├── __init__.py
│   └── data/
│       ├── okx_spot_native.txt      # Нативный формат: BTC-USDT (для WS)
│       ├── okx_spot.txt             # Нормализованный: BTCUSDT (для пересечений)
│       ├── okx_futures_native.txt   # BTC-USDT-SWAP
│       ├── okx_futures.txt          # BTCUSDT
│       ├── okx_spot_active.txt
│       └── okx_futures_active.txt
│
├── gate/
│   ├── gate_pairs.py                # REST: spot/currency_pairs + futures/usdt/contracts
│   ├── gate_ws.py                   # WS: spot/futures.book_ticker, батчи 100
│   ├── __init__.py
│   └── data/
│       ├── gate_spot_native.txt     # BTC_USDT
│       ├── gate_spot.txt            # BTCUSDT
│       ├── gate_futures_native.txt  # BTC_USDT
│       ├── gate_futures.txt         # BTCUSDT
│       ├── gate_spot_active.txt
│       └── gate_futures_active.txt
│
├── bitget/
│   ├── bitget_pairs.py              # REST: spot/public/symbols + mix/market/contracts
│   ├── bitget_ws.py                 # WS: books1, батчи 100, instType SPOT/USDT-FUTURES
│   ├── __init__.py
│   └── data/
│       ├── bitget_spot.txt          # BTCUSDT (нативный = нормализованный)
│       ├── bitget_spot_active.txt
│       ├── bitget_futures.txt
│       └── bitget_futures_active.txt
│
├── combination/                     # Генерируется main.py — пересечения активных пар
│   ├── binance_spot_bybit_futures.txt    # ~349 символов
│   ├── bybit_spot_binance_futures.txt    # ~181
│   ├── binance_spot_okx_futures.txt      # ~201
│   ├── okx_spot_binance_futures.txt      # ~173
│   ├── bybit_spot_okx_futures.txt        # ~189
│   ├── okx_spot_bybit_futures.txt        # ~229
│   ├── binance_spot_gate_futures.txt     # ~364
│   ├── gate_spot_binance_futures.txt     # ~270
│   ├── bybit_spot_gate_futures.txt       # ~316
│   ├── gate_spot_bybit_futures.txt       # ~494
│   ├── okx_spot_gate_futures.txt         # ~245
│   ├── gate_spot_okx_futures.txt         # ~245
│   ├── binance_spot_bitget_futures.txt   # генерируется после запуска
│   ├── bitget_spot_binance_futures.txt
│   ├── bybit_spot_bitget_futures.txt
│   ├── bitget_spot_bybit_futures.txt
│   ├── okx_spot_bitget_futures.txt
│   ├── bitget_spot_okx_futures.txt
│   ├── gate_spot_bitget_futures.txt
│   └── bitget_spot_gate_futures.txt
│
└── subscribe/                       # Генерируется main.py — читают коллекторы
    ├── binance/
    │   ├── binance_spot.txt              # ~383 символов (∪ по 3 combination-файлам)
    │   └── binance_futures.txt           # ~279
    ├── bybit/
    │   ├── bybit_spot.txt                # ~341
    │   └── bybit_futures.txt             # ~502
    ├── okx/
    │   ├── okx_spot.txt                  # ~278
    │   └── okx_futures.txt               # ~247
    ├── gate/
    │   ├── gate_spot.txt                 # ~540
    │   └── gate_futures.txt              # ~465
    └── bitget/
        ├── bitget_spot.txt               # генерируется после запуска
        └── bitget_futures.txt
```

> Числа символов — актуальны на момент генерации. Запустите `wc -l subscribe/*/*.txt` чтобы увидеть текущие значения.

---

## Нормализация символов

Все символы приводятся к единому формату **`BTCUSDT`** для построения пересечений между биржами.

| Биржа | Нативный формат | Нормализованный | Правило |
|-------|----------------|-----------------|---------|
| Binance | `BTCUSDT` | `BTCUSDT` | без изменений |
| Bybit | `BTCUSDT` | `BTCUSDT` | без изменений |
| OKX Spot | `BTC-USDT` | `BTCUSDT` | убрать `-` |
| OKX Futures | `BTC-USDT-SWAP` | `BTCUSDT` | убрать `-SWAP`, затем `-` |
| Gate.io | `BTC_USDT` | `BTCUSDT` | убрать `_` |
| Bitget | `BTCUSDT` | `BTCUSDT` | без изменений |

**OKX и Gate.io** хранят оба формата:
- **Нативный** (`okx_spot_native.txt`, `gate_spot_native.txt`) — используется для WS-подписки, т.к. биржа принимает только свой формат
- **Нормализованный** (`okx_spot.txt`, `gate_spot.txt`) — используется для построения пересечений с Binance/Bybit

---

## Биржевые модули

### Binance

**`binance_pairs.py`**

Обращается к двум эндпоинтам:
- Spot: `https://api.binance.com/api/v3/exchangeInfo`
- Futures (USD-M): `https://fapi.binance.com/fapi/v1/exchangeInfo`

Фильтр: `status == "TRADING"` И `quoteAsset ∈ {USDT, USDC}`.

---

**`binance_ws.py`**

| Параметр | Значение |
|----------|----------|
| Spot URL | `wss://stream.binance.com:9443/stream?streams=...` |
| Futures URL | `wss://fstream.binance.com/stream?streams=...` |
| Канал | `{symbol}@bookTicker` (лучшая цена bid/ask) |
| Чанк | 300 символов на одно соединение |
| Пинг | Стандартный (встроенный в websockets, `ping_interval=20`) |

URL строится как мультипоток: `?streams=btcusdt@bookTicker/ethusdt@bookTicker/...`

При получении сообщения извлекает `data.s` (символ) и добавляет в `received: Set[str]`.
После 60 секунд фильтрует исходный список — оставляет только символы, попавшие в `received`.

---

### Bybit

**`bybit_pairs.py`**

Обращается к API с поддержкой **cursor-пагинации** (Bybit возвращает до 1000 инструментов за запрос, при наличии `nextPageCursor` делает следующий запрос):

- Spot: `https://api.bybit.com/v5/market/instruments-info?category=spot&limit=1000`
- Futures: `?category=linear&limit=1000`

Фильтр: `status == "Trading"` И `quoteCoin ∈ {USDT, USDC}`.

---

**`bybit_ws.py`**

| Параметр | Spot | Futures |
|----------|------|---------|
| URL | `wss://stream.bybit.com/v5/public/spot` | `wss://stream.bybit.com/v5/public/linear` |
| Канал | `orderbook.1.{SYMBOL}` | `orderbook.1.{SYMBOL}` |
| Батч подписки | 10 символов/запрос | 200 символов/запрос |
| Пинг | `{"op": "ping"}` каждые 20 с | — |
| Ограничение payload | 20 000 символов/запрос | — |

Bybit **не поддерживает** мультипоток в URL — подписка отправляется через `{"op":"subscribe","args":[...]}`.
Из-за ограничений frame-размера: spot батчи по 10, futures по 200.

Символ извлекается из `topic` (`orderbook.1.BTCUSDT`) или из `data.s`.

Отдельная корутина `_ping_loop` отправляет кастомный пинг каждые 20 с, пока не выставлен `stop_evt`.

---

### OKX

**`okx_pairs.py`**

- Spot: `https://www.okx.com/api/v5/public/instruments?instType=SPOT`
- Futures (SWAP): `?instType=SWAP`

Фильтр Spot: `state == "live"` И `quoteCcy ∈ {USDT, USDC}`.
Фильтр Swap: `state == "live"` И `settleCcy ∈ {USDT, USDC}`.

Сохраняет **оба формата** символов в `data/` и возвращает нормализованные.

Функция `load_native()` — читает нативные символы из уже сохранённых файлов (вызывается из `main.py` перед Фазой 2).

---

**`okx_ws.py`**

| Параметр | Значение |
|----------|----------|
| URL | `wss://ws.okx.com:8443/ws/v5/public` |
| Подписка | `{"op":"subscribe","args":[{"channel":"tickers","instId":"BTC-USDT"},...]}`|
| Чанк | 300 `instId` на одно соединение |
| Пинг | Текстовая строка `"ping"`, ответ `"pong"` |
| Задержка между коннектами | 0.15 с (чтобы не перегружать сервер) |

Каждое WS-соединение — один `asyncio` таск со своим `_ping_loop`.
OKX возвращает `data[].instId` в нативном формате (`BTC-USDT`), поэтому после получения применяется `_normalize()`.

`responded` — общий `Set[str]` для всех чанков одного рынка, доступ из разных тасков безопасен (asyncio однопоточный).

---

### Gate.io

**`gate_pairs.py`**

- Spot: `https://api.gateio.ws/api/v4/spot/currency_pairs`
- Futures: `https://api.gateio.ws/api/v4/futures/usdt/contracts`

Фильтр Spot: `trade_status == "tradable"` И `quote ∈ {USDT, USDC}`.
Фильтр Futures: `in_delisting == false` И последняя часть `name` после `_` ∈ `{USDT, USDC}`.

---

**`gate_ws.py`**

| Параметр | Spot | Futures |
|----------|------|---------|
| URL | `wss://api.gateio.ws/ws/v4/` | `wss://fx-ws.gateio.ws/v4/ws/usdt` |
| Канал | `spot.book_ticker` | `futures.book_ticker` |
| Подписка | `{"time":ts,"channel":"spot.book_ticker","event":"subscribe","payload":[...]}` | — |
| Батч | 100 символов/сообщение | 100 символов/сообщение |
| Пинг | `{"time":ts,"channel":"spot.ping"}` | `{"time":ts,"channel":"futures.ping"}` |

Gate.io использует одно WS-соединение на рынок (не несколько чанков), но разбивает подписку на батчи по 100 символов, отправляя несколько `subscribe`-сообщений последовательно.

Символ извлекается из `result.s` (нативный формат `BTC_USDT`), нормализуется в `BTCUSDT`.

---

### Bitget

**`bitget_pairs.py`**

- Spot: `https://api.bitget.com/api/v2/spot/public/symbols`
- Futures (USDT-M): `https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES`

Фильтр Spot: `status == "online"` И `quoteCoin ∈ {USDT, USDC}`.
Фильтр Futures: `symbolStatus == "normal"` И `quoteCoin ∈ {USDT, USDC}`.

Bitget использует нормализованный формат `BTCUSDT` нативно — как Binance и Bybit.
Отдельные native/normalized файлы не нужны.

---

**`bitget_ws.py`**

| Параметр | Spot | Futures |
|----------|------|---------|
| URL | `wss://ws.bitget.com/v2/ws/public` | — (тот же) |
| `instType` | `SPOT` | `USDT-FUTURES` |
| Канал | `books1` | `books1` |
| Подписка | `{"op":"subscribe","args":[{"instType":"SPOT","channel":"books1","instId":"BTCUSDT"},...]}` | — |
| Батч | 100 аргументов/сообщение | 100 аргументов/сообщение |
| Пинг | Строка `"ping"` каждые 25 с, ответ `"pong"` | — |
| Задержка между батчами | 0.1 с (≤ 10 msg/s) | — |

Символ извлекается из `arg.instId` входящего сообщения (только при `action == "snapshot"` или `"update"`).
Один WS-коннект на рынок (spot + futures параллельно через asyncio.gather).

---

## Выходные данные

### Пересечения — combination/

12 файлов, каждый — **отсортированное пересечение** двух множеств активных символов двух рынков:

```
{spot_exchange}_spot_{futures_exchange}_futures.txt  =  spot_active ∩ futures_active
```

Полная таблица:

| Файл | Смысл | ~Символов |
|------|-------|-----------|
| `binance_spot_bybit_futures.txt` | binance_spot ∩ bybit_futures | ~349 |
| `bybit_spot_binance_futures.txt` | bybit_spot ∩ binance_futures | ~181 |
| `binance_spot_okx_futures.txt` | binance_spot ∩ okx_futures | ~201 |
| `okx_spot_binance_futures.txt` | okx_spot ∩ binance_futures | ~173 |
| `bybit_spot_okx_futures.txt` | bybit_spot ∩ okx_futures | ~189 |
| `okx_spot_bybit_futures.txt` | okx_spot ∩ bybit_futures | ~229 |
| `binance_spot_gate_futures.txt` | binance_spot ∩ gate_futures | ~364 |
| `gate_spot_binance_futures.txt` | gate_spot ∩ binance_futures | ~270 |
| `bybit_spot_gate_futures.txt` | bybit_spot ∩ gate_futures | ~316 |
| `gate_spot_bybit_futures.txt` | gate_spot ∩ bybit_futures | ~494 |
| `okx_spot_gate_futures.txt` | okx_spot ∩ gate_futures | ~245 |
| `gate_spot_okx_futures.txt` | gate_spot ∩ okx_futures | ~245 |
| `binance_spot_bitget_futures.txt` | binance_spot ∩ bitget_futures | — |
| `bitget_spot_binance_futures.txt` | bitget_spot ∩ binance_futures | — |
| `bybit_spot_bitget_futures.txt` | bybit_spot ∩ bitget_futures | — |
| `bitget_spot_bybit_futures.txt` | bitget_spot ∩ bybit_futures | — |
| `okx_spot_bitget_futures.txt` | okx_spot ∩ bitget_futures | — |
| `bitget_spot_okx_futures.txt` | bitget_spot ∩ okx_futures | — |
| `gate_spot_bitget_futures.txt` | gate_spot ∩ bitget_futures | — |
| `bitget_spot_gate_futures.txt` | bitget_spot ∩ gate_futures | — |

Эти файлы читает `spread_monitor.py` для определения направлений мониторинга.

---

### Файлы подписки — subscribe/

8 файлов — по одному на каждый рынок каждой биржи.

Формируются путём **объединения** всех combination-файлов, в названии которых присутствует ключевое слово данного рынка:

```
subscribe/binance/binance_spot.txt  =  ∪ { binance_spot_bybit_futures,
                                           binance_spot_okx_futures,
                                           binance_spot_gate_futures }
```

Полная таблица:

| Файл | Источники (combination) | ~Символов |
|------|------------------------|-----------|
| `subscribe/binance/binance_spot.txt` | bn_s_bb_f + bn_s_ok_f + bn_s_gt_f | ~383 |
| `subscribe/binance/binance_futures.txt` | bb_s_bn_f + ok_s_bn_f + gt_s_bn_f | ~279 |
| `subscribe/bybit/bybit_spot.txt` | bb_s_bn_f + bb_s_ok_f + bb_s_gt_f | ~341 |
| `subscribe/bybit/bybit_futures.txt` | bn_s_bb_f + ok_s_bb_f + gt_s_bb_f | ~502 |
| `subscribe/okx/okx_spot.txt` | ok_s_bn_f + ok_s_bb_f + ok_s_gt_f | ~278 |
| `subscribe/okx/okx_futures.txt` | bn_s_ok_f + bb_s_ok_f + gt_s_ok_f | ~247 |
| `subscribe/gate/gate_spot.txt` | gt_s_bn_f + gt_s_bb_f + gt_s_ok_f | ~540 |
| `subscribe/gate/gate_futures.txt` | bn_s_gt_f + bb_s_gt_f + ok_s_gt_f | ~465 |
| `subscribe/bitget/bitget_spot.txt` | bg_s_bn_f + bg_s_bb_f + bg_s_ok_f + bg_s_gt_f | — |
| `subscribe/bitget/bitget_futures.txt` | bn_s_bg_f + bb_s_bg_f + ok_s_bg_f + gt_s_bg_f | — |

Дубли убираются через `set()`. Файлы сортированы.

**Формат файла** — один символ на строку, нормализованный (`BTCUSDT`). Для OKX и Gate.io коллекторы самостоятельно восстанавливают нативный формат при подписке (`BTCUSDT` → `BTC-USDT`, `BTC_USDT`).

```bash
# Проверить количество пар в subscribe-файлах
wc -l /root/bali3.0/dictionaries/subscribe/*/*.txt
```

---

### Словарь символов — symbol_ids.json

Файл `collectors/symbol_ids.json` генерируется автоматически после Фазы 4.

**Содержимое:** все уникальные символы из всех 12 combination-файлов, отсортированные по алфавиту, с присвоенным целочисленным ID (индекс в отсортированном списке).

```json
{
  "0GUSDT": 0,
  "1000CATUSDT": 1,
  "1INCHUSDT": 2,
  "AAVEUSDT": 5,
  "ADAUSDT": 6,
  ...
  "ZTXUSDT": 593
}
```

**Текущий размер:** ~594 уникальных символа.

**Использование:** `spread_monitor.py` и `hist_writer.py` используют этот файл для записи `symbol_id` (integer) вместо строки символа в столбце `symbol_id` снапшот-CSV. Уменьшает объём файлов и упрощает последующий анализ.

```python
# Загрузка в collectors/hist_writer.py
SYMBOL_IDS: dict[str, int] = json.loads(
    Path("symbol_ids.json").read_text()
)
# Применение
symbol_id = SYMBOL_IDS.get("BTCUSDT", -1)  # → 42 (пример)
```

> `-1` означает символ, отсутствующий в словаре (не встречается при нормальной работе).

**Обновление:** пересоздаётся при каждом запуске `main.py`. ID символов **не стабильны** между запусками — при изменении набора пар индексы могут сдвигаться. Для связки снапшотов с символами всегда используй актуальный `symbol_ids.json`.

---

## Публичный API модулей

### `main.py`

```python
main() -> None
```
Запускает полный цикл. Печатает отчёт по завершении.

---

### `binance/binance_pairs.py`

```python
fetch_pairs() -> tuple[list[str], list[str]]
```
Возвращает `(spot_pairs, futures_pairs)` — нормализованные символы.
Побочный эффект: сохраняет файлы в `binance/data/`.

---

### `binance/binance_ws.py`

```python
validate_pairs(spot, futures, duration=60) -> tuple[list[str], list[str]]
```
Принимает полные списки пар, валидирует через WS за `duration` секунд.
Возвращает `(active_spot, active_futures)`.
Побочный эффект: сохраняет `*_active.txt`.

Внутренняя `_run(spot, futures, duration)` — async-версия, используется `main.py` через `asyncio.gather`.

---

### `bybit/bybit_pairs.py`

```python
fetch_pairs() -> tuple[list[str], list[str]]
```
Аналогично Binance. Поддерживает cursor-пагинацию автоматически.

---

### `bybit/bybit_ws.py`

```python
validate_pairs(spot, futures, duration=60) -> tuple[list[str], list[str]]
```
Аналогично Binance.

---

### `okx/okx_pairs.py`

```python
fetch_pairs() -> tuple[list[str], list[str]]
# Возвращает нормализованные (spot_norm, futures_norm)
# Сохраняет и нативные файлы okx_*_native.txt

load_native() -> tuple[list[str], list[str]]
# Читает нативные символы из уже сохранённых файлов
# Возвращает (spot_native, futures_native) — BTC-USDT формат
```

---

### `okx/okx_ws.py`

```python
validate_pairs(spot_native, futures_native, spot_norm, futures_norm, duration=60)
    -> tuple[list[str], list[str]]
# Принимает нативные для WS-подписки и нормализованные для фильтрации результата
# Возвращает (active_spot_norm, active_futures_norm)
```

---

### `gate/gate_pairs.py`

```python
fetch_pairs() -> tuple[list[str], list[str]]
load_native() -> tuple[list[str], list[str]]
```
Аналогично OKX (нативный формат `BTC_USDT`).

---

### `gate/gate_ws.py`

```python
validate_pairs(spot_native, futures_native, spot_norm, futures_norm, duration=60)
    -> tuple[list[str], list[str]]
```
Аналогично OKX.

---

### `bitget/bitget_pairs.py`

```python
fetch_pairs() -> tuple[list[str], list[str]]
```
Возвращает `(spot_pairs, futures_pairs)` — уже нормализованные символы `BTCUSDT`.
Нормализация не требуется. Сохраняет файлы в `bitget/data/`.

---

### `bitget/bitget_ws.py`

```python
validate_pairs(spot, futures, duration=60) -> tuple[list[str], list[str]]
```
Аналогично Binance/Bybit — принимает нормализованные символы напрямую.
Возвращает `(active_spot, active_futures)`.

---

## Промежуточные данные

Каждый модуль сохраняет все промежуточные результаты в `{exchange}/data/`. Эти файлы не читаются коллекторами напрямую — они нужны для отладки и для `load_native()`.

| Файл | Когда обновляется | Содержимое |
|------|------------------|-----------|
| `binance/data/binance_spot.txt` | Фаза 1 | Все пары от REST API |
| `binance/data/binance_spot_active.txt` | Фаза 2 | Прошедшие WS-валидацию |
| `okx/data/okx_spot_native.txt` | Фаза 1 | Нативный формат `BTC-USDT` |
| `okx/data/okx_spot.txt` | Фаза 1 | Нормализованный `BTCUSDT` |
| `okx/data/okx_spot_active.txt` | Фаза 2 | Активные нормализованные |
| `gate/data/gate_futures_native.txt` | Фаза 1 | Нативный формат `BTC_USDT` |

---

## Зависимости

```bash
pip install websockets
```

Стандартная библиотека: `asyncio`, `concurrent.futures`, `json`, `urllib.request`, `pathlib`, `time`.

Python **3.10+** (используется `dict[str, int]` без `from __future__ import annotations`).

---

## Диагностика

### Проверить результаты последнего запуска

```bash
# Количество пар в subscribe-файлах
wc -l /root/bali3.0/dictionaries/subscribe/*/*.txt

# Количество пар в combination-файлах
wc -l /root/bali3.0/dictionaries/combination/*.txt

# Количество символов в словаре
python3 -c "import json; d=json.load(open('collectors/symbol_ids.json')); print(len(d), 'symbols')"
```

### Запустить только REST-фазу для одной биржи

```bash
cd /root/bali3.0/dictionaries
python3 binance/binance_pairs.py    # Spot: N пар, Futures: N пар
python3 bybit/bybit_pairs.py
python3 okx/okx_pairs.py
python3 gate/gate_pairs.py
```

### Запустить только WS-валидацию для одной биржи

```bash
cd /root/bali3.0/dictionaries
python3 binance/binance_ws.py   # 60 сек, покажет Spot: N/M, Futures: N/M
python3 bybit/bybit_ws.py
python3 okx/okx_ws.py
python3 gate/gate_ws.py
```

### Типичные проблемы

| Симптом | Причина | Решение |
|---------|---------|---------|
| `FileNotFoundError: combination/*.txt` | `main.py` ещё не запускался | Запустить `python3 main.py` |
| `0 symbols in symbol_ids.json` | Пустые combination-файлы | Проверить сетевой доступ к биржам, перезапустить |
| WS-валидация даёт 0 активных пар | Нет соединения с WS-сервером биржи | Проверить доступность `wss://stream.binance.com` и др. |
| Коллектор не запускается (пустой subscribe) | Устаревшие subscribe-файлы | Перезапустить `main.py`, затем коллекторы |
| OKX возвращает меньше пар чем ожидается | Часть пар в статусе `prelisted`, а не `live` | Норма — `state=live` отфильтровывает неторгуемые |
| Bitget futures возвращает 0 пар | Неверный `productType` или API изменился | Проверить `python3 bitget/bitget_pairs.py` напрямую |
| Bitget WS не получает ответы | Неверный `instType` или `channel` | Убедиться что используется `books1`, а не `ticker` |
