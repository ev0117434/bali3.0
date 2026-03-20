# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**Bali 3.0** — система генерации словарей торговых пар для мониторинга крипто-спредов. Собирает активные торговые пары с 4 бирж (Binance, Bybit, OKX, Gate.io) через REST API + WebSocket-валидацию, затем строит пересечения для арбитражных стратегий.

## Commands

```bash
# Run the dictionary generator
cd /root/bali3.0/dictionaries
python3 main.py

# Install the only external dependency
pip install websockets

# Redis setup and health check
sudo bash /root/bali3.0/setup_redis.sh          # Install & configure
sudo bash /root/bali3.0/setup_redis.sh --check  # Health check only
```

**Expected runtime:** ~65-70 seconds total (Phase 1: ~3-5s REST, Phase 2: ~60s WebSocket, Phases 3-5: <1s).

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

### Redis

Configured via `setup_redis.sh`: Unix socket only (`/var/run/redis/redis.sock`), no persistence (`volatile-lru` eviction), latency-optimized. Used by downstream collectors, not by `dictionaries/main.py` itself.
