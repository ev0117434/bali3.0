#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bitget_pairs.py - Получение всех торговых пар Bitget через REST API.

Загружает пары с Spot и Futures (USDT-M) рынков.
Фильтр Spot:    quoteCoin = USDT или USDC, status = "online"
Фильтр Futures: quoteCoin = USDT,          symbolStatus = "normal"

Bitget использует нативный нормализованный формат BTCUSDT (как Binance/Bybit),
нормализация не требуется.
Сохраняет результаты в bitget/data/.
"""

import json
from pathlib import Path
from typing import List, Tuple
from urllib.request import Request, urlopen

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

SPOT_URL         = "https://api.bitget.com/api/v2/spot/public/symbols"
FUTURES_URL      = "https://api.bitget.com/api/v2/mix/market/contracts?productType=USDT-FUTURES"

SPOT_FILE        = DATA_DIR / "bitget_spot.txt"
FUTURES_FILE     = DATA_DIR / "bitget_futures.txt"

QUOTE_ASSETS     = ("USDT", "USDC")


def _fetch_json(url: str) -> dict:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _extract_spot(data: list) -> List[str]:
    pairs = []
    for s in data:
        if s.get("status") != "online":
            continue
        if s.get("quoteCoin") not in QUOTE_ASSETS:
            continue
        pairs.append(s["symbol"])
    return sorted(pairs)


def _extract_futures(data: list) -> List[str]:
    pairs = []
    for s in data:
        if s.get("symbolStatus") != "normal":
            continue
        if s.get("quoteCoin") not in QUOTE_ASSETS:
            continue
        pairs.append(s["symbol"])
    return sorted(pairs)


def _save(path: Path, pairs: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(pairs) + "\n", encoding="utf-8")


def fetch_pairs() -> Tuple[List[str], List[str]]:
    """Загружает и возвращает (spot_pairs, futures_pairs) для Bitget."""
    spot_data = _fetch_json(SPOT_URL)
    spot = _extract_spot(spot_data.get("data", []))

    fut_data = _fetch_json(FUTURES_URL)
    futures = _extract_futures(fut_data.get("data", []))

    _save(SPOT_FILE,    spot)
    _save(FUTURES_FILE, futures)

    return spot, futures


if __name__ == "__main__":
    spot, futures = fetch_pairs()
    print(f"Bitget Spot:    {len(spot)} пар  -> {SPOT_FILE}")
    print(f"Bitget Futures: {len(futures)} пар  -> {FUTURES_FILE}")
