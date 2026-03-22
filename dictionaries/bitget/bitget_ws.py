#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
bitget_ws.py - Валидация торговых пар Bitget через WebSocket.

Подключается к Bitget WS V2 Public API (Spot и USDT-Futures).
Канал: books1 (лучший bid/ask, обновления каждые 20ms).
В течение DURATION_SECONDS фиксирует пары, по которым пришёл хотя бы 1 ответ.
Сохраняет активные пары в bitget/data/.

Особенности протокола Bitget:
- Один WS эндпоинт для spot и futures: wss://ws.bitget.com/v2/ws/public
- Подписка: {"op":"subscribe","args":[{"instType":"SPOT","channel":"books1","instId":"BTCUSDT"},...]}
- Spot instType: "SPOT" / Futures instType: "USDT-FUTURES"
- Символ в ответе: msg["arg"]["instId"]
- Пинг: строка "ping", ответ "pong"
- Батчи по BATCH_SIZE аргументов на одно subscribe-сообщение
- Ограничения: max 1000 подписок/соединение, max 10 сообщений/сек
"""

import asyncio
import json
import time
from pathlib import Path
from typing import List, Set, Tuple

import websockets

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"

WS_URL            = "wss://ws.bitget.com/v2/ws/public"

INST_TYPE_SPOT    = "SPOT"
INST_TYPE_FUTURES = "USDT-FUTURES"

BATCH_SIZE       = 100    # аргументов на одно subscribe-сообщение
DURATION_SECONDS = 60     # окно проверки (секунд)
PING_INTERVAL    = 25     # интервал пинга (сек)
SUBSCRIBE_DELAY  = 0.1    # пауза между subscribe-сообщениями (≤10 msg/s)

SPOT_ACTIVE_FILE    = DATA_DIR / "bitget_spot_active.txt"
FUTURES_ACTIVE_FILE = DATA_DIR / "bitget_futures_active.txt"


def _chunk(items: List[str], n: int) -> List[List[str]]:
    return [items[i : i + n] for i in range(0, len(items), n)]


async def _ping_loop(ws, stop_evt: asyncio.Event) -> None:
    """Кастомный пинг Bitget: строка "ping" каждые PING_INTERVAL секунд."""
    try:
        while not stop_evt.is_set():
            await asyncio.sleep(PING_INTERVAL)
            if stop_evt.is_set():
                break
            await ws.send("ping")
    except Exception:
        pass


async def _validate_market(
    inst_type: str,
    symbols: List[str],
    duration: int,
) -> Set[str]:
    """
    Одно WS-соединение для рынка.
    Подписывается батчами на books1.{inst_type}.{symbol}.
    Возвращает множество символов, по которым пришёл ответ.
    """
    responded: Set[str] = set()
    stop_evt = asyncio.Event()

    async def _recv_loop(ws):
        try:
            async for raw in ws:
                if raw == "pong":
                    continue
                try:
                    msg = json.loads(raw)
                except Exception:
                    continue
                # Только сообщения с данными (snapshot / update)
                action = msg.get("action")
                if action not in ("snapshot", "update"):
                    continue
                arg = msg.get("arg", {})
                inst_id = arg.get("instId")
                if inst_id:
                    responded.add(inst_id.upper())
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

    try:
        async with websockets.connect(
            WS_URL,
            ping_interval=None,   # используем кастомный пинг
            max_queue=2048,
            close_timeout=3,
        ) as ws:
            # Подписка батчами
            for batch in _chunk(symbols, BATCH_SIZE):
                args = [
                    {"instType": inst_type, "channel": "books1", "instId": s}
                    for s in batch
                ]
                await ws.send(json.dumps({"op": "subscribe", "args": args}))
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


async def _run(
    spot: List[str],
    futures: List[str],
    duration: int,
) -> Tuple[List[str], List[str]]:
    spot_task = asyncio.create_task(
        _validate_market(INST_TYPE_SPOT, spot, duration)
    )
    futures_task = asyncio.create_task(
        _validate_market(INST_TYPE_FUTURES, futures, duration)
    )
    spot_responded, futures_responded = await asyncio.gather(spot_task, futures_task)

    spot_active    = [s for s in spot    if s in spot_responded]
    futures_active = [s for s in futures if s in futures_responded]

    _save(SPOT_ACTIVE_FILE,    spot_active)
    _save(FUTURES_ACTIVE_FILE, futures_active)

    return spot_active, futures_active


def validate_pairs(
    spot: List[str],
    futures: List[str],
    duration: int = DURATION_SECONDS,
) -> Tuple[List[str], List[str]]:
    """
    Валидирует пары через WS.
    Возвращает (active_spot, active_futures) — пары, по которым пришёл ответ.
    """
    return asyncio.run(_run(spot, futures, duration))


if __name__ == "__main__":
    import sys

    sys.path.insert(0, str(BASE_DIR.parent))
    from bitget.bitget_pairs import fetch_pairs

    spot, futures = fetch_pairs()
    print(f"Bitget: загружено Spot={len(spot)}, Futures={len(futures)}")
    print(f"Запускаю WS-валидацию ({DURATION_SECONDS} сек)...")
    active_spot, active_futures = validate_pairs(spot, futures)
    print(f"Spot active:    {len(active_spot)}/{len(spot)}  -> {SPOT_ACTIVE_FILE}")
    print(f"Futures active: {len(active_futures)}/{len(futures)}  -> {FUTURES_ACTIVE_FILE}")
