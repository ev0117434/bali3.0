#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
telegram_alert.py — Telegram notification service for Bali 3.0.

Тейлит лог-файл (logs/collectors_*.log), фильтрует события,
отправляет сообщения через Telegram Bot API.

Конфиг: collectors/alert_config.json
Запуск: python3 collectors/telegram_alert.py  (standalone)
        или автоматически из run_all.py при наличии конфига

Формат alert_config.json:
  {
    "bot_token": "1234567890:AABBcc...",
    "chat_id":   "-1001234567890",
    "min_spread_pct": 0.0,
    "alerts": {
      "signals":      true,
      "anomalies":    true,
      "crashes":      true,
      "redis_crit":   true,
      "redis_warn":   false,
      "stale":        true,
      "disconnects":  false,
      "startup":      true
    },
    "cooldowns": {
      "redis_crit_sec":  300,
      "redis_warn_sec":  600,
      "stale_sec":       600,
      "disconnect_sec":  300,
      "crash_sec":       300
    }
  }
"""

import asyncio
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import orjson

SCRIPT      = "telegram_alert"
CONFIG_PATH = Path(__file__).parent / "alert_config.json"
LOG_DIR     = Path(__file__).parent.parent / "logs"

TELEGRAM_API = "https://api.telegram.org/bot{token}/sendMessage"

# Exchange code → short name
EX_SHORT = {"binance": "bn", "bybit": "bb", "okx": "ok", "gate": "gt"}

# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── config ────────────────────────────────────────────────────────────────────

_DEFAULTS = {
    "bot_token":      "",
    "chat_id":        "",
    "min_spread_pct": 0.0,
    "alerts": {
        "signals":     True,
        "anomalies":   True,
        "crashes":     True,
        "redis_crit":  True,
        "redis_warn":  False,
        "stale":       True,
        "disconnects": False,
        "startup":     True,
    },
    "cooldowns": {
        "redis_crit_sec":  300,
        "redis_warn_sec":  600,
        "stale_sec":       600,
        "disconnect_sec":  300,
        "crash_sec":       300,
    },
}


def load_config() -> dict:
    cfg = {
        **_DEFAULTS,
        "alerts":    dict(_DEFAULTS["alerts"]),
        "cooldowns": dict(_DEFAULTS["cooldowns"]),
    }
    if not CONFIG_PATH.exists():
        return cfg
    try:
        with open(CONFIG_PATH) as f:
            user = json.load(f)
        cfg["bot_token"]      = user.get("bot_token",      cfg["bot_token"])
        raw_id = user.get("chat_id", cfg["chat_id"])
        cfg["chat_id"] = [str(i) for i in raw_id] if isinstance(raw_id, list) else [str(raw_id)]
        cfg["min_spread_pct"] = float(user.get("min_spread_pct", cfg["min_spread_pct"]))
        if "alerts" in user:
            cfg["alerts"].update(user["alerts"])
        if "cooldowns" in user:
            cfg["cooldowns"].update(user["cooldowns"])
    except Exception as e:
        log("WARN", "config_load_error", error=str(e)[:120])
    return cfg


# ── Telegram sender ───────────────────────────────────────────────────────────

def _http_post_sync(url: str, data: bytes) -> tuple[int, str]:
    """Blocking HTTP POST — call inside run_in_executor."""
    try:
        req = urllib.request.Request(url, data=data,
                                     headers={"Content-Type": "application/x-www-form-urlencoded"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status, resp.read().decode()
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()
    except Exception as e:
        return 0, str(e)


class TelegramSender:
    """
    Rate-limited Telegram sender.
    Internal queue ensures messages are sent ≤ 1/s to stay within API limits.
    """

    def __init__(self, token: str, chat_ids: list[str]) -> None:
        self._url      = TELEGRAM_API.format(token=token)
        self._chat_ids = chat_ids
        self._queue:   asyncio.Queue = asyncio.Queue(maxsize=50)
        self._task:    asyncio.Task | None = None

    def start(self) -> None:
        self._task = asyncio.create_task(self._worker())

    def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def send(self, text: str) -> None:
        """Put message in send queue. Drops oldest if queue is full."""
        if self._queue.full():
            try:
                self._queue.get_nowait()   # drop oldest
            except asyncio.QueueEmpty:
                pass
        await self._queue.put(text)

    async def _worker(self) -> None:
        loop = asyncio.get_event_loop()
        while True:
            try:
                text = await self._queue.get()
                for chat_id in self._chat_ids:
                    payload = urllib.parse.urlencode({
                        "chat_id":                  chat_id,
                        "text":                     text,
                        "parse_mode":               "HTML",
                        "disable_web_page_preview": "true",
                    }).encode()
                    status, body = await loop.run_in_executor(
                        None, _http_post_sync, self._url, payload
                    )
                    if status == 200:
                        log("INFO", "sent", chars=len(text), chat_id_tail=chat_id[-6:])
                    elif status == 429:
                        try:
                            retry = json.loads(body).get("parameters", {}).get("retry_after", 5)
                        except Exception:
                            retry = 5
                        log("WARN", "rate_limited", retry_after=retry, chat_id_tail=chat_id[-6:])
                        await self._queue.put(text)   # re-queue
                        await asyncio.sleep(retry)
                        break
                    else:
                        log("WARN", "send_failed", status=status, body=body[:120], chat_id_tail=chat_id[-6:])

                await asyncio.sleep(1.0)  # ≤ 1 msg/s
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log("WARN", "worker_error", error=str(e)[:100])
                await asyncio.sleep(1.0)


# ── Cooldown tracker ──────────────────────────────────────────────────────────

class CooldownTracker:
    def __init__(self) -> None:
        self._exp: dict[str, float] = {}

    def check(self, key: str, seconds: float) -> bool:
        """Returns True if NOT in cooldown (i.e. allowed to fire). Sets cooldown."""
        now = time.time()
        if now < self._exp.get(key, 0.0):
            return False
        self._exp[key] = now + seconds
        return True

    def set_only(self, key: str, seconds: float) -> None:
        """Set cooldown without checking."""
        self._exp[key] = time.time() + seconds


# ── Message formatters ────────────────────────────────────────────────────────

def _dir_short(direction: str) -> str:
    """binance_spot_bybit_futures → gt→bb"""
    try:
        spot, fut = direction.split("_spot_")
        fut = fut.replace("_futures", "")
        return f"{EX_SHORT.get(spot, spot[:2])}→{EX_SHORT.get(fut, fut[:2])}"
    except Exception:
        return direction[:10]


def fmt_signal(rec: dict) -> str:
    direction = rec.get("direction", "")
    sym       = rec.get("symbol",    "")
    spread    = rec.get("spread_pct", 0.0)
    ask       = rec.get("spot_ask",  "")
    bid       = rec.get("fut_bid",   "")
    short     = _dir_short(direction)
    return (
        f"💰 <b>Сигнал</b>  {short}\n"
        f"<code>{sym}</code>  <b>+{spread:.4f}%</b>\n"
        f"spot ask: <code>{ask}</code> → fut bid: <code>{bid}</code>"
    )


def fmt_anomaly(rec: dict) -> str:
    direction = rec.get("direction", "")
    sym       = rec.get("symbol",    "")
    spread    = rec.get("spread_pct", 0.0)
    ask       = rec.get("spot_ask",  "")
    bid       = rec.get("fut_bid",   "")
    short     = _dir_short(direction)
    return (
        f"🚨 <b>АНОМАЛИЯ</b>  {short}\n"
        f"<code>{sym}</code>  <b>+{spread:.2f}%</b>\n"
        f"spot ask: <code>{ask}</code>  fut bid: <code>{bid}</code>\n"
        f"⚠️ Вероятно стейл или некорректная цена"
    )


def fmt_crash(rec: dict) -> str:
    name   = rec.get("collector", rec.get("script", "?"))
    reason = rec.get("reason", "unknown")[:150]
    return (
        f"🔴 <b>Коллектор упал</b>: <code>{name}</code>\n"
        f"{reason}"
    )


def fmt_redis_degraded(rec: dict) -> str:
    flag  = rec.get("flag",     "")
    ping  = rec.get("ping_ms",  0.0)
    write = rec.get("write_ms", 0.0)
    mem   = rec.get("mem_pct",  0.0)
    mb    = rec.get("mem_mb",   0.0)
    CRIT_FLAGS = {"ping_crit", "write_crit", "mem_crit", "blocked_clients", "oom_rejected"}
    icon = "🔴" if flag in CRIT_FLAGS else "🟡"
    return (
        f"{icon} <b>Redis деградация</b>: <code>{flag}</code>\n"
        f"PING: {ping:.1f}ms · Write: {write:.1f}ms · "
        f"Mem: {mem:.1f}% ({mb:.0f}MB)"
    )


def fmt_redis_recovered(flag: str) -> str:
    return f"✅ <b>Redis восстановился</b>: <code>{flag}</code>"


def fmt_stale(rec: dict) -> str:
    count  = rec.get("stale_count", 0)
    by_src = rec.get("stale_by_source", {})
    src_str = "  ".join(f"{k}:{v}" for k, v in by_src.items()) if by_src else ""
    msg = f"⚠️ <b>Устаревшие данные</b>: {count} ключ(а)\n"
    if src_str:
        msg += f"<code>{src_str}</code>"
    return msg


def fmt_disconnected(rec: dict) -> str:
    script = rec.get("script", "?")
    reason = rec.get("reason", "")[:100]
    return (
        f"🟡 <b>Дисконнект</b>: <code>{script}</code>\n"
        f"{reason}"
    )


def fmt_reconnected(rec: dict) -> str:
    script = rec.get("script", "?")
    sym_count = rec.get("chunk_symbols", "")
    extra = f"  {sym_count} символов" if sym_count else ""
    return f"🟢 <b>Переподключился</b>: <code>{script}</code>{extra}"


def fmt_startup(rec: dict) -> str:
    delay = rec.get("spread_delay_sec", 0)
    return (
        f"🚀 <b>BALI 3.0 запущен</b>\n"
        f"20 коллекторов · 12 направлений"
        + (f"\nЗадержка сигналов: {int(delay)}s" if delay > 0 else "")
    )


def fmt_shutdown() -> str:
    return "🛑 <b>BALI 3.0 остановлен</b>"


# ── Alert engine ──────────────────────────────────────────────────────────────

class AlertEngine:
    """
    Receives parsed log records, decides what to alert, respects cooldowns.
    """

    CRIT_REDIS = {"ping_crit", "write_crit", "mem_crit", "blocked_clients", "oom_rejected"}
    WARN_REDIS = {"ping_warn", "write_warn", "mem_warn", "frag_warn", "frag_crit"}

    def __init__(self, cfg: dict, sender: TelegramSender) -> None:
        self._cfg    = cfg
        self._alerts = cfg.get("alerts",    {})
        self._cds    = cfg.get("cooldowns", {})
        self._sender = sender
        self._cd     = CooldownTracker()
        self._min_sp = cfg.get("min_spread_pct", 0.0)

    def _a(self, key: str, default: bool = True) -> bool:
        return bool(self._alerts.get(key, default))

    def _cd_check(self, key: str, cd_name: str, default_sec: float = 300) -> bool:
        sec = float(self._cds.get(cd_name, default_sec))
        return self._cd.check(key, sec)

    async def handle(self, rec: dict) -> None:
        ev     = rec.get("event",  "")
        script = rec.get("script", "")

        # ── startup ────────────────────────────────────────────────────────
        if ev == "startup" and script == "run_all" and self._a("startup"):
            if self._cd.check("startup", 10):
                await self._sender.send(fmt_startup(rec))

        # ── shutdown ───────────────────────────────────────────────────────
        elif ev == "shutdown" and script == "run_all" and self._a("startup"):
            await self._sender.send(fmt_shutdown())

        # ── signal ─────────────────────────────────────────────────────────
        elif ev == "signal" and script == "spread_monitor" and self._a("signals"):
            spread = rec.get("spread_pct", 0.0)
            if spread >= self._min_sp:
                await self._sender.send(fmt_signal(rec))

        # ── anomaly ────────────────────────────────────────────────────────
        elif ev == "anomaly" and script == "spread_monitor" and self._a("anomalies"):
            direction = rec.get("direction", "")
            sym       = rec.get("symbol",    "")
            if self._cd_check(f"anomaly:{direction}:{sym}", "anomaly_sec", 3500):
                await self._sender.send(fmt_anomaly(rec))

        # ── collector crashed ──────────────────────────────────────────────
        elif ev == "collector_crashed" and self._a("crashes"):
            name = rec.get("collector", script)
            if self._cd_check(f"crash:{name}", "crash_sec", 300):
                await self._sender.send(fmt_crash(rec))

        # ── disconnected ───────────────────────────────────────────────────
        elif ev == "disconnected" and self._a("disconnects"):
            if self._cd_check(f"disc:{script}", "disconnect_sec", 300):
                await self._sender.send(fmt_disconnected(rec))

        # ── reconnected ────────────────────────────────────────────────────
        elif ev == "reconnected" and self._a("disconnects"):
            if self._cd.check(f"rec:{script}", 60):
                await self._sender.send(fmt_reconnected(rec))

        # ── redis degraded ─────────────────────────────────────────────────
        elif ev == "degraded" and script == "redis_monitor":
            flag = rec.get("flag", "")
            if flag in self.CRIT_REDIS and self._a("redis_crit"):
                if self._cd_check(f"redis_crit:{flag}", "redis_crit_sec", 300):
                    await self._sender.send(fmt_redis_degraded(rec))
            elif flag in self.WARN_REDIS and self._a("redis_warn"):
                if self._cd_check(f"redis_warn:{flag}", "redis_warn_sec", 600):
                    await self._sender.send(fmt_redis_degraded(rec))

        # ── redis recovered ────────────────────────────────────────────────
        elif ev == "recovered" and script == "redis_monitor":
            flag = rec.get("flag", "")
            # only notify recovery for CRIT flags (not warn, too noisy)
            if flag in self.CRIT_REDIS and self._a("redis_crit"):
                if self._cd.check(f"redis_rec:{flag}", 60):
                    await self._sender.send(fmt_redis_recovered(flag))

        # ── stale data ─────────────────────────────────────────────────────
        elif ev == "check" and script == "staleness_monitor":
            stale = rec.get("stale_count", 0) or 0
            if stale > 0 and self._a("stale"):
                if self._cd_check("stale", "stale_sec", 600):
                    await self._sender.send(fmt_stale(rec))


# ── Log file tailer ───────────────────────────────────────────────────────────

async def tail_log_file(log_dir: Path, queue: asyncio.Queue) -> None:
    """
    Follow newest log file in log_dir.
    On file rotation (run_all restart) — switches to new file automatically.
    Only follows NEW lines from the moment of start (seek to end).
    """
    current_path = None
    fh           = None

    try:
        while True:
            # Find newest log file
            files = sorted(log_dir.glob("collectors_*.log"))
            newest = files[-1] if files else None

            if newest != current_path:
                if fh:
                    fh.close()
                if newest:
                    fh = open(newest, "rb")
                    fh.seek(0, 2)   # seek to end — only follow new lines
                    current_path = newest
                    log("INFO", "following", file=newest.name)
                else:
                    await asyncio.sleep(1)
                    continue

            # Read all available lines
            if fh:
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    line = line.strip()
                    if line:
                        try:
                            await queue.put(orjson.loads(line))
                        except Exception:
                            pass

            await asyncio.sleep(0.05)   # 50ms poll interval

    except asyncio.CancelledError:
        if fh:
            fh.close()
        raise


async def _process_queue(engine: AlertEngine, queue: asyncio.Queue) -> None:
    while True:
        try:
            rec = await asyncio.wait_for(queue.get(), timeout=5.0)
            await engine.handle(rec)
        except asyncio.TimeoutError:
            pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log("WARN", "process_error", error=str(e)[:120])


# ── main ──────────────────────────────────────────────────────────────────────

async def main(log_dir: Path = None) -> None:
    if log_dir is None:
        log_dir = LOG_DIR

    cfg = load_config()

    if not cfg.get("bot_token") or not cfg.get("chat_id"):
        log("WARN", "not_configured",
            msg="alert_config.json missing or bot_token/chat_ids empty — alerts disabled")
        # Keep running silently so run_all.py task doesn't fail
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            return

    chat_ids = cfg["chat_id"]
    enabled = [k for k, v in cfg.get("alerts", {}).items() if v]
    log("INFO", "startup",
        chat_ids=[c[-6:] for c in chat_ids],
        min_spread_pct=cfg["min_spread_pct"],
        enabled_alerts=enabled)

    sender = TelegramSender(cfg["bot_token"], chat_ids)
    sender.start()

    engine = AlertEngine(cfg, sender)
    queue: asyncio.Queue = asyncio.Queue(maxsize=2000)

    try:
        await asyncio.gather(
            tail_log_file(log_dir, queue),
            _process_queue(engine, queue),
        )
    except asyncio.CancelledError:
        sender.stop()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Telegram alert service for Bali 3.0")
    parser.add_argument("--test", action="store_true",
                        help="Send a test message and exit")
    parser.add_argument("--config", default=str(CONFIG_PATH),
                        help=f"Path to alert_config.json (default: {CONFIG_PATH})")
    args = parser.parse_args()

    # Override config path if specified
    if args.config != str(CONFIG_PATH):
        CONFIG_PATH = Path(args.config)  # type: ignore

    if args.test:
        async def _test() -> None:
            cfg = load_config()
            if not cfg.get("bot_token") or not cfg.get("chat_id"):
                print("ERROR: bot_token or chat_id not set in alert_config.json")
                return
            sender = TelegramSender(cfg["bot_token"], str(cfg["chat_id"]))
            sender.start()
            await sender.send(
                "🧪 <b>BALI 3.0 — тест алёртов</b>\n"
                "Если видишь это сообщение — всё настроено верно ✅"
            )
            await asyncio.sleep(3)  # give worker time to send
            sender.stop()
            print("Test message sent.")
        asyncio.run(_test())
    else:
        asyncio.run(main())
