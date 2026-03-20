#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — запускает все 8 WS-коллекторов + staleness_monitor в одном процессе.

При старте:
  1. Очищает Redis (FLUSHDB)
  2. Запускает ротируемый лог-файл (чанки по 12 часов, хранится 2 последних)
  3. Запускает все коллекторы + staleness_monitor
  4. Рисует live-дашборд в stderr

Лог-файлы: logs/collectors_YYYY-MM-DD_HH-MM.log (автоматически, без редиректа)

Запуск:
    python3 run_all.py
    python3 run_all.py --buckets    # staleness с корзинами
    python3 run_all.py --no-dash    # только JSON-логи без TUI
"""

import argparse
import asyncio
import importlib
import sys
import time
from datetime import datetime
from pathlib import Path

import orjson
import redis.asyncio as aioredis
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text

COLLECTORS = [
    "binance_spot",
    "binance_futures",
    "bybit_spot",
    "bybit_futures",
    "okx_spot",
    "okx_futures",
    "gate_spot",
    "gate_futures",
]

ALL_SCRIPTS  = COLLECTORS + ["staleness_monitor", "spread_monitor"]
REDIS_SOCK   = "/var/run/redis/redis.sock"
LOG_DIR      = Path(__file__).parent / "logs"
CHUNK_HOURS  = 12
MAX_CHUNKS   = 2

sys.path.insert(0, str(Path(__file__).parent / "collectors"))


# ── rotating log writer ───────────────────────────────────────────────────────

class RotatingLogWriter:
    """Writes bytes to time-based log chunks. Keeps MAX_CHUNKS files, deletes oldest."""

    def __init__(self, log_dir: Path, chunk_hours: int, max_chunks: int) -> None:
        self._dir        = log_dir
        self._chunk_sec  = chunk_hours * 3600
        self._max_chunks = max_chunks
        self._file       = None
        self._chunk_start = 0.0
        log_dir.mkdir(parents=True, exist_ok=True)
        self._rotate()

    def _rotate(self) -> None:
        if self._file:
            self._file.close()
        fname        = f"collectors_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log"
        self._file   = open(self._dir / fname, "ab")
        self._chunk_start = time.monotonic()
        self._cleanup()

    def _cleanup(self) -> None:
        chunks = sorted(self._dir.glob("collectors_*.log"))
        for old in chunks[: max(0, len(chunks) - self._max_chunks)]:
            old.unlink(missing_ok=True)

    def write(self, data: bytes) -> None:
        if time.monotonic() - self._chunk_start >= self._chunk_sec:
            self._rotate()
        self._file.write(data)
        self._file.flush()

    def close(self) -> None:
        if self._file:
            self._file.close()


# ── per-script state ──────────────────────────────────────────────────────────

def make_state() -> dict:
    return {
        s: {
            "status":       "starting",
            "msgs_total":   0,
            "msgs_per_sec": 0.0,
            "flushes":      0,
            "avg_pipe_ms":  0.0,
            "errors":       0,
            "stale_count":  0,
            "total_keys":   0,
            "last_ts":      time.time(),
        }
        for s in ALL_SCRIPTS
    }


# ── stdout interceptor ────────────────────────────────────────────────────────

class LogInterceptor:
    """
    Replaces sys.stdout. All collectors call sys.stdout.buffer.write().
    This interceptor:
      - Writes bytes to the rotating log file
      - Writes bytes to the original stdout (for --no-dash pipe support)
      - Parses JSON lines to update the dashboard state
    """

    def __init__(self, real_buf, state: dict, log_writer: RotatingLogWriter) -> None:
        self._buf        = real_buf
        self._state      = state
        self._log_writer = log_writer
        self._partial    = b""

    @property
    def buffer(self):
        return self

    def write(self, data: bytes) -> int:
        self._buf.write(data)
        self._buf.flush()
        self._log_writer.write(data)

        combined      = self._partial + data
        lines         = combined.split(b"\n")
        self._partial = lines[-1]
        for line in lines[:-1]:
            if line:
                try:
                    self._update(orjson.loads(line))
                except Exception:
                    pass
        return len(data)

    def flush(self) -> None:
        self._buf.flush()

    def _update(self, rec: dict) -> None:
        script = rec.get("script")
        if not script or script not in self._state:
            return
        s     = self._state[script]
        event = rec.get("event", "")
        s["last_ts"] = rec.get("ts", time.time())

        if event == "connecting":
            s["status"] = "connecting"
        elif event == "connected":
            s["status"] = "connected"
        elif event == "subscribed":
            s["status"] = "subscribed"
        elif event == "first_message":
            s["status"] = "streaming"
        elif event == "disconnected":
            s["status"] = "disconnected"
            s["errors"] += 1
        elif event == "reconnecting":
            s["status"] = "reconnecting"
        elif event == "collector_crashed":
            s["status"] = "CRASHED"
            s["errors"] += 1
        elif event == "stats":
            s["msgs_total"]   = rec.get("msgs_total",      s["msgs_total"])
            s["msgs_per_sec"] = rec.get("msgs_per_sec",    s["msgs_per_sec"])
            s["flushes"]      = rec.get("flushes_total",   s["flushes"])
            s["avg_pipe_ms"]  = rec.get("avg_pipeline_ms", s["avg_pipe_ms"])
        elif event == "check":
            s["total_keys"]  = rec.get("total_keys",  s["total_keys"])
            s["stale_count"] = rec.get("stale_count", s["stale_count"])
            s["status"]      = f"STALE:{s['stale_count']}" if s["stale_count"] > 0 else "ok"
        elif event == "check_start":
            s["status"] = "checking"
        elif event == "check_failed":
            s["status"] = "check_err"
            s["errors"] += 1
        elif event == "directions_loaded":
            s["status"] = "scanning"
        elif event == "signal":
            s["msgs_total"] += 1
        elif event == "stats" and script == "spread_monitor":
            s["msgs_total"]   = rec.get("signals_total",        s["msgs_total"])
            s["msgs_per_sec"] = rec.get("signals_per_interval", s["msgs_per_sec"])
            s["flushes"]      = rec.get("cooldowns_active",     s["flushes"])
            s["avg_pipe_ms"]  = rec.get("last_cycle_ms",        s["avg_pipe_ms"])
        elif event == "cycle_error":
            s["errors"] += 1


# ── dashboard ─────────────────────────────────────────────────────────────────

STATUS_STYLE = {
    "starting":    "dim",
    "connecting":  "yellow",
    "connected":   "cyan",
    "subscribed":  "cyan",
    "streaming":   "green bold",
    "ok":          "green",
    "checking":    "cyan",
    "disconnected":"red",
    "reconnecting":"yellow",
    "check_err":   "red",
    "CRASHED":     "bold red",
}


def _status_text(s: dict) -> Text:
    status = s["status"]
    style  = STATUS_STYLE.get(status, "white")
    if status.startswith("STALE:"):
        style = "red bold"
    return Text(status, style=style)


def build_table(state: dict, uptime: float, log_file: str) -> Table:
    t = Table(
        title=(
            f"[bold cyan]Bali 3.0[/bold cyan]  "
            f"uptime [yellow]{int(uptime)}s[/yellow]  "
            f"log [dim]{log_file}[/dim]"
        ),
        show_header=True,
        header_style="bold",
        border_style="bright_black",
        expand=True,
    )
    t.add_column("Script",    style="bold white", min_width=18)
    t.add_column("Status",    min_width=13)
    t.add_column("msgs/s",    justify="right", min_width=8)
    t.add_column("total",     justify="right", min_width=11)
    t.add_column("flushes",   justify="right", min_width=8)
    t.add_column("pipe ms",   justify="right", min_width=8)
    t.add_column("errors",    justify="right", min_width=7)
    t.add_column("last seen", justify="right", min_width=9)

    now = time.time()
    for script in COLLECTORS:
        s = state[script]
        t.add_row(
            script,
            _status_text(s),
            f"{s['msgs_per_sec']:.1f}",
            f"{s['msgs_total']:,}",
            f"{s['flushes']:,}",
            f"{s['avg_pipe_ms']:.3f}",
            str(s["errors"]) if s["errors"] == 0 else f"[red]{s['errors']}[/red]",
            f"{now - s['last_ts']:.0f}s",
        )

    t.add_section()
    sm  = state["staleness_monitor"]
    stale_str = (
        f"[red bold]{sm['stale_count']}[/red bold]"
        if sm["stale_count"] > 0 else str(sm["stale_count"])
    )
    t.add_row(
        "staleness_monitor",
        _status_text(sm),
        "—",
        f"{sm['total_keys']:,}",
        f"stale:{stale_str}",
        "—",
        str(sm["errors"]) if sm["errors"] == 0 else f"[red]{sm['errors']}[/red]",
        f"{now - sm['last_ts']:.0f}s",
    )

    t.add_section()
    sp = state["spread_monitor"]
    t.add_row(
        "spread_monitor",
        _status_text(sp),
        str(sp["msgs_per_sec"]),                          # signals per 30s interval
        f"{sp['msgs_total']:,}",                          # total signals
        f"cd:{sp['flushes']}",                            # cooldowns active
        f"{sp['avg_pipe_ms']:.2f}",                       # last cycle ms
        str(sp["errors"]) if sp["errors"] == 0 else f"[red]{sp['errors']}[/red]",
        f"{now - sp['last_ts']:.0f}s",
    )
    return t


async def dashboard_loop(state: dict, live: Live, start: float, log_writer: RotatingLogWriter) -> None:
    while True:
        await asyncio.sleep(2)
        log_file = Path(log_writer._file.name).name if log_writer._file else ""
        live.update(build_table(state, time.time() - start, log_file))


# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": "run_all", "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush_redis() -> None:
    """FLUSHDB is disabled in config — use SCAN + DEL to clear all keys."""
    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False)
    try:
        cursor  = 0
        deleted = 0
        while True:
            cursor, keys = await r.scan(cursor, count=500)
            if keys:
                await r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        log("INFO", "redis_flushed", deleted_keys=deleted, socket=REDIS_SOCK)
    except Exception as e:
        log("WARN", "redis_flush_failed", reason=str(e))
    finally:
        await r.aclose()


# ── collector runner ──────────────────────────────────────────────────────────

async def run_collector(name: str) -> None:
    mod = importlib.import_module(name)
    log("INFO", "collector_started", collector=name)
    try:
        await mod.main()
    except Exception as e:
        log("ERROR", "collector_crashed", collector=name, reason=str(e))


# ── main ──────────────────────────────────────────────────────────────────────

async def main(with_buckets: bool = False, no_dash: bool = False) -> None:
    state      = make_state()
    real_buf   = sys.stdout.buffer
    log_writer = RotatingLogWriter(LOG_DIR, CHUNK_HOURS, MAX_CHUNKS)

    interceptor  = LogInterceptor(real_buf, state, log_writer)
    sys.stdout   = interceptor  # type: ignore

    log("INFO", "startup", collectors=COLLECTORS, staleness_buckets=with_buckets,
        log_dir=str(LOG_DIR), chunk_hours=CHUNK_HOURS, max_chunks=MAX_CHUNKS)

    # Flush Redis before starting collectors
    await flush_redis()

    tasks = [asyncio.create_task(run_collector(name)) for name in COLLECTORS]

    staleness = importlib.import_module("staleness_monitor")
    tasks.append(asyncio.create_task(staleness.main(with_buckets)))
    log("INFO", "collector_started", collector="staleness_monitor")

    spread = importlib.import_module("spread_monitor")
    tasks.append(asyncio.create_task(spread.main()))
    log("INFO", "collector_started", collector="spread_monitor")

    start   = time.time()
    console = Console(stderr=True, force_terminal=sys.stderr.isatty())

    async def _shutdown():
        log("INFO", "shutdown", msg="KeyboardInterrupt received")
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        log("INFO", "stopped")
        log_writer.close()

    if no_dash:
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            await _shutdown()
        return

    log_file = Path(log_writer._file.name).name
    with Live(
        build_table(state, 0, log_file),
        console=console,
        refresh_per_second=1,
        screen=False,
        transient=False,
    ) as live:
        tasks.append(asyncio.create_task(dashboard_loop(state, live, start, log_writer)))
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            await _shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all market data collectors")
    parser.add_argument("--buckets",  action="store_true",
                        help="Enable age-bucket distribution in staleness_monitor")
    parser.add_argument("--no-dash",  action="store_true",
                        help="Disable live dashboard, output JSON logs only")
    args = parser.parse_args()

    try:
        asyncio.run(main(args.buckets, args.no_dash))
    except KeyboardInterrupt:
        pass
