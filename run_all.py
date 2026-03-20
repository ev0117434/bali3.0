#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — запускает все 8 WS-коллекторов + staleness_monitor в одном процессе.

Каждый коллектор работает как отдельная asyncio-задача.
JSON-логи идут в stdout, живой дашборд метрик — в stderr.

Запуск:
    python3 run_all.py
    python3 run_all.py --buckets         # staleness с корзинами
    python3 run_all.py --no-dash          # только JSON-логи без TUI
    python3 run_all.py 2>&1 | tee logs/collectors.log
"""

import argparse
import asyncio
import importlib
import io
import sys
import time
from pathlib import Path

import orjson
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

ALL_SCRIPTS = COLLECTORS + ["staleness_monitor"]

sys.path.insert(0, str(Path(__file__).parent / "collectors"))


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
    """Forwards all bytes to real stdout; parses JSON lines to update dashboard state."""

    def __init__(self, real_buf, state: dict) -> None:
        self._buf     = real_buf
        self._state   = state
        self._partial = b""

    # Allow sys.stdout.buffer to be used as a drop-in buffer object
    @property
    def buffer(self):
        return self

    def write(self, data: bytes) -> int:
        self._buf.write(data)
        self._buf.flush()
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
            if s["stale_count"] > 0:
                s["status"] = f"STALE:{s['stale_count']}"
            else:
                s["status"] = "ok"
        elif event == "check_start":
            s["status"] = "checking"
        elif event == "check_failed":
            s["status"] = "check_err"
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


def build_table(state: dict, uptime: float) -> Table:
    t = Table(
        title=f"[bold cyan]Bali 3.0[/bold cyan]  uptime [yellow]{int(uptime)}s[/yellow]",
        show_header=True,
        header_style="bold",
        border_style="bright_black",
        expand=True,
    )
    t.add_column("Script",     style="bold white", min_width=18)
    t.add_column("Status",     min_width=13)
    t.add_column("msgs/s",     justify="right", min_width=8)
    t.add_column("total",      justify="right", min_width=11)
    t.add_column("flushes",    justify="right", min_width=8)
    t.add_column("pipe ms",    justify="right", min_width=8)
    t.add_column("errors",     justify="right", min_width=7)
    t.add_column("last seen",  justify="right", min_width=9)

    now = time.time()

    for script in COLLECTORS:
        s    = state[script]
        age  = now - s["last_ts"]
        t.add_row(
            script,
            _status_text(s),
            f"{s['msgs_per_sec']:.1f}",
            f"{s['msgs_total']:,}",
            f"{s['flushes']:,}",
            f"{s['avg_pipe_ms']:.3f}",
            str(s["errors"]) if s["errors"] == 0 else f"[red]{s['errors']}[/red]",
            f"{age:.0f}s",
        )

    # separator before staleness row
    t.add_section()
    sm = state["staleness_monitor"]
    sm_age = now - sm["last_ts"]
    stale_str = (
        f"[red bold]{sm['stale_count']}[/red bold]"
        if sm["stale_count"] > 0
        else str(sm["stale_count"])
    )
    t.add_row(
        "staleness_monitor",
        _status_text(sm),
        "—",
        f"{sm['total_keys']:,}",   # Redis keys tracked
        f"stale:{stale_str}",
        "—",
        str(sm["errors"]) if sm["errors"] == 0 else f"[red]{sm['errors']}[/red]",
        f"{sm_age:.0f}s",
    )

    return t


async def dashboard_loop(state: dict, live: Live, start: float) -> None:
    while True:
        await asyncio.sleep(2)
        live.update(build_table(state, time.time() - start))


# ── logger ────────────────────────────────────────────────────────────────────

def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": "run_all", "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


# ── runner ────────────────────────────────────────────────────────────────────

async def run_collector(name: str) -> None:
    mod = importlib.import_module(name)
    log("INFO", "collector_started", collector=name)
    try:
        await mod.main()
    except Exception as e:
        log("ERROR", "collector_crashed", collector=name, reason=str(e))


async def main(with_buckets: bool = False, no_dash: bool = False) -> None:
    state    = make_state()
    real_buf = sys.stdout.buffer

    # Replace sys.stdout with a wrapper whose .buffer is our interceptor.
    # Collectors call sys.stdout.buffer.write() — this routes through LogInterceptor.
    if not no_dash:
        interceptor  = LogInterceptor(real_buf, state)
        sys.stdout   = interceptor  # type: ignore

    log("INFO", "startup", collectors=COLLECTORS, staleness_buckets=with_buckets)

    tasks = [asyncio.create_task(run_collector(name)) for name in COLLECTORS]

    staleness = importlib.import_module("staleness_monitor")
    tasks.append(asyncio.create_task(staleness.main(with_buckets)))
    log("INFO", "collector_started", collector="staleness_monitor")

    console = Console(stderr=True, force_terminal=sys.stderr.isatty())
    start   = time.time()

    async def _gather():
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            pass

    if no_dash:
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            log("INFO", "shutdown", msg="KeyboardInterrupt received")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log("INFO", "stopped")
        return

    with Live(
        build_table(state, 0),
        console=console,
        refresh_per_second=1,
        screen=False,
        transient=False,
    ) as live:
        tasks.append(asyncio.create_task(dashboard_loop(state, live, start)))
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            log("INFO", "shutdown", msg="KeyboardInterrupt received")
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log("INFO", "stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all market data collectors")
    parser.add_argument(
        "--buckets",
        action="store_true",
        help="Enable age-bucket distribution in staleness_monitor",
    )
    parser.add_argument(
        "--no-dash",
        action="store_true",
        help="Disable live dashboard, output raw JSON logs only",
    )
    args = parser.parse_args()

    try:
        asyncio.run(main(args.buckets, args.no_dash))
    except KeyboardInterrupt:
        pass
