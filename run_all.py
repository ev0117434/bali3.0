#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_all.py — оркестратор Bali 3.0.

Запускает 4 subprocess-группы коллекторов, каждая со своим asyncio event loop,
пиннингует группы и Redis на выделенные ядра CPU, рисует дашборд.

Архитектура (32 ядра, 4 зарезервированы для системы):
  Cores  0-3:   системный резерв
  Cores  4-11:  Redis (io-threads=8)
  Cores 12-19:  OB-коллекторы (ob_* × 10, тяжёлые dict-сортировки)
  Cores 20-23:  MD-коллекторы (md_* × 10, I/O-bound)
  Cores 24-25:  FR-коллекторы (fr_* × 5, лёгкие)
  Cores 26-27:  Мониторы (staleness, redis_monitor, telegram, spread_monitor)
  Cores 28-31:  run_all.py + дашборд

Запуск:
    python3 run_all.py
    python3 run_all.py --no-dash       # только JSON-логи без TUI
    python3 run_all.py --buckets       # staleness с корзинами возраста
    python3 run_all.py --delay 60      # задержка 60 с перед сигналами
"""

import argparse
import asyncio
import os
import shutil
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path

import orjson
import redis.asyncio as aioredis
from rich.console import Console
from rich.live import Live
from rich.panel import Panel
from rich.text import Text

COLLECTORS_DIR = Path(__file__).parent / "collectors"
SIG_DIR        = Path(__file__).parent / "signals"
OLD_DIR        = Path(__file__).parent / "old"
LOG_DIR        = Path(__file__).parent / "logs"
REDIS_SOCK     = "/var/run/redis/redis.sock"
CHUNK_HOURS    = 12
MAX_CHUNKS     = 2

# ── CPU affinity map ──────────────────────────────────────────────────────────

CPU_REDIS    = list(range(4,  12))   # 8 cores → io-threads=8
CPU_OB       = list(range(12, 20))   # 8 cores → OB dict-sorting
CPU_MD       = list(range(20, 24))   # 4 cores → MD I/O-bound
CPU_FR       = list(range(24, 26))   # 2 cores → FR light
CPU_MONITORS = list(range(26, 28))   # 2 cores → staleness/redis_mon/spread
CPU_RUNALL   = list(range(28, 32))   # 4 cores → dashboard + pipe readers

# ── runner scripts ────────────────────────────────────────────────────────────
# Delays between group launches prevent thundering-herd WS reconnects.
# OB and MD both connect to the same exchanges (Binance futures, Gate, etc.) —
# without a gap the exchange sees a burst of connections and rate-limits them.
# With 10s gap: OB finishes most handshakes before MD starts connecting.

RUNNERS = [
    # (script_path, label, cores, extra_args_factory, pre_launch_delay_sec)
    (COLLECTORS_DIR / "runner_ob.py",       "ob_group",  CPU_OB,        lambda d, b: [],  0),
    (COLLECTORS_DIR / "runner_md.py",       "md_group",  CPU_MD,        lambda d, b: [], 10),
    (COLLECTORS_DIR / "runner_fr.py",       "fr_group",  CPU_FR,        lambda d, b: [],  5),
    (COLLECTORS_DIR / "runner_monitors.py", "monitors",  CPU_MONITORS,
        lambda d, b: [f"--delay={d}"] + (["--buckets"] if b else []),   3),
]

# ── scripts tracked in dashboard state ───────────────────────────────────────

COLLECTORS = [
    "binance_spot", "binance_futures",
    "bybit_spot",   "bybit_futures",
    "okx_spot",     "okx_futures",
    "gate_spot",    "gate_futures",
    "bitget_spot",  "bitget_futures",
    "ob_binance_spot", "ob_binance_futures",
    "ob_bybit_spot",   "ob_bybit_futures",
    "ob_okx_spot",     "ob_okx_futures",
    "ob_gate_spot",    "ob_gate_futures",
    "ob_bitget_spot",  "ob_bitget_futures",
    "fr_binance_futures", "fr_bybit_futures", "fr_okx_futures",
    "fr_gate_futures",    "fr_bitget_futures",
]
_N_MD, _N_OB, _N_FR = 10, 10, 5

ALL_SCRIPTS = COLLECTORS + ["staleness_monitor", "spread_monitor", "redis_monitor"]
EX_SHORT    = {"binance": "bn", "bybit": "bb", "okx": "ok", "gate": "gt", "bitget": "bg"}


# ── archive old data ──────────────────────────────────────────────────────────

def archive_old_data() -> str | None:
    dirs_to_move = [(LOG_DIR, "logs"), (SIG_DIR, "signals")]
    has_content  = any(d.exists() and any(d.iterdir()) for d, _ in dirs_to_move if d.exists())
    if not has_content:
        return None
    ts      = datetime.now().strftime("%Y-%m-%d_%H-%M")
    dst_run = OLD_DIR / ts
    dst_run.mkdir(parents=True, exist_ok=True)
    for src, name in dirs_to_move:
        if src.exists() and any(src.iterdir()):
            shutil.move(str(src), str(dst_run / name))
    return ts


# ── rotating log writer ───────────────────────────────────────────────────────

class RotatingLogWriter:
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
        fname             = f"collectors_{datetime.now().strftime('%Y-%m-%d_%H-%M')}.log"
        self._file        = open(self._dir / fname, "ab")
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

    def current_name(self) -> str:
        return Path(self._file.name).name if self._file else ""

    def close(self) -> None:
        if self._file:
            self._file.close()


# ── CPU affinity helpers ──────────────────────────────────────────────────────

def _cores_str(cores: list[int]) -> str:
    if not cores:
        return ""
    return f"{cores[0]}-{cores[-1]}"


def set_affinity(pid: int, cores: list[int], label: str, log_fn) -> None:
    try:
        os.sched_setaffinity(pid, set(cores))
        log_fn("INFO", "affinity_set",
               target=label, pid=pid, cores=_cores_str(cores))
    except Exception as e:
        log_fn("WARN", "affinity_failed",
               target=label, pid=pid, reason=str(e)[:80])


def pin_redis(cores: list[int], log_fn) -> None:
    try:
        import subprocess
        out = subprocess.check_output(["pgrep", "-x", "redis-server"],
                                      stderr=subprocess.DEVNULL)
        pid = int(out.strip().split()[0])
        set_affinity(pid, cores, "redis", log_fn)
    except Exception as e:
        log_fn("WARN", "redis_affinity_failed", reason=str(e)[:80])


# ── per-script state ──────────────────────────────────────────────────────────

def make_state() -> dict:
    state = {
        s: {
            "status":       "starting",
            "msgs_total":   0,
            "msgs_per_sec": 0.0,
            "avg_pipe_ms":  0.0,
            "disc":         0,
            "errors":       0,
            "books_init":   0,
            "books_total":  0,
            "stale_count":  0,
            "total_keys":   0,
            "last_ts":      time.time(),
        }
        for s in ALL_SCRIPTS
    }
    state["_signals_feed"] = deque(maxlen=6)
    state["_redis_info"]   = {
        "mem_mb": 0.0, "max_mb": 0.0, "frag": 1.0, "keys": 0,
        "ping_ms": 0.0, "write_ms": 0.0, "blocked": 0,
        "rejected_new": 0, "flags": [],
    }
    state["_spread_extra"] = {
        "anomalies": 0, "snapshots": 0, "cooldowns": 0,
        "scanned": 0,   "cycle_ms":  0.0,
    }
    return state


# ── state updater (called for every JSON line from any subprocess) ────────────

def _update_state(state: dict, rec: dict) -> None:
    script = rec.get("script")
    event  = rec.get("event", "")

    if event in ("signal", "anomaly") and script == "spread_monitor":
        feed   = state["_signals_feed"]
        ts_str = datetime.fromtimestamp(rec.get("ts", time.time())).strftime("%H:%M:%S")
        fut_ex = rec.get("fut_ex", "")
        feed.appendleft({
            "ts":         ts_str,
            "direction":  rec.get("direction", ""),
            "sym":        rec.get("symbol", ""),
            "spread":     rec.get("spread_pct", 0.0),
            "is_anomaly": event == "anomaly",
            "fr":         (rec.get("fr") or {}).get(fut_ex, {}).get("r", ""),
        })

    if not script or script not in state:
        return

    s = state[script]
    s["last_ts"] = rec.get("ts", time.time())

    if event == "connecting":
        s["status"] = "connecting"
    elif event in ("connected", "subscribed"):
        s["status"] = event
    elif event == "first_message":
        s["status"] = "streaming"
    elif event == "disconnected":
        s["status"]  = "disconnected"
        s["disc"]   += 1
        s["errors"] += 1
    elif event == "reconnecting":
        s["status"] = "reconnecting"
    elif event == "collector_crashed":
        s["status"]  = "CRASHED"
        s["errors"] += 1
    elif event == "book_init":
        s["books_init"] = rec.get("books_initialized", s["books_init"])
    elif event == "symbols_loaded":
        if script.startswith("ob_bybit") or script.startswith("ob_okx"):
            s["books_total"] = rec.get("count", s["books_total"])
    elif event == "stats":
        if script == "spread_monitor":
            ex = state["_spread_extra"]
            ex["anomalies"] = rec.get("anomalies_total",      ex["anomalies"])
            ex["snapshots"] = rec.get("snapshots_active",     ex["snapshots"])
            ex["cooldowns"] = rec.get("cooldowns_active",     ex["cooldowns"])
            ex["scanned"]   = rec.get("scanned_total",        ex["scanned"])
            ex["cycle_ms"]  = rec.get("last_cycle_ms",        ex["cycle_ms"])
            s["msgs_total"]   = rec.get("signals_total",        s["msgs_total"])
            s["msgs_per_sec"] = rec.get("signals_per_interval", s["msgs_per_sec"])
            s["avg_pipe_ms"]  = rec.get("last_cycle_ms",        s["avg_pipe_ms"])
            if s["status"] not in ("CRASHED",):
                s["status"] = "scanning"
        else:
            s["msgs_total"]   = rec.get("msgs_total",      s["msgs_total"])
            s["msgs_per_sec"] = rec.get("msgs_per_sec",    s["msgs_per_sec"])
            s["avg_pipe_ms"]  = rec.get("avg_pipeline_ms", s["avg_pipe_ms"])
            if s["status"] not in ("CRASHED", "disconnected", "reconnecting"):
                s["status"] = "streaming"
    elif event == "check":
        s["total_keys"]  = rec.get("total_keys",  s["total_keys"])
        stale            = rec.get("stale_count", 0) or 0
        s["stale_count"] = stale
        s["status"]      = f"stale:{stale}" if stale > 0 else "ok"
    elif event == "check_start":
        s["status"] = "checking"
    elif event == "check_failed":
        s["status"]  = "check_err"
        s["errors"] += 1
    elif event == "spread_monitor_waiting":
        s["status"] = f"wait:{int(rec.get('delay_sec', 0))}s"
    elif event == "directions_loaded":
        s["status"] = "scanning"
    elif event == "rest_refresh":
        if s["status"] not in ("CRASHED", "disconnected"):
            s["status"] = "streaming"
    elif event == "cycle_error":
        s["errors"] += 1
    elif event == "degraded":
        s["errors"] += 1
    elif event == "recovered":
        if not state["_redis_info"]["flags"]:
            s["status"] = "ok"

    if script == "redis_monitor" and event == "stats":
        ri = state["_redis_info"]
        ri["ping_ms"]      = rec.get("ping_ms",      ri["ping_ms"])
        ri["write_ms"]     = rec.get("write_ms",     ri["write_ms"])
        ri["blocked"]      = rec.get("blocked",      ri["blocked"])
        ri["rejected_new"] = rec.get("rejected_new", ri["rejected_new"])
        ri["flags"]        = rec.get("flags",        ri["flags"])
        flags = ri["flags"]
        crit  = {"ping_crit", "write_crit", "mem_crit", "blocked_clients", "oom_rejected"}
        if set(flags) & crit:
            s["status"] = "CRIT"
        elif flags:
            s["status"] = "warn"
        else:
            s["status"] = "ok"


# ── dashboard ─────────────────────────────────────────────────────────────────

def _fmt_uptime(seconds: float) -> str:
    s = int(seconds)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    return f"{h}h {m:02d}m {s:02d}s" if h else f"{m}m {s:02d}s"


def _dir_short(direction: str) -> str:
    try:
        spot_part, fut_part = direction.split("_spot_")
        fut_name = fut_part.replace("_futures", "")
        return f"{EX_SHORT.get(spot_part, spot_part)}→{EX_SHORT.get(fut_name, fut_name)}"
    except Exception:
        return direction[:8]


def _script_short(name: str) -> str:
    n = name.removeprefix("ob_").removeprefix("fr_")
    for ex, short in EX_SHORT.items():
        n = n.replace(ex + "_spot", short + "-s").replace(ex + "_futures", short + "-f")
    return n


def _group_line(state: dict, scripts: list) -> Text:
    t = Text()
    for sc in scripts:
        s      = state[sc]
        status = s["status"]
        bi, bt = s["books_init"], s["books_total"]

        if status in ("CRASHED", "check_err", "disconnected") or status.startswith("stale:"):
            style, pfx = "red bold", "!"
        elif status in ("streaming", "scanning", "ok"):
            style, pfx = ("yellow", " ") if bt > 0 and bi < bt else ("green bold", " ")
        elif status in ("reconnecting",):
            style, pfx = "yellow", "~"
        else:
            style, pfx = "dim", " "

        disc = s["disc"]
        t.append(f"  {pfx}", style=style)
        t.append(_script_short(sc), style=style)
        if disc > 0:
            t.append(f"({disc})", style="red bold")
    return t


def build_layout(state: dict, uptime: float, log_file: str) -> Panel:
    now_str = datetime.now().strftime("%H:%M:%S")
    up_str  = _fmt_uptime(uptime)
    body    = Text()

    body.append(" MD", style="cyan bold")
    body.append_text(_group_line(state, COLLECTORS[:_N_MD]))
    body.append("\n")

    body.append(" OB", style="cyan bold")
    body.append_text(_group_line(state, COLLECTORS[_N_MD : _N_MD + _N_OB]))
    body.append("\n")

    body.append(" FR", style="cyan bold")
    body.append_text(_group_line(state, COLLECTORS[_N_MD + _N_OB :]))
    body.append("\n")

    body.append(" " + "─" * 56 + "\n", style="bright_black")

    extra = state["_spread_extra"]
    sp    = state["spread_monitor"]
    info  = state["_redis_info"]
    sm    = state["staleness_monitor"]

    mem_mb   = info["mem_mb"]
    mem_str  = f"{mem_mb / 1024:.1f}GB" if mem_mb >= 1024 else f"{int(mem_mb)}MB"
    stale    = sm.get("stale_count", 0) or 0
    ping_ms  = info["ping_ms"]
    write_ms = info["write_ms"]
    blocked  = info["blocked"]
    r_flags  = set(info.get("flags", []))

    body.append(" signals:", style="dim")
    body.append(str(sp.get("msgs_total", 0)), style="bold white")
    body.append("  anom:", style="dim")
    body.append(str(extra["anomalies"]),
                style="red bold" if extra["anomalies"] > 0 else "bold white")
    body.append(f"  cd:{extra['cooldowns']}  snaps:{extra['snapshots']}", style="dim")

    redis_crit  = bool(r_flags & {"ping_crit", "write_crit", "mem_crit",
                                   "blocked_clients", "oom_rejected"})
    redis_warn  = bool(r_flags) and not redis_crit
    redis_style = "red bold" if redis_crit else ("yellow" if redis_warn else "dim")
    body.append(f"  redis:{mem_str}", style=redis_style)
    body.append(f"  keys:{info['keys']:,}", style="dim")
    if ping_ms > 0:
        ping_style = "red bold" if ping_ms >= 10 else ("yellow" if ping_ms >= 2 else "dim")
        body.append(f"  ping:{ping_ms:.1f}ms", style=ping_style)
    if write_ms > 0:
        wr_style = "red bold" if write_ms >= 20 else ("yellow" if write_ms >= 5 else "dim")
        body.append(f"  wr:{write_ms:.1f}ms", style=wr_style)
    if blocked > 0:
        body.append(f"  blocked:{blocked}", style="red bold")
    if stale > 0:
        body.append(f"  stale:{stale}", style="red bold")
    else:
        body.append("  stale:0", style="dim")
    body.append("\n")

    body.append(" " + "─" * 56 + "\n", style="bright_black")

    feed = state["_signals_feed"]
    if not feed:
        body.append(" no signals yet\n", style="dim")
    else:
        for sig in list(feed)[:5]:
            is_a  = sig["is_anomaly"]
            icon  = "!" if is_a else ">"
            color = "red bold" if is_a else "green"
            body.append(f" {icon} ", style=color)
            body.append(f"{sig['ts']}  ", style="dim")
            body.append(f"{_dir_short(sig['direction'])}  ", style="cyan")
            body.append(sig["sym"], style="bold white")
            body.append(f"  +{sig['spread']:.2f}%", style=color)
            if sig.get("fr"):
                body.append(f"  fr:{sig['fr']}", style="dim")
            body.append("\n")

    title  = f"[bold cyan]BALI 3.0[/]  [yellow]{up_str}[/]  [dim]{now_str}  {log_file}[/]"
    border = "red" if stale > 0 else "cyan"
    return Panel(body, title=title, border_style=border, padding=(0, 0))


async def dashboard_loop(
    state: dict, live: Live, start: float, log_writer: RotatingLogWriter
) -> None:
    while True:
        await asyncio.sleep(1)
        live.update(build_layout(state, time.time() - start, log_writer.current_name()))


# ── Redis stats poller ────────────────────────────────────────────────────────

async def redis_stats_loop(state: dict) -> None:
    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    try:
        while True:
            try:
                info   = await r.info("memory")
                dbsize = await r.dbsize()
                state["_redis_info"].update({
                    "mem_mb": info.get("used_memory",             0) / 1_048_576,
                    "max_mb": info.get("maxmemory",               0) / 1_048_576,
                    "frag":   info.get("mem_fragmentation_ratio", 1.0),
                    "keys":   dbsize,
                })
            except Exception:
                pass
            await asyncio.sleep(5)
    finally:
        await r.aclose()


# ── Redis flush ───────────────────────────────────────────────────────────────

async def flush_redis(log_fn) -> None:
    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=False)
    try:
        cursor = deleted = 0
        while True:
            cursor, keys = await r.scan(cursor, count=500)
            if keys:
                await r.delete(*keys)
                deleted += len(keys)
            if cursor == 0:
                break
        log_fn("INFO", "redis_flushed", deleted_keys=deleted, socket=REDIS_SOCK)
    except Exception as e:
        log_fn("WARN", "redis_flush_failed", reason=str(e))
    finally:
        await r.aclose()


# ── subprocess pipe reader ────────────────────────────────────────────────────

async def read_pipe(
    proc: asyncio.subprocess.Process,
    label: str,
    state: dict,
    log_writer: RotatingLogWriter,
    echo: bool = False,
) -> None:
    async for line in proc.stdout:
        if not line:
            continue
        log_writer.write(line)
        if echo:
            sys.stdout.buffer.write(line)
            sys.stdout.buffer.flush()
        try:
            _update_state(state, orjson.loads(line.rstrip(b"\n")))
        except Exception:
            pass
    rc = await proc.wait()
    _write_log(log_writer, "WARN", "subprocess_exited",
               group=label, returncode=rc)


def _write_log(log_writer: RotatingLogWriter, lvl: str, event: str, **kw) -> None:
    rec  = {"ts": time.time(), "lvl": lvl, "script": "run_all", "event": event, **kw}
    data = orjson.dumps(rec) + b"\n"
    log_writer.write(data)


# ── main ──────────────────────────────────────────────────────────────────────

async def main(
    with_buckets: bool = False,
    no_dash: bool = False,
    spread_delay: float = 0.0,
) -> None:

    archived   = archive_old_data()
    log_writer = RotatingLogWriter(LOG_DIR, CHUNK_HOURS, MAX_CHUNKS)

    def log(lvl: str, event: str, **kw) -> None:
        _write_log(log_writer, lvl, event, **kw)

    log("INFO", "startup",
        staleness_buckets=with_buckets,
        log_dir=str(LOG_DIR),
        chunk_hours=CHUNK_HOURS,
        max_chunks=MAX_CHUNKS,
        spread_delay_sec=spread_delay,
        cpu_ob=_cores_str(CPU_OB),
        cpu_md=_cores_str(CPU_MD),
        cpu_fr=_cores_str(CPU_FR),
        cpu_monitors=_cores_str(CPU_MONITORS),
        cpu_runall=_cores_str(CPU_RUNALL))

    if archived:
        log("INFO", "archived", folder=str(OLD_DIR / archived),
            note="logs and signals moved before start")

    await flush_redis(log)

    # ── CPU affinity ─────────────────────────────────────────────────────────
    set_affinity(os.getpid(), CPU_RUNALL, "run_all", log)
    pin_redis(CPU_REDIS, log)

    state = make_state()
    procs: list[asyncio.subprocess.Process] = []
    tasks: list[asyncio.Task] = []

    console = Console(stderr=True, force_terminal=sys.stderr.isatty())

    async def _shutdown() -> None:
        log("INFO", "shutdown", msg="KeyboardInterrupt received")
        for t in tasks:
            t.cancel()
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        await asyncio.gather(*tasks, return_exceptions=True)
        for p in procs:
            try:
                await asyncio.wait_for(p.wait(), timeout=5)
            except Exception:
                p.kill()
        log("INFO", "stopped")
        log_writer.close()

    async def _launch_all() -> None:
        for script_path, label, cores, extra_factory, pre_delay in RUNNERS:
            if pre_delay > 0:
                log("INFO", "group_launch_wait",
                    group=label, delay_sec=pre_delay,
                    note="staggered to avoid WS rate-limiting on shared exchanges")
                await asyncio.sleep(pre_delay)
            extra = extra_factory(spread_delay, with_buckets)
            proc  = await asyncio.create_subprocess_exec(
                sys.executable,
                str(script_path),
                *extra,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            procs.append(proc)
            set_affinity(proc.pid, cores, label, log)
            log("INFO", "group_launched",
                group=label, pid=proc.pid, cores=_cores_str(cores))
            tasks.append(asyncio.create_task(
                read_pipe(proc, label, state, log_writer, echo=no_dash)
            ))

    if no_dash:
        try:
            tasks.append(asyncio.create_task(_launch_all()))
            tasks.append(asyncio.create_task(redis_stats_loop(state)))
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            await _shutdown()
        return

    start    = time.time()
    log_file = log_writer.current_name()
    with Live(
        build_layout(state, 0, log_file),
        console=console,
        refresh_per_second=1,
        screen=True,
    ) as live:
        tasks.append(asyncio.create_task(dashboard_loop(state, live, start, log_writer)))
        tasks.append(asyncio.create_task(_launch_all()))
        tasks.append(asyncio.create_task(redis_stats_loop(state)))
        try:
            await asyncio.gather(*tasks)
        except KeyboardInterrupt:
            await _shutdown()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run all market data collectors")
    parser.add_argument("--buckets", action="store_true",
                        help="Enable age-bucket distribution in staleness_monitor")
    parser.add_argument("--no-dash", action="store_true",
                        help="Disable live dashboard, output JSON logs only")
    parser.add_argument("--delay", type=float, default=None,
                        help="Delay in seconds before signals/snapshots start")
    args = parser.parse_args()

    if args.delay is not None:
        spread_delay = max(0.0, args.delay)
    elif sys.stdin.isatty():
        try:
            raw = input("Delay before signals/snapshots [seconds, Enter = 0]: ").strip()
            spread_delay = max(0.0, float(raw)) if raw else 0.0
        except (ValueError, EOFError):
            spread_delay = 0.0
    else:
        spread_delay = 0.0

    try:
        asyncio.run(main(args.buckets, args.no_dash, spread_delay))
    except KeyboardInterrupt:
        pass
