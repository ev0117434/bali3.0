#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
redis_monitor.py — мониторинг производительности и деградации Redis.

Fast-check каждые 5s:
  - PING latency (RTT до Redis)
  - Write benchmark (реальная задержка pipeline.execute())
  - used_memory / maxmemory (% заполнения)
  - connected_clients / blocked_clients
  - rejected_connections (OOM-индикатор)
  - mem_fragmentation_ratio

Slowlog-check каждые 60s:
  - новые записи SLOWLOG (команды > 10 ms)

Events:
  startup, redis_connected,
  stats (каждые 5s),
  degraded (WARN — при превышении порога),
  recovered (INFO — при возврате в норму),
  slowlog (WARN — новые медленные команды)
"""

import asyncio
import sys
import time

import orjson
import redis.asyncio as aioredis

SCRIPT     = "redis_monitor"
REDIS_SOCK = "/var/run/redis/redis.sock"

FAST_SEC  = 5    # интервал fast-check
SLOW_SEC  = 60   # интервал slowlog-check

# Пороги
PING_WARN_MS  = 6.0
PING_CRIT_MS  = 30.0
WRITE_WARN_MS = 15.0
WRITE_CRIT_MS = 60.0
MEM_WARN_PCT  = 0.70   # 70%
MEM_CRIT_PCT  = 0.85   # 85%
FRAG_WARN     = 1.5
FRAG_CRIT     = 2.0
SLOWLOG_US    = 10_000  # команды > 10 ms попадают в SLOWLOG


def log(lvl: str, event: str, **kw) -> None:
    rec = {"ts": time.time(), "lvl": lvl, "script": SCRIPT, "event": event, **kw}
    sys.stdout.buffer.write(orjson.dumps(rec) + b"\n")
    sys.stdout.buffer.flush()


async def main() -> None:
    log("INFO", "startup",
        fast_interval_sec=FAST_SEC,
        slow_interval_sec=SLOW_SEC,
        thresholds={
            "ping_warn_ms":  PING_WARN_MS,
            "ping_crit_ms":  PING_CRIT_MS,
            "write_warn_ms": WRITE_WARN_MS,
            "write_crit_ms": WRITE_CRIT_MS,
            "mem_warn_pct":  MEM_WARN_PCT * 100,
            "mem_crit_pct":  MEM_CRIT_PCT * 100,
            "frag_warn":     FRAG_WARN,
            "frag_crit":     FRAG_CRIT,
        })

    r = aioredis.Redis(unix_socket_path=REDIS_SOCK, decode_responses=True)
    log("INFO", "redis_connected", socket=REDIS_SOCK)

    prev_rejected  = 0
    prev_slowlog_id = -1
    last_slow_check = 0.0
    degraded_flags  = set()

    try:
        while True:
            t_loop = time.monotonic()

            # ── PING latency ──────────────────────────────────────────────────
            t0 = time.monotonic()
            await r.ping()
            ping_ms = (time.monotonic() - t0) * 1000

            # ── Write benchmark (один SET через pipeline) ─────────────────────
            t0 = time.monotonic()
            async with r.pipeline(transaction=False) as pipe:
                pipe.set("redis_monitor:bench", "1", ex=30)
                await pipe.execute()
            write_ms = (time.monotonic() - t0) * 1000

            # ── INFO memory / stats / clients ─────────────────────────────────
            info_mem     = await r.info("memory")
            info_stats   = await r.info("stats")
            info_clients = await r.info("clients")

            used_mb  = info_mem.get("used_memory",               0) / 1_048_576
            max_mb   = info_mem.get("maxmemory",                  0) / 1_048_576
            frag     = info_mem.get("mem_fragmentation_ratio",  1.0)
            mem_pct  = used_mb / max_mb if max_mb > 0 else 0.0

            clients  = info_clients.get("connected_clients",  0)
            blocked  = info_clients.get("blocked_clients",    0)

            ops_sec        = info_stats.get("instantaneous_ops_per_sec", 0)
            rejected_total = int(info_stats.get("rejected_connections",  0))
            new_rejected   = max(0, rejected_total - prev_rejected)
            prev_rejected  = rejected_total

            # ── Determine degradation flags ───────────────────────────────────
            flags = set()

            if ping_ms >= PING_CRIT_MS:
                flags.add("ping_crit")
            elif ping_ms >= PING_WARN_MS:
                flags.add("ping_warn")

            if write_ms >= WRITE_CRIT_MS:
                flags.add("write_crit")
            elif write_ms >= WRITE_WARN_MS:
                flags.add("write_warn")

            if mem_pct >= MEM_CRIT_PCT:
                flags.add("mem_crit")
            elif mem_pct >= MEM_WARN_PCT:
                flags.add("mem_warn")

            if blocked > 0:
                flags.add("blocked_clients")

            if new_rejected > 0:
                flags.add("oom_rejected")

            if frag >= FRAG_CRIT:
                flags.add("frag_crit")
            elif frag >= FRAG_WARN:
                flags.add("frag_warn")

            # ── Emit degraded / recovered events ──────────────────────────────
            for f in flags - degraded_flags:
                log("WARN", "degraded", flag=f,
                    ping_ms=round(ping_ms,  3),
                    write_ms=round(write_ms, 3),
                    mem_pct=round(mem_pct * 100, 1),
                    mem_mb=round(used_mb, 1),
                    frag=round(frag, 2),
                    blocked=blocked,
                    new_rejected=new_rejected)

            for f in degraded_flags - flags:
                log("INFO", "recovered", flag=f)

            degraded_flags = flags

            # ── Overall log level ─────────────────────────────────────────────
            crit_flags = {"ping_crit", "write_crit", "mem_crit", "blocked_clients", "oom_rejected"}
            if flags & crit_flags:
                lvl = "ERROR"
            elif flags:
                lvl = "WARN"
            else:
                lvl = "INFO"

            log(lvl, "stats",
                ping_ms=round(ping_ms,   3),
                write_ms=round(write_ms,  3),
                mem_mb=round(used_mb,   1),
                max_mb=round(max_mb,    1),
                mem_pct=round(mem_pct * 100, 1),
                frag=round(frag,      2),
                clients=clients,
                blocked=blocked,
                ops_sec=ops_sec,
                rejected_new=new_rejected,
                rejected_total=rejected_total,
                flags=sorted(flags) if flags else [])

            # ── Slowlog check (every SLOW_SEC) ────────────────────────────────
            now = time.time()
            if now - last_slow_check >= SLOW_SEC:
                last_slow_check = now
                try:
                    slowlog = await r.slowlog_get(20)
                    new_entries = [e for e in slowlog
                                   if e.get("id", 0) > prev_slowlog_id]
                    if new_entries:
                        prev_slowlog_id = new_entries[0].get("id", prev_slowlog_id)
                        for entry in new_entries:
                            cmd_args = entry.get("command", [])
                            log("WARN", "slowlog",
                                cmd=" ".join(str(a) for a in cmd_args[:4]),
                                duration_ms=round(entry.get("duration", 0) / 1000, 1),
                                ts_cmd=entry.get("start_time", 0))
                except Exception:
                    pass

            elapsed = time.monotonic() - t_loop
            await asyncio.sleep(max(0.0, FAST_SEC - elapsed))

    except asyncio.CancelledError:
        pass
    finally:
        await r.aclose()


if __name__ == "__main__":
    asyncio.run(main())
