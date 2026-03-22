#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
fr_hist_writer.py — shared history writer for funding-rate collectors.

Key scheme:  fr:hist:{ex}:{sym}:{chunk_id}   ZSET
  member:    {r}|{nr}|{ft}|{ts_ms}  (bytes, pipe-separated)
  score:     ts_ms

  r  = current funding rate
  nr = predicted next rate (empty if unavailable)
  ft = next funding settlement time unix ms (empty if unavailable)

Config key:  fr:hist:config   STRING (orjson)

Chunks: 20-min windows, keep 4 (80 min). Throttle: 1 write/sec per symbol.
"""

import time

import orjson

CHUNK_SEC  = 1200
MAX_CHUNKS = 4
SAMPLE_SEC = 1
CONFIG_KEY = b"fr:hist:config"


def chunk_id(ts_sec: float) -> int:
    return int(ts_sec // CHUNK_SEC)


def hist_key(ex: str, sym: str, cid: int) -> bytes:
    return f"fr:hist:{ex}:{sym}:{cid}".encode()


class FrHistWriter:
    """
    One instance per collector (exchange).
    Call add_to_pipe() and ensure_config() inside your existing pipeline flush.

    batch item format: (sym: str, r_b: bytes, nr_b: bytes, ft_b: bytes, ts_b: bytes)
    """

    def __init__(self, ex: str, symbols: list) -> None:
        self.ex      = ex
        self.symbols = symbols
        self._cid    = chunk_id(time.time())
        self._last_sec: dict = {}   # sym -> last written ts_sec (int)
        self._dirty  = True         # write config on first flush

    def add_to_pipe(self, pipe, batch: list, now_sec: float) -> None:
        """
        Add ZADD history commands to an open pipeline.
        batch: list of (sym, r_b, nr_b, ft_b, ts_b)
        Also handles chunk rotation and expired-chunk deletes.
        """
        new_cid = chunk_id(now_sec)
        if new_cid != self._cid:
            old_cid = new_cid - MAX_CHUNKS
            for s in self.symbols:
                pipe.delete(hist_key(self.ex, s, old_cid))
            self._cid   = new_cid
            self._dirty = True

        ts_sec_now = int(now_sec)
        ts_ms      = int(now_sec * 1000)

        for sym, r_b, nr_b, ft_b, _ in batch:
            if self._last_sec.get(sym) == ts_sec_now:
                continue
            self._last_sec[sym] = ts_sec_now
            val = r_b + b"|" + nr_b + b"|" + ft_b + b"|" + str(ts_ms).encode()
            pipe.zadd(hist_key(self.ex, sym, new_cid), {val: ts_ms})

    def ensure_config(self, pipe, now_sec: float) -> None:
        """Write fr:hist:config to the pipeline. No-op unless dirty."""
        if not self._dirty:
            return
        cid    = chunk_id(now_sec)
        chunks = []
        for i in range(MAX_CHUNKS - 1, -1, -1):
            c = cid - i
            if c < 0:
                continue
            chunks.append({
                "id":       c,
                "start_ms": c * CHUNK_SEC * 1000,
                "end_ms":   (c + 1) * CHUNK_SEC * 1000,
                "active":   c == cid,
            })
        pipe.set(CONFIG_KEY, orjson.dumps({
            "chunk_sec":       CHUNK_SEC,
            "max_chunks":      MAX_CHUNKS,
            "sample_sec":      SAMPLE_SEC,
            "active_chunk_id": cid,
            "chunks":          chunks,
            "key_pattern":     "fr:hist:{ex}:{sym}:{chunk_id}",
            "sources":         ["bn", "bb", "ok", "gt", "bg"],
        }))
        self._dirty = False


# ── history reader (used by spread_monitor via hist_writer) ──────────────────

async def read_fr_history(
    r,
    ex:        str,
    sym:       str,
    since_sec: float = 3600.0,
) -> dict:
    """
    Return dict  ts_sec -> (r_str, nr_str, ft_str).
    Reads all MAX_CHUNKS fr:hist chunks in one pipeline round-trip.
    """
    import time as _time
    now_ms   = int(_time.time() * 1000)
    since_ms = now_ms - int(since_sec * 1000)
    cid      = chunk_id(_time.time())

    async with r.pipeline(transaction=False) as pipe:
        for i in range(MAX_CHUNKS):
            pipe.zrangebyscore(hist_key(ex, sym, cid - i), since_ms, "+inf")
        results = await pipe.execute()

    fr_by_sec: dict = {}
    for chunk_rows in results:
        for entry in chunk_rows:
            parts = entry.split(b"|")
            if len(parts) != 4:   # r|nr|ft|ts_ms
                continue
            try:
                ts_sec = int(parts[3]) // 1000
            except (ValueError, IndexError):
                continue
            fr_by_sec[ts_sec] = (
                parts[0].decode(),
                parts[1].decode(),
                parts[2].decode(),
            )
    return fr_by_sec
