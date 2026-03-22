#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ob_hist_writer.py — shared order-book history writer for all OB collectors.

Key scheme:  ob:hist:{ex}:{mkt}:{sym}:{chunk_id}   ZSET
  member:    b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
  score:     ts_ms

Config key:  ob:hist:config   STRING (orjson)

Chunks are 20-min windows.  chunk_id = int(unix_sec // CHUNK_SEC).
Keep MAX_CHUNKS=4 chunks (80 min window, always covers last 60 min).
Throttle: at most 1 write per second per symbol.
"""

import time

import orjson

CHUNK_SEC   = 1200      # 20 minutes per chunk
MAX_CHUNKS  = 4         # keep 4 chunks, delete oldest when starting 5th
SAMPLE_SEC  = 1         # max 1 write per second per symbol
LEVELS      = 10
CONFIG_KEY  = b"ob:hist:config"


def chunk_id(ts_sec: float) -> int:
    return int(ts_sec // CHUNK_SEC)


def hist_key(ex: str, mkt: str, sym: str, cid: int) -> bytes:
    return f"ob:hist:{ex}:{mkt}:{sym}:{cid}".encode()


def _enc(v) -> bytes:
    """Ensure value is bytes."""
    if isinstance(v, bytes):
        return v
    return str(v).encode()


class OBHistWriter:
    """
    One instance per collector (ex + mkt pair).
    Call add_to_pipe() and ensure_config() inside your existing pipeline flush.

    batch items: (key_bytes, mapping_dict)
      key_bytes format: ob:{ex}:{mkt}:{sym}
      mapping_dict keys: b"b1"..b"b10", b"bq1"..b"bq10",
                         b"a1"..b"a10", b"aq1"..b"aq10", b"t"
    """

    def __init__(self, ex: str, mkt: str, symbols: list) -> None:
        self.ex       = ex
        self.mkt      = mkt
        self.symbols  = symbols
        self._cid     = chunk_id(time.time())
        self._last_sec: dict = {}   # sym -> last written ts_sec (int)
        self._dirty   = True        # write config on first flush

    def add_to_pipe(self, pipe, batch: list, now_sec: float) -> None:
        new_cid = chunk_id(now_sec)

        if new_cid != self._cid:
            # Chunk rolled over — delete the chunk that falls outside the window
            old_cid = new_cid - MAX_CHUNKS
            for sym in self.symbols:
                pipe.delete(hist_key(self.ex, self.mkt, sym, old_cid))
            self._cid   = new_cid
            self._dirty = True

        ts_sec_now = int(now_sec)
        ts_ms      = int(now_sec * 1000)

        for key_b, mapping in batch:
            # key format: ob:{ex}:{mkt}:{sym}
            parts = key_b.split(b":", 3)
            if len(parts) != 4:
                continue
            sym = parts[3].decode()

            # Throttle: max 1 sample per second per symbol
            if self._last_sec.get(sym) == ts_sec_now:
                continue
            self._last_sec[sym] = ts_sec_now

            # Build member: b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms
            fields = []
            for i in range(1, LEVELS + 1):
                fields.append(_enc(mapping.get(f"b{i}".encode(),  b"")))
                fields.append(_enc(mapping.get(f"bq{i}".encode(), b"")))
            for i in range(1, LEVELS + 1):
                fields.append(_enc(mapping.get(f"a{i}".encode(),  b"")))
                fields.append(_enc(mapping.get(f"aq{i}".encode(), b"")))
            fields.append(str(ts_ms).encode())

            val = b"|".join(fields)
            pipe.zadd(hist_key(self.ex, self.mkt, sym, new_cid), {val: ts_ms})

    def ensure_config(self, pipe, now_sec: float) -> None:
        """Write ob:hist:config to the pipeline. No-op unless dirty."""
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
        config = {
            "chunk_sec":       CHUNK_SEC,
            "max_chunks":      MAX_CHUNKS,
            "sample_sec":      SAMPLE_SEC,
            "levels":          LEVELS,
            "active_chunk_id": cid,
            "chunks":          chunks,
            "key_pattern":     "ob:hist:{ex}:{mkt}:{sym}:{chunk_id}",
            "member_format":   "b1|bq1|b2|bq2|...|b10|bq10|a1|aq1|...|a10|aq10|ts_ms",
            "sources":         ["bn:s","bn:f","bb:s","bb:f","ok:s","ok:f","gt:s","gt:f","bg:s","bg:f"],
        }
        pipe.set(CONFIG_KEY, orjson.dumps(config))
        self._dirty = False
