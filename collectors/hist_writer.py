#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
hist_writer.py — shared history writer for all market-data collectors.

Key scheme:  md:hist:{ex}:{mkt}:{sym}:{chunk_id}   ZSET
  member:    {bid}|{ask}|{ts_ms}  (bytes)
  score:     ts_ms (integer milliseconds)

Config key:  md:hist:config   STRING (orjson)

Chunks are 20-min windows.  chunk_id = int(unix_sec // CHUNK_SEC).
Keep MAX_CHUNKS=4 chunks (80 min window, always covers last 60 min).
Throttle: at most 1 write per second per symbol to keep memory reasonable.
"""

import time
from typing import Optional

import orjson
import redis.asyncio as aioredis

CHUNK_SEC   = 1200      # 20 minutes per chunk
MAX_CHUNKS  = 4         # keep 4 chunks (delete oldest when starting 5th)
SAMPLE_SEC  = 1         # max 1 write per second per symbol
CONFIG_KEY  = b"md:hist:config"

OB_LEVELS = 10
OB_EMPTY  = "," * (OB_LEVELS * 4 - 1)   # 39 commas → 40 empty CSV fields

# Column names for snapshot CSV (exported, used by spread_monitor)
_s_ob_cols = ",".join(
    [f"s_b{i}"  for i in range(1, OB_LEVELS + 1)] +
    [f"s_bq{i}" for i in range(1, OB_LEVELS + 1)] +
    [f"s_a{i}"  for i in range(1, OB_LEVELS + 1)] +
    [f"s_aq{i}" for i in range(1, OB_LEVELS + 1)]
)
_f_ob_cols = ",".join(
    [f"f_b{i}"  for i in range(1, OB_LEVELS + 1)] +
    [f"f_bq{i}" for i in range(1, OB_LEVELS + 1)] +
    [f"f_a{i}"  for i in range(1, OB_LEVELS + 1)] +
    [f"f_aq{i}" for i in range(1, OB_LEVELS + 1)]
)
SNAPSHOT_CSV_HEADER = (
    f"spot_exch,fut_exch,symbol,ask_spot,bid_futures,spread_pct,ts,"
    f"{_s_ob_cols},{_f_ob_cols}\n"
)


def chunk_id(ts_sec: float) -> int:
    return int(ts_sec // CHUNK_SEC)


def hist_key(ex: str, mkt: str, sym: str, cid: int) -> bytes:
    return f"md:hist:{ex}:{mkt}:{sym}:{cid}".encode()


class HistWriter:
    """
    One instance per collector (ex + mkt pair).
    Call add_to_pipe() and ensure_config() inside your existing pipeline flush.
    """

    def __init__(self, ex: str, mkt: str, symbols: list) -> None:
        self.ex       = ex
        self.mkt      = mkt
        self.symbols  = symbols
        self._cid     = chunk_id(time.time())
        self._last_sec: dict = {}   # sym -> last written ts_sec (int)
        self._dirty   = True        # write config on first flush

    def add_to_pipe(self, pipe, batch: list, now_sec: float) -> None:
        """
        Add ZADD history commands to an open pipeline.
        batch: list of (key_bytes, mapping_dict)  — same format used by collectors.
        Also adds DELETE commands for the expired chunk on rotation.
        """
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
            # Extract sym: key format is  md:{ex}:{mkt}:{sym}
            parts = key_b.split(b":", 3)
            if len(parts) != 4:
                continue
            sym = parts[3].decode()

            # Throttle: max 1 sample per second per symbol
            if self._last_sec.get(sym) == ts_sec_now:
                continue
            self._last_sec[sym] = ts_sec_now

            bid_b = mapping.get(b"b")
            ask_b = mapping.get(b"a")
            if not bid_b or not ask_b:
                continue

            val = bid_b + b"|" + ask_b + b"|" + str(ts_ms).encode()
            pipe.zadd(hist_key(self.ex, self.mkt, sym, new_cid), {val: ts_ms})

    def ensure_config(self, pipe, now_sec: float) -> None:
        """Write md:hist:config to the pipeline. No-op unless dirty (startup or rotation)."""
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
            "active_chunk_id": cid,
            "chunks":          chunks,
            "key_pattern":     "md:hist:{ex}:{mkt}:{sym}:{chunk_id}",
            "sources":         ["bn:s","bn:f","bb:s","bb:f","ok:s","ok:f","gt:s","gt:f"],
        }
        pipe.set(CONFIG_KEY, orjson.dumps(config))
        self._dirty = False


# ── history readers (used by spread_monitor) ─────────────────────────────────

def _ob_hist_key(ex: str, mkt: str, sym: str, cid: int) -> bytes:
    return f"ob:hist:{ex}:{mkt}:{sym}:{cid}".encode()


async def read_ob_history(
    r:         aioredis.Redis,
    ex:        str,
    mkt:       str,
    sym:       str,
    since_sec: float = 3600.0,
) -> dict:
    """
    Return dict  ts_sec -> comma-separated string of 40 OB values
    (b1,bq1,...,b10,bq10,a1,aq1,...,a10,aq10).
    Reads all MAX_CHUNKS ob:hist chunks in one pipeline round-trip.
    """
    now_ms   = int(time.time() * 1000)
    since_ms = now_ms - int(since_sec * 1000)
    cid      = chunk_id(time.time())

    async with r.pipeline(transaction=False) as pipe:
        for i in range(MAX_CHUNKS):
            pipe.zrangebyscore(_ob_hist_key(ex, mkt, sym, cid - i), since_ms, "+inf")
        results = await pipe.execute()

    ob_by_sec: dict = {}
    for chunk_rows in results:
        for entry in chunk_rows:
            parts = entry.split(b"|")
            if len(parts) != 41:          # 40 ob fields + ts_ms
                continue
            try:
                ts_sec = int(parts[40]) // 1000
            except (ValueError, IndexError):
                continue
            ob_by_sec[ts_sec] = ",".join(p.decode() for p in parts[:40])
    return ob_by_sec


async def read_history(
    r:        aioredis.Redis,
    ex:       str,
    mkt:      str,
    sym:      str,
    since_sec: float = 3600.0,
) -> list:
    """
    Return sorted list of (ts_ms: int, bid: str, ask: str) for the last since_sec seconds.
    Reads all MAX_CHUNKS chunks in one pipeline round-trip.
    """
    now_ms    = int(time.time() * 1000)
    since_ms  = now_ms - int(since_sec * 1000)
    cid       = chunk_id(time.time())

    async with r.pipeline(transaction=False) as pipe:
        for i in range(MAX_CHUNKS):
            pipe.zrangebyscore(hist_key(ex, mkt, sym, cid - i), since_ms, "+inf")
        results = await pipe.execute()

    rows = []
    for chunk_rows in results:
        for entry in chunk_rows:
            parts = entry.split(b"|")
            if len(parts) == 3:
                try:
                    rows.append((int(parts[2]), parts[0].decode(), parts[1].decode()))
                except (ValueError, IndexError):
                    pass

    rows.sort(key=lambda x: x[0])
    return rows


async def write_snapshot_history(
    r:          aioredis.Redis,
    fh,                         # writable binary file handle
    spot_ex:    str,
    fut_ex:     str,
    sym:        str,
    spot_name:  str,
    fut_name:   str,
    since_sec:  float = 3600.0,
) -> int:
    """
    Read 1-hour history for spot+futures, match by second bucket,
    compute spread_pct, write CSV rows (with OB data) to fh.
    Returns number of rows written.
    """
    spot_hist = await read_history(r, spot_ex, "s", sym, since_sec)
    fut_hist  = await read_history(r, fut_ex,  "f", sym, since_sec)

    if not spot_hist or not fut_hist:
        return 0

    # OB history — best-effort; missing seconds get empty OB fields
    spot_ob = await read_ob_history(r, spot_ex, "s", sym, since_sec)
    fut_ob  = await read_ob_history(r, fut_ex,  "f", sym, since_sec)

    # Build second-bucket dicts: ts_sec -> ask (spot) / bid (futures)
    spot_ask_by_sec: dict = {}
    for ts_ms, bid, ask in spot_hist:
        spot_ask_by_sec[ts_ms // 1000] = ask

    fut_bid_by_sec: dict = {}
    for ts_ms, bid, ask in fut_hist:
        fut_bid_by_sec[ts_ms // 1000] = bid

    written = 0
    for ts_sec in sorted(set(spot_ask_by_sec) & set(fut_bid_by_sec)):
        try:
            spot_ask = float(spot_ask_by_sec[ts_sec])
            fut_bid  = float(fut_bid_by_sec[ts_sec])
        except (ValueError, TypeError):
            continue
        if spot_ask <= 0 or fut_bid <= 0:
            continue
        spread_r = round((fut_bid - spot_ask) / spot_ask * 100, 4)
        ts_ms_val = ts_sec * 1000
        s_ob = spot_ob.get(ts_sec, OB_EMPTY)
        f_ob = fut_ob.get(ts_sec, OB_EMPTY)
        row = (
            f"{spot_name},{fut_name},{sym},"
            f"{spot_ask_by_sec[ts_sec]},{fut_bid_by_sec[ts_sec]},{spread_r},{ts_ms_val},"
            f"{s_ob},{f_ob}\n"
        ).encode()
        fh.write(row)
        written += 1

    if written:
        fh.flush()
    return written
