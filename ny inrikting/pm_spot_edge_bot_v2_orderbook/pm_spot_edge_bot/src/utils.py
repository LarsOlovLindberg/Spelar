"C:\Users\lars-\pm_spot_edge_bot\src\utils.py"
from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable, List
from dateutil import parser

def parse_ts(ts_iso: str) -> datetime:
    dt = parser.isoparse(ts_iso)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new / old - 1.0) * 100.0

def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def rolling_last(values: List[float], n: int) -> List[float]:
    return values[-n:] if len(values) >= n else values[:]
