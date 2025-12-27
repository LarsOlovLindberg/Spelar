"C:\Users\lars-\pm_spot_edge_bot\src\models.py"
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime

@dataclass(frozen=True)
class Tick:
    ts: datetime
    price: float

@dataclass
class Position:
    side: str  # YES or NO
    entry_ts: datetime
    entry_price: float
    last_price: float
    is_open: bool = True

@dataclass(frozen=True)
class Trade:
    side: str
    entry_ts: datetime
    exit_ts: datetime
    entry_price: float
    exit_price: float

    @property
    def pnl_pct(self) -> float:
        # approximate (pm share price changes)
        return (self.exit_price / self.entry_price - 1.0) * 100.0
