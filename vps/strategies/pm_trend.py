from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new / old - 1.0) * 100.0


def _hist_list() -> list[tuple[datetime, float]]:
    return []


@dataclass
class PmTrendHistory:
    pm: list[tuple[datetime, float]] = field(default_factory=_hist_list)

    def add(self, *, ts: datetime, pm_price: float, max_len: int) -> None:
        self.pm.append((ts, pm_price))
        if max_len > 0 and len(self.pm) > max_len:
            self.pm = self.pm[-max_len:]


@dataclass(frozen=True)
class PmTrendSnapshot:
    pm_ret_pct: float


class PmTrendEngine:
    """PM-only trend/momentum calculator.

    Keeps a rolling mid-price history per key and computes a simple % return
    over `lookback_points`.
    """

    def __init__(self) -> None:
        self._hist: dict[str, PmTrendHistory] = {}

    def update_and_compute(
        self,
        *,
        key: str,
        ts: datetime,
        pm_mid_price: float,
        lookback_points: int,
    ) -> Optional[PmTrendSnapshot]:
        h = self._hist.get(key)
        if h is None:
            h = PmTrendHistory()
            self._hist[key] = h

        h.add(ts=ts, pm_price=pm_mid_price, max_len=max(lookback_points * 3, 50))

        n = int(lookback_points)
        if n <= 0:
            return None

        # Need at least (lookback_points + 1) samples to compute a return.
        # Example: lookback_points=1 compares the last two points.
        if len(h.pm) < (n + 1):
            return None

        old = h.pm[-(n + 1)][1]
        now = h.pm[-1][1]
        ret = pct_change(old, now)
        return PmTrendSnapshot(pm_ret_pct=float(ret))
