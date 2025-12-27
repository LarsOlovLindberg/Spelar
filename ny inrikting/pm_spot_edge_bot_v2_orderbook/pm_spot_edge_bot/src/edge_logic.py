"C:\Users\lars-\pm_spot_edge_bot\src\edge_logic.py"
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional, Tuple

from .utils import pct_change
from .models import Position, Trade

@dataclass(frozen=True)
class EdgeSnapshot:
    spot_ret_pct: float
    pm_ret_pct: float
    edge_pct: float
    spot_now: float
    pm_now: float

class EdgeTrader:
    """
    Lead–lag logic:
      - Compute spot_ret over lookback
      - Compute pm_ret over lookback
      - edge = spot_ret - pm_ret (YES-bias). For NO-bias, invert sign.
    """
    def __init__(
        self,
        side: str,
        lookback_points: int,
        spot_move_min_pct: float,
        edge_min_pct: float,
        edge_exit_pct: float,
        max_hold_secs: int,
        pm_stop_pct: float,
        avoid_price_above: float,
        avoid_price_below: float,
    ) -> None:
        self.side = side.upper()
        self.lookback_points = lookback_points
        self.spot_move_min_pct = spot_move_min_pct
        self.edge_min_pct = edge_min_pct
        self.edge_exit_pct = edge_exit_pct
        self.max_hold_secs = max_hold_secs
        self.pm_stop_pct = pm_stop_pct
        self.avoid_price_above = avoid_price_above
        self.avoid_price_below = avoid_price_below

        self.spot_hist: List[Tuple[datetime, float]] = []
        self.pm_hist: List[Tuple[datetime, float]] = []

        self.position: Optional[Position] = None
        self.trades: List[Trade] = []

    def on_tick(self, ts: datetime, spot_price: float, pm_price: float) -> Optional[Trade]:
        self.spot_hist.append((ts, spot_price))
        self.pm_hist.append((ts, pm_price))

        snap = self._compute_edge()
        if snap is None:
            return None

        if self.position is None:
            if self._should_enter(ts, snap):
                self.position = Position(
                    side=self.side,
                    entry_ts=ts,
                    entry_price=pm_price,
                    last_price=pm_price,
                    is_open=True,
                )
                return None
            return None

        # Update last price
        self.position.last_price = pm_price

        # Exit rules
        if self._should_exit(ts, snap):
            tr = Trade(
                side=self.position.side,
                entry_ts=self.position.entry_ts,
                exit_ts=ts,
                entry_price=self.position.entry_price,
                exit_price=pm_price,
            )
            self.trades.append(tr)
            self.position = None
            return tr

        return None

    def _compute_edge(self) -> Optional[EdgeSnapshot]:
        n = self.lookback_points
        if len(self.spot_hist) < n or len(self.pm_hist) < n:
            return None
        spot_old = self.spot_hist[-n][1]
        spot_now = self.spot_hist[-1][1]
        pm_old = self.pm_hist[-n][1]
        pm_now = self.pm_hist[-1][1]

        spot_ret = pct_change(spot_old, spot_now)
        pm_ret = pct_change(pm_old, pm_now)

        # For NO, a downward spot move should be positive "edge" when pm lags.
        if self.side == "YES":
            edge = spot_ret - pm_ret
        else:
            edge = (-spot_ret) - pm_ret  # interpret spot down as "positive" for NO

        return EdgeSnapshot(
            spot_ret_pct=spot_ret,
            pm_ret_pct=pm_ret,
            edge_pct=edge,
            spot_now=spot_now,
            pm_now=pm_now,
        )

    def _price_ok(self, pm_price: float) -> bool:
        return (pm_price <= self.avoid_price_above) and (pm_price >= self.avoid_price_below)

    def _should_enter(self, ts: datetime, snap: EdgeSnapshot) -> bool:
        if not self._price_ok(snap.pm_now):
            return False

        # need a meaningful spot move in the direction that supports our side
        if self.side == "YES":
            if snap.spot_ret_pct < self.spot_move_min_pct:
                return False
        else:
            if (-snap.spot_ret_pct) < self.spot_move_min_pct:
                return False

        return snap.edge_pct >= self.edge_min_pct

    def _should_exit(self, ts: datetime, snap: EdgeSnapshot) -> bool:
        assert self.position is not None

        hold_secs = (ts - self.position.entry_ts).total_seconds()
        if hold_secs >= self.max_hold_secs:
            return True

        # pm caught up
        if snap.edge_pct <= self.edge_exit_pct:
            return True

        # optional stop on pm adverse move
        if self.pm_stop_pct and self.pm_stop_pct > 0:
            pm_move_pct = pct_change(self.position.entry_price, snap.pm_now)
            if pm_move_pct <= -abs(self.pm_stop_pct):
                return True

        return False
