from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import math
from typing import Optional


def pct_change(old: float, new: float) -> float:
    if old == 0:
        return 0.0
    return (new / old - 1.0) * 100.0


def _hist_list() -> list[tuple[datetime, float]]:
    return []


@dataclass
class PriceHistory:
    spot: list[tuple[datetime, float]] = field(default_factory=_hist_list)
    pm: list[tuple[datetime, float]] = field(default_factory=_hist_list)

    def add(self, *, ts: datetime, spot_price: float, pm_price: float, max_len: int) -> None:
        self.spot.append((ts, spot_price))
        self.pm.append((ts, pm_price))
        if max_len > 0:
            if len(self.spot) > max_len:
                self.spot = self.spot[-max_len:]
            if len(self.pm) > max_len:
                self.pm = self.pm[-max_len:]


@dataclass(frozen=True)
class LeadLagSnapshot:
    spot_ret_pct: float
    pm_ret_pct: float
    edge_pct: float


@dataclass(frozen=True)
class LeadLagParams:
    side: str  # YES or NO
    lookback_points: int
    spot_move_min_pct: float
    edge_min_pct: float
    edge_exit_pct: float
    max_hold_secs: int
    pm_stop_pct: float
    avoid_price_above: float
    avoid_price_below: float


@dataclass(frozen=True)
class MarketLagEstimate:
    ok: bool
    lag_ms: float | None
    lag_points: int | None
    dt_ms: float | None
    best_corr: float | None
    second_best_corr: float | None
    corr_gap: float | None
    reason: str | None


class LeadLagEngine:
    """Keeps rolling history needed for lead–lag edge computation."""

    def __init__(self) -> None:
        self._hist: dict[str, PriceHistory] = {}

    def update_and_compute(
        self,
        *,
        key: str,
        ts: datetime,
        spot_price: float,
        pm_mid_price: float,
        lookback_points: int,
    ) -> Optional[LeadLagSnapshot]:
        h = self._hist.get(key)
        if h is None:
            h = PriceHistory()
            self._hist[key] = h

        # keep a bit more than lookback
        h.add(ts=ts, spot_price=spot_price, pm_price=pm_mid_price, max_len=max(lookback_points * 3, 50))

        n = lookback_points
        if n <= 1 or len(h.spot) < n or len(h.pm) < n:
            return None

        spot_old = h.spot[-n][1]
        spot_now = h.spot[-1][1]
        pm_old = h.pm[-n][1]
        pm_now = h.pm[-1][1]

        spot_ret = pct_change(spot_old, spot_now)
        pm_ret = pct_change(pm_old, pm_now)

        return LeadLagSnapshot(spot_ret_pct=spot_ret, pm_ret_pct=pm_ret, edge_pct=(spot_ret - pm_ret))

    def estimate_market_lag_ms(
        self,
        *,
        key: str,
        max_lag_points: int = 20,
        min_points: int = 30,
    ) -> Optional[float]:
        """Estimate how many milliseconds PM appears to lag spot.

        Uses a simple lagged-correlation scan on per-tick returns.
        Returns None if there isn't enough signal for a stable estimate.
        """

        est = self.estimate_market_lag(key=key, max_lag_points=max_lag_points, min_points=min_points)
        return float(est.lag_ms) if est.ok and est.lag_ms is not None else None

    def estimate_market_lag(
        self,
        *,
        key: str,
        max_lag_points: int = 20,
        min_points: int = 30,
        min_corr_points: int = 10,
        min_abs_corr: float = 0.20,
        min_corr_gap: float = 0.05,
    ) -> MarketLagEstimate:
        """Return a richer lag estimate with confidence hints.

        Notes:
        - `min_points` is the minimum number of *prices* required.
        - `min_corr_points` is the minimum number of aligned return points per lag candidate.
        """

        h = self._hist.get(key)
        if h is None:
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=None,
                dt_ms=None,
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason="no_history",
            )

        n_prices = min(len(h.spot), len(h.pm))
        if n_prices < max(min_points, 3):
            need_prices = max(min_points, 3)
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=int(n_prices),
                dt_ms=None,
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason=f"not_enough_prices(count={n_prices},need={need_prices})",
            )

        spot = h.spot[-n_prices:]
        pm = h.pm[-n_prices:]
        # Build return series (one per interval)
        s_ret: list[float] = []
        p_ret: list[float] = []
        ts_list: list[datetime] = []
        for i in range(1, n_prices):
            ts_list.append(spot[i][0])
            s_ret.append(pct_change(spot[i - 1][1], spot[i][1]))
            p_ret.append(pct_change(pm[i - 1][1], pm[i][1]))

        if len(s_ret) < max(min_points - 1, 3):
            need_returns = max(min_points - 1, 3)
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=int(len(s_ret)),
                dt_ms=None,
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason=f"not_enough_returns(count={len(s_ret)},need={need_returns})",
            )

        # Approximate tick spacing (ms)
        dts_ms: list[float] = []
        for i in range(1, len(ts_list)):
            dts_ms.append((ts_list[i] - ts_list[i - 1]).total_seconds() * 1000.0)
        if not dts_ms:
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=None,
                dt_ms=None,
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason="no_dt",
            )
        dts_ms_sorted = sorted(dts_ms)
        dt_ms = dts_ms_sorted[len(dts_ms_sorted) // 2]
        if dt_ms <= 0:
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=None,
                dt_ms=None,
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason="bad_dt",
            )

        # Correlation helper
        def _corr(a: list[float], b: list[float]) -> float:
            if len(a) != len(b) or len(a) < 5:
                return float("nan")
            ma = sum(a) / len(a)
            mb = sum(b) / len(b)
            num = 0.0
            da = 0.0
            db = 0.0
            for i in range(len(a)):
                xa = a[i] - ma
                xb = b[i] - mb
                num += xa * xb
                da += xa * xa
                db += xb * xb
            if da <= 0 or db <= 0:
                return float("nan")
            return num / (da**0.5 * db**0.5)

        # Bail out early if there is essentially no movement.
        # This avoids spurious 0ms estimates when the series is flat.
        def _std(xs: list[float]) -> float:
            if not xs:
                return 0.0
            m = sum(xs) / len(xs)
            var = sum((x - m) * (x - m) for x in xs) / max(len(xs) - 1, 1)
            return math.sqrt(var) if var > 0 else 0.0

        if _std(s_ret) < 1e-9 or _std(p_ret) < 1e-9:
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=len(s_ret),
                dt_ms=float(dt_ms),
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason="low_variance",
            )

        best_lag = None
        best_c = float("-inf")
        second_c = float("-inf")
        max_lag = max(0, int(max_lag_points))
        for lag in range(0, max_lag + 1):
            if lag == 0:
                a = s_ret
                b = p_ret
            else:
                a = s_ret[:-lag]
                b = p_ret[lag:]

            # Require enough aligned return points *for this lag*.
            if len(a) != len(b):
                continue
            if len(a) < max(int(min_corr_points), 5):
                continue
            c = _corr(a, b)
            if not (c == c):
                continue
            if c > best_c:
                second_c = best_c
                best_c = c
                best_lag = lag
            elif c > second_c:
                second_c = c

        if best_lag is None or best_c == float("-inf"):
            return MarketLagEstimate(
                ok=False,
                lag_ms=None,
                lag_points=None,
                dt_ms=float(dt_ms),
                best_corr=None,
                second_best_corr=None,
                corr_gap=None,
                reason="no_valid_corr",
            )

        lag_ms = float(best_lag) * float(dt_ms)
        gap = None
        if second_c != float("-inf"):
            gap = float(best_c - second_c)

        ok = (abs(float(best_c)) >= float(min_abs_corr)) and (gap is None or float(gap) >= float(min_corr_gap))
        reason = None
        if not ok:
            if abs(float(best_c)) < float(min_abs_corr):
                reason = f"corr_too_low<{min_abs_corr}"
            else:
                reason = f"corr_gap_too_low<{min_corr_gap}"

        return MarketLagEstimate(
            ok=bool(ok),
            lag_ms=float(lag_ms),
            lag_points=int(len(s_ret) - int(best_lag)),
            dt_ms=float(dt_ms),
            best_corr=float(best_c),
            second_best_corr=float(second_c) if second_c != float("-inf") else None,
            corr_gap=float(gap) if gap is not None else None,
            reason=reason,
        )

    def estimate_spot_noise_pct(
        self,
        *,
        key: str,
        window_points: int = 40,
        min_points: int = 10,
    ) -> Optional[float]:
        """Estimate spot return noise (std dev, in percent points).

        This is intended for adaptive move-thresholding: require a move larger
        than recent spot micro-noise.
        """

        h = self._hist.get(key)
        if h is None:
            return None

        n_prices = len(h.spot)
        if n_prices < max(min_points + 1, 3):
            return None

        n_take = min(n_prices, max(int(window_points) + 1, min_points + 1))
        spot = h.spot[-n_take:]

        rets: list[float] = []
        for i in range(1, len(spot)):
            rets.append(pct_change(spot[i - 1][1], spot[i][1]))

        if len(rets) < int(min_points):
            return None

        m = sum(rets) / len(rets)
        var = sum((x - m) * (x - m) for x in rets) / max(len(rets) - 1, 1)
        if var < 0:
            return None
        return math.sqrt(var)

    @staticmethod
    def compute_edge_for_side(*, side: str, snap: LeadLagSnapshot) -> float:
        """YES: edge = spot_ret - pm_ret

        NO: interpret a downward spot move as positive and compare vs pm.
        """
        s = side.strip().upper()
        if s == "YES":
            return snap.edge_pct
        # NO-bias: spot down is good => invert spot_ret
        return (-snap.spot_ret_pct) - snap.pm_ret_pct
