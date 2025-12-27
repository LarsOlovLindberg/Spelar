"C:\Users\lars-\pm_spot_edge_bot\src\config.py"
from __future__ import annotations

from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    # --- Live market selection
    kraken_pair: str = "XBTUSD"
    pm_market_slug: str = ""
    pm_side: str = "YES"  # YES or NO

    # --- Timing
    poll_secs: float = 5.0
    lookback_points: int = 6  # with poll_secs=5 => 30s lookback

    # --- Edge thresholds (percent units, e.g. 0.30 means 0.30%)
    spot_move_min_pct: float = 0.25
    edge_min_pct: float = 0.20
    edge_exit_pct: float = 0.05

    # --- Risk controls
    max_hold_secs: int = 180  # force exit after N seconds
    pm_stop_pct: float = 0.25  # optional; 0 disables if set to 0

    # --- Avoid "too-late" prices (binary tail risk zones)
    # For YES, avoid buying above 0.90; for NO, avoid buying above 0.90 (i.e., side too expensive).
    avoid_price_above: float = 0.90
    avoid_price_below: float = 0.02  # avoid near-zero (bad fills / weird books)

    # --- Orderbook sizing (CLOB /book)
    enable_orderbook_sizing: bool = True
    slippage_cap: float = 0.01                 # absolute price band, e.g. 0.01 = 1 cent
    max_fraction_of_band_liquidity: float = 0.10
    hard_cap_usdc: float = 2000.0

    # --- Backtest
    is_backtest: bool = False

    @staticmethod
    def from_env(backtest: bool = False) -> "Settings":
        def f(name: str, default: float) -> float:
            v = os.getenv(name)
            return default if v is None or v.strip() == "" else float(v)

        def i(name: str, default: int) -> int:
            v = os.getenv(name)
            return default if v is None or v.strip() == "" else int(v)

        def s(name: str, default: str) -> str:
            v = os.getenv(name)
            return default if v is None or v.strip() == "" else v.strip()

        return Settings(
            kraken_pair=s("KRAKEN_PAIR", "XBTUSD"),
            pm_market_slug=s("PM_MARKET_SLUG", ""),
            pm_side=s("PM_SIDE", "YES").upper(),
            poll_secs=f("POLL_SECS", 5.0),
            lookback_points=i("LOOKBACK_POINTS", 6),
            spot_move_min_pct=f("SPOT_MOVE_MIN_PCT", 0.25),
            edge_min_pct=f("EDGE_MIN_PCT", 0.20),
            edge_exit_pct=f("EDGE_EXIT_PCT", 0.05),
            max_hold_secs=i("MAX_HOLD_SECS", 180),
            pm_stop_pct=f("PM_STOP_PCT", 0.25),
            avoid_price_above=f("AVOID_PRICE_ABOVE", 0.90),
            avoid_price_below=f("AVOID_PRICE_BELOW", 0.02),
            enable_orderbook_sizing=(s("ENABLE_ORDERBOOK_SIZING", "1") not in ("0","false","False")),
            slippage_cap=f("SLIPPAGE_CAP", 0.01),
            max_fraction_of_band_liquidity=f("MAX_FRACTION_OF_BAND_LIQUIDITY", 0.10),
            hard_cap_usdc=f("HARD_CAP_USDC", 2000.0),
            is_backtest=backtest,
        )
