from __future__ import annotations

# pyright: reportUnusedImport=false, reportUnusedVariable=false, reportUnusedFunction=false

import csv
import json
import os
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from ftplib import FTP
from pathlib import Path
from typing import Any, cast

from vps.connectors.kraken_public import fetch_public_snapshot as fetch_kraken_public
from vps.connectors.polymarket_public import fetch_public_snapshot as fetch_pm_public
from vps.connectors.kraken_futures_api import KrakenFuturesApi, KrakenFuturesKeys
from vps.connectors.deribit_options_public import DeribitOptionsPublic
from vps.connectors.polymarket_gamma import PolymarketGammaPublic
from vps.connectors.polymarket_clob_public import PolymarketClobPublic, best_bid_ask
from vps.connectors.kraken_spot_public import KrakenSpotPublic
from vps.strategies.lead_lag import LeadLagEngine
from vps.connectors.polymarket_clob_trading import (
    PolymarketClobApiCreds,
    PolymarketClobLiveConfig,
    cancel_all_orders as pm_cancel_all_orders,
    cancel_token_orders as pm_cancel_token_orders,
    get_open_orders as pm_get_open_orders,
    make_live_client as pm_make_live_client,
    post_limit_order as pm_post_limit_order,
)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _coerce_float(x: Any) -> float | None:
    try:
        return float(x)
    except Exception:
        return None


def _safe_top_levels(side: Any, *, max_levels: int) -> list[dict[str, float]]:
    if not isinstance(side, list):
        return []
    out: list[dict[str, float]] = []
    side_list = cast(list[Any], side)
    for item_any in side_list[: max_levels if max_levels > 0 else 0]:
        if not isinstance(item_any, dict):
            continue
        item = cast(dict[str, Any], item_any)
        price = _coerce_float(item.get("price") if "price" in item else item.get("p"))
        size = _coerce_float(item.get("size") if "size" in item else item.get("s"))
        if price is None:
            continue
        out.append({"price": float(price), "size": float(size or 0.0)})
    return out


def _percentile_sorted(sorted_vals: list[float], p: float) -> float | None:
    if not sorted_vals:
        return None
    if p <= 0:
        return float(sorted_vals[0])
    if p >= 100:
        return float(sorted_vals[-1])
    # Nearest-rank style index.
    k = int(round((p / 100.0) * (len(sorted_vals) - 1)))
    k = max(0, min(len(sorted_vals) - 1, k))
    return float(sorted_vals[k])


@dataclass
class RollingWindow:
    maxlen: int

    def __post_init__(self) -> None:
        self._vals: deque[float] = deque(maxlen=int(self.maxlen))

    def add(self, x: float | None) -> None:
        if x is None:
            return
        try:
            fx = float(x)
        except Exception:
            return
        if not (fx == fx):
            return
        self._vals.append(fx)

    def snapshot(self) -> dict[str, Any]:
        vals = list(self._vals)
        if not vals:
            return {
                "count": 0,
                "mean": None,
                "min": None,
                "max": None,
                "p50": None,
                "p95": None,
                "p99": None,
            }
        vals_sorted = sorted(vals)
        total = sum(vals)
        return {
            "count": int(len(vals)),
            "mean": float(total / max(len(vals), 1)),
            "min": float(vals_sorted[0]),
            "max": float(vals_sorted[-1]),
            "p50": _percentile_sorted(vals_sorted, 50.0),
            "p95": _percentile_sorted(vals_sorted, 95.0),
            "p99": _percentile_sorted(vals_sorted, 99.0),
        }


@dataclass
class LatencyTracker:
    max_points: int = 600

    def __post_init__(self) -> None:
        self.tick_total_ms = RollingWindow(self.max_points)
        self.kraken_spot_fetch_ms = RollingWindow(self.max_points)
        self.pm_orderbook_fetch_ms = RollingWindow(self.max_points)
        self.gamma_fetch_ms = RollingWindow(self.max_points)
        self.kraken_futures_public_fetch_ms = RollingWindow(self.max_points)
        self.kraken_futures_private_fetch_ms = RollingWindow(self.max_points)

    def record_tick_total(self, ms: float | None) -> None:
        self.tick_total_ms.add(ms)

    def record_spot_fetch(self, ms: float | None) -> None:
        self.kraken_spot_fetch_ms.add(ms)

    def record_orderbook_fetch(self, ms: float | None) -> None:
        self.pm_orderbook_fetch_ms.add(ms)

    def record_gamma_fetch(self, ms: float | None) -> None:
        self.gamma_fetch_ms.add(ms)

    def record_kraken_futures_public_fetch(self, ms: float | None) -> None:
        self.kraken_futures_public_fetch_ms.add(ms)

    def record_kraken_futures_private_fetch(self, ms: float | None) -> None:
        self.kraken_futures_private_fetch_ms.add(ms)

    def snapshot(self) -> dict[str, Any]:
        return {
            "tick_total_ms": self.tick_total_ms.snapshot(),
            "kraken_spot_fetch_ms": self.kraken_spot_fetch_ms.snapshot(),
            "pm_orderbook_fetch_ms": self.pm_orderbook_fetch_ms.snapshot(),
            "gamma_fetch_ms": self.gamma_fetch_ms.snapshot(),
            "kraken_futures_public_fetch_ms": self.kraken_futures_public_fetch_ms.snapshot(),
            "kraken_futures_private_fetch_ms": self.kraken_futures_private_fetch_ms.snapshot(),
        }


@dataclass
class RuntimeCache:
    """Cross-tick caches to keep the lead–lag loop fast."""

    gamma_market_by_slug: dict[str, Any] = field(default_factory=lambda: cast(dict[str, Any], {}))
    token_id_by_slug_outcome: dict[tuple[str, str], str] = field(default_factory=lambda: cast(dict[tuple[str, str], str], {}))

    kraken_futures_public_snapshot: dict[str, Any] | None = None
    kraken_futures_public_fetched_at_ms: int = 0

    kraken_futures_private_snapshot: dict[str, Any] | None = None
    kraken_futures_private_fetched_at_ms: int = 0


@dataclass
class LeadLagHealthTracker:
    max_points: int = 2000
    max_decisions: int = 2000

    def __post_init__(self) -> None:
        self.edge_raw_pct = RollingWindow(self.max_points)
        self.edge_abs_pct = RollingWindow(self.max_points)
        self.net_edge_pct = RollingWindow(self.max_points)
        self.spread_cost_pct = RollingWindow(self.max_points)
        self.lag_ms = RollingWindow(self.max_points)
        self.spot_ret_abs_pct = RollingWindow(self.max_points)
        self.max_usdc = RollingWindow(self.max_points)

        self._reasons: deque[str] = deque(maxlen=int(self.max_decisions))
        self._exec_statuses: deque[str] = deque(maxlen=int(self.max_decisions))

        self.last: dict[str, Any] = {}

    def record(
        self,
        *,
        market: str,
        token_id: str,
        edge_pct: float | None,
        net_edge_pct: float | None,
        spread_cost_pct: float | None,
        lag_ms: float | None,
        spot_ret_pct: float | None,
        max_usdc: float | None,
        execution_status: str | None,
        reason: str | None,
    ) -> None:
        self.edge_raw_pct.add(edge_pct)
        self.edge_abs_pct.add(abs(float(edge_pct)) if edge_pct is not None else None)
        self.net_edge_pct.add(net_edge_pct)
        self.spread_cost_pct.add(spread_cost_pct)
        self.lag_ms.add(lag_ms)
        self.spot_ret_abs_pct.add(abs(float(spot_ret_pct)) if spot_ret_pct is not None else None)
        self.max_usdc.add(max_usdc)

        if reason:
            self._reasons.append(str(reason))
        if execution_status:
            self._exec_statuses.append(str(execution_status))

        self.last = {
            "market": market,
            "token_id": token_id,
            "edge_pct": float(edge_pct) if edge_pct is not None else None,
            "net_edge_pct": float(net_edge_pct) if net_edge_pct is not None else None,
            "spread_cost_pct": float(spread_cost_pct) if spread_cost_pct is not None else None,
            "lag_ms": float(lag_ms) if lag_ms is not None else None,
            "spot_ret_pct": float(spot_ret_pct) if spot_ret_pct is not None else None,
            "max_usdc": float(max_usdc) if max_usdc is not None else None,
            "execution_status": str(execution_status) if execution_status is not None else None,
            "reason": str(reason) if reason is not None else None,
        }

    def snapshot(self, *, ts: str, cfg: Config, pm_status: dict[str, Any] | None = None) -> dict[str, Any]:
        reasons = Counter(self._reasons)
        execs = Counter(self._exec_statuses)

        actions: list[str] = []
        spread_p95 = self.spread_cost_pct.snapshot().get("p95")
        net_p95 = self.net_edge_pct.snapshot().get("p95")
        lag_p50 = self.lag_ms.snapshot().get("p50")

        try:
            if isinstance(spread_p95, (int, float)) and float(spread_p95) > float(cfg.lead_lag_spread_cost_cap_pct):
                actions.append("spread_high: widen spreads -> skip more trades")
        except Exception:
            pass

        try:
            if isinstance(net_p95, (int, float)) and float(net_p95) < float(cfg.lead_lag_net_edge_min_pct):
                actions.append("net_edge_low: after-cost edge weak")
        except Exception:
            pass

        try:
            if float(cfg.lead_lag_min_market_lag_ms) > 0 and isinstance(lag_p50, (int, float)) and float(lag_p50) < float(cfg.lead_lag_min_market_lag_ms):
                actions.append("lag_short: markets sync fast")
        except Exception:
            pass

        # Simple regime label for portal scanability.
        regime = {
            "spread": "wide" if isinstance(spread_p95, (int, float)) and float(spread_p95) > float(cfg.lead_lag_spread_cost_cap_pct) else "ok",
            "lag": "short" if isinstance(lag_p50, (int, float)) and float(cfg.lead_lag_min_market_lag_ms) > 0 and float(lag_p50) < float(cfg.lead_lag_min_market_lag_ms) else "ok",
        }

        out: dict[str, Any] = {
            "generated_at": ts,
            "service": "vps_agent",
            "strategy_mode": cfg.strategy_mode,
            "regime": regime,
            "stats": {
                "edge_raw_pct": self.edge_raw_pct.snapshot(),
                "edge_abs_pct": self.edge_abs_pct.snapshot(),
                "net_edge_pct": self.net_edge_pct.snapshot(),
                "spread_cost_pct": self.spread_cost_pct.snapshot(),
                "lag_ms": self.lag_ms.snapshot(),
                "spot_ret_abs_pct": self.spot_ret_abs_pct.snapshot(),
                "max_usdc": self.max_usdc.snapshot(),
            },
            "decisions": {
                "execution_status_counts": dict(execs),
                "reason_counts": dict(reasons),
            },
            "last": self.last,
            "recommended_actions": actions,
        }

        if isinstance(pm_status, dict):
            out["pm"] = {
                "edges_computed": pm_status.get("edges_computed"),
                "signals_emitted": pm_status.get("signals_emitted"),
            }
        return out


@dataclass(frozen=True)
class Config:
    out_dir: Path
    interval_s: float

    # Strategy selection
    strategy_mode: str  # fair_model|lead_lag

    # public endpoints (optional)
    polymarket_public_url: str | None
    kraken_public_url: str | None

    # Polymarket CLOB (public) inputs
    polymarket_clob_base_url: str
    polymarket_clob_token_id: str | None  # outcome token id

    # Lead-lag inputs (Kraken spot -> pm CLOB lag)
    kraken_spot_base_url: str
    kraken_spot_pair: str
    lead_lag_side: str  # YES|NO
    lead_lag_lookback_points: int
    lead_lag_spot_move_min_pct: float
    lead_lag_spot_noise_window_points: int
    lead_lag_spot_noise_mult: float
    lead_lag_spread_move_mult: float
    lead_lag_edge_min_pct: float
    lead_lag_edge_exit_pct: float
    lead_lag_max_hold_secs: int
    lead_lag_pm_stop_pct: float
    lead_lag_avoid_price_above: float
    lead_lag_avoid_price_below: float

    # Lead-lag quality gates (stability): apply after-costs and microstructure constraints
    lead_lag_net_edge_min_pct: float
    lead_lag_spread_cost_cap_pct: float
    lead_lag_min_market_lag_ms: float
    lead_lag_min_trade_notional_usdc: float

    # Lead-lag risk sizing (CLOB orderbook)
    lead_lag_enable_orderbook_sizing: bool
    lead_lag_slippage_cap: float
    lead_lag_max_fraction_of_band_liquidity: float
    lead_lag_hard_cap_usdc: float

    # Drift / stability
    freshness_max_age_s: float

    # Snapshot sizing
    clob_depth_levels: int

    # Polymarket live trading (optional; requires explicit gates)
    poly_chain_id: int
    poly_private_key: str | None
    poly_api_key: str | None
    poly_api_secret: str | None
    poly_api_passphrase: str | None
    poly_signature_type: int
    poly_funder: str | None
    poly_live_confirm: str

    pm_order_size_shares: float
    pm_max_orders_per_tick: int

    # Optional: restrict candidates by decimal odds interval (e.g. 1.15–1.30)
    pm_min_odds: float | None
    pm_max_odds: float | None

    # Optional: widen odds filter for demo/testing
    pm_odds_test_mode: bool

    # Optional: mapping file to tie PM token <-> Kraken symbol <-> hedge rule
    market_map_path: Path | None

    # Kraken Futures (public/private)
    kraken_futures_symbol: str | None  # e.g. PF_XBTUSD
    kraken_futures_testnet: bool
    kraken_keys_path: Path | None

    # Paper portfolio
    paper_start_balance_usd: float

    # Simple paper thresholds
    edge_threshold: float

    # Estimated frictions for "edge after costs" (observability)
    # Expressed as fractions (e.g. 0.02 = 2%).
    pm_est_fee_pct: float
    pm_edge_extra_cost_pct: float

    # upload target (optional)
    ftp_host: str | None
    ftp_user: str | None
    ftp_pass: str | None
    ftp_remote_dir: str

    trading_mode: str  # paper|live
    killswitch_file: Path | None


def load_config() -> Config:
    out_dir = Path(os.getenv("OUT_DIR", "./out")).resolve()
    interval_s = float(os.getenv("INTERVAL_S", "15"))

    strategy_mode = (os.getenv("STRATEGY_MODE", "lead_lag") or "lead_lag").strip().lower()

    polymarket_public_url = os.getenv("POLYMARKET_PUBLIC_URL") or None
    kraken_public_url = os.getenv("KRAKEN_PUBLIC_URL") or None

    polymarket_clob_base_url = (os.getenv("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com") or "https://clob.polymarket.com").rstrip("/")
    polymarket_clob_token_id = os.getenv("POLYMARKET_CLOB_TOKEN_ID") or None

    kraken_spot_base_url = (os.getenv("KRAKEN_SPOT_BASE_URL", "https://api.kraken.com/0/public") or "https://api.kraken.com/0/public").rstrip("/")
    kraken_spot_pair = (os.getenv("KRAKEN_SPOT_PAIR", "XBTUSD") or "XBTUSD").strip()

    lead_lag_side = (os.getenv("LEAD_LAG_SIDE", "YES") or "YES").strip().upper()
    lead_lag_lookback_points = int(os.getenv("LEAD_LAG_LOOKBACK_POINTS", os.getenv("LOOKBACK_POINTS", "6") or "6") or "6")
    lead_lag_spot_move_min_pct = float(os.getenv("LEAD_LAG_SPOT_MOVE_MIN_PCT", os.getenv("SPOT_MOVE_MIN_PCT", "0.25") or "0.25") or "0.25")
    lead_lag_spot_noise_window_points = int(os.getenv("LEAD_LAG_SPOT_NOISE_WINDOW_POINTS", "40") or "40")
    lead_lag_spot_noise_mult = float(os.getenv("LEAD_LAG_SPOT_NOISE_MULT", "2.0") or "2.0")
    lead_lag_spread_move_mult = float(os.getenv("LEAD_LAG_SPREAD_MOVE_MULT", "1.0") or "1.0")
    lead_lag_edge_min_pct = float(os.getenv("LEAD_LAG_EDGE_MIN_PCT", os.getenv("EDGE_MIN_PCT", "0.20") or "0.20") or "0.20")
    lead_lag_edge_exit_pct = float(os.getenv("LEAD_LAG_EDGE_EXIT_PCT", os.getenv("EDGE_EXIT_PCT", "0.05") or "0.05") or "0.05")
    lead_lag_max_hold_secs = int(os.getenv("LEAD_LAG_MAX_HOLD_SECS", os.getenv("MAX_HOLD_SECS", "180") or "180") or "180")
    lead_lag_pm_stop_pct = float(os.getenv("LEAD_LAG_PM_STOP_PCT", os.getenv("PM_STOP_PCT", "0.25") or "0.25") or "0.25")
    lead_lag_avoid_price_above = float(os.getenv("LEAD_LAG_AVOID_PRICE_ABOVE", os.getenv("AVOID_PRICE_ABOVE", "0.90") or "0.90") or "0.90")
    lead_lag_avoid_price_below = float(os.getenv("LEAD_LAG_AVOID_PRICE_BELOW", os.getenv("AVOID_PRICE_BELOW", "0.02") or "0.02") or "0.02")

    lead_lag_net_edge_min_pct = float(os.getenv("LEAD_LAG_NET_EDGE_MIN_PCT", "0.05") or "0.05")
    lead_lag_spread_cost_cap_pct = float(os.getenv("LEAD_LAG_SPREAD_COST_CAP_PCT", "1.00") or "1.00")
    lead_lag_min_market_lag_ms = float(os.getenv("LEAD_LAG_MIN_MARKET_LAG_MS", "0") or "0")
    lead_lag_min_trade_notional_usdc = float(os.getenv("LEAD_LAG_MIN_TRADE_NOTIONAL_USDC", "5") or "5")

    lead_lag_enable_orderbook_sizing = (os.getenv("LEAD_LAG_ENABLE_ORDERBOOK_SIZING", os.getenv("ENABLE_ORDERBOOK_SIZING", "1") or "1") or "1").strip().lower() not in {"0", "false", "no"}
    lead_lag_slippage_cap = float(os.getenv("LEAD_LAG_SLIPPAGE_CAP", os.getenv("SLIPPAGE_CAP", "0.01") or "0.01") or "0.01")
    lead_lag_max_fraction_of_band_liquidity = float(
        os.getenv("LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY", os.getenv("MAX_FRACTION_OF_BAND_LIQUIDITY", "0.10") or "0.10") or "0.10"
    )
    lead_lag_hard_cap_usdc = float(os.getenv("LEAD_LAG_HARD_CAP_USDC", os.getenv("HARD_CAP_USDC", "2000") or "2000") or "2000")

    freshness_max_age_s = float(os.getenv("FRESHNESS_MAX_AGE_SECS", "60") or "60")

    clob_depth_levels = int(os.getenv("CLOB_DEPTH_LEVELS", "10") or "10")

    poly_chain_id = int(os.getenv("POLY_CHAIN_ID", "137") or "137")
    poly_private_key = (os.getenv("POLY_PRIVATE_KEY") or os.getenv("POLY_PK") or "").strip() or None
    poly_api_key = (os.getenv("POLY_CLOB_API_KEY") or os.getenv("CLOB_API_KEY") or "").strip() or None
    poly_api_secret = (os.getenv("POLY_CLOB_SECRET") or os.getenv("CLOB_SECRET") or "").strip() or None
    poly_api_passphrase = (os.getenv("POLY_CLOB_PASS_PHRASE") or os.getenv("CLOB_PASS_PHRASE") or "").strip() or None
    poly_signature_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0") or "0")
    poly_funder = (os.getenv("POLY_FUNDER") or "").strip() or None
    poly_live_confirm = (os.getenv("POLY_LIVE_CONFIRM", "NO") or "NO").strip().upper()

    pm_order_size_shares = float(os.getenv("PM_ORDER_SIZE_SHARES", "10") or "10")
    pm_max_orders_per_tick = int(os.getenv("PM_MAX_ORDERS_PER_TICK", "1") or "1")

    pm_min_odds_raw = (os.getenv("PM_MIN_ODDS") or "").strip()
    pm_max_odds_raw = (os.getenv("PM_MAX_ODDS") or "").strip()
    pm_min_odds = float(pm_min_odds_raw) if pm_min_odds_raw else None
    pm_max_odds = float(pm_max_odds_raw) if pm_max_odds_raw else None

    pm_odds_test_mode = (os.getenv("PM_ODDS_TEST_MODE", "0") or "0").strip().lower() in {"1", "true", "yes"}
    if pm_odds_test_mode:
        # Widen the band to make it easier to see paper trades in the portal.
        pm_min_odds = 1.01
        pm_max_odds = 10.0

    market_map_path_raw = os.getenv("MARKET_MAP_PATH")
    market_map_path = Path(market_map_path_raw).expanduser() if market_map_path_raw else None

    kraken_futures_symbol = os.getenv("KRAKEN_FUTURES_SYMBOL") or None
    kraken_futures_testnet = (os.getenv("KRAKEN_FUTURES_TESTNET", "0") or "0").strip().lower() in {"1", "true", "yes"}

    # Support either a generic name or Markov-style env var.
    kraken_keys_path_raw = os.getenv("KRAKEN_KEYS_PATH") or os.getenv("MARKOV_KRAKEN_KEYS_PATH")
    kraken_keys_path = Path(kraken_keys_path_raw).expanduser() if kraken_keys_path_raw else None

    paper_start_balance_usd = float(os.getenv("PAPER_START_BALANCE_USD", "1000") or "1000")

    edge_threshold = float(os.getenv("EDGE_THRESHOLD", "0.02"))

    # Friction model used for reporting edge_net:
    # - spread is taken from observed bid/ask (half-spread approximates entry cost vs mid)
    # - fee and extra_cost are applied as % of execution price
    pm_est_fee_pct = float(os.getenv("PM_EST_FEE_PCT", "0.0") or "0.0")
    pm_edge_extra_cost_pct = float(os.getenv("PM_EDGE_EXTRA_COST_PCT", "0.0") or "0.0")

    ftp_host = os.getenv("FTP_HOST") or None
    ftp_user = os.getenv("FTP_USER") or None
    ftp_pass = os.getenv("FTP_PASS") or None
    ftp_remote_dir = os.getenv("FTP_REMOTE_DIR", "/web/data").rstrip("/")

    trading_mode = (os.getenv("TRADING_MODE", "paper") or "paper").strip().lower()

    killswitch_file_raw = os.getenv("KILLSWITCH_FILE")
    killswitch_file = Path(killswitch_file_raw).expanduser() if killswitch_file_raw else None

    return Config(
        out_dir=out_dir,
        interval_s=interval_s,
        strategy_mode=strategy_mode,
        polymarket_public_url=polymarket_public_url,
        kraken_public_url=kraken_public_url,
        polymarket_clob_base_url=polymarket_clob_base_url,
        polymarket_clob_token_id=polymarket_clob_token_id,
        kraken_spot_base_url=kraken_spot_base_url,
        kraken_spot_pair=kraken_spot_pair,
        lead_lag_side=lead_lag_side,
        lead_lag_lookback_points=lead_lag_lookback_points,
        lead_lag_spot_move_min_pct=lead_lag_spot_move_min_pct,
        lead_lag_spot_noise_window_points=lead_lag_spot_noise_window_points,
        lead_lag_spot_noise_mult=lead_lag_spot_noise_mult,
        lead_lag_spread_move_mult=lead_lag_spread_move_mult,
        lead_lag_edge_min_pct=lead_lag_edge_min_pct,
        lead_lag_edge_exit_pct=lead_lag_edge_exit_pct,
        lead_lag_max_hold_secs=lead_lag_max_hold_secs,
        lead_lag_pm_stop_pct=lead_lag_pm_stop_pct,
        lead_lag_avoid_price_above=lead_lag_avoid_price_above,
        lead_lag_avoid_price_below=lead_lag_avoid_price_below,
        lead_lag_net_edge_min_pct=lead_lag_net_edge_min_pct,
        lead_lag_spread_cost_cap_pct=lead_lag_spread_cost_cap_pct,
        lead_lag_min_market_lag_ms=lead_lag_min_market_lag_ms,
        lead_lag_min_trade_notional_usdc=lead_lag_min_trade_notional_usdc,
        lead_lag_enable_orderbook_sizing=lead_lag_enable_orderbook_sizing,
        lead_lag_slippage_cap=lead_lag_slippage_cap,
        lead_lag_max_fraction_of_band_liquidity=lead_lag_max_fraction_of_band_liquidity,
        lead_lag_hard_cap_usdc=lead_lag_hard_cap_usdc,
        freshness_max_age_s=freshness_max_age_s,
        clob_depth_levels=clob_depth_levels,
        poly_chain_id=poly_chain_id,
        poly_private_key=poly_private_key,
        poly_api_key=poly_api_key,
        poly_api_secret=poly_api_secret,
        poly_api_passphrase=poly_api_passphrase,
        poly_signature_type=poly_signature_type,
        poly_funder=poly_funder,
        poly_live_confirm=poly_live_confirm,
        pm_order_size_shares=pm_order_size_shares,
        pm_max_orders_per_tick=pm_max_orders_per_tick,
        pm_min_odds=pm_min_odds,
        pm_max_odds=pm_max_odds,
        pm_odds_test_mode=pm_odds_test_mode,
        market_map_path=market_map_path,
        kraken_futures_symbol=kraken_futures_symbol,
        kraken_futures_testnet=kraken_futures_testnet,
        kraken_keys_path=kraken_keys_path,
        paper_start_balance_usd=paper_start_balance_usd,
        edge_threshold=edge_threshold,
        pm_est_fee_pct=pm_est_fee_pct,
        pm_edge_extra_cost_pct=pm_edge_extra_cost_pct,
        ftp_host=ftp_host,
        ftp_user=ftp_user,
        ftp_pass=ftp_pass,
        ftp_remote_dir=ftp_remote_dir,
        trading_mode=trading_mode,
        killswitch_file=killswitch_file,
    )


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _load_paper_state(*, path: Path, ts: str, start_balance_usd: float) -> dict[str, Any]:
    if path.exists():
        try:
            raw: Any = read_json(path)
            if isinstance(raw, dict) and "cash_usd" in raw and "positions" in raw:
                return cast(dict[str, Any], raw)
        except Exception:
            pass

    return {
        "started_at": ts,
        "start_balance_usd": float(start_balance_usd),
        "cash_usd": float(start_balance_usd),
        "realized_pnl_usd": 0.0,
        "positions": {},  # token_id -> {market,outcome,shares,avg_entry,opened_at}
    }


def _price_to_decimal_odds(p: float) -> float | None:
    if p <= 0:
        return None
    return 1.0 / p


def _price_allowed_by_odds(cfg: Config, *, price: float) -> bool:
    """Filter by odds interval if configured.

    Polymarket token mid-price is an implied probability p in (0,1].
    Decimal odds are approx 1/p.

    If you want odds in [min_odds, max_odds], that corresponds to price in [1/max_odds, 1/min_odds].
    """

    if cfg.pm_min_odds is None and cfg.pm_max_odds is None:
        return True
    odds = _price_to_decimal_odds(price)
    if odds is None:
        return False

    lo_price = 0.0
    hi_price = 1.0
    if cfg.pm_max_odds is not None and cfg.pm_max_odds > 0:
        lo_price = 1.0 / cfg.pm_max_odds
    if cfg.pm_min_odds is not None and cfg.pm_min_odds > 0:
        hi_price = 1.0 / cfg.pm_min_odds
    return lo_price <= price <= hi_price


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def write_json(path: Path, obj: Any) -> None:
    ensure_parent(path)
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def append_csv_row(path: Path, header: list[str], row: list[Any], *, keep_last: int = 200) -> None:
    """Append a row to a CSV, keeping only the last N rows (plus header)."""
    ensure_parent(path)
    existing_rows: list[list[str]] = []
    if path.exists():
        try:
            with path.open("r", newline="", encoding="utf-8") as f:
                r = csv.reader(f)
                found_header = False
                for line in r:
                    if not line:
                        continue
                    if not found_header:
                        found_header = True
                        continue
                    existing_rows.append(line)
        except Exception:
            existing_rows = []

    existing_rows.append([str(x) for x in row])
    if keep_last > 0 and len(existing_rows) > keep_last:
        existing_rows = existing_rows[-keep_last:]
    write_csv(path, header, existing_rows)


def killswitch_active(cfg: Config) -> bool:
    if not cfg.killswitch_file:
        return False
    return cfg.killswitch_file.exists()


def compute_edge_stub(*, ts: str, pm: dict[str, Any] | None, kraken: dict[str, Any] | None) -> list[dict[str, Any]]:
    """Return a minimal edge_signals list.

    This is a stub: wire in real pricing/hedge-cost logic later.
    Output is written as CSV for the portal.
    """
    # If both sources exist, emit one row to prove the pipe is alive.
    srcs: list[str] = []
    if pm is not None:
        srcs.append("pm")
    if kraken is not None:
        srcs.append("kraken")

    return [
        {
            "ts": ts,
            "market": "demo-market",
            "fair_p": 0.50,
            "pm_price": 0.50,
            "edge": 0.00,
            "sources": "+".join(srcs) or "none",
            "notes": "stub",
        }
    ]


def _parse_iso_dt(ts: str) -> datetime:
    # ts is generated by utc_now_iso() and is always UTC.
    # Example: 2025-12-26T00:57:38+00:00
    return datetime.fromisoformat(ts)


def _sum_book_usdc_in_band(levels: list[dict[str, Any]], *, price_leq: float | None = None, price_geq: float | None = None) -> tuple[float, float]:
    shares = 0.0
    usdc = 0.0
    for lv in levels:
        p_any = lv.get("price")
        s_any = lv.get("size")
        p = _coerce_float(p_any)
        if p is None:
            continue
        s = _coerce_float(s_any)
        s = float(s or 0.0)
        if price_leq is not None and p > price_leq:
            continue
        if price_geq is not None and p < price_geq:
            continue
        shares += s
        usdc += p * s
    return shares, usdc


def load_kraken_keys(path: Path) -> KrakenFuturesKeys:
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("kraken keys file must be a JSON object")
    obj = cast(dict[str, Any], raw)
    api_key = str(obj.get("futures_api_key", "") or "").strip()
    api_secret = str(obj.get("futures_api_secret", "") or "").strip()
    if not api_key or not api_secret:
        raise ValueError("Missing futures_api_key/futures_api_secret in kraken keys JSON")
    return KrakenFuturesKeys(api_key=api_key, api_secret=api_secret)


def clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


def load_market_map(path: Path) -> dict[str, Any]:
    raw: Any = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise TypeError("market map must be a JSON object")
    return cast(dict[str, Any], raw)


def select_markets(map_obj: dict[str, Any]) -> list[dict[str, Any]]:
    markets = map_obj.get("markets")
    if not isinstance(markets, list):
        return []
    markets_list = cast(list[Any], markets)
    out: list[dict[str, Any]] = []
    for m in markets_list:
        if isinstance(m, dict):
            out.append(cast(dict[str, Any], m))
    return out


def compute_fair_probability(*, model: dict[str, Any], ref_price: float) -> float:
    mode = str(model.get("mode", "constant")).strip().lower()
    if mode == "constant":
        return clamp01(float(model.get("p", 0.5)))
    if mode == "linear_range":
        if "min_ref" not in model or "max_ref" not in model:
            raise ValueError("fair_model.linear_range requires min_ref and max_ref")
        min_ref = float(model["min_ref"])
        max_ref = float(model["max_ref"])
        if max_ref <= min_ref:
            raise ValueError("fair_model.linear_range requires max_ref > min_ref")
        return clamp01((ref_price - min_ref) / (max_ref - min_ref))
    if mode == "deribit_rn":
        raise ValueError("fair_model.deribit_rn requires Deribit options data; handled in write_outputs")
    raise ValueError(f"Unknown fair_model.mode: {mode}")


def write_outputs(  # pyright: ignore
    cfg: Config,
    *,
    pm: dict[str, Any] | None,
    kraken: dict[str, Any] | None,
    lead_lag_engine: LeadLagEngine | None = None,
    health_tracker: LeadLagHealthTracker | None = None,
    latency_tracker: LatencyTracker | None = None,
    runtime_cache: RuntimeCache | None = None,
) -> list[Path]:  # pyright: ignore[reportGeneralTypeIssues]
    ts = utc_now_iso()
    ts_dt = _parse_iso_dt(ts)
    t0 = time.perf_counter()

    out = cfg.out_dir
    out.mkdir(parents=True, exist_ok=True)

    live_status: dict[str, Any] = {
        "ts": ts,
        "trading_mode": cfg.trading_mode,
        "killswitch": bool(killswitch_active(cfg)),
        "strategy_mode": cfg.strategy_mode,
        "system_latency_ms": None,
        "market_lag_ms": None,
        "market_lag_confidence": None,
        "market_lag_points": None,
        "market_lag_reason": None,
        # Lead–lag gating parameters (portal-facing, non-secret)
        "lead_lag_net_edge_min_pct": cfg.lead_lag_net_edge_min_pct,
        "lead_lag_spread_cost_cap_pct": cfg.lead_lag_spread_cost_cap_pct,
        "lead_lag_min_market_lag_ms": cfg.lead_lag_min_market_lag_ms,
        "lead_lag_min_trade_notional_usdc": cfg.lead_lag_min_trade_notional_usdc,
        "lead_lag_spot_move_min_pct_base": cfg.lead_lag_spot_move_min_pct,
        "lead_lag_spot_noise_window_points": cfg.lead_lag_spot_noise_window_points,
        "lead_lag_spot_noise_mult": cfg.lead_lag_spot_noise_mult,
        "lead_lag_spread_move_mult": cfg.lead_lag_spread_move_mult,
        "polymarket_public_url": cfg.polymarket_public_url,
        "kraken_public_url": cfg.kraken_public_url,
        "polymarket_clob_base_url": cfg.polymarket_clob_base_url,
        "polymarket_clob_token_id": cfg.polymarket_clob_token_id,
        "kraken_spot_pair": cfg.kraken_spot_pair,
        "clob_depth_levels": cfg.clob_depth_levels,
        "poly_chain_id": cfg.poly_chain_id,
        "poly_signature_type": cfg.poly_signature_type,
        "poly_funder": cfg.poly_funder,
        "pm_order_size_shares": cfg.pm_order_size_shares,
        "pm_max_orders_per_tick": cfg.pm_max_orders_per_tick,
        "pm_odds_test_mode": bool(cfg.pm_odds_test_mode),
        "pm_est_fee_pct": cfg.pm_est_fee_pct,
        "pm_edge_extra_cost_pct": cfg.pm_edge_extra_cost_pct,
        "poly_trading_enabled": bool(
            cfg.trading_mode == "live"
            and cfg.poly_live_confirm == "YES"
            and cfg.poly_private_key
            and cfg.poly_api_key
            and cfg.poly_api_secret
            and cfg.poly_api_passphrase
        ),
        "market_map_path": str(cfg.market_map_path) if cfg.market_map_path else None,
        "kraken_futures_symbol": cfg.kraken_futures_symbol,
        "kraken_futures_testnet": cfg.kraken_futures_testnet,
        "kraken_keys_path": str(cfg.kraken_keys_path) if cfg.kraken_keys_path else None,
        "edge_threshold": cfg.edge_threshold,
        "paper_start_balance_usd": cfg.paper_start_balance_usd,
    }

    files: list[Path] = []

    p_live = out / "live_status.json"
    write_json(p_live, live_status)
    files.append(p_live)

    p_lead_lag_health = out / "lead_lag_health.json"

    # Portal-facing Polymarket status snapshot (lightweight, non-secret)
    p_pm_status = out / "polymarket_status.json"
    pm_status: dict[str, Any] = {
        "generated_at": ts,
        "service": "vps_agent",
        "ok": True,
        "polymarket_clob_base_url": cfg.polymarket_clob_base_url,
        "market_map_path": str(cfg.market_map_path) if cfg.market_map_path else None,
        "notes": [],
        "lead_lag_net_edge_min_pct": cfg.lead_lag_net_edge_min_pct,
        "lead_lag_spread_cost_cap_pct": cfg.lead_lag_spread_cost_cap_pct,
        "lead_lag_min_market_lag_ms": cfg.lead_lag_min_market_lag_ms,
        "lead_lag_min_trade_notional_usdc": cfg.lead_lag_min_trade_notional_usdc,
    }

    # Additional snapshots to make the jump to real-money easier later.
    # These are read-only/observability and do not place orders.
    sources_health: dict[str, Any] = {
        "generated_at": ts,
        "polymarket": {"clob": {}},
        "kraken": {"futures": {}},
        "options": {"deribit": {}},
    }

    # Edge signals: keep a stable schema for the portal.
    # In fair_model mode we will populate this later; default to stub row.
    edge_rows: list[dict[str, Any]] = []
    if cfg.strategy_mode != "lead_lag":
        edge_rows = compute_edge_stub(ts=ts, pm=pm, kraken=kraken)
    p_edge = out / "edge_signals_live.csv"
    write_csv(
        p_edge,
        ["ts", "market", "fair_p", "pm_price", "edge", "spread", "cost_est", "edge_net", "sources", "notes"],
        [
            [
                r.get("ts"),
                r.get("market"),
                r.get("fair_p"),
                r.get("pm_price"),
                r.get("edge"),
                r.get("spread"),
                r.get("cost_est"),
                r.get("edge_net"),
                r.get("sources"),
                r.get("notes"),
            ]
            for r in edge_rows
        ],
    )
    files.append(p_edge)

    # Lead–lag edge breakdown used by the dashboard.
    p_edge_calc = out / "edge_calculator_live.csv"
    if not p_edge_calc.exists():
        write_csv(
            p_edge_calc,
            [
                "ts",
                "market",
                "signal_strength",
                "raw_edge",
                "spread_cost",
                "fees",
                "net_edge",
                "execution_status",
                "reason",
            ],
            [],
        )
    files.append(p_edge_calc)

    # Optional files used by the portal (kept stable even if empty)
    p_pm_orders = out / "pm_orders.csv"
    if not p_pm_orders.exists():
        write_csv(p_pm_orders, ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"], [])
    files.append(p_pm_orders)

    # Paper portfolio snapshots (Polymarket-only, no secrets)
    p_pm_paper_portfolio = out / "pm_paper_portfolio.json"
    p_pm_paper_positions = out / "pm_paper_positions.csv"
    p_pm_paper_trades = out / "pm_paper_trades.csv"
    p_pm_paper_candidates = out / "pm_paper_candidates.csv"
    if not p_pm_paper_positions.exists():
        write_csv(
            p_pm_paper_positions,
            ["ts", "market", "token", "outcome", "shares", "avg_entry", "last_price", "value", "unrealized_pnl"],
            [],
        )
    if not p_pm_paper_trades.exists():
        write_csv(
            p_pm_paper_trades,
            ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
            [],
        )
    if not p_pm_paper_candidates.exists():
        write_csv(
            p_pm_paper_candidates,
            [
                "ts",
                "market",
                "market_ref",
                "token",
                "outcome",
                "pm_bid",
                "pm_ask",
                "pm_mid",
                "odds",
                "odds_allowed",
                "fair_p",
                "ev",
                "edge",
                "spread",
                "cost_est",
                "edge_net",
                "signal",
                "decision",
                "reason",
            ],
            [],
        )
    # Always keep portfolio JSON stable for the portal.
    if not p_pm_paper_portfolio.exists():
        write_json(
            p_pm_paper_portfolio,
            {
                "generated_at": ts,
                "started_at": ts,
                "start_balance_usd": float(cfg.paper_start_balance_usd),
                "cash_usd": float(cfg.paper_start_balance_usd),
                "equity_usd": float(cfg.paper_start_balance_usd),
                "unrealized_pnl_usd": 0.0,
                "realized_pnl_usd": 0.0,
                "open_positions": 0,
                "notes": ["initialized"],
            },
        )
    files.append(p_pm_paper_portfolio)
    files.append(p_pm_paper_positions)
    files.append(p_pm_paper_trades)
    files.append(p_pm_paper_candidates)

    p_kr_sig = out / "kraken_futures_signals.csv"
    if not p_kr_sig.exists():
        write_csv(p_kr_sig, ["ts", "symbol", "signal", "confidence", "edge", "ref_price", "notes"], [])
    files.append(p_kr_sig)

    p_kr_fill = out / "kraken_futures_fills.csv"
    if not p_kr_fill.exists():
        write_csv(p_kr_fill, ["ts", "symbol", "side", "qty", "price", "fee", "order_id", "position_id", "notes"], [])
    files.append(p_kr_fill)

    p_exec = out / "executed_trades.csv"
    if not p_exec.exists():
        write_csv(p_exec, ["ts", "venue", "symbol", "side", "qty", "price", "status", "notes"], [])
    files.append(p_exec)

    # Scanner log (one row per loop) used by the portal.
    p_pm_scan = out / "pm_scanner_log.csv"
    if not p_pm_scan.exists():
        write_csv(p_pm_scan, ["ts", "markets_seen", "edges_computed", "signals_emitted", "status", "notes"], [])
    files.append(p_pm_scan)

    # If configured, compute a simple edge using Polymarket CLOB best bid/ask vs Kraken Futures ticker.
    try:
        # Load mapping (preferred) to tie token<->symbol<->fair-model.
        mm: dict[str, Any] | None = None
        mkts: list[dict[str, Any]] = []
        if cfg.market_map_path and cfg.market_map_path.exists():
            mm = load_market_map(cfg.market_map_path)
            mkts = select_markets(mm)

        # Fallback: if no market map is present, treat env vars as a single market.
        if not mkts:
            mkts = [
                {
                    "name": "env-market",
                    "polymarket": {"clob_token_id": cfg.polymarket_clob_token_id},
                    "kraken_futures": {"symbol": cfg.kraken_futures_symbol, "testnet": cfg.kraken_futures_testnet},
                    "fair_model": {"mode": "constant", "p": 0.5},
                    "hedge": {"yes_side": "sell", "no_side": "buy"},
                }
            ]

        pm_clob = PolymarketClobPublic(base_url=cfg.polymarket_clob_base_url)
        kr_spot = KrakenSpotPublic(base_url=cfg.kraken_spot_base_url)
        deribit = DeribitOptionsPublic()
        gamma = PolymarketGammaPublic()
        cache = runtime_cache or RuntimeCache()

        paper_state = _load_paper_state(path=p_pm_paper_portfolio, ts=ts, start_balance_usd=cfg.paper_start_balance_usd)
        paper_cash = float(paper_state.get("cash_usd") or cfg.paper_start_balance_usd)
        paper_realized = float(paper_state.get("realized_pnl_usd") or 0.0)
        paper_positions_any = paper_state.get("positions")
        paper_positions: dict[str, dict[str, Any]] = {}
        if isinstance(paper_positions_any, dict):
            for k, v in cast(dict[Any, Any], paper_positions_any).items():
                if isinstance(k, str) and isinstance(v, dict):
                    paper_positions[k] = cast(dict[str, Any], v)

        # Public snapshots: Polymarket CLOB orderbooks (summarized) + Kraken Futures instruments/tickers
        # These are safe to generate even in paper mode.
        clob_summary: dict[str, Any] = {
            "generated_at": ts,
            "base_url": cfg.polymarket_clob_base_url,
            "depth_levels": cfg.clob_depth_levels,
            "markets": [],
        }

        # Compute these from the same market list we use for edge so it's aligned.
        # Note: token_id is mandatory to query /book.
        clob_t0 = _now_ms()
        for mkt in mkts:
            market_name = str(mkt.get("name") or "market")
            token_id: str | None = None
            pm_block = mkt.get("polymarket")
            if isinstance(pm_block, dict):
                pm_cfg = cast(dict[str, Any], pm_block)
                token_id = str(pm_cfg.get("clob_token_id", "") or "").strip() or None

                # Optional: resolve token id automatically from market URL/slug via Gamma.
                if not token_id:
                    market_ref = str(pm_cfg.get("market_url") or pm_cfg.get("market_slug") or "").strip() or None
                    if market_ref:
                        try:
                            gm = cache.gamma_market_by_slug.get(market_ref)
                            if gm is None:
                                t_g0 = time.perf_counter()
                                gm = gamma.get_market_by_slug(slug=market_ref)
                                if latency_tracker is not None:
                                    latency_tracker.record_gamma_fetch(float((time.perf_counter() - t_g0) * 1000.0))
                                cache.gamma_market_by_slug[market_ref] = gm

                            fair_mode_for_infer = str(cast(dict[str, Any], (mkt.get("fair_model") or {})).get("mode", "")).strip().lower()
                            outcome = str(pm_cfg.get("outcome") or "").strip()

                            chosen: str | None = None
                            if outcome:
                                chosen = outcome
                            elif fair_mode_for_infer == "deribit_touch":
                                # Try to infer YES/NO mapping for reach/touch-style questions.
                                direction = str(cast(dict[str, Any], (mkt.get("fair_model") or {})).get("direction") or "").strip().lower()
                                if direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"}:
                                    chosen = gamma.infer_yes_no_for_touch_event(market=gm, event=direction)

                            if not chosen:
                                raise ValueError(
                                    "Could not infer Polymarket outcome automatically. "
                                    "Set polymarket.outcome to 'Yes' or 'No' (or exact label)."
                                )

                            cache_key = (market_ref, chosen)
                            if cache_key in cache.token_id_by_slug_outcome:
                                token_id = cache.token_id_by_slug_outcome[cache_key]
                            else:
                                token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen)
                                cache.token_id_by_slug_outcome[cache_key] = token_id

                            pm_status.setdefault("gamma_resolved", []).append(
                                {
                                    "market": market_name,
                                    "slug": getattr(gm, "slug", market_ref),
                                    "question": getattr(gm, "question", None),
                                    "outcomes": getattr(gm, "outcomes", None),
                                    "chosen_outcome": chosen,
                                }
                            )
                            sources_health["polymarket"].setdefault("gamma", {"ok": True})
                        except Exception as e:
                            pm_status.setdefault("gamma_errors", []).append({"market": market_name, "error": str(e)})
                            sources_health["polymarket"]["gamma"] = {"ok": False, "error": str(e)}
                            token_id = None

            if not token_id:
                continue

            try:
                ob = pm_clob.get_orderbook(token_id)
                bid, ask = best_bid_ask(ob)
                bids = _safe_top_levels(ob.get("bids"), max_levels=cfg.clob_depth_levels)
                asks = _safe_top_levels(ob.get("asks"), max_levels=cfg.clob_depth_levels)
                mid = None
                spread = None
                if bid is not None and ask is not None:
                    mid = (bid + ask) / 2.0
                    spread = ask - bid
                clob_summary["markets"].append(
                    {
                        "name": market_name,
                        "token_id": token_id,
                        "best_bid": bid,
                        "best_ask": ask,
                        "mid": mid,
                        "spread": spread,
                        "bids": bids,
                        "asks": asks,
                    }
                )
            except Exception as e:
                clob_summary["markets"].append(
                    {
                        "name": market_name,
                        "token_id": token_id,
                        "error": str(e),
                    }
                )
        sources_health["polymarket"]["clob"] = {
            "ok": True,
            "markets": len(clob_summary.get("markets") or []),
            "ms": _now_ms() - clob_t0,
        }

        p_clob = out / "polymarket_clob_public.json"
        write_json(p_clob, clob_summary)
        files.append(p_clob)

        # Kraken Futures public snapshot.
        # We fetch *all* instruments/tickers, then also provide a small filtered view for mapped symbols.
        # This keeps it future-proof when you add more markets.
        kf_public_refresh_s = float(os.getenv("KRAKEN_FUTURES_PUBLIC_REFRESH_S", "300") or "300")
        kf_now_ms = _now_ms()
        use_cached_kf_pub = bool(
            cache.kraken_futures_public_snapshot
            and cache.kraken_futures_public_fetched_at_ms > 0
            and (kf_now_ms - cache.kraken_futures_public_fetched_at_ms) < int(kf_public_refresh_s * 1000.0)
        )

        instruments: list[dict[str, Any]]
        tickers: list[dict[str, Any]]

        if use_cached_kf_pub:
            snap_any = cache.kraken_futures_public_snapshot or {}
            instruments_any = snap_any.get("instruments")
            tickers_any = snap_any.get("tickers")
            instruments = cast(list[dict[str, Any]], instruments_any) if isinstance(instruments_any, list) else []
            tickers = cast(list[dict[str, Any]], tickers_any) if isinstance(tickers_any, list) else []
            sources_health["kraken"]["futures"]["public"] = {
                "ok": True,
                "cached": True,
                "cache_age_s": float((kf_now_ms - cache.kraken_futures_public_fetched_at_ms) / 1000.0),
                "ms": 0,
            }
        else:
            kf_t0 = _now_ms()
            t_kf0 = time.perf_counter()
            kf_public = KrakenFuturesApi(testnet=cfg.kraken_futures_testnet)
            instruments = kf_public.get_instruments()
            tickers = kf_public.get_tickers()
            if latency_tracker is not None:
                latency_tracker.record_kraken_futures_public_fetch(float((time.perf_counter() - t_kf0) * 1000.0))
            cache.kraken_futures_public_snapshot = {
                "generated_at": ts,
                "testnet": cfg.kraken_futures_testnet,
                "base_url": "https://demo-futures.kraken.com" if cfg.kraken_futures_testnet else "https://futures.kraken.com",
                "instruments": instruments,
                "tickers": tickers,
            }
            cache.kraken_futures_public_fetched_at_ms = kf_now_ms
            sources_health["kraken"]["futures"]["public"] = {"ok": True, "cached": False, "ms": _now_ms() - kf_t0}

        mapped_symbols: list[str] = []
        for mkt in mkts:
            k_block = mkt.get("kraken_futures")
            if isinstance(k_block, dict):
                k_cfg = cast(dict[str, Any], k_block)
                sym = str(k_cfg.get("symbol", "") or "").strip()
                if sym:
                    mapped_symbols.append(sym)
        if cfg.kraken_futures_symbol:
            mapped_symbols.append(cfg.kraken_futures_symbol)
        mapped_symbols = sorted(set(mapped_symbols))

        tickers_by_symbol: dict[str, Any] = {}
        for t in tickers:
            sym = str(t.get("symbol", "") or "").strip()
            if sym in mapped_symbols:
                tickers_by_symbol[sym] = t

        kraken_futures_public_snapshot: dict[str, Any] = {
            "generated_at": (cache.kraken_futures_public_snapshot or {}).get("generated_at") or ts,
            "published_at": ts,
            "cache_age_s": float((kf_now_ms - cache.kraken_futures_public_fetched_at_ms) / 1000.0) if use_cached_kf_pub else 0.0,
            "testnet": cfg.kraken_futures_testnet,
            "base_url": "https://demo-futures.kraken.com" if cfg.kraken_futures_testnet else "https://futures.kraken.com",
            "mapped_symbols": mapped_symbols,
            "instruments_count": len(instruments),
            "tickers_count": len(tickers),
            "tickers_by_symbol": tickers_by_symbol,
            "instruments": instruments,
            "tickers": tickers,
        }

        p_kf_pub = out / "kraken_futures_public.json"
        try:
            if (not use_cached_kf_pub) or (not p_kf_pub.exists()):
                write_json(p_kf_pub, kraken_futures_public_snapshot)
        except Exception:
            pass
        files.append(p_kf_pub)

        # Kraken Futures private snapshot (read-only).
        # Always emit the file (even when keys are missing) so sync/deploy stays green.
        p_kf_priv = out / "kraken_futures_private.json"
        kraken_futures_private_snapshot: dict[str, Any] = {
            "generated_at": ts,
            "testnet": cfg.kraken_futures_testnet,
            "ok": False,
            "error": "keys_missing",
            "accounts": None,
            "open_positions": None,
        }
        if cfg.kraken_keys_path and cfg.kraken_keys_path.exists():
            kf_priv_refresh_s = float(os.getenv("KRAKEN_FUTURES_PRIVATE_REFRESH_S", "300") or "300")
            kf_priv_now_ms = _now_ms()
            use_cached_kf_priv = bool(
                cache.kraken_futures_private_snapshot
                and cache.kraken_futures_private_fetched_at_ms > 0
                and (kf_priv_now_ms - cache.kraken_futures_private_fetched_at_ms) < int(kf_priv_refresh_s * 1000.0)
            )

            if use_cached_kf_priv:
                try:
                    kraken_futures_private_snapshot.update(cache.kraken_futures_private_snapshot or {})
                    kraken_futures_private_snapshot["published_at"] = ts
                    kraken_futures_private_snapshot["cache_age_s"] = float((kf_priv_now_ms - cache.kraken_futures_private_fetched_at_ms) / 1000.0)
                    sources_health["kraken"]["futures"]["private"] = {
                        "ok": True,
                        "cached": True,
                        "cache_age_s": float((kf_priv_now_ms - cache.kraken_futures_private_fetched_at_ms) / 1000.0),
                        "ms": 0,
                    }
                except Exception:
                    use_cached_kf_priv = False

            if not use_cached_kf_priv:
                kf_priv_t0 = _now_ms()
                try:
                    t_kf1 = time.perf_counter()
                    keys = load_kraken_keys(cfg.kraken_keys_path)
                    kf_private = KrakenFuturesApi(keys=keys, testnet=cfg.kraken_futures_testnet)
                    accounts = kf_private.get_accounts()
                    open_positions = kf_private.get_openpositions()
                    if latency_tracker is not None:
                        latency_tracker.record_kraken_futures_private_fetch(float((time.perf_counter() - t_kf1) * 1000.0))
                    kraken_futures_private_snapshot.update(
                        {
                            "generated_at": ts,
                            "published_at": ts,
                            "cache_age_s": 0.0,
                            "ok": True,
                            "error": None,
                            "accounts": accounts,
                            "open_positions": open_positions,
                        }
                    )
                    cache.kraken_futures_private_snapshot = dict(kraken_futures_private_snapshot)
                    cache.kraken_futures_private_fetched_at_ms = kf_priv_now_ms
                    sources_health["kraken"]["futures"]["private"] = {"ok": True, "cached": False, "ms": _now_ms() - kf_priv_t0}
                except Exception as e:
                    kraken_futures_private_snapshot["error"] = str(e)
                    sources_health["kraken"]["futures"]["private"] = {"ok": False, "error": str(e)}
        else:
            sources_health["kraken"]["futures"]["private"] = {"ok": False, "error": "keys_missing"}

        try:
            # Private snapshot can also be large; don't rewrite it on cached ticks.
            if ("cached" not in (sources_health.get("kraken", {}).get("futures", {}).get("private", {}) or {})) or (
                not bool((sources_health.get("kraken", {}).get("futures", {}).get("private", {}) or {}).get("cached"))
            ) or (not p_kf_priv.exists()):
                write_json(p_kf_priv, kraken_futures_private_snapshot)
        except Exception:
            pass
        files.append(p_kf_priv)

        pm_status["markets_configured"] = len(mkts)
        pm_status["markets"] = [str(m.get("name") or "market") for m in mkts]

        computed_rows: list[dict[str, Any]] = []
        signals_emitted = 0

        # Optional: Deribit options snapshot (only generated when at least one market requests it).
        # This is public, read-only and helps compute risk-neutral fair probabilities.
        deribit_snapshot: dict[str, Any] = {
            "generated_at": ts,
            "base_url": "https://www.deribit.com/api/v2",
            "markets": [],
        }
        deribit_used = 0

        # Optional: Polymarket live client (only created when live trading is explicitly enabled).
        pm_live_client: Any | None = None
        pm_live_error: str | None = None
        poly_trading_enabled = bool(
            cfg.trading_mode == "live"
            and cfg.poly_live_confirm == "YES"
            and cfg.poly_private_key
            and cfg.poly_api_key
            and cfg.poly_api_secret
            and cfg.poly_api_passphrase
        )
        if poly_trading_enabled:
            try:
                pm_live_client = pm_make_live_client(
                    PolymarketClobLiveConfig(
                        host=cfg.polymarket_clob_base_url,
                        chain_id=cfg.poly_chain_id,
                        private_key=str(cfg.poly_private_key),
                        creds=PolymarketClobApiCreds(
                            api_key=str(cfg.poly_api_key),
                            api_secret=str(cfg.poly_api_secret),
                            api_passphrase=str(cfg.poly_api_passphrase),
                        ),
                        signature_type=cfg.poly_signature_type,
                        funder=cfg.poly_funder,
                    )
                )
            except Exception as e:
                pm_live_error = str(e)
                live_status["polymarket_live_error"] = pm_live_error
                write_json(p_live, live_status)
                pm_live_client = None

        # If killswitch is active and we have a live client, cancel all open orders and skip trading actions.
        if killswitch_active(cfg) and pm_live_client is not None:
            try:
                resp = pm_cancel_all_orders(pm_live_client)
                append_csv_row(
                    p_pm_orders,
                    ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                    [ts, "*", "*", "*", "", "", "canceled_all", "", json.dumps(resp, ensure_ascii=False)[:500]],
                )
            except Exception as e:
                append_csv_row(
                    p_pm_orders,
                    ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                    [ts, "*", "*", "*", "", "", "cancel_all_error", "", str(e)[:500]],
                )
            # Still record scan row below.

        # Always snapshot open orders (stubbed when live client is not configured).
        p_open = out / "pm_open_orders.json"
        open_orders_payload: dict[str, Any] = {
            "generated_at": ts,
            "ok": False,
            "error": "live_client_not_configured",
            "count": 0,
            "data": [],
        }
        if pm_live_client is not None:
            try:
                open_orders = pm_get_open_orders(pm_live_client)
                open_orders_payload.update(
                    {
                        "ok": True,
                        "error": None,
                        "count": len(open_orders),
                        "data": open_orders,
                    }
                )
            except Exception as e:
                open_orders_payload["error"] = str(e)
                live_status["pm_open_orders_error"] = str(e)
                write_json(p_live, live_status)

        write_json(p_open, open_orders_payload)
        files.append(p_open)
        # Lead-lag strategy path: Kraken spot leads, Polymarket CLOB lags.
        if cfg.strategy_mode == "lead_lag":
            if lead_lag_engine is None:
                raise RuntimeError("lead_lag_engine is required when STRATEGY_MODE=lead_lag")

            # Cache spot tickers per pair per tick.
            spot_by_pair: dict[str, float] = {}
            spot_ts_by_pair: dict[str, datetime] = {}

            for mkt in mkts:
                market_name = str(mkt.get("name") or "market")

                # Resolve desired PM outcome token
                token_id: str | None = None
                chosen_outcome: str | None = None
                market_ref: str | None = None
                pm_block = mkt.get("polymarket")
                if isinstance(pm_block, dict):
                    pm_cfg = cast(dict[str, Any], pm_block)
                    token_id = str(pm_cfg.get("clob_token_id", "") or "").strip() or None
                    chosen_outcome = str(pm_cfg.get("outcome") or "").strip() or None
                    market_ref = str(pm_cfg.get("market_url") or pm_cfg.get("market_slug") or "").strip() or None

                    # Allow per-market side if outcome not set
                    if not chosen_outcome:
                        side_raw = str(pm_cfg.get("side") or "").strip().upper()
                        if side_raw in {"YES", "NO"}:
                            chosen_outcome = "Yes" if side_raw == "YES" else "No"

                if not chosen_outcome:
                    # Default to the strategy side (YES/NO), but allow fair-model driven inference
                    # for reach/touch-style questions.
                    desired_outcome = "Yes" if cfg.lead_lag_side == "YES" else "No"
                    try:
                        fm_any = mkt.get("fair_model")
                        fm = cast(dict[str, Any], fm_any) if isinstance(fm_any, dict) else {}
                        fair_mode = str(fm.get("mode") or "").strip().lower()
                        if fair_mode == "deribit_touch":
                            direction = str(fm.get("direction") or "").strip().lower()
                            if direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"}:
                                gm = None
                                if market_ref:
                                    gm = cache.gamma_market_by_slug.get(market_ref)
                                    if gm is None:
                                        t_g0 = time.perf_counter()
                                        gm = gamma.get_market_by_slug(slug=market_ref)
                                        if latency_tracker is not None:
                                            latency_tracker.record_gamma_fetch(float((time.perf_counter() - t_g0) * 1000.0))
                                        cache.gamma_market_by_slug[market_ref] = gm
                                if gm is not None:
                                    desired_outcome = gamma.infer_yes_no_for_touch_event(market=gm, event=direction)
                    except Exception:
                        pass
                    chosen_outcome = desired_outcome

                if not token_id and market_ref and chosen_outcome:
                    try:
                        cache_key = (market_ref, chosen_outcome)
                        if cache_key in cache.token_id_by_slug_outcome:
                            token_id = cache.token_id_by_slug_outcome[cache_key]
                        else:
                            gm = cache.gamma_market_by_slug.get(market_ref)
                            if gm is None:
                                t_g0 = time.perf_counter()
                                gm = gamma.get_market_by_slug(slug=market_ref)
                                if latency_tracker is not None:
                                    latency_tracker.record_gamma_fetch(float((time.perf_counter() - t_g0) * 1000.0))
                                cache.gamma_market_by_slug[market_ref] = gm
                            token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen_outcome)
                            cache.token_id_by_slug_outcome[cache_key] = token_id
                        sources_health["polymarket"]["gamma"] = {"ok": True}
                    except Exception as e:
                        sources_health["polymarket"]["gamma"] = {"ok": False, "error": str(e)}
                        token_id = None

                if not token_id:
                    append_csv_row(
                        p_pm_paper_candidates,
                        [
                            "ts",
                            "market",
                            "market_ref",
                            "token",
                            "outcome",
                            "pm_bid",
                            "pm_ask",
                            "pm_mid",
                            "odds",
                            "odds_allowed",
                            "fair_p",
                            "ev",
                            "edge",
                            "spread",
                            "cost_est",
                            "edge_net",
                            "signal",
                            "decision",
                            "reason",
                        ],
                        [
                            ts,
                            market_name,
                            market_ref or "",
                            "",
                            chosen_outcome or "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "skip",
                            "no_token",
                        ],
                        keep_last=5000,
                    )
                    continue

                # Spot pair: global default or per-market override
                pair = cfg.kraken_spot_pair
                kspot_block = mkt.get("kraken_spot")
                if isinstance(kspot_block, dict):
                    pair = str(cast(dict[str, Any], kspot_block).get("pair") or pair).strip() or pair

                # Fetch spot once per pair
                if pair not in spot_by_pair:
                    try:
                        t_spot0 = time.perf_counter()
                        tick = kr_spot.get_ticker_last(pair=pair)
                        if latency_tracker is not None:
                            latency_tracker.record_spot_fetch(float((time.perf_counter() - t_spot0) * 1000.0))
                        spot_by_pair[pair] = float(tick.last)
                        spot_ts_by_pair[pair] = tick.ts
                    except Exception as e:
                        sources_health["kraken"].setdefault("spot", {})
                        sources_health["kraken"]["spot"] = {"ok": False, "error": str(e)}
                        spot_by_pair[pair] = float("nan")
                        spot_ts_by_pair[pair] = ts_dt

                spot_price = float(spot_by_pair[pair])

                # PM orderbook (bid/ask/mid)
                bid: float | None = None
                ask: float | None = None
                pm_mid: float | None = None
                ob: dict[str, Any] | None = None
                try:
                    t_ob0 = time.perf_counter()
                    ob = pm_clob.get_orderbook(token_id)
                    if latency_tracker is not None:
                        latency_tracker.record_orderbook_fetch(float((time.perf_counter() - t_ob0) * 1000.0))
                    bid, ask = best_bid_ask(ob)
                    if bid is not None and ask is not None and bid > 0 and ask > 0:
                        pm_mid = (bid + ask) / 2.0
                except Exception as e:
                    pm_mid = None

                if pm_mid is None or not (spot_price == spot_price):
                    append_csv_row(
                        p_pm_paper_candidates,
                        [
                            "ts",
                            "market",
                            "market_ref",
                            "token",
                            "outcome",
                            "pm_bid",
                            "pm_ask",
                            "pm_mid",
                            "odds",
                            "odds_allowed",
                            "fair_p",
                            "ev",
                            "edge",
                            "spread",
                            "cost_est",
                            "edge_net",
                            "signal",
                            "decision",
                            "reason",
                        ],
                        [
                            ts,
                            market_name,
                            market_ref or "",
                            token_id,
                            chosen_outcome or "",
                            bid if bid is not None else "",
                            ask if ask is not None else "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "",
                            "skip",
                            "missing_price",
                        ],
                        keep_last=5000,
                    )
                    continue

                # Freshness gating (safety): if last successful tick is too old, do not trade.
                spot_age = (ts_dt - spot_ts_by_pair.get(pair, ts_dt)).total_seconds()
                # pm_mid is computed this tick; treat as age 0 when we got it.
                pm_age = 0.0
                is_fresh = (spot_age <= cfg.freshness_max_age_s) and (pm_age <= cfg.freshness_max_age_s)

                # Update lead-lag history and compute edge
                ll_key = f"{market_name}:{token_id}:{pair}"

                snap = lead_lag_engine.update_and_compute(
                    key=ll_key,
                    ts=ts_dt,
                    spot_price=spot_price,
                    pm_mid_price=float(pm_mid),
                    lookback_points=int(cfg.lead_lag_lookback_points),
                )

                # Estimate PM lag behind spot (observability).
                try:
                    # Use a lower min_points so the portal shows a lag estimate sooner after restart.
                    # Keep it >= 6 so correlation has enough samples to be meaningful.
                    est = lead_lag_engine.estimate_market_lag(key=ll_key, min_points=6)
                    # Only treat lag as available when the estimate is OK.
                    # If we already saw an OK estimate earlier in this tick, do not overwrite it with a later failure.
                    if est.ok:
                        live_status["market_lag_reason"] = est.reason
                        live_status["market_lag_points"] = int(est.lag_points) if est.lag_points is not None else None
                        live_status["market_lag_confidence"] = float(abs(est.best_corr)) if est.best_corr is not None else None
                        if est.lag_ms is not None:
                            live_status.setdefault("market_lag_ms_samples", [])
                            cast(list[Any], live_status["market_lag_ms_samples"]).append(float(est.lag_ms))
                    else:
                        if live_status.get("market_lag_reason") is None:
                            live_status["market_lag_reason"] = est.reason
                            live_status["market_lag_points"] = int(est.lag_points) if est.lag_points is not None else None
                            live_status["market_lag_confidence"] = float(abs(est.best_corr)) if est.best_corr is not None else None
                except Exception:
                    pass

                spot_ret = None
                pm_ret = None
                edge_pct = None
                if snap is not None:
                    spot_ret = float(snap.spot_ret_pct)
                    pm_ret = float(snap.pm_ret_pct)
                    edge_pct = float(LeadLagEngine.compute_edge_for_side(side=cfg.lead_lag_side, snap=snap))

                # Always write an edge row per market when we have enough history
                if edge_pct is not None:
                    computed_rows.append(
                        {
                            "ts": ts,
                            "market": market_name,
                            "fair_p": 0.0,
                            "pm_price": float(pm_mid),
                            "edge": float(edge_pct),
                            "sources": "kraken_spot+pm_clob",
                            "notes": f"lead_lag side={cfg.lead_lag_side} pair={pair} spot_ret={spot_ret:.4f}% pm_ret={pm_ret:.4f}%",
                        }
                    )

                # Trading decisions only when fresh + enough history
                if not is_fresh or edge_pct is None:
                    append_csv_row(
                        p_pm_paper_candidates,
                        [
                            "ts",
                            "market",
                            "market_ref",
                            "token",
                            "outcome",
                            "pm_bid",
                            "pm_ask",
                            "pm_mid",
                            "odds",
                            "odds_allowed",
                            "fair_p",
                            "ev",
                            "edge",
                            "spread",
                            "cost_est",
                            "edge_net",
                            "signal",
                            "decision",
                            "reason",
                        ],
                        [
                            ts,
                            market_name,
                            market_ref or "",
                            token_id,
                            chosen_outcome or "",
                            bid if bid is not None else "",
                            ask if ask is not None else "",
                            float(pm_mid),
                            "",
                            "",
                            "",
                            "",
                            float(edge_pct) if edge_pct is not None else "",
                            "",
                            "",
                            "",
                            "",
                            "skip",
                            "stale_or_warmup" if not is_fresh else "warmup",
                        ],
                        keep_last=5000,
                    )
                    continue

                # Price zone guards
                if float(pm_mid) > cfg.lead_lag_avoid_price_above or float(pm_mid) < cfg.lead_lag_avoid_price_below:
                    append_csv_row(
                        p_pm_paper_candidates,
                        [
                            "ts",
                            "market",
                            "market_ref",
                            "token",
                            "outcome",
                            "pm_bid",
                            "pm_ask",
                            "pm_mid",
                            "odds",
                            "odds_allowed",
                            "fair_p",
                            "ev",
                            "edge",
                            "spread",
                            "cost_est",
                            "edge_net",
                            "signal",
                            "decision",
                            "reason",
                        ],
                        [
                            ts,
                            market_name,
                            market_ref or "",
                            token_id,
                            chosen_outcome or "",
                            bid if bid is not None else "",
                            ask if ask is not None else "",
                            float(pm_mid),
                            "",
                            "",
                            "",
                            "",
                            float(edge_pct),
                            "",
                            "",
                            "",
                            "",
                            "skip",
                            "avoid_price_zone",
                        ],
                        keep_last=5000,
                    )
                    continue

                # Determine whether we are already in position for this token
                pos = paper_positions.get(token_id)
                in_pos = pos is not None and float(pos.get("shares") or 0.0) > 0

                # Entry safety: avoid trading into very wide spreads or extreme executable prices.
                # (Entry executes at ask; using mid for gating can otherwise create false-positive edges.)
                if not in_pos:
                    try:
                        spread = float(ask) - float(bid)  # type: ignore[arg-type]
                    except Exception:
                        spread = float("inf")

                    if spread > float(cfg.lead_lag_slippage_cap):
                        append_csv_row(
                            p_pm_paper_candidates,
                            [
                                "ts",
                                "market",
                                "market_ref",
                                "token",
                                "outcome",
                                "pm_bid",
                                "pm_ask",
                                "pm_mid",
                                "odds",
                                "odds_allowed",
                                "fair_p",
                                "ev",
                                "edge",
                                "spread",
                                "cost_est",
                                "edge_net",
                                "signal",
                                "decision",
                                "reason",
                            ],
                            [
                                ts,
                                market_name,
                                market_ref or "",
                                token_id,
                                chosen_outcome or "",
                                bid if bid is not None else "",
                                ask if ask is not None else "",
                                float(pm_mid),
                                "",
                                "",
                                "",
                                "",
                                float(edge_pct) if edge_pct is not None else "",
                                float(spread),
                                "",
                                "",
                                "watch",
                                "skip",
                                f"wide_spread>{cfg.lead_lag_slippage_cap}",
                            ],
                            keep_last=5000,
                        )
                        continue

                    # Executable entry price guard (BUY at ask).
                    if float(ask) > cfg.lead_lag_avoid_price_above or float(ask) < cfg.lead_lag_avoid_price_below:  # type: ignore[arg-type]
                        append_csv_row(
                            p_pm_paper_candidates,
                            [
                                "ts",
                                "market",
                                "market_ref",
                                "token",
                                "outcome",
                                "pm_bid",
                                "pm_ask",
                                "pm_mid",
                                "odds",
                                "odds_allowed",
                                "fair_p",
                                "ev",
                                "edge",
                                "spread",
                                "cost_est",
                                "edge_net",
                                "signal",
                                "decision",
                                "reason",
                            ],
                            [
                                ts,
                                market_name,
                                market_ref or "",
                                token_id,
                                chosen_outcome or "",
                                bid if bid is not None else "",
                                ask if ask is not None else "",
                                float(pm_mid),
                                "",
                                "",
                                "",
                                "",
                                float(edge_pct) if edge_pct is not None else "",
                                "",
                                "",
                                "",
                                "watch",
                                "skip",
                                "avoid_price_zone_executable",
                            ],
                            keep_last=5000,
                        )
                        continue

                # Precompute spread cost (percent points) so we can use it in adaptive move gating.
                spread_cost_pct: float | None = None
                try:
                    if bid is not None and ask is not None:
                        spread = float(ask) - float(bid)
                        half_spread = spread / 2.0
                        denom = max(float(pm_mid), 1e-12)
                        spread_cost_pct = (half_spread / denom) * 100.0
                except Exception:
                    spread_cost_pct = None

                # Adaptive spot move threshold: require spot move > recent noise and > spread cost proxy.
                spot_noise_pct: float | None = None
                try:
                    spot_noise_pct = lead_lag_engine.estimate_spot_noise_pct(
                        key=ll_key,
                        window_points=int(cfg.lead_lag_spot_noise_window_points),
                        min_points=10,
                    )
                except Exception:
                    spot_noise_pct = None

                spot_move_min_dyn = float(cfg.lead_lag_spot_move_min_pct)
                if spot_noise_pct is not None:
                    spot_move_min_dyn = max(spot_move_min_dyn, float(cfg.lead_lag_spot_noise_mult) * float(spot_noise_pct))
                if spread_cost_pct is not None:
                    spot_move_min_dyn = max(spot_move_min_dyn, float(cfg.lead_lag_spread_move_mult) * float(spread_cost_pct))

                # Surface the current adaptive threshold in live_status (last processed market).
                live_status["lead_lag_spot_move_min_pct_dynamic"] = float(spot_move_min_dyn)
                live_status["lead_lag_spot_noise_pct"] = float(spot_noise_pct) if spot_noise_pct is not None else None
                live_status["lead_lag_spread_cost_pct"] = float(spread_cost_pct) if spread_cost_pct is not None else None

                # Entry direction gating based on side
                if cfg.lead_lag_side == "YES":
                    spot_move_ok = spot_ret is not None and float(spot_ret) >= float(spot_move_min_dyn)
                else:
                    # NO: spot down should be a positive move
                    spot_move_ok = spot_ret is not None and (-float(spot_ret)) >= float(spot_move_min_dyn)

                # Exit signals
                hold_secs = 0.0
                if in_pos:
                    opened_at = str(pos.get("opened_at") or ts)
                    try:
                        hold_secs = (ts_dt - _parse_iso_dt(opened_at)).total_seconds()
                    except Exception:
                        hold_secs = 0.0

                enter_raw = (not in_pos) and spot_move_ok and float(edge_pct) >= float(cfg.lead_lag_edge_min_pct)
                exit_ok = False
                exit_reason = ""
                if in_pos:
                    if float(edge_pct) <= float(cfg.lead_lag_edge_exit_pct):
                        exit_ok = True
                        exit_reason = "edge_exit"
                    elif hold_secs >= float(cfg.lead_lag_max_hold_secs):
                        exit_ok = True
                        exit_reason = "max_hold"
                    elif cfg.lead_lag_pm_stop_pct and float(cfg.lead_lag_pm_stop_pct) > 0:
                        entry_price = float(pos.get("avg_entry") or pm_mid)
                        pm_move_pct = (float(pm_mid) / max(entry_price, 1e-12) - 1.0) * 100.0
                        if pm_move_pct <= -abs(float(cfg.lead_lag_pm_stop_pct)):
                            exit_ok = True
                            exit_reason = "stop"

                # Update edge calculator snapshot (percent points).

                fees_pct = (float(cfg.pm_est_fee_pct) + float(cfg.pm_edge_extra_cost_pct)) * 100.0
                net_edge_pct: float | None = None
                if edge_pct is not None and spread_cost_pct is not None:
                    net_edge_pct = float(edge_pct) - float(spread_cost_pct) - float(fees_pct)

                # Quality gates for entering a position (after-cost and microstructure constraints)
                enter_ok = bool(enter_raw)
                enter_block_reason = ""

                # Gate 1: estimated market lag must be large enough (optional; only blocks when lag is known)
                try:
                    if enter_ok and float(cfg.lead_lag_min_market_lag_ms) > 0 and lag_ms is not None:
                        if float(lag_ms) < float(cfg.lead_lag_min_market_lag_ms):
                            enter_ok = False
                            enter_block_reason = "lag_too_short"
                except Exception:
                    pass

                # Gate 2: spread cost too high (percent points)
                if enter_ok and spread_cost_pct is not None:
                    if float(spread_cost_pct) > float(cfg.lead_lag_spread_cost_cap_pct):
                        enter_ok = False
                        enter_block_reason = "spread_too_high"

                # Gate 3: net edge must be positive enough after spread+fees
                if enter_ok and net_edge_pct is not None:
                    if float(net_edge_pct) < float(cfg.lead_lag_net_edge_min_pct):
                        enter_ok = False
                        enter_block_reason = "net_edge_too_low"

                # Orderbook sizing for entry
                desired_shares = float(cfg.pm_order_size_shares)
                max_usdc = None
                if enter_ok and ob is not None and cfg.lead_lag_enable_orderbook_sizing:
                    try:
                        asks = _safe_top_levels(ob.get("asks"), max_levels=200)
                        best_ask = float(ask) if ask is not None else (float(asks[0]["price"]) if asks else float(pm_mid))
                        limit = float(best_ask) + float(cfg.lead_lag_slippage_cap)
                        _liq_shares, liq_usdc = _sum_book_usdc_in_band(asks, price_leq=limit)
                        max_usdc = min(float(cfg.lead_lag_hard_cap_usdc), float(liq_usdc) * float(cfg.lead_lag_max_fraction_of_band_liquidity))
                        max_shares = 0.0 if best_ask <= 0 else float(max_usdc) / float(best_ask)
                        if desired_shares <= 0:
                            desired_shares = max_shares
                        else:
                            desired_shares = min(desired_shares, max_shares)
                    except Exception:
                        max_usdc = None

                # Gate 4: insufficient liquidity (based on orderbook sizing band)
                if enter_ok:
                    if max_usdc is not None and float(max_usdc) < float(cfg.lead_lag_min_trade_notional_usdc):
                        enter_ok = False
                        enter_block_reason = "insufficient_liquidity"

                # Gate 5: throttle (max orders per tick)
                if enter_ok and signals_emitted >= cfg.pm_max_orders_per_tick:
                    enter_ok = False
                    enter_block_reason = "throttled"

                # Decide final status/reason for observability
                execution_status = "SKIPPED"
                reason = ""
                if exit_ok:
                    execution_status = "TRIGGERED"
                    reason = exit_reason or "exit"
                elif enter_ok:
                    execution_status = "TRIGGERED"
                    reason = "enter"
                else:
                    if not spot_move_ok:
                        reason = "spot_move_too_small"
                    elif float(edge_pct) < float(cfg.lead_lag_edge_min_pct):
                        reason = "low_edge"
                    elif enter_block_reason:
                        reason = enter_block_reason
                    elif in_pos:
                        reason = "hold"
                    else:
                        reason = "no_signal"

                append_csv_row(
                    p_edge_calc,
                    [
                        "ts",
                        "market",
                        "signal_strength",
                        "raw_edge",
                        "spread_cost",
                        "fees",
                        "net_edge",
                        "execution_status",
                        "reason",
                    ],
                    [
                        ts,
                        market_name,
                        abs(float(edge_pct)) if edge_pct is not None else "",
                        float(edge_pct) if edge_pct is not None else "",
                        float(spread_cost_pct) if spread_cost_pct is not None else "",
                        float(fees_pct),
                        float(net_edge_pct) if net_edge_pct is not None else "",
                        execution_status,
                        reason,
                    ],
                    keep_last=2000,
                )

                if health_tracker is not None:
                    try:
                        health_tracker.record(
                            market=market_name,
                            token_id=token_id,
                            edge_pct=edge_pct,
                            net_edge_pct=net_edge_pct,
                            spread_cost_pct=spread_cost_pct,
                            lag_ms=lag_ms if "lag_ms" in locals() else None,
                            spot_ret_pct=spot_ret,
                            max_usdc=max_usdc,
                            execution_status=execution_status,
                            reason=reason,
                        )
                    except Exception:
                        pass

                # Enter: BUY at best ask
                if enter_raw and not enter_ok and enter_block_reason in {"throttled", "insufficient_liquidity", "spread_too_high", "net_edge_too_low", "lag_too_short"}:
                    # Surface the block in orders log (helps explain skipped opportunities)
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, "buy", token_id, float(ask or pm_mid), float(desired_shares), "skipped", "", f"blocked:{enter_block_reason}"],
                    )

                if enter_ok:
                    fill_price = float(ask or pm_mid)
                    notional = float(fill_price) * float(desired_shares)
                    paper_status = "filled"
                    paper_notes = ""
                    if desired_shares <= 0:
                        paper_status = "rejected"
                        paper_notes = "size_zero"
                    elif paper_cash + 1e-9 < notional:
                        paper_status = "rejected"
                        paper_notes = "insufficient_cash"
                    else:
                        prev = paper_positions.get(token_id)
                        prev_shares = float(prev.get("shares") or 0.0) if prev is not None else 0.0
                        prev_avg = float(prev.get("avg_entry") or fill_price) if prev is not None else float(fill_price)
                        new_shares = prev_shares + float(desired_shares)
                        new_avg = ((prev_shares * prev_avg) + (float(desired_shares) * float(fill_price))) / max(new_shares, 1e-9)
                        paper_positions[token_id] = {
                            "market": market_name,
                            "outcome": chosen_outcome,
                            "shares": float(new_shares),
                            "avg_entry": float(new_avg),
                            "opened_at": ts,
                        }
                        paper_cash -= notional
                        paper_notes = f"lead_lag edge={edge_pct:.4f}% max_usdc={max_usdc:.2f}" if max_usdc is not None else f"lead_lag edge={edge_pct:.4f}%"

                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, "buy", token_id, float(fill_price), float(desired_shares), "paper", "", paper_notes],
                    )
                    append_csv_row(
                        p_pm_paper_trades,
                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                        [ts, market_name, token_id, chosen_outcome or "", "BUY", float(fill_price), float(desired_shares), float(notional), float(paper_cash), paper_status, paper_notes],
                        keep_last=500,
                    )
                    if paper_status in {"filled", "rejected"}:
                        signals_emitted += 1
                    continue

                # Exit: SELL all at best bid
                if exit_ok:
                    shares_to_sell = float(pos.get("shares") or 0.0) if pos is not None else 0.0
                    fill_price = float(bid or pm_mid)
                    notional = float(fill_price) * float(shares_to_sell)
                    avg_entry = float(pos.get("avg_entry") or fill_price) if pos is not None else float(fill_price)
                    paper_cash += notional
                    paper_realized += (float(fill_price) - float(avg_entry)) * float(shares_to_sell)
                    paper_positions.pop(token_id, None)

                    notes = f"lead_lag exit={exit_reason} edge={edge_pct:.4f}%"
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, "sell", token_id, float(fill_price), float(shares_to_sell), "paper", "", notes],
                    )
                    append_csv_row(
                        p_pm_paper_trades,
                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                        [ts, market_name, token_id, chosen_outcome or "", "SELL", float(fill_price), float(shares_to_sell), float(notional), float(paper_cash), "filled", notes],
                        keep_last=500,
                    )
                    signals_emitted += 1
                    continue

                # No trade this tick, but log candidate
                append_csv_row(
                    p_pm_paper_candidates,
                    [
                        "ts",
                        "market",
                        "market_ref",
                        "token",
                        "outcome",
                        "pm_bid",
                        "pm_ask",
                        "pm_mid",
                        "odds",
                        "odds_allowed",
                        "fair_p",
                        "ev",
                        "edge",
                        "spread",
                        "cost_est",
                        "edge_net",
                        "signal",
                        "decision",
                        "reason",
                    ],
                    [
                        ts,
                        market_name,
                        market_ref or "",
                        token_id,
                        chosen_outcome or "",
                        bid if bid is not None else "",
                        ask if ask is not None else "",
                        float(pm_mid),
                        "",
                        "",
                        "",
                        "",
                        float(edge_pct),
                        "",
                        "",
                        "",
                        "hold" if in_pos else "watch",
                        "skip",
                        reason or "no_signal",
                    ],
                    keep_last=5000,
                )

            # After lead-lag loop
            pm_status["edges_computed"] = len(computed_rows)
            pm_status["signals_emitted"] = signals_emitted

        mkts_fair = [] if cfg.strategy_mode == "lead_lag" else mkts
        for mkt in mkts_fair:
            market_name = str(mkt.get("name") or "market")

            token_id: str | None = None
            chosen_outcome: str | None = None
            chosen_yes_no: str | None = None  # 'yes' | 'no' | None
            market_ref: str | None = None
            pm_block = mkt.get("polymarket")
            if isinstance(pm_block, dict):
                pm_cfg = cast(dict[str, Any], pm_block)
                token_id = str(pm_cfg.get("clob_token_id", "") or "").strip() or None
                chosen_outcome = str(pm_cfg.get("outcome") or "").strip() or None
                market_ref = str(pm_cfg.get("market_url") or pm_cfg.get("market_slug") or "").strip() or None

            symbol: str | None = None
            testnet = cfg.kraken_futures_testnet
            ref_field: str | None = None
            k_block = mkt.get("kraken_futures")
            if isinstance(k_block, dict):
                k_cfg = cast(dict[str, Any], k_block)
                symbol = str(k_cfg.get("symbol", "") or "").strip() or None
                testnet = bool(k_cfg.get("testnet", testnet))
                ref_field = str(k_cfg.get("ref_price_field", "") or "").strip() or None

            fair_model: dict[str, Any] = {"mode": "constant", "p": 0.5}
            fm = mkt.get("fair_model")
            if isinstance(fm, dict):
                fair_model = cast(dict[str, Any], fm)

            # Compute fair probability. Default: uses Kraken Futures ref price.
            fair_mode = str(fair_model.get("mode", "constant")).strip().lower()
            fair_p: float
            kr_ref: float | None = None
            rn_debug: dict[str, Any] | None = None
            if fair_mode == "deribit_rn":
                try:
                    rn = deribit.compute_rn_probability_from_model(model=fair_model)
                    rn_debug = rn
                    rn_prob_any = rn.get("rn_prob")
                    if rn_prob_any is None:
                        raise ValueError("Deribit RN result missing rn_prob")
                    fair_p = float(rn_prob_any)
                    deribit_used += 1
                    deribit_snapshot["markets"].append({"name": market_name, "ok": True, "data": rn})
                except Exception as e:
                    deribit_snapshot["markets"].append({"name": market_name, "ok": False, "error": str(e)})
                    continue
            elif fair_mode == "deribit_touch":
                try:
                    touch = deribit.compute_touch_probability_from_model(model=fair_model)
                    rn_debug = touch
                    event_prob_any = touch.get("event_prob")
                    if event_prob_any is None:
                        raise ValueError("Deribit touch result missing event_prob")
                    fair_p = float(event_prob_any)
                    deribit_used += 1
                    deribit_snapshot["markets"].append({"name": market_name, "ok": True, "data": touch})
                except Exception as e:
                    deribit_snapshot["markets"].append({"name": market_name, "ok": False, "error": str(e)})
                    continue
            else:
                if not symbol:
                    continue
                kr_ref = None
                k = KrakenFuturesApi(testnet=testnet)
                t = k.get_ticker(symbol)
                fields = [ref_field] if ref_field else []
                fields += ["markPrice", "indexPrice", "last", "lastPrice"]
                for key in fields:
                    if not key:
                        continue
                    if key in t:
                        try:
                            kr_ref = float(t[key])
                            break
                        except Exception:
                            pass
                if kr_ref is None or kr_ref <= 0:
                    continue
                fair_p = compute_fair_probability(model=fair_model, ref_price=kr_ref)

            # Resolve Polymarket token_id automatically when market URL/slug is provided.
            # If no explicit outcome is configured, compare YES/NO (binary) and choose the best edge within the odds band.
            auto_skip_reason: str | None = None
            if not token_id and market_ref:
                try:
                    gm = gamma.get_market_by_slug(slug=market_ref)

                    # Normalize YES/NO mapping for fair_p.
                    event_outcome_label: str | None = None
                    if fair_mode == "deribit_touch":
                        direction = str(fair_model.get("direction") or "").strip().lower()
                        if direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"}:
                            event_outcome_label = gamma.infer_yes_no_for_touch_event(market=gm, event=direction)
                    if event_outcome_label is None:
                        event_outcome_label = "Yes"

                    def _yn(label: str | None) -> str | None:
                        if label is None:
                            return None
                        s = label.strip().lower()
                        if s == "yes":
                            return "yes"
                        if s == "no":
                            return "no"
                        return None

                    # If explicit outcome is provided, respect it.
                    if chosen_outcome:
                        token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen_outcome)
                        chosen_yes_no = _yn(chosen_outcome)
                    else:
                        # Auto-compare outcomes only for simple binary markets.
                        outcomes = list(gm.outcomes)
                        token_ids = list(gm.clob_token_ids)
                        if len(outcomes) != 2 or len(token_ids) != 2:
                            raise ValueError(
                                "Market is not a simple 2-outcome (YES/NO) market; set polymarket.outcome explicitly."
                            )

                        candidates: list[dict[str, Any]] = []
                        observed: list[dict[str, Any]] = []
                        saw_orderbook = False
                        rejected_by_odds = 0
                        rejected_by_ev = 0
                        for out_label, out_token in zip(outcomes, token_ids, strict=False):
                            try:
                                ob = pm_clob.get_orderbook(out_token)
                                bid, ask = best_bid_ask(ob)
                                if bid is None or ask is None or bid <= 0 or ask <= 0:
                                    continue
                                saw_orderbook = True
                                mid = (bid + ask) / 2.0
                                fair_outcome_p = fair_p
                                if out_label.strip().lower() != event_outcome_label.strip().lower():
                                    fair_outcome_p = 1.0 - fair_p

                                # Positive EV for BUY is (fair - price).
                                ev = float(fair_outcome_p) - float(mid)
                                edge_outcome = float(mid) - float(fair_outcome_p)

                                odds_allowed = _price_allowed_by_odds(cfg, price=float(mid))
                                odds_any = _price_to_decimal_odds(float(mid))
                                observed.append(
                                    {
                                        "outcome": out_label,
                                        "token_id": out_token,
                                        "pm_bid": float(bid),
                                        "pm_ask": float(ask),
                                        "pm_price": float(mid),
                                        "odds": float(odds_any) if odds_any is not None else None,
                                        "odds_allowed": bool(odds_allowed),
                                        "fair_p": float(fair_outcome_p),
                                        "edge": float(edge_outcome),
                                        "ev": float(ev),
                                    }
                                )

                                if not odds_allowed:
                                    rejected_by_odds += 1
                                    continue

                                if ev <= 0:
                                    rejected_by_ev += 1
                                    continue
                                candidates.append(
                                    {
                                        "outcome": out_label,
                                        "token_id": out_token,
                                        "pm_price": float(mid),
                                        "fair_p": float(fair_outcome_p),
                                        "edge": float(edge_outcome),
                                        "ev": float(ev),
                                    }
                                )
                            except Exception:
                                continue

                        if not candidates:
                            # Market was found, but nothing was eligible.
                            if not saw_orderbook:
                                auto_skip_reason = "no_liquidity"
                                token_id = None
                            else:
                                if rejected_by_odds >= 2:
                                    auto_skip_reason = "odds_filter"
                                elif rejected_by_ev >= 2:
                                    auto_skip_reason = "negative_ev"
                                else:
                                    auto_skip_reason = "no_candidate"

                                # Still pick a representative outcome token so we can log pm bid/ask/mid/odds in pm_paper_candidates.csv.
                                # This does NOT place trades (the later decision gate will still skip).
                                rep: dict[str, Any] | None = None
                                if observed:
                                    min_odds = cfg.pm_min_odds
                                    max_odds = cfg.pm_max_odds

                                    def _odds_distance(o: dict[str, Any]) -> float:
                                        if min_odds is None or max_odds is None:
                                            return 0.0
                                        ov = o.get("odds")
                                        if not isinstance(ov, (int, float)):
                                            return 0.0
                                        odds_v = float(ov)
                                        if min_odds <= odds_v <= max_odds:
                                            return 0.0
                                        if odds_v < min_odds:
                                            return min_odds - odds_v
                                        return odds_v - max_odds

                                    # Prefer the one closest to odds band; break ties by higher EV.
                                    rep = sorted(
                                        observed,
                                        key=lambda o: (
                                            _odds_distance(o),
                                            -float(o.get("ev") or 0.0),
                                        ),
                                    )[0]

                                if rep is None:
                                    token_id = None
                                else:
                                    token_id = str(rep["token_id"])
                                    chosen_outcome = str(rep["outcome"])
                                    chosen_yes_no = _yn(chosen_outcome)
                                    fair_p = float(rep["fair_p"])
                        else:
                            best = sorted(candidates, key=lambda r: float(r.get("ev") or 0.0), reverse=True)[0]
                            token_id = str(best["token_id"])
                            chosen_outcome = str(best["outcome"])
                            chosen_yes_no = _yn(chosen_outcome)

                            # Override fair_p/pm_price for this chosen outcome.
                            fair_p = float(best["fair_p"])
                except Exception as e:
                    pm_status.setdefault("gamma_errors", []).append({"market": market_name, "error": str(e)})
                    sources_health["polymarket"]["gamma"] = {"ok": False, "error": str(e)}
                    token_id = None

            if chosen_yes_no is None and chosen_outcome is not None:
                s = chosen_outcome.strip().lower()
                if s == "yes":
                    chosen_yes_no = "yes"
                elif s == "no":
                    chosen_yes_no = "no"

            if not token_id:
                append_csv_row(
                    p_pm_paper_candidates,
                    [
                        "ts",
                        "market",
                        "market_ref",
                        "token",
                        "outcome",
                        "pm_bid",
                        "pm_ask",
                        "pm_mid",
                        "odds",
                        "odds_allowed",
                        "fair_p",
                        "ev",
                        "edge",
                        "spread",
                        "cost_est",
                        "edge_net",
                        "signal",
                        "decision",
                        "reason",
                    ],
                    [
                        ts,
                        market_name,
                        market_ref or "",
                        "",
                        chosen_outcome or "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        fair_p,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "skip",
                        auto_skip_reason or "no_token",
                    ],
                    keep_last=5000,
                )
                continue

            pm_price: float | None = None
            bid: float | None = None
            ask: float | None = None
            try:
                ob = pm_clob.get_orderbook(token_id)
                bid, ask = best_bid_ask(ob)
                if bid is not None and ask is not None and bid > 0 and ask > 0:
                    pm_price = (bid + ask) / 2.0
            except Exception:
                pm_price = None

            if pm_price is None:
                append_csv_row(
                    p_pm_paper_candidates,
                    [
                        "ts",
                        "market",
                        "market_ref",
                        "token",
                        "outcome",
                        "pm_bid",
                        "pm_ask",
                        "pm_mid",
                        "odds",
                        "odds_allowed",
                        "fair_p",
                        "ev",
                        "edge",
                        "spread",
                        "cost_est",
                        "edge_net",
                        "signal",
                        "decision",
                        "reason",
                    ],
                    [
                        ts,
                        market_name,
                        market_ref or "",
                        token_id,
                        chosen_outcome or "",
                        bid if bid is not None else "",
                        ask if ask is not None else "",
                        "",
                        "",
                        "",
                        fair_p,
                        "",
                        "",
                        "",
                        "",
                        "",
                        "",
                        "skip",
                        "no_price",
                    ],
                    keep_last=5000,
                )
                continue

            odds_allowed = _price_allowed_by_odds(cfg, price=float(pm_price))
            odds_any = _price_to_decimal_odds(float(pm_price))
            odds_str = f"{odds_any:.4f}" if odds_any is not None else ""

            hedge: dict[str, Any] = {}
            h = mkt.get("hedge")
            if isinstance(h, dict):
                hedge = cast(dict[str, Any], h)

            edge = pm_price - fair_p
            ev = float(fair_p) - float(pm_price)

            spread: float | None = None
            half_spread: float | None = None
            cost_est: float = 0.0
            edge_net: float | None = None
            if bid is not None and ask is not None and float(ask) > 0 and float(bid) > 0 and float(ask) >= float(bid):
                spread = float(ask) - float(bid)
                half_spread = spread / 2.0

            # Approximate "edge after costs" for BUY decisions:
            # Start from ev=(fair-mid) and subtract:
            # - half-spread (entry vs mid)
            # - estimated fee and extra cost as % of execution price (use ask when available)
            exec_px = float(ask) if ask is not None and float(ask) > 0 else float(pm_price)
            cost_est = float(half_spread or 0.0) + float(cfg.pm_est_fee_pct) * exec_px + float(cfg.pm_edge_extra_cost_pct) * exec_px
            edge_net = float(ev) - float(cost_est)

            sig_preview = "buy" if pm_price < fair_p else "sell"
            decision = "skip"
            reason = ""
            if not odds_allowed:
                reason = "odds_filter"
            elif abs(edge) < cfg.edge_threshold:
                reason = "below_threshold"
            elif ev <= 0:
                reason = "negative_ev"
            else:
                decision = "trade"
                reason = "ok"

            append_csv_row(
                p_pm_paper_candidates,
                [
                    "ts",
                    "market",
                    "market_ref",
                    "token",
                    "outcome",
                    "pm_bid",
                    "pm_ask",
                    "pm_mid",
                    "odds",
                    "odds_allowed",
                    "fair_p",
                    "ev",
                    "edge",
                    "spread",
                    "cost_est",
                    "edge_net",
                    "signal",
                    "decision",
                    "reason",
                ],
                [
                    ts,
                    market_name,
                    market_ref or "",
                    token_id,
                    chosen_outcome or "",
                    bid if bid is not None else "",
                    ask if ask is not None else "",
                    float(pm_price),
                    odds_str,
                    bool(odds_allowed),
                    float(fair_p),
                    float(ev),
                    float(edge),
                    spread if spread is not None else "",
                    float(cost_est),
                    edge_net if edge_net is not None else "",
                    sig_preview,
                    decision,
                    reason,
                ],
                keep_last=5000,
            )

            computed_rows.append(
                {
                    "ts": ts,
                    "market": market_name,
                    "fair_p": fair_p,
                    "pm_price": pm_price,
                    "edge": edge,
                    "spread": spread,
                    "cost_est": cost_est,
                    "edge_net": edge_net,
                    "sources": "pm_clob+deribit_options" if rn_debug is not None else "pm_clob+kraken_futures",
                    "notes": (
                        f"token={token_id}; outcome={chosen_outcome}; odds_allowed={odds_allowed}; symbol={symbol}; kr_ref={kr_ref}"
                        if rn_debug is None
                        else f"token={token_id}; outcome={chosen_outcome}; odds_allowed={odds_allowed}; symbol={symbol}; options=deribit; instrument={rn_debug.get('instrument_name')}"
                    ),
                }
            )

            if not odds_allowed:
                continue

            if abs(edge) >= cfg.edge_threshold:
                # If pm_price < fair_p -> buy (undervalued). If pm_price > fair_p -> sell (overvalued).
                sig = "buy" if pm_price < fair_p else "sell"

                yes_side = str(hedge.get("yes_side") or "").strip() or "sell"
                no_side = str(hedge.get("no_side") or "").strip() or "buy"

                def _invert_side(side: str) -> str:
                    s = side.strip().lower()
                    if s == "buy":
                        return "sell"
                    if s == "sell":
                        return "buy"
                    return side

                long_hedge_side: str
                if chosen_yes_no == "yes":
                    long_hedge_side = yes_side
                elif chosen_yes_no == "no":
                    long_hedge_side = no_side
                else:
                    long_hedge_side = yes_side

                hedge_side = long_hedge_side if sig == "buy" else _invert_side(long_hedge_side)

                if symbol:
                    append_csv_row(
                        p_kr_sig,
                        ["ts", "symbol", "signal", "confidence", "edge", "ref_price", "notes"],
                        [ts, symbol, hedge_side, 0.5, edge, kr_ref, f"market={market_name}"],
                    )

                # Polymarket action: paper logs only, unless explicit live trading is enabled.
                if pm_live_client is None or killswitch_active(cfg):
                    # Keep paper behavior aligned with live: cap how many trades we simulate per tick.
                    if signals_emitted >= cfg.pm_max_orders_per_tick:
                        append_csv_row(
                            p_pm_orders,
                            ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                            [
                                ts,
                                market_name,
                                sig,
                                token_id,
                                pm_price,
                                cfg.pm_order_size_shares,
                                "skipped",
                                "",
                                "max orders per tick reached (paper)",
                            ],
                        )
                        append_csv_row(
                            p_pm_paper_trades,
                            ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                            [
                                ts,
                                market_name,
                                token_id,
                                chosen_outcome or "",
                                "BUY" if sig == "buy" else "SELL",
                                float(pm_price),
                                float(cfg.pm_order_size_shares),
                                float(float(pm_price) * float(cfg.pm_order_size_shares)),
                                float(paper_cash),
                                "skipped",
                                "max orders per tick reached (paper)",
                            ],
                            keep_last=500,
                        )
                        continue

                    # Use best ask/bid for a more realistic fill assumption.
                    try:
                        ob2 = pm_clob.get_orderbook(token_id)
                        bb, ba = best_bid_ask(ob2)
                    except Exception:
                        bb, ba = (None, None)

                    paper_status = "skipped"
                    paper_notes = ""
                    action = "BUY" if sig == "buy" else "SELL"
                    fill_price = float(pm_price)
                    if action == "BUY" and ba is not None:
                        fill_price = float(ba)
                    if action == "SELL" and bb is not None:
                        fill_price = float(bb)

                    # Paper execution model:
                    # - BUY: open/increase a long position in this outcome token.
                    # - SELL: close the existing position in this token (if any).
                    if action == "BUY":
                        shares = float(cfg.pm_order_size_shares)
                        notional = float(fill_price) * shares
                        if paper_cash + 1e-9 < notional:
                            paper_status = "rejected"
                            paper_notes = "insufficient_cash"
                        else:
                            pos = paper_positions.get(token_id)
                            prev_shares = float(pos.get("shares") or 0.0) if pos is not None else 0.0
                            prev_avg = float(pos.get("avg_entry") or fill_price) if pos is not None else float(fill_price)
                            opened_at = str(pos.get("opened_at") or ts) if pos is not None else ts

                            new_shares = prev_shares + shares
                            new_avg = ((prev_shares * prev_avg) + (shares * float(fill_price))) / max(new_shares, 1e-9)
                            paper_positions[token_id] = {
                                "market": market_name,
                                "outcome": chosen_outcome,
                                "shares": float(new_shares),
                                "avg_entry": float(new_avg),
                                "opened_at": opened_at,
                            }
                            paper_cash -= notional
                            paper_status = "filled"
                    else:
                        pos = paper_positions.get(token_id)
                        if pos is None or float(pos.get("shares") or 0.0) <= 0:
                            paper_status = "skipped"
                            paper_notes = "no_position"
                        else:
                            shares = float(pos.get("shares") or 0.0)
                            avg_entry = float(pos.get("avg_entry") or fill_price)
                            notional = float(fill_price) * shares
                            paper_cash += notional
                            paper_realized += (float(fill_price) - avg_entry) * shares
                            paper_positions.pop(token_id, None)
                            paper_status = "filled"

                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, sig, token_id, fill_price, cfg.pm_order_size_shares, "paper", "", paper_notes or "paper"],
                    )

                    append_csv_row(
                        p_pm_paper_trades,
                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                        [
                            ts,
                            market_name,
                            token_id,
                            chosen_outcome or "",
                            action,
                            float(fill_price),
                            float(cfg.pm_order_size_shares),
                            float(float(fill_price) * float(cfg.pm_order_size_shares)),
                            float(paper_cash),
                            paper_status,
                            paper_notes,
                        ],
                        keep_last=500,
                    )

                    if paper_status in {"filled", "rejected"}:
                        signals_emitted += 1
                    continue

                # Hard cap on how many Polymarket orders we try per tick.
                if signals_emitted >= cfg.pm_max_orders_per_tick:
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, sig, token_id, pm_price, cfg.pm_order_size_shares, "skipped", "", "max orders per tick reached"],
                    )
                    continue

                # Price selection: use best ask for BUY, best bid for SELL to avoid accidental worse pricing.
                try:
                    ob2 = pm_clob.get_orderbook(token_id)
                    bb, ba = best_bid_ask(ob2)
                except Exception:
                    bb, ba = (None, None)

                desired_side = "BUY" if sig == "buy" else "SELL"
                desired_price = pm_price
                if desired_side == "BUY" and ba is not None:
                    desired_price = float(ba)
                if desired_side == "SELL" and bb is not None:
                    desired_price = float(bb)

                # Best-effort: cancel existing orders for this token before placing a new one.
                try:
                    _ = pm_cancel_token_orders(pm_live_client, token_id=token_id)
                except Exception:
                    pass

                try:
                    resp = pm_post_limit_order(
                        pm_live_client,
                        token_id=token_id,
                        side=desired_side,
                        price=float(desired_price),
                        size=float(cfg.pm_order_size_shares),
                        order_type="GTC",
                    )
                    order_id = str(resp.get("orderID") or resp.get("orderId") or "")
                    status = str(resp.get("status") or "submitted")
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, sig, token_id, desired_price, cfg.pm_order_size_shares, status, order_id, "live"],
                    )
                    signals_emitted += 1
                except Exception as e:
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, sig, token_id, desired_price, cfg.pm_order_size_shares, "error", "", str(e)[:500]],
                    )

        if computed_rows:
            edge_rows = computed_rows
            live_status["mapped_markets"] = len(computed_rows)
            write_json(p_live, live_status)

        # Ensure the portal edge CSV always reflects the latest computed rows (not just the stub written earlier).
        if edge_rows:
            write_csv(
                p_edge,
                ["ts", "market", "fair_p", "pm_price", "edge", "spread", "cost_est", "edge_net", "sources", "notes"],
                [
                    [
                        r.get("ts"),
                        r.get("market"),
                        r.get("fair_p"),
                        r.get("pm_price"),
                        r.get("edge"),
                        r.get("spread"),
                        r.get("cost_est"),
                        r.get("edge_net"),
                        r.get("sources"),
                        r.get("notes"),
                    ]
                    for r in edge_rows
                ],
            )

        # Always append a scan row so the portal shows the agent is alive.
        append_csv_row(
            p_pm_scan,
            ["ts", "markets_seen", "edges_computed", "signals_emitted", "status", "notes"],
            [ts, len(mkts), len(computed_rows), signals_emitted, "ok", ""],
        )

        pm_status["edges_computed"] = len(computed_rows)
        pm_status["signals_emitted"] = signals_emitted
        if computed_rows:
            pm_status["sample"] = computed_rows[0]

        if deribit_used > 0:
            p_deribit = out / "deribit_options_public.json"
            write_json(p_deribit, deribit_snapshot)
            files.append(p_deribit)
            sources_health["options"]["deribit"] = {"ok": True, "markets": deribit_used}

        # Paper portfolio mark-to-market (always updated, even if no new trades)
        mtm_rows: list[list[Any]] = []
        unrealized = 0.0
        equity = float(paper_cash)
        for tok, pos_any in list(paper_positions.items()):
            shares = float(pos_any.get("shares") or 0.0)
            if shares <= 0:
                continue
            avg_entry = float(pos_any.get("avg_entry") or 0.0)
            mname = str(pos_any.get("market") or "")
            outcome = str(pos_any.get("outcome") or "")

            last_price: float | None = None
            try:
                ob = pm_clob.get_orderbook(tok)
                bid, ask = best_bid_ask(ob)
                # Mark long positions at the best bid (liquidation price), not mid.
                if bid is not None and bid > 0:
                    last_price = float(bid)
                elif bid is not None and ask is not None and bid > 0 and ask > 0:
                    last_price = (bid + ask) / 2.0
            except Exception:
                last_price = None

            lp = float(last_price) if last_price is not None else avg_entry
            value = shares * lp
            upnl = shares * (lp - avg_entry)
            unrealized += upnl
            equity += value
            mtm_rows.append([ts, mname, tok, outcome, shares, avg_entry, lp, value, upnl])

        write_csv(
            p_pm_paper_positions,
            ["ts", "market", "token", "outcome", "shares", "avg_entry", "last_price", "value", "unrealized_pnl"],
            mtm_rows,
        )

        paper_state_prev = paper_state
        open_positions = sum(1 for _tok, p in paper_positions.items() if float(p.get("shares") or 0.0) > 0)
        paper_state_out: dict[str, Any] = {
            "generated_at": ts,
            "started_at": str(paper_state_prev.get("started_at") or ts),
            "start_balance_usd": float(paper_state_prev.get("start_balance_usd") or cfg.paper_start_balance_usd),
            "cash_usd": float(paper_cash),
            "equity_usd": float(equity),
            "unrealized_pnl_usd": float(unrealized),
            "realized_pnl_usd": float(paper_realized),
            "open_positions": int(open_positions),
            "positions": paper_positions,
        }
        write_json(p_pm_paper_portfolio, paper_state_out)

    except Exception as e:
        # Still record timing + best-available lag to keep portal diagnostics informative.
        _finalize_live_status(
            live_status=live_status,
            t0=t0,
            latency_tracker=None,
            health_tracker=health_tracker,
        )
        live_status["edge_error"] = str(e)
        write_json(p_live, live_status)

        pm_status["ok"] = False
        pm_status["error"] = str(e)
        append_csv_row(
            p_pm_scan,
            ["ts", "markets_seen", "edges_computed", "signals_emitted", "status", "notes"],
            [ts, 0, 0, 0, "error", str(e)],
        )

    # Write polymarket status after attempting edge computation
    write_json(p_pm_status, pm_status)
    files.append(p_pm_status)

    # Write a single health snapshot for ops/debug (no secrets).
    p_health = out / "sources_health.json"
    write_json(p_health, sources_health)
    files.append(p_health)

    # Raw snapshots (debug)
    if pm is not None:
        p_pm = out / "_polymarket_raw.json"
        write_json(p_pm, pm)
        files.append(p_pm)

    if kraken is not None:
        p_kr = out / "_kraken_raw.json"
        write_json(p_kr, kraken)
        files.append(p_kr)

    # Finalize live_status with timing + best-available lag.
    _finalize_live_status(
        live_status=live_status,
        t0=t0,
        latency_tracker=latency_tracker,
        health_tracker=health_tracker,
    )

    # Rewrite live_status so these fields are included.
    try:
        write_json(p_live, live_status)
    except Exception:
        pass

    # Write lead-lag health snapshot (observability, no secrets).
    try:
        if cfg.strategy_mode == "lead_lag" and health_tracker is not None:
            ll = health_tracker.snapshot(ts=ts, cfg=cfg, pm_status=pm_status)
            if latency_tracker is not None:
                ll["latency"] = latency_tracker.snapshot()

            # Mirror market lag diagnostics into the health snapshot so the portal
            # can show a single cohesive "last" block.
            try:
                last_any = ll.get("last")
                if isinstance(last_any, dict):
                    last_any["market_lag_ms"] = live_status.get("market_lag_ms")
                    last_any["market_lag_confidence"] = live_status.get("market_lag_confidence")
                    last_any["market_lag_points"] = live_status.get("market_lag_points")
                    last_any["market_lag_reason"] = live_status.get("market_lag_reason")
            except Exception:
                pass

            write_json(p_lead_lag_health, ll)
            files.append(p_lead_lag_health)
    except Exception:
        pass

    return files


def _median_from_floats(values: list[float]) -> float | None:
    if not values:
        return None
    vs = sorted(values)
    return float(vs[len(vs) // 2])


def _best_available_market_lag_ms(
    *,
    live_status: dict[str, Any],
) -> float | None:
    # Prefer in-tick samples first (most specific to this tick).
    try:
        samples_any = live_status.get("market_lag_ms_samples")
        if isinstance(samples_any, list) and samples_any:
            samples: list[float] = []
            items = cast(list[object], samples_any)
            for item in items:
                if isinstance(item, (int, float)):
                    samples.append(float(item))
            med = _median_from_floats(samples)
            if med is not None:
                return float(med)
    except Exception:
        pass

    return None


def _finalize_live_status(
    *,
    live_status: dict[str, Any],
    t0: float,
    latency_tracker: LatencyTracker | None,
    health_tracker: LeadLagHealthTracker | None,
    keep_samples: bool = False,
) -> None:
    try:
        tick_total_ms = float((time.perf_counter() - t0) * 1000.0)
        live_status["system_latency_ms"] = tick_total_ms
        if latency_tracker is not None:
            latency_tracker.record_tick_total(tick_total_ms)
    except Exception:
        pass

    try:
        lag = _best_available_market_lag_ms(live_status=live_status)
        if lag is not None:
            live_status["market_lag_ms"] = float(lag)
    except Exception:
        pass

    if not keep_samples:
        try:
            live_status.pop("market_lag_ms_samples", None)
        except Exception:
            pass


def ftp_upload_files(cfg: Config, files: list[Path]) -> None:
    if not (cfg.ftp_host and cfg.ftp_user and cfg.ftp_pass):
        return

    # Upload only the portal-facing files (not raw debug)
    allow = {
        "live_status.json",
        "lead_lag_health.json",
        "sources_health.json",
        "deribit_options_public.json",
        "polymarket_status.json",
        "polymarket_clob_public.json",
        "pm_open_orders.json",
        "pm_scanner_log.csv",
        "edge_signals_live.csv",
        "edge_calculator_live.csv",
        "pm_orders.csv",
        "pm_paper_portfolio.json",
        "pm_paper_positions.csv",
        "pm_paper_trades.csv",
        "pm_paper_candidates.csv",
        "kraken_futures_public.json",
        "kraken_futures_private.json",
        "kraken_futures_signals.csv",
        "kraken_futures_fills.csv",
        "executed_trades.csv",
    }

    with FTP(cfg.ftp_host) as ftp:
        ftp.login(cfg.ftp_user, cfg.ftp_pass)

        # Ensure remote directory exists by walking segments
        remote = cfg.ftp_remote_dir.strip("/")
        if remote:
            parts = remote.split("/")
            ftp.cwd("/")
            for p in parts:
                if not p:
                    continue
                try:
                    ftp.mkd(p)
                except Exception:
                    pass
                ftp.cwd(p)

        for path in files:
            if path.name not in allow:
                continue
            with path.open("rb") as f:
                ftp.storbinary(f"STOR {path.name}", f)


def main() -> None:
    cfg = load_config()
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    lead_lag_engine: LeadLagEngine | None = None
    if cfg.strategy_mode == "lead_lag":
        lead_lag_engine = LeadLagEngine()

    health_tracker = LeadLagHealthTracker() if cfg.strategy_mode == "lead_lag" else None
    latency_tracker = LatencyTracker()
    runtime_cache = RuntimeCache()

    run_once = (os.getenv("RUN_ONCE", "0") or "0").strip().lower() in {"1", "true", "yes"}

    print(f"[agent] out_dir={cfg.out_dir}")
    print(f"[agent] interval_s={cfg.interval_s}")
    print(f"[agent] trading_mode={cfg.trading_mode}")

    consecutive_failures = 0

    while True:
        ts = utc_now_iso()

        pm: dict[str, Any] | None = None
        kraken: dict[str, Any] | None = None

        try:
            if cfg.polymarket_public_url:
                pm = fetch_pm_public(base_url=cfg.polymarket_public_url)
        except Exception as e:
            pm = {"error": str(e), "ts": ts}

        try:
            if cfg.kraken_public_url:
                kraken = fetch_kraken_public(base_url=cfg.kraken_public_url)
        except Exception as e:
            kraken = {"error": str(e), "ts": ts}

        # Optional: verify keys via a private endpoint (paper-first, no orders)
        if cfg.kraken_keys_path and cfg.kraken_keys_path.exists():
            try:
                keys = load_kraken_keys(cfg.kraken_keys_path)
                k = KrakenFuturesApi(keys=keys, testnet=cfg.kraken_futures_testnet)
                _ = k.get_accounts()
            except Exception as e:
                # Surface in the raw kraken object so we see it in _kraken_raw.json
                if kraken is None:
                    kraken = {"ts": ts}
                kraken["private_error"] = str(e)

        try:
            files = write_outputs(
                cfg,
                pm=pm,
                kraken=kraken,
                lead_lag_engine=lead_lag_engine,
                health_tracker=health_tracker,
                latency_tracker=latency_tracker,
                runtime_cache=runtime_cache,
            )
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            # Best-effort: keep the portal freshness signal alive even on crashes.
            try:
                write_json(
                    cfg.out_dir / "live_status.json",
                    {
                        "ts": ts,
                        "trading_mode": cfg.trading_mode,
                        "strategy_mode": cfg.strategy_mode,
                        "killswitch": bool(killswitch_active(cfg)),
                        "error": str(e),
                        "consecutive_failures": consecutive_failures,
                    },
                )
            except Exception:
                pass
            print(f"[agent] tick failed ({consecutive_failures}): {e}")
            files = []

        # No live trading yet: the goal here is to make end-to-end data → portal work.
        if killswitch_active(cfg):
            # In a later step: log killswitch events and prevent any live actions.
            pass

        try:
            ftp_upload_files(cfg, files)
        except Exception as e:
            # Keep loop alive; surface error in live_status on next tick.
            print(f"[agent] ftp upload failed: {e}")

        if run_once:
            print("[agent] RUN_ONCE=1 -> exiting after single tick")
            return

        # Drift stability: simple exponential backoff on repeated failures.
        sleep_s = float(cfg.interval_s)
        if consecutive_failures > 0:
            sleep_s = min(float(cfg.interval_s) * (2 ** min(consecutive_failures, 4)), 300.0)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()
