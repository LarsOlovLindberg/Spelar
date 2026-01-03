from __future__ import annotations

# pyright: reportUnusedImport=false, reportUnusedVariable=false, reportUnusedFunction=false

import csv
import json
import os
import time
import threading
import urllib.parse
import zipfile
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from ftplib import FTP
from pathlib import Path
from typing import Any, cast

_ftp_last_upload_mono: float | None = None
_ftp_last_uploaded_mtime: dict[str, float] = {}

_http_last_upload_mono: float | None = None
_http_last_uploaded_mtime: dict[str, float] = {}
import requests

from vps.connectors.kraken_public import fetch_public_snapshot as fetch_kraken_public
from vps.connectors.polymarket_public import fetch_public_snapshot as fetch_pm_public
from vps.connectors.kraken_futures_api import KrakenFuturesApi, KrakenFuturesKeys
from vps.connectors.deribit_options_public import DeribitOptionsPublic
from vps.connectors.polymarket_gamma import GammaMarketListing, PolymarketGammaPublic
from vps.connectors.polymarket_clob_public import PolymarketClobPublic, best_bid_ask
from vps.connectors.kraken_spot_public import KrakenSpotPublic
from vps.strategies.lead_lag import LeadLagEngine
from vps.strategies.pm_trend import PmTrendEngine
from vps.connectors.polymarket_clob_trading import (
    PolymarketClobApiCreds,
    PolymarketClobLiveConfig,
    cancel_all_orders as pm_cancel_all_orders,
    cancel_token_orders as pm_cancel_token_orders,
    get_open_orders as pm_get_open_orders,
    make_live_client as pm_make_live_client,
    post_limit_order as pm_post_limit_order,
)
from vps.connectors.polymarket_position_store import PolymarketPositionStore, fill_from_loose_dict
from vps.connectors.polymarket_user_wss import PolymarketUserWssAuth, PolymarketUserWssClient, PolymarketUserWssConfig


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _ttl_ok(*, fetched_at_ms: int | None, ttl_ms: int, now_ms: int) -> bool:
    if ttl_ms <= 0:
        return False
    if not fetched_at_ms:
        return False
    return (now_ms - int(fetched_at_ms)) <= int(ttl_ms)


def _cache_get_gamma_market(cache: RuntimeCache, *, key: str, now_ms: int, ttl_s: float) -> Any | None:
    ttl_ms = int(max(0.0, float(ttl_s)) * 1000.0)
    if key in cache.gamma_market_by_slug and _ttl_ok(fetched_at_ms=cache.gamma_market_fetched_at_ms.get(key), ttl_ms=ttl_ms, now_ms=now_ms):
        return cache.gamma_market_by_slug.get(key)
    return None


def _cache_set_gamma_market(cache: RuntimeCache, *, key: str, market: Any, now_ms: int) -> None:
    cache.gamma_market_by_slug[key] = market
    cache.gamma_market_fetched_at_ms[key] = int(now_ms)


def _cache_get_token_id(cache: RuntimeCache, *, key: tuple[str, str], now_ms: int, ttl_s: float) -> str | None:
    ttl_ms = int(max(0.0, float(ttl_s)) * 1000.0)
    if key in cache.token_id_by_slug_outcome and _ttl_ok(fetched_at_ms=cache.token_id_fetched_at_ms.get(key), ttl_ms=ttl_ms, now_ms=now_ms):
        v = cache.token_id_by_slug_outcome.get(key)
        return str(v) if v else None
    return None


def _cache_set_token_id(cache: RuntimeCache, *, key: tuple[str, str], token_id: str, now_ms: int) -> None:
    cache.token_id_by_slug_outcome[key] = str(token_id)
    cache.token_id_fetched_at_ms[key] = int(now_ms)


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
    gamma_market_fetched_at_ms: dict[str, int] = field(default_factory=lambda: cast(dict[str, int], {}))
    token_id_by_slug_outcome: dict[tuple[str, str], str] = field(default_factory=lambda: cast(dict[tuple[str, str], str], {}))
    token_id_fetched_at_ms: dict[tuple[str, str], int] = field(default_factory=lambda: cast(dict[tuple[str, str], int], {}))

    kraken_futures_public_snapshot: dict[str, Any] | None = None
    kraken_futures_public_fetched_at_ms: int = 0

    kraken_futures_private_snapshot: dict[str, Any] | None = None
    kraken_futures_private_fetched_at_ms: int = 0

    pm_scan_last_run_ms: int = 0
    # Optional: use Gamma scan results as the active trading universe.
    pm_scan_selected_mkts: list[dict[str, Any]] = field(default_factory=lambda: cast(list[dict[str, Any]], []))
    pm_scan_selected_at_ms: int = 0

    # Token metadata derived from the latest Gamma scan. Used for paper housekeeping.
    # Keys are CLOB token ids (strings).
    pm_scan_token_meta: dict[str, dict[str, Any]] = field(default_factory=lambda: cast(dict[str, dict[str, Any]], {}))
    pm_scan_token_meta_at_ms: int = 0

    # Deadline-ladder scan (derived from pm_markets_index.json) + trade throttling.
    pm_deadline_last_run_ms: int = 0
    pm_deadline_last_trade_ms: int = 0
    pm_deadline_last_trade_key: str | None = None


def _parse_gamma_end_date(s: str | None) -> datetime | None:
    if not s:
        return None
    raw = str(s).strip()
    if not raw:
        return None
    # Gamma frequently returns RFC3339 with 'Z'. datetime.fromisoformat expects '+00:00'.
    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except Exception:
        return None
    if dt.tzinfo is None:
        # Assume UTC if timezone missing.
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


_PM_DEADLINE_RE_BY = re.compile(r"\bby\b", re.IGNORECASE)
_PM_DEADLINE_RE_YEAR = re.compile(r"\b20\d{2}\b")
_PM_DEADLINE_RE_TIME_HINT = re.compile(r"\b(by|before|after|in|on|during|until|through)\b", re.IGNORECASE)
_PM_DEADLINE_RE_ISO_DATE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")


def _pm_deadline_normalize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _pm_deadline_looks_like_market(*, slug: str, question: str) -> bool:
    slug_l = (slug or "").lower()
    q_l = (question or "").lower()
    if "-by-" in slug_l:
        return True
    if _PM_DEADLINE_RE_BY.search(q_l):
        return True
    if " before " in q_l:
        return True
    if "-in-20" in slug_l:
        return True
    if _PM_DEADLINE_RE_YEAR.search(q_l) and _PM_DEADLINE_RE_TIME_HINT.search(q_l):
        return True
    if _PM_DEADLINE_RE_ISO_DATE.search(q_l) or _PM_DEADLINE_RE_YEAR.search(q_l):
        return True
    if re.search(r"-20\d{2}(-\d{2}-\d{2})?$", slug_l):
        return True
    return False


def _pm_deadline_base_key(*, slug: str, question: str) -> str:
    s = _pm_deadline_normalize_key(slug)
    if "-by-" in s:
        return s.split("-by-", 1)[0]

    s2 = re.sub(r"-(in|by|before|after|until|through)-20\d{2}$", "", s)
    s2 = re.sub(r"-(in|by|before|after|until|through)-20\d{2}s$", "", s2)
    s2 = re.sub(r"-(in|by|before|after|until|through)-\d{4}-\d{2}-\d{2}$", "", s2)

    s2 = re.sub(r"20\d{2}-\d{2}-\d{2}", "", s2)
    s2 = re.sub(r"20\d{2}", "", s2)
    s2 = re.sub(r"-{2,}", "-", s2).strip("-")
    if s2:
        return s2

    q = _pm_deadline_normalize_key(question)
    q = re.sub(r"20\d{2}-\d{2}-\d{2}", "", q)
    q = re.sub(r"20\d{2}", "", q)
    q = re.sub(
        r"\s+(by|before|after|in|on|during|until|through)\s+([a-z]{3,9}\s+\d{1,2}(,\s*20\d{2})?|20\d{2}(-\d{2}-\d{2})?)\s*\??$",
        "",
        q,
    )
    q = re.sub(r"\s+\?$", "", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q


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
    strategy_mode: str  # fair_model|lead_lag|pm_trend

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

    # Lead-lag scaling (paper): add to winners when PM odds/price improves.
    lead_lag_scale_on_odds_change_pct: float
    lead_lag_scale_cooldown_s: float
    lead_lag_scale_max_adds: int
    lead_lag_scale_size_mult: float
    lead_lag_scale_max_total_shares: float

    # PM trend (PM-only, no external reference)
    pm_trend_lookback_points: int
    pm_trend_move_min_pct: float
    pm_trend_exit_move_min_pct: float
    pm_trend_auto_side: bool

    # Drift / stability
    freshness_max_age_s: float

    # Snapshot sizing
    clob_depth_levels: int

    # Performance: parallel orderbook fetching (Polymarket CLOB)
    pm_orderbook_workers: int

    # Performance: Gamma (slug->market) caching + parallel prefetch
    gamma_cache_ttl_s: float
    gamma_workers: int

    # Polymarket live trading (optional; requires explicit gates)
    poly_chain_id: int
    poly_private_key: str | None
    poly_api_key: str | None
    poly_api_secret: str | None
    poly_api_passphrase: str | None
    poly_signature_type: int
    poly_funder: str | None
    poly_live_confirm: str

    # Optional: Polymarket user websocket + reconcile (live safety)
    poly_wss_url: str | None
    pm_user_wss_enable: bool
    pm_user_reconcile_interval_s: float

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
    ftp_protocol: str  # ftp|sftp
    ftp_port: int

    # Optional HTTPS push upload (preferred when FTP/SFTP is blocked)
    upload_url: str | None
    upload_api_key: str | None

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

    lead_lag_scale_on_odds_change_pct = float(os.getenv("LEAD_LAG_SCALE_ON_ODDS_CHANGE_PCT", "0.40") or "0.40")
    lead_lag_scale_cooldown_s = float(os.getenv("LEAD_LAG_SCALE_COOLDOWN_S", "20") or "20")
    lead_lag_scale_max_adds = int(os.getenv("LEAD_LAG_SCALE_MAX_ADDS", "3") or "3")
    lead_lag_scale_size_mult = float(os.getenv("LEAD_LAG_SCALE_SIZE_MULT", "0.50") or "0.50")
    lead_lag_scale_max_total_shares = float(os.getenv("LEAD_LAG_SCALE_MAX_TOTAL_SHARES", "50") or "50")

    pm_trend_lookback_points = int(os.getenv("PM_TREND_LOOKBACK_POINTS", str(lead_lag_lookback_points)) or str(lead_lag_lookback_points))
    pm_trend_move_min_pct = float(os.getenv("PM_TREND_MOVE_MIN_PCT", "0.10") or "0.10")
    pm_trend_exit_move_min_pct = float(os.getenv("PM_TREND_EXIT_MOVE_MIN_PCT", "0.00") or "0.00")
    pm_trend_auto_side = (os.getenv("PM_TREND_AUTO_SIDE", "1") or "1").strip().lower() not in {"0", "false", "no"}

    freshness_max_age_s = float(os.getenv("FRESHNESS_MAX_AGE_SECS", "60") or "60")

    clob_depth_levels = int(os.getenv("CLOB_DEPTH_LEVELS", "10") or "10")

    pm_orderbook_workers = int(os.getenv("PM_ORDERBOOK_WORKERS", "1") or "1")
    # Safety caps: prevent accidental fork-bombs.
    pm_orderbook_workers = max(1, min(int(pm_orderbook_workers), 32))

    gamma_cache_ttl_s = float(os.getenv("GAMMA_CACHE_TTL_S", "900") or "900")
    if gamma_cache_ttl_s < 0:
        gamma_cache_ttl_s = 0.0

    gamma_workers = int(os.getenv("GAMMA_WORKERS", "1") or "1")
    gamma_workers = max(1, min(int(gamma_workers), 32))

    poly_chain_id = int(os.getenv("POLY_CHAIN_ID", "137") or "137")
    poly_private_key = (os.getenv("POLY_PRIVATE_KEY") or os.getenv("POLY_PK") or "").strip() or None
    poly_api_key = (os.getenv("POLY_CLOB_API_KEY") or os.getenv("CLOB_API_KEY") or "").strip() or None
    poly_api_secret = (os.getenv("POLY_CLOB_SECRET") or os.getenv("CLOB_SECRET") or "").strip() or None
    poly_api_passphrase = (os.getenv("POLY_CLOB_PASS_PHRASE") or os.getenv("CLOB_PASS_PHRASE") or "").strip() or None
    poly_signature_type = int(os.getenv("POLY_SIGNATURE_TYPE", "0") or "0")
    poly_funder = (os.getenv("POLY_FUNDER") or "").strip() or None
    poly_live_confirm = (os.getenv("POLY_LIVE_CONFIRM", "NO") or "NO").strip().upper()

    poly_wss_url = (
        (os.getenv("POLY_WSS_URL") or "").strip()
        or (os.getenv("POLYMARKET_WSS_URL") or "").strip()
        or (os.getenv("POLY_WS_URL") or "").strip()
        or None
    )
    pm_user_wss_enable = (os.getenv("PM_USER_WSS_ENABLE", "1") or "1").strip().lower() not in {"0", "false", "no"}
    pm_user_reconcile_interval_s = float(os.getenv("PM_USER_RECONCILE_INTERVAL_S", "60") or "60")

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

    ftp_protocol = (os.getenv("FTP_PROTOCOL", "ftp") or "ftp").strip().lower()
    if ftp_protocol not in {"ftp", "sftp"}:
        ftp_protocol = "ftp"

    ftp_port_raw = (os.getenv("FTP_PORT") or "").strip()
    if ftp_port_raw:
        try:
            ftp_port = int(ftp_port_raw)
        except Exception:
            ftp_port = 21 if ftp_protocol == "ftp" else 22
    else:
        ftp_port = 21 if ftp_protocol == "ftp" else 22

    upload_url = (os.getenv("UPLOAD_URL") or os.getenv("HTTP_UPLOAD_URL") or "").strip() or None
    # Support both project-style names, plus a generic.
    upload_api_key = (
        (os.getenv("UPLOAD_API_KEY") or "").strip()
        or (os.getenv("SPELAR_UPLOAD_API_KEY") or "").strip()
        or (os.getenv("MARKOV_UPLOAD_API_KEY") or "").strip()
        or None
    )

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
        lead_lag_scale_on_odds_change_pct=lead_lag_scale_on_odds_change_pct,
        lead_lag_scale_cooldown_s=lead_lag_scale_cooldown_s,
        lead_lag_scale_max_adds=lead_lag_scale_max_adds,
        lead_lag_scale_size_mult=lead_lag_scale_size_mult,
        lead_lag_scale_max_total_shares=lead_lag_scale_max_total_shares,
        pm_trend_lookback_points=pm_trend_lookback_points,
        pm_trend_move_min_pct=pm_trend_move_min_pct,
        pm_trend_exit_move_min_pct=pm_trend_exit_move_min_pct,
        pm_trend_auto_side=pm_trend_auto_side,
        freshness_max_age_s=freshness_max_age_s,
        clob_depth_levels=clob_depth_levels,
        pm_orderbook_workers=pm_orderbook_workers,
        gamma_cache_ttl_s=gamma_cache_ttl_s,
        gamma_workers=gamma_workers,
        poly_chain_id=poly_chain_id,
        poly_private_key=poly_private_key,
        poly_api_key=poly_api_key,
        poly_api_secret=poly_api_secret,
        poly_api_passphrase=poly_api_passphrase,
        poly_signature_type=poly_signature_type,
        poly_funder=poly_funder,
        poly_live_confirm=poly_live_confirm,
        poly_wss_url=poly_wss_url,
        pm_user_wss_enable=pm_user_wss_enable,
        pm_user_reconcile_interval_s=pm_user_reconcile_interval_s,
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
        ftp_protocol=ftp_protocol,
        ftp_port=ftp_port,
        upload_url=upload_url,
        upload_api_key=upload_api_key,
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


def write_json_compact(path: Path, obj: Any) -> None:
    """Write JSON without whitespace to keep snapshots small for FTP hosting."""
    ensure_parent(path)
    path.write_text(json.dumps(obj, ensure_ascii=False, separators=(",", ":")) + "\n", encoding="utf-8")


def write_csv(path: Path, header: list[str], rows: list[list[Any]]) -> None:
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)


@dataclass
class _CsvAppendState:
    """Fast-path state for append_csv_row.

    We keep a cheap estimate of the number of data rows in the file so we can
    append quickly and only compact occasionally.
    """

    data_rows: int = 0
    last_compact_at_ms: int = 0


_CSV_APPEND_STATE: dict[str, _CsvAppendState] = {}


def _csv_state_for(path: Path) -> _CsvAppendState:
    key = str(path)
    st = _CSV_APPEND_STATE.get(key)
    if st is None:
        st = _CsvAppendState()
        _CSV_APPEND_STATE[key] = st
    return st


def _count_csv_data_rows(path: Path) -> int:
    """Count data rows (excluding header). Used only on first touch / compaction."""
    if not path.exists():
        return 0
    try:
        n = 0
        with path.open("r", encoding="utf-8", newline="") as f:
            for _ in f:
                n += 1
        return max(0, n - 1)
    except Exception:
        return 0


def append_csv_row(path: Path, header: list[str], row: list[Any], *, keep_last: int = 200) -> None:
    """Append a row to a CSV, keeping only the last N rows (plus header).

    Performance notes:
    - We append in O(1) without re-reading the whole file.
    - We compact (read tail + rewrite) only when the file grows beyond a threshold.
    """

    ensure_parent(path)
    st = _csv_state_for(path)

    # Ensure header exists.
    if not path.exists() or path.stat().st_size == 0:
        write_csv(path, header, [])
        st.data_rows = 0
        st.last_compact_at_ms = 0
    elif st.data_rows <= 0:
        # First touch after process start: do a one-time line count.
        st.data_rows = _count_csv_data_rows(path)

    # Append row.
    try:
        with path.open("a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow([str(x) for x in row])
        st.data_rows += 1
    except Exception:
        # Fallback: if append fails for any reason, do a safe rewrite.
        write_csv(path, header, [[str(x) for x in row]])
        st.data_rows = 1
        st.last_compact_at_ms = _now_ms()
        return

    # Compaction: only when necessary.
    if keep_last <= 0:
        return

    # Compact when we're sufficiently above keep_last.
    # For large keep_last values (e.g. 5000) this prevents constant rewrite churn.
    compact_threshold = max(int(keep_last * 1.20), keep_last + 200)
    if st.data_rows <= compact_threshold:
        return

    now_ms = _now_ms()
    # Avoid pathological compaction loops under heavy logging.
    if st.last_compact_at_ms and (now_ms - st.last_compact_at_ms) < 1000:
        return

    try:
        tail: deque[list[str]] = deque(maxlen=int(keep_last))
        with path.open("r", newline="", encoding="utf-8") as f:
            r = csv.reader(f)
            found_header = False
            for line in r:
                if not line:
                    continue
                if not found_header:
                    found_header = True
                    continue
                tail.append(list(line))

        write_csv(path, header, list(tail))
        st.data_rows = len(tail)
        st.last_compact_at_ms = now_ms
    except Exception:
        # If compaction fails, keep going; we'll try again later.
        st.last_compact_at_ms = now_ms


def killswitch_active(cfg: Config) -> bool:
    if not cfg.killswitch_file:
        return False
    return cfg.killswitch_file.exists()


def _topic_guess(text: str) -> str:
    t = (text or "").lower()
    if any(k in t for k in ["bitcoin", " btc", "xbt", "sats", "satoshi"]):
        return "BTC"
    if any(k in t for k in ["ethereum", " eth", "ether"]):
        return "ETH"
    if any(k in t for k in ["solana", " sol "]):
        return "SOL"
    if any(k in t for k in ["trump", "biden", "election", "president", "senate", "house"]):
        return "POLITICS"
    if any(k in t for k in ["fed", "rates", "cpi", "inflation", "jobs", "gdp"]):
        return "MACRO"
    return "OTHER"


def _coerce_yes_no_tokens(m: GammaMarketListing) -> tuple[str | None, str | None]:
    outs = [str(x).strip() for x in (m.outcomes or [])]
    toks = [str(x).strip() for x in (m.clob_token_ids or [])]
    if len(outs) != 2 or len(toks) != 2:
        return None, None
    o0 = outs[0].lower()
    o1 = outs[1].lower()
    if o0 == "yes" and o1 == "no":
        return toks[0], toks[1]
    if o0 == "no" and o1 == "yes":
        return toks[1], toks[0]
    return None, None


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
    pm_trend_engine: PmTrendEngine | None = None,
    health_tracker: LeadLagHealthTracker | None = None,
    latency_tracker: LatencyTracker | None = None,
    runtime_cache: RuntimeCache | None = None,
    pm_orderbook_executor: ThreadPoolExecutor | None = None,
    pm_live_client: Any | None = None,
    pm_live_error: str | None = None,
    pm_position_store: PolymarketPositionStore | None = None,
    pm_user_wss_status: dict[str, Any] | None = None,
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
        "pm_user_wss_enabled": bool(cfg.pm_user_wss_enable),
        "pm_user_reconcile_interval_s": float(cfg.pm_user_reconcile_interval_s),
        "ftp_upload_enabled": bool(cfg.ftp_host and cfg.ftp_user and cfg.ftp_pass),
        "ftp_host": cfg.ftp_host,
        "ftp_remote_dir": cfg.ftp_remote_dir if cfg.ftp_host else None,
        "ftp_protocol": cfg.ftp_protocol if cfg.ftp_host else None,
        "ftp_port": int(cfg.ftp_port) if cfg.ftp_host else None,
        "upload_enabled": bool(cfg.upload_url and cfg.upload_api_key),
        "upload_url": cfg.upload_url,
        "system_latency_ms": None,
        "market_lag_ms": None,
        "market_lag_confidence": None,
        "market_lag_points": None,
        "market_lag_reason": None,
        "pm_trend_lookback_points": int(cfg.pm_trend_lookback_points),
        "pm_trend_move_min_pct": float(cfg.pm_trend_move_min_pct),
        "pm_trend_exit_move_min_pct": float(cfg.pm_trend_exit_move_min_pct),
        "pm_trend_auto_side": bool(cfg.pm_trend_auto_side),
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
            [
                "ts",
                "market",
                "token",
                "outcome",
                "shares",
                "avg_entry",
                "last_price",
                "value",
                "unrealized_pnl",
                "adds",
                "last_mid",
                "last_scale_at",
            ],
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

    # Market discovery (Gamma scan) outputs.
    p_pm_markets_index = out / "pm_markets_index.json"
    p_pm_markets_index_full = out / "pm_markets_index_full.json"
    if not p_pm_markets_index.exists():
        write_json(
            p_pm_markets_index,
            {
                "generated_at": ts,
                "source": "gamma",
                "scan": {"ok": False, "error": "not_scanned_yet"},
                "items": [],
            },
        )
    files.append(p_pm_markets_index)

    p_pm_scan_candidates = out / "pm_scan_candidates.csv"
    p_pm_scan_candidates_full = out / "pm_scan_candidates_full.csv"
    if not p_pm_scan_candidates.exists():
        write_csv(
            p_pm_scan_candidates,
            [
                "ts",
                "slug",
                "question",
                "topic",
                "category",
                "created_at",
                "end_date",
                "outcomes",
                "token_ids",
                "yes_token",
                "no_token",
                "volume_usd",
                "liquidity_usd",
                "yes_bid",
                "yes_ask",
                "yes_spread",
                "no_bid",
                "no_ask",
                "no_spread",
            ],
            [],
        )
    files.append(p_pm_scan_candidates)

    # Deadline-ladder scan outputs (derived from pm_markets_index.json + CLOB orderbooks).
    p_pm_deadline_edges = out / "pm_deadline_edges.csv"
    if not p_pm_deadline_edges.exists():
        write_csv(
            p_pm_deadline_edges,
            [
                "ts",
                "base",
                "early_slug",
                "late_slug",
                "early_end_date",
                "late_end_date",
                "early_question",
                "late_question",
                "early_no_token",
                "late_yes_token",
                "early_no_ask",
                "late_yes_ask",
                "cost",
                "guaranteed_profit",
                "between_deadlines_profit",
                "decision",
                "reason",
            ],
            [],
        )
    files.append(p_pm_deadline_edges)

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
        kr_spot = KrakenSpotPublic(base_url=cfg.kraken_spot_base_url) if cfg.strategy_mode != "pm_trend" else None
        deribit = DeribitOptionsPublic()
        gamma = PolymarketGammaPublic()
        # Portal expects Gamma to be present; mark it OK by default and flip to FAIL on actual errors.
        try:
            sources_health.setdefault("polymarket", {})
            if isinstance(sources_health.get("polymarket"), dict):
                cast(dict[str, Any], sources_health["polymarket"]).setdefault("gamma", {"ok": True})
        except Exception:
            pass
        cache = runtime_cache or RuntimeCache()

        # Deep market discovery: periodically scan Gamma for many active markets.
        # Optional: drive the active trading universe from the scan results.
        scan_note = ""
        try:
            pm_scan_enabled = (os.getenv("PM_SCAN_ENABLE", "1") or "1").strip().lower() not in {"0", "false", "no"}
            pm_scan_interval_s = float(os.getenv("PM_SCAN_INTERVAL_S", "300") or "300")
            pm_scan_limit = int(os.getenv("PM_SCAN_LIMIT", "200") or "200")
            pm_scan_pages = int(os.getenv("PM_SCAN_PAGES", "3") or "3")
            pm_scan_orderbook_sample = int(os.getenv("PM_SCAN_ORDERBOOK_SAMPLE", "25") or "25")
            pm_scan_active_only = (os.getenv("PM_SCAN_ACTIVE_ONLY", "1") or "1").strip().lower() not in {"0", "false", "no"}
            pm_scan_binary_only = (os.getenv("PM_SCAN_BINARY_ONLY", "1") or "1").strip().lower() not in {"0", "false", "no"}
            pm_scan_search = (os.getenv("PM_SCAN_SEARCH") or "").strip() or None
            pm_scan_order = (os.getenv("PM_SCAN_ORDER") or "createdAt").strip() or "createdAt"
            pm_scan_direction = (os.getenv("PM_SCAN_DIRECTION") or "desc").strip() or "desc"
            pm_scan_offset = int(os.getenv("PM_SCAN_OFFSET", "0") or "0")
            pm_scan_offset = max(0, int(pm_scan_offset))

            pm_scan_use_for_trading = (os.getenv("PM_SCAN_USE_FOR_TRADING", "0") or "0").strip().lower() in {"1", "true", "yes"}
            pm_scan_trade_max_markets = int(os.getenv("PM_SCAN_TRADE_MAX_MARKETS", "20") or "20")
            pm_scan_trade_max_markets = max(0, min(int(pm_scan_trade_max_markets), 500))

            # When driving the active universe from a scan, avoid illiquid one-sided books.
            # If this is off, the agent may select markets that later produce `missing_price` (no bid/ask => no mid).
            pm_scan_require_two_sided = (os.getenv("PM_SCAN_REQUIRE_TWO_SIDED", "1") or "1").strip().lower() not in {"0", "false", "no"}
            pm_scan_max_spread = float(os.getenv("PM_SCAN_MAX_SPREAD", "0.10") or "0.10")

            if pm_scan_enabled:
                now_ms = _now_ms()
                due = (cache.pm_scan_last_run_ms <= 0) or ((now_ms - cache.pm_scan_last_run_ms) >= int(pm_scan_interval_s * 1000.0))
                if due:
                    print(
                        "[agent] pm_scan: start "
                        + f"limit={pm_scan_limit} pages={pm_scan_pages} offset={pm_scan_offset} "
                        + f"closed={('false' if pm_scan_active_only else 'any')} "
                        + f"binary_only={pm_scan_binary_only} orderbook_sample={pm_scan_orderbook_sample} "
                        + f"order={pm_scan_order}/{pm_scan_direction} search={pm_scan_search!r}",
                        flush=True,
                    )
                    t_scan0 = time.perf_counter()
                    markets = gamma.list_markets(
                        limit=pm_scan_limit,
                        pages=pm_scan_pages,
                        offset=pm_scan_offset,
                        # Gamma's `active` flag is not a reliable proxy for "currently open".
                        # Filtering on `closed=false` is what yields current markets.
                        active=None,
                        closed=False if pm_scan_active_only else None,
                        order=pm_scan_order,
                        direction=pm_scan_direction,
                        search=pm_scan_search,
                    )
                    scan_ms = float((time.perf_counter() - t_scan0) * 1000.0)
                    print(f"[agent] pm_scan: listed {len(markets)} markets in {scan_ms:.0f}ms", flush=True)
                    cache.pm_scan_last_run_ms = now_ms

                    items: list[dict[str, Any]] = []
                    candidates_rows: list[list[Any]] = []
                    sampled = 0
                    binary_count = 0

                    for m in markets:
                        outs = [str(x) for x in (m.outcomes or [])]
                        toks = [str(x) for x in (m.clob_token_ids or [])]
                        is_binary = (len(outs) == 2 and len(toks) == 2)
                        if pm_scan_binary_only and not is_binary:
                            continue
                        if is_binary:
                            binary_count += 1

                        q = m.question or ""
                        topic = _topic_guess(q)
                        yes_tok, no_tok = _coerce_yes_no_tokens(m)

                        bid_yes = ask_yes = spread_yes = None
                        bid_no = ask_no = spread_no = None
                        if pm_scan_orderbook_sample > 0 and sampled < pm_scan_orderbook_sample and is_binary:
                            try:
                                tok0 = toks[0]
                                tok1 = toks[1]
                                ob0 = pm_clob.get_orderbook(tok0)
                                b0, a0 = best_bid_ask(ob0)
                                ob1 = pm_clob.get_orderbook(tok1)
                                b1, a1 = best_bid_ask(ob1)
                                if b0 is not None and a0 is not None:
                                    bid_yes = float(b0)
                                    ask_yes = float(a0)
                                    spread_yes = float(a0 - b0)
                                if b1 is not None and a1 is not None:
                                    bid_no = float(b1)
                                    ask_no = float(a1)
                                    spread_no = float(a1 - b1)
                                sampled += 1
                            except Exception:
                                pass

                        items.append(
                            {
                                "slug": m.slug,
                                "question": m.question,
                                "outcomes": outs,
                                "clob_token_ids": toks,
                                "active": m.active,
                                "closed": m.closed,
                                "created_at": m.created_at,
                                "end_date": m.end_date,
                                "volume_usd": m.volume_usd,
                                "liquidity_usd": m.liquidity_usd,
                                "category": m.category,
                                "topic": topic,
                            }
                        )

                        candidates_rows.append(
                            [
                                ts,
                                m.slug,
                                (m.question or "")[:240],
                                topic,
                                str(m.category or ""),
                                str(m.created_at or ""),
                                str(m.end_date or ""),
                                "|".join(outs)[:120],
                                "|".join(toks)[:120],
                                yes_tok or "",
                                no_tok or "",
                                m.volume_usd if m.volume_usd is not None else "",
                                m.liquidity_usd if m.liquidity_usd is not None else "",
                                bid_yes if bid_yes is not None else "",
                                ask_yes if ask_yes is not None else "",
                                spread_yes if spread_yes is not None else "",
                                bid_no if bid_no is not None else "",
                                ask_no if ask_no is not None else "",
                                spread_no if spread_no is not None else "",
                            ]
                        )

                    print(
                        f"[agent] pm_scan: kept={len(items)} binary={binary_count} sampled_orderbooks={sampled}",
                        flush=True,
                    )

                    # Cache token->market metadata to allow paper positions to be cleaned up
                    # (e.g., auto-close after end_date has passed).
                    try:
                        token_meta: dict[str, dict[str, Any]] = {}
                        for m in markets:
                            toks = [str(x) for x in (m.clob_token_ids or [])]
                            if not toks:
                                continue
                            for tok in toks:
                                tok_s = str(tok).strip()
                                if not tok_s:
                                    continue
                                token_meta[tok_s] = {
                                    "slug": str(m.slug or ""),
                                    "question": str(m.question or ""),
                                    "end_date": str(m.end_date or ""),
                                    "closed": bool(m.closed) if m.closed is not None else None,
                                }
                        cache.pm_scan_token_meta = token_meta
                        cache.pm_scan_token_meta_at_ms = int(now_ms)
                    except Exception:
                        pass

                    # Some FTP hosts have tight per-file limits. Keep portal snapshots small,
                    # but still write full snapshots for debugging/offline analysis.
                    pm_portal_markets_max_items = int(os.getenv("PM_PORTAL_MARKETS_MAX_ITEMS", "2000") or "2000")
                    pm_portal_candidates_max_rows = int(os.getenv("PM_PORTAL_SCAN_CANDIDATES_MAX_ROWS", "2000") or "2000")
                    pm_portal_markets_max_items = max(0, pm_portal_markets_max_items)
                    pm_portal_candidates_max_rows = max(0, pm_portal_candidates_max_rows)

                    idx_obj = {
                        "generated_at": ts,
                        "source": "gamma",
                        "scan": {
                            "ok": True,
                            "active_only": pm_scan_active_only,
                            "binary_only": pm_scan_binary_only,
                            "limit": pm_scan_limit,
                            "pages": pm_scan_pages,
                            "search": pm_scan_search,
                            "order": pm_scan_order,
                            "direction": pm_scan_direction,
                            "offset": pm_scan_offset,
                            "orderbook_sample_target": pm_scan_orderbook_sample,
                            "markets_total": len(markets),
                            "markets_emitted": len(items),
                            "binary_emitted": binary_count,
                            "orderbook_sampled": sampled,
                            "ms": scan_ms,
                        },
                        "items": items,
                    }

                    # Full
                    write_json_compact(p_pm_markets_index_full, idx_obj)

                    # Portal (trim + compact)
                    if pm_portal_markets_max_items > 0:
                        idx_obj_portal = dict(idx_obj)
                        idx_obj_portal["items"] = items[:pm_portal_markets_max_items]
                    else:
                        idx_obj_portal = idx_obj
                    write_json_compact(p_pm_markets_index, idx_obj_portal)

                    header = [
                        "ts",
                        "slug",
                        "question",
                        "topic",
                        "category",
                        "created_at",
                        "end_date",
                        "outcomes",
                        "token_ids",
                        "yes_token",
                        "no_token",
                        "volume_usd",
                        "liquidity_usd",
                        "yes_bid",
                        "yes_ask",
                        "yes_spread",
                        "no_bid",
                        "no_ask",
                        "no_spread",
                    ]

                    # Full
                    write_csv(p_pm_scan_candidates_full, header, candidates_rows)

                    # Portal (trim)
                    if pm_portal_candidates_max_rows > 0:
                        candidates_rows_portal = candidates_rows[:pm_portal_candidates_max_rows]
                    else:
                        candidates_rows_portal = candidates_rows
                    write_csv(p_pm_scan_candidates, header, candidates_rows_portal)

                    sources_health["polymarket"]["gamma_scan"] = {
                        "ok": True,
                        "markets_total": len(markets),
                        "markets_emitted": len(items),
                        "binary_emitted": binary_count,
                        "orderbook_sampled": sampled,
                        "ms": scan_ms,
                    }

                    scan_note = (
                        f"gamma_scan ok: total={len(markets)} emitted={len(items)} binary={binary_count} "
                        f"sampled={sampled}/{pm_scan_orderbook_sample} ms={int(scan_ms)} "
                        f"order={pm_scan_order} dir={pm_scan_direction} offset={pm_scan_offset} "
                        f"search={pm_scan_search or ''}"
                    )

                    # Select a subset for active monitoring/trading (optional).
                    if pm_scan_use_for_trading and pm_scan_trade_max_markets > 0:
                        def _score(m: GammaMarketListing) -> tuple[float, float, str]:
                            liq = float(m.liquidity_usd or 0.0)
                            vol = float(m.volume_usd or 0.0)
                            created = str(m.created_at or "")
                            return (liq, vol, created)

                        desired_outcome = "Yes" if cfg.lead_lag_side == "YES" else "No"
                        eligible: list[GammaMarketListing] = []
                        for m in markets:
                            outs = list(m.outcomes or [])
                            toks = list(m.clob_token_ids or [])
                            is_binary = (len(outs) == 2 and len(toks) == 2)
                            if pm_scan_binary_only and not is_binary:
                                continue
                            if not is_binary:
                                continue
                            if not m.slug:
                                continue
                            eligible.append(m)

                        # Sort by (liquidity, volume, created) first, then enforce orderbook sanity for the chosen side.
                        eligible_sorted = sorted(eligible, key=_score, reverse=True)

                        def _token_for_desired_outcome(m: GammaMarketListing) -> str | None:
                            y, n = _coerce_yes_no_tokens(m)
                            if not y or not n:
                                return None
                            return y if desired_outcome.lower() == "yes" else n

                        selected: list[GammaMarketListing] = []
                        rejected_one_sided = 0
                        rejected_wide_spread = 0
                        checked_books = 0

                        # Cache bid/ask checks within the scan tick to limit API calls.
                        bidask_cache: dict[str, tuple[float | None, float | None]] = {}

                        for m in eligible_sorted:
                            if len(selected) >= int(pm_scan_trade_max_markets):
                                break

                            tok = _token_for_desired_outcome(m)
                            if not tok:
                                continue

                            if not pm_scan_require_two_sided:
                                selected.append(m)
                                continue

                            try:
                                if tok in bidask_cache:
                                    b, a = bidask_cache[tok]
                                else:
                                    ob = pm_clob.get_orderbook(tok)
                                    b, a = best_bid_ask(ob if isinstance(ob, dict) else {"data": ob})
                                    bidask_cache[tok] = (b, a)
                                checked_books += 1
                                if b is None or a is None:
                                    rejected_one_sided += 1
                                    continue
                                spread = float(a) - float(b)
                                if float(pm_scan_max_spread) > 0 and spread > float(pm_scan_max_spread):
                                    rejected_wide_spread += 1
                                    continue
                                selected.append(m)
                            except Exception:
                                rejected_one_sided += 1
                                continue

                        cache.pm_scan_selected_mkts = [
                            {
                                "name": str(m.question or m.slug or "pm-scan"),
                                "polymarket": {"market_slug": str(m.slug), "outcome": desired_outcome},
                                "kraken_spot": {"pair": cfg.kraken_spot_pair},
                            }
                            for m in selected
                        ]
                        cache.pm_scan_selected_at_ms = int(now_ms)

                        if pm_scan_require_two_sided:
                            scan_note = (scan_note + " | " if scan_note else "") + (
                                f"universe_filter=two_sided checked={checked_books} "
                                f"rej_one_sided={rejected_one_sided} rej_wide_spread={rejected_wide_spread} "
                                f"max_spread={pm_scan_max_spread}"
                            )

                # Even when a scan isn't due this tick, we can still use the cached selection.
                if pm_scan_use_for_trading and cache.pm_scan_selected_mkts:
                    mkts = list(cache.pm_scan_selected_mkts)
                    scan_note = (scan_note + " | " if scan_note else "") + f"universe=pm_scan selected={len(mkts)}"
        except Exception as e:
            sources_health["polymarket"]["gamma_scan"] = {"ok": False, "error": str(e)}
            scan_note = f"gamma_scan error: {str(e)[:180]}"

        paper_state = _load_paper_state(path=p_pm_paper_portfolio, ts=ts, start_balance_usd=cfg.paper_start_balance_usd)
        paper_cash = float(paper_state.get("cash_usd") or cfg.paper_start_balance_usd)
        paper_realized = float(paper_state.get("realized_pnl_usd") or 0.0)
        paper_positions_any = paper_state.get("positions")
        paper_positions: dict[str, dict[str, Any]] = {}
        if isinstance(paper_positions_any, dict):
            for k, v in cast(dict[Any, Any], paper_positions_any).items():
                if isinstance(k, str) and isinstance(v, dict):
                    paper_positions[k] = cast(dict[str, Any], v)

        # If the active market universe is dynamic (scan-driven), make sure we always keep open
        # paper positions in the active set so we can mark-to-market and evaluate exits.
        try:
            existing_tokens: set[str] = set()
            for mkt in mkts:
                pm_block = mkt.get("polymarket")
                if isinstance(pm_block, dict):
                    tok = str(cast(dict[str, Any], pm_block).get("clob_token_id") or "").strip()
                    if tok:
                        existing_tokens.add(tok)

            for tok, pos_any in list(paper_positions.items()):
                try:
                    shares = float(pos_any.get("shares") or 0.0)
                except Exception:
                    shares = 0.0
                if shares <= 0:
                    continue
                tok_s = str(tok).strip()
                if not tok_s or tok_s in existing_tokens:
                    continue
                mkts.append(
                    {
                        "name": str(pos_any.get("market") or f"pos:{tok_s}"),
                        "polymarket": {"clob_token_id": tok_s, "outcome": str(pos_any.get("outcome") or "")},
                        "kraken_spot": {"pair": cfg.kraken_spot_pair},
                    }
                )
                existing_tokens.add(tok_s)
        except Exception:
            pass

        # Observability: record how the agent chose its market universe.
        try:
            pm_scan_use_for_trading = (os.getenv("PM_SCAN_USE_FOR_TRADING", "0") or "0").strip().lower() in {"1", "true", "yes"}
            pm_scan_trade_max_markets = int(os.getenv("PM_SCAN_TRADE_MAX_MARKETS", "20") or "20")
            pm_scan_trade_max_markets = max(0, min(int(pm_scan_trade_max_markets), 500))
            universe_mode = "market_map" if (cfg.market_map_path and cfg.market_map_path.exists()) else "env"
            if pm_scan_use_for_trading and cache.pm_scan_selected_mkts:
                universe_mode = "pm_scan"

            live_status["pm_universe_mode"] = universe_mode
            live_status["pm_universe_markets"] = int(len(mkts))
            live_status["pm_scan_use_for_trading"] = bool(pm_scan_use_for_trading)
            live_status["pm_scan_trade_max_markets"] = int(pm_scan_trade_max_markets)
            live_status["pm_scan_selected_updated_at_ms"] = int(cache.pm_scan_selected_at_ms or 0)

            pm_status["pm_universe_mode"] = universe_mode
            pm_status["pm_universe_markets"] = int(len(mkts))
            pm_status["pm_scan_use_for_trading"] = bool(pm_scan_use_for_trading)
            pm_status["pm_scan_trade_max_markets"] = int(pm_scan_trade_max_markets)
        except Exception:
            pass

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
        clob_ok_markets = 0
        clob_error_markets = 0
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
                            now_ms = _now_ms()
                            gm = _cache_get_gamma_market(cache, key=market_ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                            if gm is None:
                                t_g0 = time.perf_counter()
                                gm = gamma.get_market_by_slug(slug=market_ref)
                                if latency_tracker is not None:
                                    latency_tracker.record_gamma_fetch(float((time.perf_counter() - t_g0) * 1000.0))
                                _cache_set_gamma_market(cache, key=market_ref, market=gm, now_ms=now_ms)

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
                            token_id_cached = _cache_get_token_id(cache, key=cache_key, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                            if token_id_cached:
                                token_id = token_id_cached
                            else:
                                token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen)
                                _cache_set_token_id(cache, key=cache_key, token_id=str(token_id), now_ms=now_ms)

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
                clob_ok_markets += 1
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
                clob_error_markets += 1
                clob_summary["markets"].append(
                    {
                        "name": market_name,
                        "token_id": token_id,
                        "error": str(e),
                    }
                )
        sources_health["polymarket"]["clob"] = {
            # Endpoint health: OK if at least one orderbook request succeeded.
            "ok": clob_ok_markets > 0,
            "markets": len(clob_summary.get("markets") or []),
            "ok_markets": clob_ok_markets,
            "error_markets": clob_error_markets,
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

        # Optional: Polymarket live client is created once in main() and passed in.
        poly_trading_enabled = bool(
            cfg.trading_mode == "live"
            and cfg.poly_live_confirm == "YES"
            and cfg.poly_private_key
            and cfg.poly_api_key
            and cfg.poly_api_secret
            and cfg.poly_api_passphrase
            and pm_live_client is not None
        )

        if pm_live_error:
            live_status["polymarket_live_error"] = str(pm_live_error)
        if pm_user_wss_status is not None:
            live_status["pm_user_wss"] = dict(pm_user_wss_status)

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

        # Optional: snapshot live positions inferred from user fills (best-effort).
        if pm_position_store is not None:
            try:
                p_pos = out / "pm_live_positions.json"
                pos_snap = pm_position_store.snapshot(ts_iso=ts)
                write_json(p_pos, pos_snap)
                files.append(p_pos)
                live_status["pm_live_positions"] = {
                    "count": int(len((pos_snap.get("positions") or []))),
                    "fills_total": int(pos_snap.get("fills_total") or 0),
                }
            except Exception as e:
                live_status["pm_live_positions_error"] = str(e)
        # Lead-lag / PM-trend strategy path:
        # - lead_lag: Kraken spot leads, Polymarket CLOB lags
        # - pm_trend: Polymarket-only trend following (no external reference)
        if cfg.strategy_mode in {"lead_lag", "pm_trend"}:
            if cfg.strategy_mode == "lead_lag" and lead_lag_engine is None:
                raise RuntimeError("lead_lag_engine is required when STRATEGY_MODE=lead_lag")

            # Cache spot tickers per pair per tick.
            spot_by_pair: dict[str, float] = {}
            spot_ts_by_pair: dict[str, datetime] = {}

            # Thread-local PM CLOB client (Session is not shared across threads).
            _pm_tls = threading.local()

            def _pm_client_threadlocal() -> PolymarketClobPublic:
                c = getattr(_pm_tls, "client", None)
                base = getattr(_pm_tls, "base_url", None)
                timeout = getattr(_pm_tls, "timeout_s", None)
                if c is None or base != cfg.polymarket_clob_base_url or timeout != 10.0:
                    sess = requests.Session()
                    c = PolymarketClobPublic(base_url=cfg.polymarket_clob_base_url, timeout_s=10.0, session=sess)
                    setattr(_pm_tls, "client", c)
                    setattr(_pm_tls, "base_url", cfg.polymarket_clob_base_url)
                    setattr(_pm_tls, "timeout_s", 10.0)
                return cast(PolymarketClobPublic, c)

            ctxs: list[dict[str, Any]] = []

            # Build per-market items first, then prefetch Gamma for all missing refs.
            market_items: list[dict[str, Any]] = []
            for mkt in mkts:
                market_name = str(mkt.get("name") or "market")

                token_id: str | None = None
                chosen_outcome: str | None = None
                market_ref: str | None = None
                direction: str | None = None
                fair_mode = ""

                pm_block = mkt.get("polymarket")
                if isinstance(pm_block, dict):
                    pm_cfg = cast(dict[str, Any], pm_block)
                    token_id = str(pm_cfg.get("clob_token_id", "") or "").strip() or None
                    chosen_outcome = str(pm_cfg.get("outcome") or "").strip() or None
                    market_ref = str(pm_cfg.get("market_url") or pm_cfg.get("market_slug") or "").strip() or None

                    # PM-trend: optionally auto-pick YES/NO per market based on trend.
                    # Only possible when we have a market_ref (slug) to resolve both outcomes.
                    pm_auto_side = bool(cfg.strategy_mode == "pm_trend" and cfg.pm_trend_auto_side and market_ref)

                    if not chosen_outcome:
                        side_raw = str(pm_cfg.get("side") or "").strip().upper()
                        if side_raw in {"YES", "NO"}:
                            chosen_outcome = "Yes" if side_raw == "YES" else "No"

                    # Explicit override from the map/scan object.
                    if "auto_side" in pm_cfg:
                        try:
                            pm_auto_side = bool(pm_cfg.get("auto_side")) and bool(cfg.strategy_mode == "pm_trend") and bool(cfg.pm_trend_auto_side) and bool(market_ref)
                        except Exception:
                            pass
                else:
                    pm_auto_side = False

                fm_any = mkt.get("fair_model")
                fm = cast(dict[str, Any], fm_any) if isinstance(fm_any, dict) else {}
                fair_mode = str(fm.get("mode") or "").strip().lower()
                direction = str(fm.get("direction") or "").strip().lower() or None

                # Spot pair: global default or per-market override
                pair = cfg.kraken_spot_pair
                if cfg.strategy_mode != "pm_trend":
                    kspot_block = mkt.get("kraken_spot")
                    if isinstance(kspot_block, dict):
                        pair = str(cast(dict[str, Any], kspot_block).get("pair") or pair).strip() or pair

                market_items.append(
                    {
                        "mkt": mkt,
                        "market_name": market_name,
                        "token_id": token_id,
                        "token_id_yes": None,
                        "token_id_no": None,
                        "chosen_outcome": chosen_outcome,
                        "market_ref": market_ref,
                        "pair": pair,
                        "fair_mode": fair_mode,
                        "direction": direction,
                        "pm_auto_side": bool(pm_auto_side),
                    }
                )

            # Thread-local Gamma client (Session is not shared across threads).
            _gamma_tls = threading.local()

            def _gamma_client_threadlocal() -> PolymarketGammaPublic:
                g = getattr(_gamma_tls, "client", None)
                if g is None:
                    sess = requests.Session()
                    g = PolymarketGammaPublic(timeout_s=20.0, session=sess)
                    setattr(_gamma_tls, "client", g)
                return cast(PolymarketGammaPublic, g)

            # Prefetch Gamma markets for all refs that need it and are missing/expired.
            now_ms = _now_ms()
            refs_to_fetch: list[str] = []
            for it in market_items:
                market_ref = cast(str | None, it.get("market_ref"))
                if not market_ref:
                    continue

                token_id = cast(str | None, it.get("token_id"))
                chosen_outcome = cast(str | None, it.get("chosen_outcome"))
                fair_mode = str(it.get("fair_mode") or "").strip().lower()
                direction = str(it.get("direction") or "").strip().lower()
                needs_infer = (not chosen_outcome) and (fair_mode == "deribit_touch") and (direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"})
                needs_token = (not token_id) and bool(chosen_outcome)
                needs_market = needs_infer or needs_token
                if not needs_market:
                    continue

                gm_cached = _cache_get_gamma_market(cache, key=market_ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                if gm_cached is None and market_ref not in refs_to_fetch:
                    refs_to_fetch.append(market_ref)

            if refs_to_fetch:
                # Limit concurrency even if the shared executor is larger.
                sem = threading.Semaphore(int(cfg.gamma_workers))

                def _fetch_gamma(ref: str) -> tuple[str, Any | None, float, str | None]:
                    t_g0 = time.perf_counter()
                    try:
                        sem.acquire()
                        gm = _gamma_client_threadlocal().get_market_by_slug(slug=ref)
                        ms = float((time.perf_counter() - t_g0) * 1000.0)
                        return ref, gm, ms, None
                    except Exception as e:
                        ms = float((time.perf_counter() - t_g0) * 1000.0)
                        return ref, None, ms, str(e)
                    finally:
                        try:
                            sem.release()
                        except Exception:
                            pass

                use_parallel_gamma = pm_orderbook_executor is not None and int(cfg.gamma_workers) > 1 and len(refs_to_fetch) > 1
                if use_parallel_gamma:
                    futs = [pm_orderbook_executor.submit(_fetch_gamma, ref) for ref in refs_to_fetch]
                    for fut in as_completed(futs):
                        ref, gm, ms, err = fut.result()
                        if latency_tracker is not None:
                            latency_tracker.record_gamma_fetch(ms)
                        if gm is not None:
                            _cache_set_gamma_market(cache, key=ref, market=gm, now_ms=now_ms)
                        if err is not None:
                            sources_health["polymarket"]["gamma"] = {"ok": False, "error": err}
                else:
                    for ref in refs_to_fetch:
                        ref2, gm, ms, err = _fetch_gamma(ref)
                        if latency_tracker is not None:
                            latency_tracker.record_gamma_fetch(ms)
                        if gm is not None:
                            _cache_set_gamma_market(cache, key=ref2, market=gm, now_ms=now_ms)
                        if err is not None:
                            sources_health["polymarket"]["gamma"] = {"ok": False, "error": err}

            # Phase 1: determine outcome for all markets (so we can batch token resolution).
            for it in market_items:
                chosen_outcome = cast(str | None, it.get("chosen_outcome"))
                if chosen_outcome:
                    continue

                desired_outcome = "Yes" if cfg.lead_lag_side == "YES" else "No"
                market_ref = cast(str | None, it.get("market_ref"))
                fair_mode = str(it.get("fair_mode") or "").strip().lower()
                direction = str(it.get("direction") or "").strip().lower()

                if fair_mode == "deribit_touch" and direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"} and market_ref:
                    try:
                        gm = _cache_get_gamma_market(cache, key=market_ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        if gm is not None:
                            desired_outcome = gamma.infer_yes_no_for_touch_event(market=gm, event=direction)
                    except Exception:
                        pass

                it["chosen_outcome"] = desired_outcome

            # Phase 2: batch/parallel resolve token_id for all missing (market_ref, chosen_outcome).
            # Note: keep cache writes on the main thread (avoid concurrent dict mutation).
            now_ms = _now_ms()
            token_jobs: list[tuple[str, str]] = []
            token_job_keys: set[tuple[str, str]] = set()
            for it in market_items:
                token_id = cast(str | None, it.get("token_id"))
                # If PM-trend auto-side is enabled, we still want to resolve both sides
                # even if a single token_id was provided.
                pm_auto_side = bool(cfg.strategy_mode == "pm_trend" and cfg.pm_trend_auto_side and it.get("pm_auto_side"))
                if token_id and not pm_auto_side:
                    continue

                market_ref = cast(str | None, it.get("market_ref"))
                chosen_outcome = cast(str | None, it.get("chosen_outcome"))
                if not market_ref or not chosen_outcome:
                    continue

                if pm_auto_side:
                    for outcome in ("Yes", "No"):
                        cache_key = (market_ref, outcome)
                        tok_cached = _cache_get_token_id(cache, key=cache_key, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        if tok_cached:
                            it["token_id_yes" if outcome == "Yes" else "token_id_no"] = tok_cached
                            continue
                        if cache_key not in token_job_keys:
                            token_job_keys.add(cache_key)
                            token_jobs.append(cache_key)
                else:
                    cache_key = (market_ref, chosen_outcome)
                    tok_cached = _cache_get_token_id(cache, key=cache_key, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                    if tok_cached:
                        it["token_id"] = tok_cached
                        continue

                    if cache_key not in token_job_keys:
                        token_job_keys.add(cache_key)
                        token_jobs.append(cache_key)

            if token_jobs:
                # Snapshot Gamma markets from cache in the main thread (avoid concurrent dict read/write).
                gamma_market_by_ref: dict[str, Any] = {}
                for ref, _outcome in token_jobs:
                    if ref in gamma_market_by_ref:
                        continue
                    gm = _cache_get_gamma_market(cache, key=ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                    if gm is not None:
                        gamma_market_by_ref[ref] = gm

                sem = threading.Semaphore(int(cfg.gamma_workers))

                def _resolve_token(cache_key: tuple[str, str]) -> tuple[tuple[str, str], str | None, Any | None, float, str | None]:
                    ref, outcome = cache_key
                    t0 = time.perf_counter()
                    fetched_market: Any | None = None
                    try:
                        sem.acquire()

                        gm = gamma_market_by_ref.get(ref)
                        if gm is None:
                            fetched_market = _gamma_client_threadlocal().get_market_by_slug(slug=ref)
                            gm = fetched_market

                        token_id_local = _gamma_client_threadlocal().resolve_token_id(market=gm, desired_outcome=outcome)
                        ms = float((time.perf_counter() - t0) * 1000.0)
                        return cache_key, str(token_id_local), fetched_market, ms, None
                    except Exception as e:
                        ms = float((time.perf_counter() - t0) * 1000.0)
                        return cache_key, None, fetched_market, ms, str(e)
                    finally:
                        try:
                            sem.release()
                        except Exception:
                            pass

                use_parallel_tokens = pm_orderbook_executor is not None and int(cfg.gamma_workers) > 1 and len(token_jobs) > 1
                if use_parallel_tokens:
                    futs = [pm_orderbook_executor.submit(_resolve_token, k) for k in token_jobs]
                    for fut in as_completed(futs):
                        cache_key, tok, gm_fetched, ms, err = fut.result()
                        if latency_tracker is not None:
                            latency_tracker.record_gamma_fetch(ms)
                        if gm_fetched is not None:
                            _cache_set_gamma_market(cache, key=cache_key[0], market=gm_fetched, now_ms=now_ms)
                        if tok is not None:
                            _cache_set_token_id(cache, key=cache_key, token_id=tok, now_ms=now_ms)
                            sources_health["polymarket"]["gamma"] = {"ok": True}
                        if err is not None:
                            sources_health["polymarket"]["gamma"] = {"ok": False, "error": err}
                else:
                    for k in token_jobs:
                        cache_key, tok, gm_fetched, ms, err = _resolve_token(k)
                        if latency_tracker is not None:
                            latency_tracker.record_gamma_fetch(ms)
                        if gm_fetched is not None:
                            _cache_set_gamma_market(cache, key=cache_key[0], market=gm_fetched, now_ms=now_ms)
                        if tok is not None:
                            _cache_set_token_id(cache, key=cache_key, token_id=tok, now_ms=now_ms)
                            sources_health["polymarket"]["gamma"] = {"ok": True}
                        if err is not None:
                            sources_health["polymarket"]["gamma"] = {"ok": False, "error": err}

                # Fill in token_id on all market items from cache.
                for it in market_items:
                    token_id = cast(str | None, it.get("token_id"))
                    pm_auto_side = bool(cfg.strategy_mode == "pm_trend" and cfg.pm_trend_auto_side and it.get("pm_auto_side"))
                    if token_id and not pm_auto_side:
                        continue
                    market_ref = cast(str | None, it.get("market_ref"))
                    chosen_outcome = cast(str | None, it.get("chosen_outcome"))
                    if not market_ref or not chosen_outcome:
                        continue
                    if pm_auto_side:
                        tok_y = _cache_get_token_id(cache, key=(market_ref, "Yes"), now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        tok_n = _cache_get_token_id(cache, key=(market_ref, "No"), now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        if tok_y:
                            it["token_id_yes"] = tok_y
                        if tok_n:
                            it["token_id_no"] = tok_n
                    else:
                        tok_cached = _cache_get_token_id(cache, key=(market_ref, chosen_outcome), now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        if tok_cached:
                            it["token_id"] = tok_cached

            for it in market_items:
                mkt = cast(dict[str, Any], it.get("mkt") or {})
                market_name = str(it.get("market_name") or "market")
                token_id = cast(str | None, it.get("token_id"))
                token_id_yes = cast(str | None, it.get("token_id_yes"))
                token_id_no = cast(str | None, it.get("token_id_no"))
                chosen_outcome = cast(str | None, it.get("chosen_outcome"))
                market_ref = cast(str | None, it.get("market_ref"))
                pair = str(it.get("pair") or cfg.kraken_spot_pair).strip() or cfg.kraken_spot_pair
                fair_mode = str(it.get("fair_mode") or "").strip().lower()
                direction = str(it.get("direction") or "").strip().lower()
                pm_auto_side = bool(cfg.strategy_mode == "pm_trend" and cfg.pm_trend_auto_side and it.get("pm_auto_side"))

                # Determine outcome if missing.
                if not chosen_outcome:
                    desired_outcome = "Yes" if cfg.lead_lag_side == "YES" else "No"
                    if fair_mode == "deribit_touch" and direction in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"} and market_ref:
                        try:
                            gm = _cache_get_gamma_market(cache, key=market_ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                            if gm is not None:
                                desired_outcome = gamma.infer_yes_no_for_touch_event(market=gm, event=direction)
                        except Exception:
                            pass
                    chosen_outcome = desired_outcome

                # Resolve token id if missing.
                if not token_id and market_ref and chosen_outcome:
                    try:
                        cache_key = (market_ref, chosen_outcome)
                        tok_cached = _cache_get_token_id(cache, key=cache_key, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                        if tok_cached:
                            token_id = tok_cached
                        else:
                            gm = _cache_get_gamma_market(cache, key=market_ref, now_ms=now_ms, ttl_s=cfg.gamma_cache_ttl_s)
                            if gm is None:
                                # As a fallback, do a direct fetch (serial) if prefetch missed.
                                t_g0 = time.perf_counter()
                                gm = gamma.get_market_by_slug(slug=market_ref)
                                if latency_tracker is not None:
                                    latency_tracker.record_gamma_fetch(float((time.perf_counter() - t_g0) * 1000.0))
                                _cache_set_gamma_market(cache, key=market_ref, market=gm, now_ms=now_ms)
                            token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen_outcome)
                            _cache_set_token_id(cache, key=cache_key, token_id=str(token_id), now_ms=now_ms)
                        sources_health["polymarket"]["gamma"] = {"ok": True}
                    except Exception as e:
                        sources_health["polymarket"]["gamma"] = {"ok": False, "error": str(e)}
                        token_id = None

                # PM-trend auto-side: if we have both tokens, create one ctx for each side.
                if cfg.strategy_mode == "pm_trend" and pm_auto_side and token_id_yes and token_id_no:
                    group_key = market_ref or market_name
                    ctxs.append(
                        {
                            "market_name": market_name,
                            "market_ref": market_ref,
                            "token_id": str(token_id_yes),
                            "chosen_outcome": "Yes",
                            "auto_side_group": group_key,
                        }
                    )
                    ctxs.append(
                        {
                            "market_name": market_name,
                            "market_ref": market_ref,
                            "token_id": str(token_id_no),
                            "chosen_outcome": "No",
                            "auto_side_group": group_key,
                        }
                    )
                    continue

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

                if cfg.strategy_mode != "pm_trend":
                    # Fetch spot once per pair
                    if pair not in spot_by_pair:
                        try:
                            t_spot0 = time.perf_counter()
                            if kr_spot is None:
                                raise RuntimeError("kraken spot client not available")
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

                    if not (spot_price == spot_price):
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
                                "missing_spot",
                            ],
                            keep_last=5000,
                        )
                        continue

                    ctxs.append(
                        {
                            "market_name": market_name,
                            "market_ref": market_ref,
                            "token_id": token_id,
                            "chosen_outcome": chosen_outcome,
                            "pair": pair,
                            "spot_price": spot_price,
                        }
                    )
                else:
                    # PM-only strategy: no external reference.
                    ctxs.append(
                        {
                            "market_name": market_name,
                            "market_ref": market_ref,
                            "token_id": token_id,
                            "chosen_outcome": chosen_outcome,
                            "auto_side_group": market_ref or None,
                        }
                    )

            # Fetch PM orderbooks (optionally in parallel).
            orderbook_by_token: dict[str, dict[str, Any] | None] = {}
            if ctxs:
                unique_tokens = sorted({str(c.get("token_id") or "") for c in ctxs if str(c.get("token_id") or "").strip()})

                def _fetch_ob(tok: str) -> tuple[str, dict[str, Any] | None, float, str | None]:
                    t_ob0 = time.perf_counter()
                    try:
                        ob_any = _pm_client_threadlocal().get_orderbook(tok)
                        ms = float((time.perf_counter() - t_ob0) * 1000.0)
                        return tok, cast(dict[str, Any], ob_any) if isinstance(ob_any, dict) else {"data": ob_any}, ms, None
                    except Exception as e:
                        ms = float((time.perf_counter() - t_ob0) * 1000.0)
                        return tok, None, ms, str(e)

                # If no executor passed in, or workers=1, fall back to serial.
                use_parallel = pm_orderbook_executor is not None and cfg.pm_orderbook_workers > 1 and len(unique_tokens) > 1
                if use_parallel:
                    futs = [pm_orderbook_executor.submit(_fetch_ob, tok) for tok in unique_tokens]
                    for fut in as_completed(futs):
                        tok, ob, ms, err = fut.result()
                        if latency_tracker is not None:
                            latency_tracker.record_orderbook_fetch(ms)
                        if err is not None:
                            # Surface last error in health for observability without overriding endpoint OK.
                            try:
                                sources_health.setdefault("polymarket", {})
                                clob_h = sources_health["polymarket"].get("clob") if isinstance(sources_health.get("polymarket"), dict) else None
                                if isinstance(clob_h, dict):
                                    clob_h["last_error"] = err
                                    clob_h["tick_errors"] = int(clob_h.get("tick_errors") or 0) + 1
                                else:
                                    cast(dict[str, Any], sources_health["polymarket"])["clob"] = {"ok": False, "last_error": err, "tick_errors": 1}
                            except Exception:
                                pass
                        orderbook_by_token[tok] = ob
                else:
                    for tok in unique_tokens:
                        tok2, ob, ms, err = _fetch_ob(tok)
                        if latency_tracker is not None:
                            latency_tracker.record_orderbook_fetch(ms)
                        if err is not None:
                            try:
                                sources_health.setdefault("polymarket", {})
                                clob_h = sources_health["polymarket"].get("clob") if isinstance(sources_health.get("polymarket"), dict) else None
                                if isinstance(clob_h, dict):
                                    clob_h["last_error"] = err
                                    clob_h["tick_errors"] = int(clob_h.get("tick_errors") or 0) + 1
                                else:
                                    cast(dict[str, Any], sources_health["polymarket"])["clob"] = {"ok": False, "last_error": err, "tick_errors": 1}
                            except Exception:
                                pass
                        orderbook_by_token[tok2] = ob

            # PM-trend prepass: compute per-token trend returns so we can pick the best side.
            pm_trend_ret_by_token: dict[str, float | None] = {}
            best_token_by_group: dict[str, str] = {}
            group_has_open_pos: set[str] = set()
            if cfg.strategy_mode == "pm_trend" and pm_trend_engine is not None:
                # Mark groups that already have an open position in either side.
                try:
                    for ctx in ctxs:
                        g = str(ctx.get("auto_side_group") or "").strip()
                        if not g:
                            continue
                        tok = str(ctx.get("token_id") or "").strip()
                        if not tok:
                            continue
                        pos = paper_positions.get(tok)
                        if pos is None:
                            continue
                        try:
                            if float(pos.get("shares") or 0.0) > 0:
                                group_has_open_pos.add(g)
                        except Exception:
                            pass
                except Exception:
                    pass

                # Compute trend return for each token.
                for ctx in ctxs:
                    tok = str(ctx.get("token_id") or "").strip()
                    if not tok:
                        continue
                    ob = orderbook_by_token.get(tok)
                    if not isinstance(ob, dict):
                        pm_trend_ret_by_token[tok] = None
                        continue
                    try:
                        b, a = best_bid_ask(ob)
                        if b is None or a is None or float(b) <= 0 or float(a) <= 0:
                            pm_trend_ret_by_token[tok] = None
                            continue
                        pm_mid0 = (float(b) + float(a)) / 2.0
                    except Exception:
                        pm_trend_ret_by_token[tok] = None
                        continue

                    try:
                        snap_tr = pm_trend_engine.update_and_compute(
                            key=f"tok:{tok}",
                            ts=ts_dt,
                            pm_mid_price=float(pm_mid0),
                            lookback_points=int(cfg.pm_trend_lookback_points),
                        )
                        pm_trend_ret_by_token[tok] = float(snap_tr.pm_ret_pct) if snap_tr is not None else None
                    except Exception:
                        pm_trend_ret_by_token[tok] = None

                # Pick best token per group (max positive return).
                for ctx in ctxs:
                    g = str(ctx.get("auto_side_group") or "").strip()
                    if not g:
                        continue
                    tok = str(ctx.get("token_id") or "").strip()
                    if not tok:
                        continue
                    ret = pm_trend_ret_by_token.get(tok)
                    if ret is None:
                        continue
                    if tok not in best_token_by_group:
                        best_token_by_group[g] = tok
                        continue
                    cur = best_token_by_group.get(g)
                    cur_ret = pm_trend_ret_by_token.get(str(cur)) if cur else None
                    if cur_ret is None or float(ret) > float(cur_ret):
                        best_token_by_group[g] = tok

            for ctx in ctxs:
                market_name = str(ctx.get("market_name") or "market")
                token_id = str(ctx.get("token_id") or "").strip()
                chosen_outcome = cast(str | None, ctx.get("chosen_outcome"))
                market_ref = cast(str | None, ctx.get("market_ref"))
                pair = str(ctx.get("pair") or cfg.kraken_spot_pair).strip() or cfg.kraken_spot_pair
                spot_price = float(ctx.get("spot_price") or float("nan"))

                # PM orderbook (bid/ask/mid)
                bid: float | None = None
                ask: float | None = None
                pm_mid: float | None = None
                ob = orderbook_by_token.get(token_id)
                if isinstance(ob, dict):
                    try:
                        bid, ask = best_bid_ask(ob)
                        if bid is not None and ask is not None and bid > 0 and ask > 0:
                            pm_mid = (bid + ask) / 2.0
                    except Exception:
                        pm_mid = None
                else:
                    pm_mid = None

                if pm_mid is None or (cfg.strategy_mode != "pm_trend" and not (spot_price == spot_price)):
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
                if cfg.strategy_mode == "pm_trend":
                    # pm_mid is computed this tick; treat as age 0.
                    pm_age = 0.0
                    is_fresh = pm_age <= cfg.freshness_max_age_s
                else:
                    spot_age = (ts_dt - spot_ts_by_pair.get(pair, ts_dt)).total_seconds()
                    # pm_mid is computed this tick; treat as age 0 when we got it.
                    pm_age = 0.0
                    is_fresh = (spot_age <= cfg.freshness_max_age_s) and (pm_age <= cfg.freshness_max_age_s)

                ll_key = f"{market_name}:{token_id}:{pair}"

                lag_ms: float | None = None
                spot_ret = None
                pm_ret = None
                edge_pct = None

                if cfg.strategy_mode == "pm_trend":
                    pm_ret = None
                    edge_pct = None
                    try:
                        pm_ret_any = pm_trend_ret_by_token.get(token_id)
                        if pm_ret_any is not None:
                            pm_ret = float(pm_ret_any)
                            edge_pct = float(pm_ret)
                    except Exception:
                        pm_ret = None
                        edge_pct = None
                else:
                    # Lead-lag: update history and compute edge
                    if lead_lag_engine is not None:
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
                                    lag_ms = float(est.lag_ms)
                                    live_status.setdefault("market_lag_ms_samples", [])
                                    cast(list[Any], live_status["market_lag_ms_samples"]).append(float(est.lag_ms))
                            else:
                                if live_status.get("market_lag_reason") is None:
                                    live_status["market_lag_reason"] = est.reason
                                    live_status["market_lag_points"] = int(est.lag_points) if est.lag_points is not None else None
                                    live_status["market_lag_confidence"] = float(abs(est.best_corr)) if est.best_corr is not None else None
                        except Exception:
                            pass

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
                            "sources": "pm_clob" if cfg.strategy_mode == "pm_trend" else "kraken_spot+pm_clob",
                            "notes": (
                                f"pm_trend lookback={cfg.pm_trend_lookback_points} pm_ret={pm_ret:.4f}%"
                                if cfg.strategy_mode == "pm_trend" and pm_ret is not None
                                else f"lead_lag side={cfg.lead_lag_side} pair={pair} spot_ret={spot_ret:.4f}% pm_ret={pm_ret:.4f}%"
                            ),
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

                # Move gating
                if cfg.strategy_mode == "pm_trend":
                    # PM-only: require the chosen token's mid-price to be trending up.
                    spot_noise_pct = None
                    spot_move_min_dyn = float(cfg.pm_trend_move_min_pct)
                    live_status["lead_lag_spot_move_min_pct_dynamic"] = None
                    live_status["lead_lag_spot_noise_pct"] = None
                    live_status["lead_lag_spread_cost_pct"] = float(spread_cost_pct) if spread_cost_pct is not None else None
                    spot_move_ok = edge_pct is not None and float(edge_pct) >= float(spot_move_min_dyn)
                else:
                    # Adaptive spot move threshold: require spot move > recent noise and > spread cost proxy.
                    spot_noise_pct: float | None = None
                    try:
                        if lead_lag_engine is not None:
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
                pm_up_move_pct = 0.0
                if in_pos:
                    opened_at = str(pos.get("opened_at") or ts)
                    try:
                        hold_secs = (ts_dt - _parse_iso_dt(opened_at)).total_seconds()
                    except Exception:
                        hold_secs = 0.0

                    try:
                        last_mid = float(pos.get("last_mid") or pm_mid)
                        pm_up_move_pct = (float(pm_mid) / max(last_mid, 1e-12) - 1.0) * 100.0
                    except Exception:
                        pm_up_move_pct = 0.0

                enter_raw = (not in_pos) and spot_move_ok and float(edge_pct) >= float(cfg.lead_lag_edge_min_pct)
                exit_ok = False
                exit_reason = ""
                if in_pos:
                    if cfg.strategy_mode == "pm_trend":
                        if float(edge_pct) <= float(cfg.pm_trend_exit_move_min_pct):
                            exit_ok = True
                            exit_reason = "trend_gone"
                    else:
                        if float(edge_pct) <= float(cfg.lead_lag_edge_exit_pct):
                            exit_ok = True
                            exit_reason = "edge_exit"

                    if (not exit_ok) and hold_secs >= float(cfg.lead_lag_max_hold_secs):
                        exit_ok = True
                        exit_reason = "max_hold"
                    elif (not exit_ok) and cfg.lead_lag_pm_stop_pct and float(cfg.lead_lag_pm_stop_pct) > 0:
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

                # PM-trend auto-side gate: only allow entry on the best side per market group.
                auto_side_reason = ""
                try:
                    if cfg.strategy_mode == "pm_trend" and bool(cfg.pm_trend_auto_side):
                        g = str(ctx.get("auto_side_group") or "").strip()
                        if g:
                            if (not in_pos) and g in group_has_open_pos:
                                enter_ok = False
                                auto_side_reason = "other_side_open"
                            elif not in_pos:
                                best_tok = best_token_by_group.get(g)
                                if best_tok and str(best_tok) != str(token_id):
                                    enter_ok = False
                                    auto_side_reason = "not_best_side"
                                elif not best_tok:
                                    # No best token yet (e.g. not enough history); don't enter.
                                    enter_ok = False
                                    auto_side_reason = "no_best_side"
                except Exception:
                    pass

                # Gate 1: estimated market lag must be large enough (optional; only blocks when lag is known)
                try:
                    if cfg.strategy_mode != "pm_trend":
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

                # Optional: scale in (paper) when PM odds/price moves in our favor.
                scale_raw = False
                scale_ok = False
                scale_block_reason = ""
                scale_desired_shares = 0.0
                scale_max_usdc = None
                if in_pos and float(cfg.lead_lag_scale_on_odds_change_pct) > 0:
                    try:
                        adds = int(pos.get("adds") or 0)
                    except Exception:
                        adds = 0
                    try:
                        last_scale_at_raw = str(pos.get("last_scale_at") or "").strip()
                        last_scale_dt = _parse_iso_dt(last_scale_at_raw) if last_scale_at_raw else None
                        cooldown_ok = (last_scale_dt is None) or ((ts_dt - last_scale_dt).total_seconds() >= float(cfg.lead_lag_scale_cooldown_s))
                    except Exception:
                        cooldown_ok = True

                    try:
                        shares_now = float(pos.get("shares") or 0.0)
                    except Exception:
                        shares_now = 0.0

                    max_total_ok = True
                    if float(cfg.lead_lag_scale_max_total_shares) > 0:
                        max_total_ok = shares_now < float(cfg.lead_lag_scale_max_total_shares) - 1e-9

                    scale_raw = (
                        (pm_up_move_pct >= float(cfg.lead_lag_scale_on_odds_change_pct))
                        and cooldown_ok
                        and (adds < int(cfg.lead_lag_scale_max_adds))
                        and max_total_ok
                    )

                scale_ok = bool(scale_raw)

                # Scale gate: reuse microstructure/after-cost constraints.
                if scale_ok:
                    # Avoid scaling into wide spreads or extreme executable prices.
                    try:
                        spread2 = float(ask) - float(bid)  # type: ignore[arg-type]
                    except Exception:
                        spread2 = float("inf")
                    if spread2 > float(cfg.lead_lag_slippage_cap):
                        scale_ok = False
                        scale_block_reason = f"wide_spread>{cfg.lead_lag_slippage_cap}"

                if scale_ok:
                    try:
                        if float(ask) > cfg.lead_lag_avoid_price_above or float(ask) < cfg.lead_lag_avoid_price_below:  # type: ignore[arg-type]
                            scale_ok = False
                            scale_block_reason = "avoid_price_zone_executable"
                    except Exception:
                        pass

                if scale_ok and spread_cost_pct is not None:
                    if float(spread_cost_pct) > float(cfg.lead_lag_spread_cost_cap_pct):
                        scale_ok = False
                        scale_block_reason = "spread_too_high"

                if scale_ok and net_edge_pct is not None:
                    if float(net_edge_pct) < float(cfg.lead_lag_net_edge_min_pct):
                        scale_ok = False
                        scale_block_reason = "net_edge_too_low"

                if scale_ok:
                    scale_desired_shares = float(cfg.pm_order_size_shares) * float(cfg.lead_lag_scale_size_mult)
                    if scale_desired_shares <= 0:
                        scale_desired_shares = float(cfg.pm_order_size_shares)

                    # Cap by remaining position limit.
                    if float(cfg.lead_lag_scale_max_total_shares) > 0:
                        try:
                            shares_now = float(pos.get("shares") or 0.0)
                        except Exception:
                            shares_now = 0.0
                        remaining = float(cfg.lead_lag_scale_max_total_shares) - float(shares_now)
                        if remaining <= 0:
                            scale_ok = False
                            scale_block_reason = "max_position"
                        else:
                            scale_desired_shares = min(scale_desired_shares, remaining)

                # Orderbook sizing for scale-in.
                if scale_ok and ob is not None and cfg.lead_lag_enable_orderbook_sizing:
                    try:
                        asks = _safe_top_levels(ob.get("asks"), max_levels=200)
                        best_ask = float(ask) if ask is not None else (float(asks[0]["price"]) if asks else float(pm_mid))
                        limit = float(best_ask) + float(cfg.lead_lag_slippage_cap)
                        _liq_shares, liq_usdc = _sum_book_usdc_in_band(asks, price_leq=limit)
                        scale_max_usdc = min(float(cfg.lead_lag_hard_cap_usdc), float(liq_usdc) * float(cfg.lead_lag_max_fraction_of_band_liquidity))
                        max_shares = 0.0 if best_ask <= 0 else float(scale_max_usdc) / float(best_ask)
                        if scale_desired_shares <= 0:
                            scale_desired_shares = max_shares
                        else:
                            scale_desired_shares = min(scale_desired_shares, max_shares)
                    except Exception:
                        scale_max_usdc = None

                if scale_ok and scale_max_usdc is not None and float(scale_max_usdc) < float(cfg.lead_lag_min_trade_notional_usdc):
                    scale_ok = False
                    scale_block_reason = "insufficient_liquidity"

                if scale_ok and signals_emitted >= cfg.pm_max_orders_per_tick:
                    scale_ok = False
                    scale_block_reason = "throttled"

                # Decide final status/reason for observability
                execution_status = "SKIPPED"
                reason = ""
                if exit_ok:
                    execution_status = "TRIGGERED"
                    reason = exit_reason or "exit"
                elif enter_ok:
                    execution_status = "TRIGGERED"
                    reason = "enter"
                elif scale_ok:
                    execution_status = "TRIGGERED"
                    reason = "scale_in"
                else:
                    if auto_side_reason:
                        reason = auto_side_reason
                    elif not spot_move_ok:
                        reason = "trend_move_too_small" if cfg.strategy_mode == "pm_trend" else "spot_move_too_small"
                    elif float(edge_pct) < float(cfg.lead_lag_edge_min_pct):
                        reason = "low_edge"
                    elif enter_block_reason:
                        reason = enter_block_reason
                    elif scale_block_reason:
                        reason = scale_block_reason
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
                            lag_ms=lag_ms,
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
                            "adds": 0,
                            "last_mid": float(pm_mid),
                        }
                        paper_cash -= notional
                        if cfg.strategy_mode == "pm_trend":
                            paper_notes = f"pm_trend pm_ret={edge_pct:.4f}% max_usdc={max_usdc:.2f}" if max_usdc is not None else f"pm_trend pm_ret={edge_pct:.4f}%"
                        else:
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

                    notes = (
                        f"pm_trend exit={exit_reason} pm_ret={edge_pct:.4f}%"
                        if cfg.strategy_mode == "pm_trend"
                        else f"lead_lag exit={exit_reason} edge={edge_pct:.4f}%"
                    )
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

                # Scale-in: BUY at best ask while already in position (pyramiding on improving odds).
                if scale_ok:
                    fill_price = float(ask or pm_mid)
                    notional = float(fill_price) * float(scale_desired_shares)
                    paper_status = "filled"
                    paper_notes = ""
                    if scale_desired_shares <= 0:
                        paper_status = "rejected"
                        paper_notes = "size_zero"
                    elif paper_cash + 1e-9 < notional:
                        paper_status = "rejected"
                        paper_notes = "insufficient_cash"
                    else:
                        prev = paper_positions.get(token_id) or {}
                        prev_shares = float(prev.get("shares") or 0.0)
                        prev_avg = float(prev.get("avg_entry") or fill_price)
                        new_shares = prev_shares + float(scale_desired_shares)
                        new_avg = ((prev_shares * prev_avg) + (float(scale_desired_shares) * float(fill_price))) / max(new_shares, 1e-9)
                        prev_opened_at = str(prev.get("opened_at") or ts)
                        try:
                            adds = int(prev.get("adds") or 0) + 1
                        except Exception:
                            adds = 1

                        paper_positions[token_id] = {
                            "market": market_name,
                            "outcome": chosen_outcome,
                            "shares": float(new_shares),
                            "avg_entry": float(new_avg),
                            "opened_at": prev_opened_at,
                            "adds": int(adds),
                            "last_scale_at": ts,
                            "last_mid": float(pm_mid),
                        }
                        paper_cash -= notional
                        mode_tag = "pm_trend" if cfg.strategy_mode == "pm_trend" else "lead_lag"
                        paper_notes = (
                            f"{mode_tag} scale_in pm_up_move={pm_up_move_pct:.3f}% edge={edge_pct:.4f}%"
                            + (f" max_usdc={scale_max_usdc:.2f}" if scale_max_usdc is not None else "")
                        )

                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, market_name, "buy", token_id, float(fill_price), float(scale_desired_shares), "paper", "", paper_notes],
                    )
                    append_csv_row(
                        p_pm_paper_trades,
                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                        [
                            ts,
                            market_name,
                            token_id,
                            chosen_outcome or "",
                            "BUY",
                            float(fill_price),
                            float(scale_desired_shares),
                            float(notional),
                            float(paper_cash),
                            paper_status,
                            paper_notes,
                        ],
                        keep_last=500,
                    )
                    if paper_status in {"filled", "rejected"}:
                        signals_emitted += 1
                    continue

                # Keep a lightweight per-position last_mid snapshot for scale-in logic.
                if in_pos:
                    try:
                        pos["last_mid"] = float(pm_mid)
                    except Exception:
                        pass

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

            # Optional: Polymarket "deadline ladder" edge scan + paper auto-trade.
            # This looks for maturity pairs that should be monotone (early event implies late event), and trades the
            # riskless structure: BUY early NO + BUY late YES when executable cost < 1.
            try:
                pm_deadline_enable = (os.getenv("PM_DEADLINE_ENABLE", "0") or "0").strip().lower() in {"1", "true", "yes"}
                pm_deadline_interval_s = float(os.getenv("PM_DEADLINE_INTERVAL_S", "300") or "300")
                pm_deadline_min_profit = float(os.getenv("PM_DEADLINE_MIN_GUARANTEED_PROFIT", "0.01") or "0.01")
                pm_deadline_max_pairs = int(os.getenv("PM_DEADLINE_MAX_PAIRS", "200") or "200")
                pm_deadline_max_pairs = max(10, min(int(pm_deadline_max_pairs), 5000))
                pm_deadline_auto_trade = (os.getenv("PM_DEADLINE_AUTO_TRADE", "0") or "0").strip().lower() in {"1", "true", "yes"}
                pm_deadline_trade_cooldown_s = float(os.getenv("PM_DEADLINE_TRADE_COOLDOWN_S", "3600") or "3600")
                pm_deadline_max_usd = float(os.getenv("PM_DEADLINE_MAX_USD", "10") or "10")
                pm_deadline_max_frac_cash = float(os.getenv("PM_DEADLINE_MAX_FRACTION_CASH", "0.05") or "0.05")

                if pm_deadline_enable and runtime_cache is not None:
                    now_ms = _now_ms()
                    due = (runtime_cache.pm_deadline_last_run_ms <= 0) or ((now_ms - runtime_cache.pm_deadline_last_run_ms) >= int(pm_deadline_interval_s * 1000.0))

                    # Build candidate maturity pairs from the latest Gamma scan index.
                    if due:
                        idx = None
                        try:
                            if p_pm_markets_index_full.exists():
                                idx = read_json(p_pm_markets_index_full)
                            else:
                                idx = read_json(p_pm_markets_index)
                        except Exception:
                            idx = None

                        items_any = None
                        if isinstance(idx, dict):
                            items_any = cast(dict[str, Any], idx).get("items")

                        groups: dict[str, list[dict[str, Any]]] = {}
                        if isinstance(items_any, list):
                            for it_any in cast(list[Any], items_any):
                                if not isinstance(it_any, dict):
                                    continue
                                it = cast(dict[str, Any], it_any)
                                slug = str(it.get("slug") or "").strip()
                                q = str(it.get("question") or "").strip()
                                end_s = str(it.get("end_date") or "").strip()
                                end_dt = _parse_gamma_end_date(end_s)
                                toks = it.get("clob_token_ids")
                                outs = it.get("outcomes")

                                if not slug or not q:
                                    continue
                                if end_dt is None:
                                    continue
                                if not isinstance(toks, list) or len(cast(list[Any], toks)) < 2:
                                    continue
                                if not isinstance(outs, list) or len(cast(list[Any], outs)) < 2:
                                    continue
                                if not _pm_deadline_looks_like_market(slug=slug, question=q):
                                    continue

                                base = _pm_deadline_base_key(slug=slug, question=q)
                                if not base:
                                    continue

                                it2 = dict(it)
                                it2["_end_dt"] = end_dt
                                groups.setdefault(base, []).append(it2)

                        # Compare adjacent maturities per base.
                        pairs: list[dict[str, Any]] = []
                        for base, group in groups.items():
                            if len(group) < 2:
                                continue
                            gs = sorted(group, key=lambda x: cast(datetime, x.get("_end_dt")))
                            for i in range(len(gs) - 1):
                                early = gs[i]
                                late = gs[i + 1]
                                e_dt = cast(datetime, early.get("_end_dt"))
                                l_dt = cast(datetime, late.get("_end_dt"))
                                if l_dt <= e_dt:
                                    continue
                                pairs.append(
                                    {
                                        "base": base,
                                        "early": {"slug": early.get("slug"), "question": early.get("question"), "end_date": early.get("end_date"), "outcomes": early.get("outcomes"), "token_ids": early.get("clob_token_ids")},
                                        "late": {"slug": late.get("slug"), "question": late.get("question"), "end_date": late.get("end_date"), "outcomes": late.get("outcomes"), "token_ids": late.get("clob_token_ids")},
                                    }
                                )

                        # Keep the list bounded for performance.
                        if len(pairs) > pm_deadline_max_pairs:
                            pairs = pairs[:pm_deadline_max_pairs]

                        runtime_cache.pm_deadline_last_run_ms = int(now_ms)
                        runtime_cache.gamma_market_by_slug["__pm_deadline_pairs"] = pairs

                        # Write a debug CSV row per pair (unpriced until trade-time), so we can see groupings.
                        rows_out: list[list[Any]] = []
                        for p in pairs[: min(len(pairs), 500)]:
                            early = cast(dict[str, Any], p.get("early") or {})
                            late = cast(dict[str, Any], p.get("late") or {})
                            rows_out.append(
                                [
                                    ts,
                                    str(p.get("base") or ""),
                                    str(early.get("slug") or ""),
                                    str(late.get("slug") or ""),
                                    str(early.get("end_date") or ""),
                                    str(late.get("end_date") or ""),
                                    str(early.get("question") or "")[:240],
                                    str(late.get("question") or "")[:240],
                                    "",
                                    "",
                                    "",
                                    "",
                                    "",
                                    "",
                                    "",
                                    "scan",
                                    "unpriced",
                                ]
                            )
                        write_csv(
                            p_pm_deadline_edges,
                            [
                                "ts",
                                "base",
                                "early_slug",
                                "late_slug",
                                "early_end_date",
                                "late_end_date",
                                "early_question",
                                "late_question",
                                "early_no_token",
                                "late_yes_token",
                                "early_no_ask",
                                "late_yes_ask",
                                "cost",
                                "guaranteed_profit",
                                "between_deadlines_profit",
                                "decision",
                                "reason",
                            ],
                            rows_out,
                        )

                    # Auto-trade: price pairs using current orderbooks and trade the first that meets the threshold.
                    if pm_deadline_auto_trade and cfg.trading_mode == "paper":
                        last_trade_age_s = (now_ms - int(runtime_cache.pm_deadline_last_trade_ms or 0)) / 1000.0
                        if last_trade_age_s < float(pm_deadline_trade_cooldown_s):
                            pass
                        else:
                            pairs_any = runtime_cache.gamma_market_by_slug.get("__pm_deadline_pairs")
                            pairs = cast(list[dict[str, Any]], pairs_any) if isinstance(pairs_any, list) else []

                            def _pick_token(*, outcomes: list[Any], token_ids: list[Any], desired: str) -> str | None:
                                d = (desired or "").strip().lower()
                                if len(outcomes) != len(token_ids):
                                    return None
                                for i, o in enumerate(outcomes):
                                    if str(o).strip().lower() == d:
                                        return str(token_ids[i]).strip()
                                return None

                            priced_rows: list[list[Any]] = []
                            traded = False
                            for p in pairs:
                                early = cast(dict[str, Any], p.get("early") or {})
                                late = cast(dict[str, Any], p.get("late") or {})
                                e_outs = list(early.get("outcomes") or [])
                                e_toks = list(early.get("token_ids") or [])
                                l_outs = list(late.get("outcomes") or [])
                                l_toks = list(late.get("token_ids") or [])

                                early_no = _pick_token(outcomes=e_outs, token_ids=e_toks, desired="No")
                                late_yes = _pick_token(outcomes=l_outs, token_ids=l_toks, desired="Yes")
                                if not early_no or not late_yes:
                                    continue

                                # Avoid duplicate stacking: if we already hold either leg, skip.
                                if float((paper_positions.get(early_no) or {}).get("shares") or 0.0) > 0:
                                    continue
                                if float((paper_positions.get(late_yes) or {}).get("shares") or 0.0) > 0:
                                    continue

                                # Price legs at best ask.
                                try:
                                    ob_no = pm_clob.get_orderbook(early_no)
                                    _b_no, a_no = best_bid_ask(ob_no if isinstance(ob_no, dict) else {"data": ob_no})
                                    ob_yes = pm_clob.get_orderbook(late_yes)
                                    _b_yes, a_yes = best_bid_ask(ob_yes if isinstance(ob_yes, dict) else {"data": ob_yes})
                                except Exception:
                                    continue

                                if a_no is None or a_yes is None:
                                    continue
                                if float(a_no) <= 0 or float(a_yes) <= 0:
                                    continue

                                cost = float(a_no) + float(a_yes)
                                guaranteed_profit = 1.0 - cost
                                between_profit = 2.0 - cost

                                # Record priced row for visibility.
                                priced_rows.append(
                                    [
                                        ts,
                                        str(p.get("base") or ""),
                                        str(early.get("slug") or ""),
                                        str(late.get("slug") or ""),
                                        str(early.get("end_date") or ""),
                                        str(late.get("end_date") or ""),
                                        str(early.get("question") or "")[:240],
                                        str(late.get("question") or "")[:240],
                                        early_no,
                                        late_yes,
                                        float(a_no),
                                        float(a_yes),
                                        float(cost),
                                        float(guaranteed_profit),
                                        float(between_profit),
                                        "watch",
                                        "priced",
                                    ]
                                )

                                if guaranteed_profit < float(pm_deadline_min_profit):
                                    continue

                                # Size in shares based on max USD and cash.
                                max_budget = min(float(pm_deadline_max_usd), float(paper_cash) * max(0.0, float(pm_deadline_max_frac_cash)))
                                if max_budget <= 0:
                                    continue
                                shares = max_budget / max(cost, 1e-9)
                                if shares <= 0:
                                    continue

                                trade_key = f"{p.get('base')}|{early_no}|{late_yes}"
                                if runtime_cache.pm_deadline_last_trade_key and runtime_cache.pm_deadline_last_trade_key == trade_key:
                                    continue

                                def _paper_buy(tok: str, *, market_name: str, outcome_name: str, price: float, shares: float, notes: str) -> bool:
                                    nonlocal paper_cash
                                    fill_price = float(price)
                                    notional = float(fill_price) * float(shares)
                                    if float(shares) <= 0:
                                        return False
                                    if paper_cash + 1e-9 < notional:
                                        return False
                                    prev = paper_positions.get(tok)
                                    prev_shares = float(prev.get("shares") or 0.0) if prev is not None else 0.0
                                    prev_avg = float(prev.get("avg_entry") or fill_price) if prev is not None else float(fill_price)
                                    new_shares = prev_shares + float(shares)
                                    new_avg = ((prev_shares * prev_avg) + (float(shares) * float(fill_price))) / max(new_shares, 1e-9)
                                    paper_positions[tok] = {
                                        "market": market_name,
                                        "outcome": outcome_name,
                                        "shares": float(new_shares),
                                        "avg_entry": float(new_avg),
                                        "opened_at": ts,
                                    }
                                    paper_cash -= notional
                                    append_csv_row(
                                        p_pm_orders,
                                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                                        [ts, market_name, "buy", tok, float(fill_price), float(shares), "paper", "", notes],
                                    )
                                    append_csv_row(
                                        p_pm_paper_trades,
                                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                                        [ts, market_name, tok, outcome_name, "BUY", float(fill_price), float(shares), float(notional), float(paper_cash), "filled", notes],
                                        keep_last=500,
                                    )
                                    return True

                                base = str(p.get("base") or "")
                                notes = f"deadline_edge base={base} gp={guaranteed_profit:.4f} between={between_profit:.4f} cost={cost:.4f}"
                                ok1 = _paper_buy(
                                    early_no,
                                    market_name=str(early.get("question") or early.get("slug") or "deadline-early"),
                                    outcome_name="No",
                                    price=float(a_no),
                                    shares=float(shares),
                                    notes=notes,
                                )
                                ok2 = _paper_buy(
                                    late_yes,
                                    market_name=str(late.get("question") or late.get("slug") or "deadline-late"),
                                    outcome_name="Yes",
                                    price=float(a_yes),
                                    shares=float(shares),
                                    notes=notes,
                                )
                                if ok1 and ok2:
                                    runtime_cache.pm_deadline_last_trade_ms = int(now_ms)
                                    runtime_cache.pm_deadline_last_trade_key = trade_key
                                    traded = True
                                    signals_emitted += 1
                                    break

                            # Overwrite file with priced rows (latest tick). Keep it human-readable and bounded.
                            if priced_rows:
                                write_csv(
                                    p_pm_deadline_edges,
                                    [
                                        "ts",
                                        "base",
                                        "early_slug",
                                        "late_slug",
                                        "early_end_date",
                                        "late_end_date",
                                        "early_question",
                                        "late_question",
                                        "early_no_token",
                                        "late_yes_token",
                                        "early_no_ask",
                                        "late_yes_ask",
                                        "cost",
                                        "guaranteed_profit",
                                        "between_deadlines_profit",
                                        "decision",
                                        "reason",
                                    ],
                                    priced_rows[-500:],
                                )
                            if traded:
                                pm_status["signals_emitted"] = signals_emitted
            except Exception:
                pass

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
            [ts, len(mkts), len(computed_rows), signals_emitted, "ok", scan_note],
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
        now_dt = _parse_iso_dt(ts)

        # Auto-close paper positions whose market end_date has passed.
        # This keeps the portal from showing stale positions forever.
        paper_auto_exit_after_end = (os.getenv("PAPER_AUTO_EXIT_AFTER_ENDDATE", "1") or "1").strip().lower() not in {"0", "false", "no"}
        paper_auto_exit_grace_hours = float(os.getenv("PAPER_AUTO_EXIT_GRACE_H", "24") or "24")
        paper_auto_exit_grace_s = max(0.0, paper_auto_exit_grace_hours) * 3600.0
        paper_auto_exit_on_closed = (os.getenv("PAPER_AUTO_EXIT_ON_CLOSED", "1") or "1").strip().lower() not in {"0", "false", "no"}
        paper_auto_exit_meta_lookup = (os.getenv("PAPER_AUTO_EXIT_META_LOOKUP", "1") or "1").strip().lower() not in {"0", "false", "no"}
        paper_auto_exit_meta_lookup_max = int(os.getenv("PAPER_AUTO_EXIT_META_LOOKUP_MAX", "2") or "2")
        paper_auto_exit_meta_lookup_max = max(0, min(int(paper_auto_exit_meta_lookup_max), 50))
        meta_lookups_used = 0

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

            # If we have an end_date for this token (from the latest Gamma scan), auto-exit after it passes.
            if paper_auto_exit_after_end and runtime_cache is not None:
                meta = runtime_cache.pm_scan_token_meta.get(tok) if hasattr(runtime_cache, "pm_scan_token_meta") else None

                # If the token is known but marked closed, we may still need a token-id lookup to obtain outcomePrices
                # for correct settlement pricing.
                if (
                    isinstance(meta, dict)
                    and paper_auto_exit_on_closed
                    and meta.get("closed") is True
                    and (meta.get("outcome_prices") is None or meta.get("outcomes") is None)
                    and paper_auto_exit_meta_lookup
                    and meta_lookups_used < paper_auto_exit_meta_lookup_max
                ):
                    try:
                        found = gamma.get_market_listing_by_token_id(token_id=tok)
                        if found is not None:
                            raw = cast(dict[str, Any], found.raw or {})
                            outcome_prices_any = raw.get("outcomePrices") or raw.get("outcome_prices")
                            outcome_prices: list[Any] = []
                            if isinstance(outcome_prices_any, list):
                                outcome_prices = list(outcome_prices_any)
                            elif isinstance(outcome_prices_any, str):
                                s = outcome_prices_any.strip()
                                if s.startswith("[") and s.endswith("]"):
                                    try:
                                        loaded = json.loads(s)
                                        if isinstance(loaded, list):
                                            outcome_prices = list(loaded)
                                    except Exception:
                                        outcome_prices = []
                            meta["outcomes"] = list(found.outcomes or [])
                            meta["outcome_prices"] = [str(x) for x in outcome_prices]
                            meta["end_date"] = str(found.end_date or meta.get("end_date") or "")
                            meta["closed"] = bool(found.closed) if found.closed is not None else meta.get("closed")
                            runtime_cache.pm_scan_token_meta[tok] = meta
                            runtime_cache.pm_scan_token_meta_at_ms = int(_now_ms())
                        meta_lookups_used += 1
                    except Exception:
                        meta_lookups_used += 1

                # Fallback: if token meta is missing (scan window may not include this token), fetch by token id.
                if (
                    meta is None
                    and paper_auto_exit_meta_lookup
                    and meta_lookups_used < paper_auto_exit_meta_lookup_max
                ):
                    try:
                        found = gamma.get_market_listing_by_token_id(token_id=tok)
                        if found is not None:
                            raw = cast(dict[str, Any], found.raw or {})
                            outcome_prices_any = raw.get("outcomePrices") or raw.get("outcome_prices")
                            outcome_prices: list[Any] = []
                            if isinstance(outcome_prices_any, list):
                                outcome_prices = list(outcome_prices_any)
                            elif isinstance(outcome_prices_any, str):
                                s = outcome_prices_any.strip()
                                if s.startswith("[") and s.endswith("]"):
                                    try:
                                        loaded = json.loads(s)
                                        if isinstance(loaded, list):
                                            outcome_prices = list(loaded)
                                    except Exception:
                                        outcome_prices = []
                            meta = {
                                "slug": str(found.slug or ""),
                                "question": str(found.question or ""),
                                "end_date": str(found.end_date or ""),
                                "closed": bool(found.closed) if found.closed is not None else None,
                                "outcomes": list(found.outcomes or []),
                                "outcome_prices": [str(x) for x in outcome_prices],
                            }
                            runtime_cache.pm_scan_token_meta[tok] = meta
                            runtime_cache.pm_scan_token_meta_at_ms = int(_now_ms())
                        meta_lookups_used += 1
                    except Exception:
                        meta_lookups_used += 1

                end_dt = _parse_gamma_end_date(meta.get("end_date") if isinstance(meta, dict) else None)
                if end_dt is not None:
                    if now_dt >= (end_dt + timedelta(seconds=paper_auto_exit_grace_s)):
                        exit_px: float
                        meta_closed = None
                        if isinstance(meta, dict):
                            meta_closed = meta.get("closed")
                        # If Gamma says the market is closed and provides settlement prices, use them.
                        if meta_closed is True and isinstance(meta, dict):
                            outs = meta.get("outcomes")
                            prs = meta.get("outcome_prices")
                            if isinstance(outs, list) and isinstance(prs, list) and len(outs) == len(prs) and outcome:
                                try:
                                    idx = next(i for i, o in enumerate(outs) if str(o).strip().lower() == outcome.strip().lower())
                                    exit_px = float(prs[idx])
                                except Exception:
                                    exit_px = float(last_price) if last_price is not None else float(avg_entry)
                            else:
                                exit_px = float(last_price) if last_price is not None else float(avg_entry)
                        else:
                            exit_px = float(last_price) if last_price is not None else float(avg_entry)
                        notional = float(exit_px) * float(shares)
                        paper_cash += notional
                        paper_realized += (float(exit_px) - float(avg_entry)) * float(shares)
                        paper_positions.pop(tok, None)

                        notes = f"auto_exit_after_end_date end_date={end_dt.isoformat()} grace_h={paper_auto_exit_grace_hours:g} closed={meta_closed}"
                        append_csv_row(
                            p_pm_orders,
                            ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                            [ts, mname, "sell", tok, float(exit_px), float(shares), "paper", "", notes],
                        )
                        append_csv_row(
                            p_pm_paper_trades,
                            ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                            [ts, mname, tok, outcome, "AUTO_SELL", float(exit_px), float(shares), float(notional), float(paper_cash), "filled", notes],
                            keep_last=500,
                        )
                        continue

                # If market is closed, settle immediately (even if end_date is missing or not yet passed).
                if paper_auto_exit_on_closed and isinstance(meta, dict) and meta.get("closed") is True:
                    exit_px: float
                    outs = meta.get("outcomes")
                    prs = meta.get("outcome_prices")
                    if isinstance(outs, list) and isinstance(prs, list) and len(outs) == len(prs) and outcome:
                        try:
                            idx = next(i for i, o in enumerate(outs) if str(o).strip().lower() == outcome.strip().lower())
                            exit_px = float(prs[idx])
                        except Exception:
                            exit_px = float(last_price) if last_price is not None else float(avg_entry)
                    else:
                        exit_px = float(last_price) if last_price is not None else float(avg_entry)

                    notional = float(exit_px) * float(shares)
                    paper_cash += notional
                    paper_realized += (float(exit_px) - float(avg_entry)) * float(shares)
                    paper_positions.pop(tok, None)

                    notes = "auto_exit_closed"
                    append_csv_row(
                        p_pm_orders,
                        ["ts", "market", "side", "token", "price", "size", "status", "tx_id", "notes"],
                        [ts, mname, "sell", tok, float(exit_px), float(shares), "paper", "", notes],
                    )
                    append_csv_row(
                        p_pm_paper_trades,
                        ["ts", "market", "token", "outcome", "action", "price", "shares", "notional", "cash_after", "status", "notes"],
                        [ts, mname, tok, outcome, "AUTO_SELL", float(exit_px), float(shares), float(notional), float(paper_cash), "filled", notes],
                        keep_last=500,
                    )
                    continue

            lp = float(last_price) if last_price is not None else avg_entry
            value = shares * lp
            upnl = shares * (lp - avg_entry)
            unrealized += upnl
            equity += value
            try:
                adds = int(p.get("adds") or 0)
            except Exception:
                adds = 0
            try:
                last_mid = float(p.get("last_mid") or lp)
            except Exception:
                last_mid = lp
            last_scale_at = str(p.get("last_scale_at") or "")
            mtm_rows.append([ts, mname, tok, outcome, shares, avg_entry, lp, value, upnl, adds, last_mid, last_scale_at])

        write_csv(
            p_pm_paper_positions,
            [
                "ts",
                "market",
                "token",
                "outcome",
                "shares",
                "avg_entry",
                "last_price",
                "value",
                "unrealized_pnl",
                "adds",
                "last_mid",
                "last_scale_at",
            ],
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
    if cfg.ftp_host and not (cfg.ftp_user and cfg.ftp_pass):
        print("[agent] ftp configured partially: missing FTP_USER/FTP_PASS", flush=True)
        return
    if not (cfg.ftp_host and cfg.ftp_user and cfg.ftp_pass):
        return

    ftp_host = str(cfg.ftp_host)
    ftp_user = str(cfg.ftp_user)
    ftp_pass = str(cfg.ftp_pass)

    ftp_debug = (os.getenv("FTP_DEBUG", "0") or "0").strip().lower() in {"1", "true", "yes"}
    ftp_upload_interval_s = float(os.getenv("FTP_UPLOAD_INTERVAL_S", "60") or "60")

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
        "pm_markets_index.json",
        "pm_scan_candidates.csv",
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

    if not files:
        if ftp_debug:
            print("[agent] ftp: no files produced this tick", flush=True)
        return

    matched = [p for p in files if p.name in allow]
    if ftp_debug:
        print(f"[agent] ftp: tick produced {len(files)} file(s), {len(matched)} eligible", flush=True)
    if not matched:
        if ftp_debug:
            sample = ", ".join([p.name for p in files[:10]])
            print(f"[agent] ftp: sample produced names: {sample}", flush=True)
        return

    # Throttle upload frequency (FTP servers often disconnect on too-frequent sessions)
    now_mono = time.monotonic()
    global _ftp_last_upload_mono
    global _ftp_last_uploaded_mtime
    if _ftp_last_upload_mono is not None and ftp_upload_interval_s > 0:
        if (now_mono - _ftp_last_upload_mono) < ftp_upload_interval_s:
            if ftp_debug:
                print("[agent] ftp: throttled", flush=True)
            return

    # Only upload files that changed since last successful upload (always upload live_status.json)
    to_upload: list[Path] = []
    for p in matched:
        try:
            mtime = float(p.stat().st_mtime)
        except Exception:
            mtime = None  # best-effort

        if p.name == "live_status.json":
            to_upload.append(p)
            continue

        if mtime is None:
            to_upload.append(p)
            continue

        last_mtime = _ftp_last_uploaded_mtime.get(p.name)
        if last_mtime is None or mtime > (last_mtime + 1e-6):
            to_upload.append(p)

    if not to_upload:
        if ftp_debug:
            print("[agent] ftp: nothing changed; skipping", flush=True)
        return

    def _upload_once_ftp(paths: list[Path]) -> list[str]:
        uploaded_local: list[str] = []
        with FTP() as ftp:
            try:
                ftp.connect(ftp_host, int(cfg.ftp_port), timeout=20)
            except Exception as e:
                raise RuntimeError(f"ftp connect failed: {type(e).__name__}: {e!r}") from e

            try:
                ftp.login(ftp_user, ftp_pass)
            except Exception as e:
                raise RuntimeError(f"ftp login failed: {type(e).__name__}: {e!r}") from e

            # Ensure remote directory exists by walking segments
            remote = cfg.ftp_remote_dir.strip("/")
            if remote:
                parts = remote.split("/")
                try:
                    ftp.cwd("/")
                except Exception as e:
                    raise RuntimeError(f"ftp cwd('/') failed: {type(e).__name__}: {e!r}") from e

                for part in parts:
                    if not part:
                        continue
                    try:
                        ftp.mkd(part)
                    except Exception:
                        pass
                    try:
                        ftp.cwd(part)
                    except Exception as e:
                        raise RuntimeError(f"ftp cwd({part!r}) failed: {type(e).__name__}: {e!r}") from e

            for path in paths:
                try:
                    with path.open("rb") as f:
                        ftp.storbinary(f"STOR {path.name}", f)
                except Exception as e:
                    raise RuntimeError(f"ftp stor failed for {path.name}: {type(e).__name__}: {e!r}") from e
                uploaded_local.append(path.name)

        return uploaded_local

    def _sftp_mkdir_p(sftp: Any, remote_dir: str) -> None:
        remote_dir = (remote_dir or "").strip()
        if not remote_dir:
            return
        # Normalize: make it absolute-ish if caller passed /...
        if remote_dir.startswith("/"):
            sftp.chdir("/")
            rel = remote_dir.strip("/")
        else:
            rel = remote_dir

        if not rel:
            return

        for part in rel.split("/"):
            if not part:
                continue
            try:
                sftp.mkdir(part)
            except Exception:
                pass
            sftp.chdir(part)

    def _upload_once_sftp(paths: list[Path]) -> list[str]:
        try:
            import paramiko  # type: ignore
        except Exception as e:
            raise RuntimeError(
                "SFTP upload requested (FTP_PROTOCOL=sftp) but paramiko is not installed. "
                "Install dependencies from vps/requirements.txt on the VPS."
            ) from e

        uploaded_local: list[str] = []
        transport: Any = None
        sftp: Any = None
        try:
            try:
                transport = paramiko.Transport((ftp_host, int(cfg.ftp_port)))
            except Exception as e:
                raise RuntimeError(f"sftp connect failed: {type(e).__name__}: {e!r}") from e

            try:
                transport.connect(username=ftp_user, password=ftp_pass)
            except Exception as e:
                raise RuntimeError(f"sftp login failed: {type(e).__name__}: {e!r}") from e

            try:
                sftp = paramiko.SFTPClient.from_transport(transport)
            except Exception as e:
                raise RuntimeError(f"sftp client init failed: {type(e).__name__}: {e!r}") from e

            try:
                _sftp_mkdir_p(sftp, cfg.ftp_remote_dir)
            except Exception as e:
                raise RuntimeError(f"sftp ensure remote dir failed: {type(e).__name__}: {e!r}") from e

            for path in paths:
                try:
                    sftp.put(str(path), str(Path(path.name)))
                except Exception as e:
                    raise RuntimeError(f"sftp put failed for {path.name}: {type(e).__name__}: {e!r}") from e
                uploaded_local.append(path.name)
        finally:
            try:
                if sftp is not None:
                    sftp.close()
            except Exception:
                pass
            try:
                if transport is not None:
                    transport.close()
            except Exception:
                pass

        return uploaded_local

    # Retry once on transient disconnects (EOFError often means the server dropped the connection)
    try:
        if cfg.ftp_protocol == "sftp":
            uploaded = _upload_once_sftp(to_upload)
        else:
            uploaded = _upload_once_ftp(to_upload)
    except EOFError:
        time.sleep(2)
        if cfg.ftp_protocol == "sftp":
            uploaded = _upload_once_sftp(to_upload)
        else:
            uploaded = _upload_once_ftp(to_upload)

    if uploaded:
        _ftp_last_upload_mono = now_mono
        for p in to_upload:
            try:
                _ftp_last_uploaded_mtime[p.name] = float(p.stat().st_mtime)
            except Exception:
                pass

        print(
            f"[agent] ftp uploaded {len(uploaded)} file(s) via {cfg.ftp_protocol} to {cfg.ftp_host}:{cfg.ftp_remote_dir}: {', '.join(uploaded[:8])}{' ...' if len(uploaded) > 8 else ''}",
            flush=True,
        )


def http_upload_files(cfg: Config, files: list[Path]) -> None:
    if not (cfg.upload_url and cfg.upload_api_key):
        return

    upload_debug = (os.getenv("UPLOAD_DEBUG", "0") or "0").strip().lower() in {"1", "true", "yes"}
    upload_interval_s = float(os.getenv("UPLOAD_INTERVAL_S", "60") or "60")
    upload_bundle_zip = (os.getenv("UPLOAD_BUNDLE_ZIP", "0") or "0").strip().lower() in {"1", "true", "yes"}

    allow = {
        "live_status.json",
        "lead_lag_health.json",
        "sources_health.json",
        "deribit_options_public.json",
        "polymarket_status.json",
        "polymarket_clob_public.json",
        "pm_open_orders.json",
        "pm_scanner_log.csv",
        "pm_markets_index.json",
        "pm_scan_candidates.csv",
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

    if not files:
        if upload_debug:
            print("[agent] upload: no files produced this tick", flush=True)
        return

    matched = [p for p in files if p.name in allow]
    if upload_debug:
        print(f"[agent] upload: tick produced {len(files)} file(s), {len(matched)} eligible", flush=True)
    if not matched:
        return

    now_mono = time.monotonic()
    global _http_last_upload_mono
    global _http_last_uploaded_mtime
    if _http_last_upload_mono is not None and upload_interval_s > 0:
        if (now_mono - _http_last_upload_mono) < upload_interval_s:
            if upload_debug:
                print("[agent] upload: throttled", flush=True)
            return

    to_upload: list[Path] = []
    for p in matched:
        try:
            mtime = float(p.stat().st_mtime)
        except Exception:
            mtime = None

        if p.name == "live_status.json":
            to_upload.append(p)
            continue

        if mtime is None:
            to_upload.append(p)
            continue

        last_mtime = _http_last_uploaded_mtime.get(p.name)
        if last_mtime is None or mtime > (last_mtime + 1e-6):
            to_upload.append(p)

    if not to_upload:
        if upload_debug:
            print("[agent] upload: nothing changed; skipping", flush=True)
        return

    headers = {
        "X-API-KEY": str(cfg.upload_api_key),
        "Content-Type": "application/octet-stream",
    }

    def _append_query(url: str, extra: dict[str, str]) -> str:
        u = urllib.parse.urlparse(url)
        q = dict(urllib.parse.parse_qsl(u.query, keep_blank_values=True))
        q.update(extra)
        new_q = urllib.parse.urlencode(q)
        return urllib.parse.urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

    # Bundle mode: send a zip with multiple files in one request.
    if upload_bundle_zip:
        base = str(cfg.upload_url)
        url = _append_query(base, {"bundle": "zip"})

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
            for path in to_upload:
                try:
                    data = path.read_bytes()
                except Exception as e:
                    raise RuntimeError(f"upload bundle read failed for {path.name}: {type(e).__name__}: {e!r}") from e
                # Store as basename only
                z.writestr(path.name, data)

        payload = buf.getvalue()
        if not payload:
            if upload_debug:
                print("[agent] upload: bundle empty; skipping", flush=True)
            return

        h = dict(headers)
        h["Content-Type"] = "application/zip"
        try:
            resp = requests.post(url, data=payload, headers=h, timeout=30)
        except Exception as e:
            raise RuntimeError(f"upload bundle request failed: {type(e).__name__}: {e!r}") from e
        if resp.status_code != 200:
            body = (resp.text or "").strip()[:500]
            raise RuntimeError(f"upload bundle failed: HTTP {resp.status_code}: {body}")

        uploaded = [p.name for p in to_upload]
        _http_last_upload_mono = now_mono
        for p in to_upload:
            try:
                _http_last_uploaded_mtime[p.name] = float(p.stat().st_mtime)
            except Exception:
                pass

        print(
            f"[agent] upload posted {len(uploaded)} file(s) (bundle zip) to {cfg.upload_url}: {', '.join(uploaded[:8])}{' ...' if len(uploaded) > 8 else ''}",
            flush=True,
        )
        return

    uploaded: list[str] = []
    base = str(cfg.upload_url)
    for path in to_upload:
        # Preserve existing query params (e.g. ?type=futures) while adding name=...
        try:
            url = _append_query(base, {"name": path.name})
        except Exception:
            url = base

        try:
            with path.open("rb") as f:
                resp = requests.post(url, data=f, headers=headers, timeout=20)
        except Exception as e:
            raise RuntimeError(f"upload request failed for {path.name}: {type(e).__name__}: {e!r}") from e

        if resp.status_code != 200:
            body = (resp.text or "").strip()
            body = body[:500]
            raise RuntimeError(f"upload failed for {path.name}: HTTP {resp.status_code}: {body}")

        uploaded.append(path.name)

    if uploaded:
        _http_last_upload_mono = now_mono
        for p in to_upload:
            try:
                _http_last_uploaded_mtime[p.name] = float(p.stat().st_mtime)
            except Exception:
                pass

        print(
            f"[agent] upload posted {len(uploaded)} file(s) to {cfg.upload_url}: {', '.join(uploaded[:8])}{' ...' if len(uploaded) > 8 else ''}",
            flush=True,
        )


def main() -> None:
    cfg = load_config()
    cfg.out_dir.mkdir(parents=True, exist_ok=True)

    lead_lag_engine: LeadLagEngine | None = None
    if cfg.strategy_mode == "lead_lag":
        lead_lag_engine = LeadLagEngine()

    pm_trend_engine: PmTrendEngine | None = None
    if cfg.strategy_mode == "pm_trend":
        pm_trend_engine = PmTrendEngine()

    health_tracker = LeadLagHealthTracker() if cfg.strategy_mode == "lead_lag" else None
    latency_tracker = LatencyTracker()
    runtime_cache = RuntimeCache()

    run_once = (os.getenv("RUN_ONCE", "0") or "0").strip().lower() in {"1", "true", "yes"}
    run_ticks_raw = (os.getenv("RUN_TICKS") or "").strip()
    try:
        run_ticks = int(run_ticks_raw) if run_ticks_raw else 0
    except Exception:
        run_ticks = 0
    run_ticks = max(0, int(run_ticks))
    tick_count = 0

    print(f"[agent] out_dir={cfg.out_dir}", flush=True)
    print(f"[agent] interval_s={cfg.interval_s}", flush=True)
    print(f"[agent] trading_mode={cfg.trading_mode}", flush=True)
    print(
        "[agent] ftp="
        + ("enabled" if (cfg.ftp_host and cfg.ftp_user and cfg.ftp_pass) else "disabled")
        + f" proto={cfg.ftp_protocol!r} port={cfg.ftp_port!r} host={cfg.ftp_host!r} remote_dir={cfg.ftp_remote_dir!r}",
        flush=True,
    )
    print(
        "[agent] upload="
        + ("enabled" if (cfg.upload_url and cfg.upload_api_key) else "disabled")
        + f" url={cfg.upload_url!r}",
        flush=True,
    )

    consecutive_failures = 0

    # Live runtime: create a single live client + optional user websocket.
    pm_live_client: Any | None = None
    pm_live_error: str | None = None
    pm_position_store: PolymarketPositionStore | None = None
    pm_user_wss: PolymarketUserWssClient | None = None
    pm_user_wss_status: dict[str, Any] = {}

    poly_trading_requested = bool(
        cfg.trading_mode == "live"
        and cfg.poly_live_confirm == "YES"
        and cfg.poly_private_key
        and cfg.poly_api_key
        and cfg.poly_api_secret
        and cfg.poly_api_passphrase
    )
    if poly_trading_requested:
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
            pm_live_client = None

    if pm_live_client is not None and cfg.pm_user_wss_enable and cfg.poly_wss_url and cfg.poly_api_key and cfg.poly_api_secret and cfg.poly_api_passphrase:
        pm_position_store = PolymarketPositionStore()
        try:
            def _on_fill_store(f: Any) -> None:
                try:
                    # store is non-None in this branch
                    pm_position_store.apply_fill(f)
                except Exception:
                    pass

            pm_user_wss = PolymarketUserWssClient(
                cfg=PolymarketUserWssConfig(
                    wss_url=str(cfg.poly_wss_url),
                    auth=PolymarketUserWssAuth(
                        api_key=str(cfg.poly_api_key),
                        api_secret=str(cfg.poly_api_secret),
                        api_passphrase=str(cfg.poly_api_passphrase),
                    ),
                ),
                on_fill=_on_fill_store,
                status_sink=pm_user_wss_status,
            )
            pm_user_wss.start()
        except Exception as e:
            pm_user_wss_status.update({"ok": False, "error": str(e)})

    pm_exec: ThreadPoolExecutor | None = None
    try:
        if cfg.pm_orderbook_workers > 1:
            pm_exec = ThreadPoolExecutor(max_workers=int(cfg.pm_orderbook_workers), thread_name_prefix="pm_ob")

        while True:
            ts = utc_now_iso()
            tick_count += 1

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
                # Periodic best-effort reconcile via REST (when user-ws is enabled).
                if pm_live_client is not None and pm_position_store is not None and pm_position_store.should_reconcile(interval_s=float(cfg.pm_user_reconcile_interval_s)):
                    try:
                        # py-clob-client trade history API is not stable across versions; duck-typing.
                        if hasattr(pm_live_client, "get_trades"):
                            trades_any = pm_live_client.get_trades()  # type: ignore[attr-defined]
                            if isinstance(trades_any, list):
                                for t_any in cast(list[Any], trades_any):
                                    if isinstance(t_any, dict):
                                        fe = fill_from_loose_dict(cast(dict[str, Any], t_any))
                                        if fe:
                                            pm_position_store.apply_fill(fe)
                        pm_position_store.mark_reconciled()
                    except Exception as e:
                        pm_user_wss_status["reconcile_error"] = str(e)
                        pm_position_store.mark_reconciled()

                files = write_outputs(
                    cfg,
                    pm=pm,
                    kraken=kraken,
                    lead_lag_engine=lead_lag_engine,
                    pm_trend_engine=pm_trend_engine,
                    health_tracker=health_tracker,
                    latency_tracker=latency_tracker,
                    runtime_cache=runtime_cache,
                    pm_orderbook_executor=pm_exec,
                    pm_live_client=pm_live_client,
                    pm_live_error=pm_live_error,
                    pm_position_store=pm_position_store,
                    pm_user_wss_status=pm_user_wss_status,
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
                if cfg.upload_url and cfg.upload_api_key:
                    http_upload_files(cfg, files)
                else:
                    ftp_upload_files(cfg, files)
            except Exception as e:
                # Keep loop alive; surface error in live_status on next tick.
                print(f"[agent] upload failed: {type(e).__name__}: {e!r}", flush=True)

            if run_once:
                print("[agent] RUN_ONCE=1 -> exiting after single tick")
                return

            if run_ticks > 0 and tick_count >= run_ticks:
                print(f"[agent] RUN_TICKS={run_ticks} -> exiting after {tick_count} ticks")
                return

            # Drift stability: simple exponential backoff on repeated failures.
            sleep_s = float(cfg.interval_s)
            if consecutive_failures > 0:
                sleep_s = min(float(cfg.interval_s) * (2 ** min(consecutive_failures, 4)), 300.0)
            time.sleep(sleep_s)
    finally:
        try:
            if pm_exec is not None:
                pm_exec.shutdown(wait=False, cancel_futures=True)
        except Exception:
            pass

        try:
            if pm_user_wss is not None:
                pm_user_wss.stop(timeout_s=2.0)
        except Exception:
            pass


if __name__ == "__main__":
    main()
