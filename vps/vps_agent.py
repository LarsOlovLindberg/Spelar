from __future__ import annotations

# pyright: reportUnusedImport=false, reportUnusedVariable=false, reportUnusedFunction=false

import csv
import json
import os
import time
from dataclasses import dataclass
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


@dataclass(frozen=True)
class Config:
    out_dir: Path
    interval_s: float

    # public endpoints (optional)
    polymarket_public_url: str | None
    kraken_public_url: str | None

    # Polymarket CLOB (public) inputs
    polymarket_clob_base_url: str
    polymarket_clob_token_id: str | None  # outcome token id

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

    polymarket_public_url = os.getenv("POLYMARKET_PUBLIC_URL") or None
    kraken_public_url = os.getenv("KRAKEN_PUBLIC_URL") or None

    polymarket_clob_base_url = (os.getenv("POLYMARKET_CLOB_BASE_URL", "https://clob.polymarket.com") or "https://clob.polymarket.com").rstrip("/")
    polymarket_clob_token_id = os.getenv("POLYMARKET_CLOB_TOKEN_ID") or None

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
        polymarket_public_url=polymarket_public_url,
        kraken_public_url=kraken_public_url,
        polymarket_clob_base_url=polymarket_clob_base_url,
        polymarket_clob_token_id=polymarket_clob_token_id,
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


def write_outputs(cfg: Config, *, pm: dict[str, Any] | None, kraken: dict[str, Any] | None) -> list[Path]:  # pyright: ignore[reportGeneralTypeIssues]
    ts = utc_now_iso()

    out = cfg.out_dir
    out.mkdir(parents=True, exist_ok=True)

    live_status: dict[str, Any] = {
        "ts": ts,
        "trading_mode": cfg.trading_mode,
        "killswitch": bool(killswitch_active(cfg)),
        "polymarket_public_url": cfg.polymarket_public_url,
        "kraken_public_url": cfg.kraken_public_url,
        "polymarket_clob_base_url": cfg.polymarket_clob_base_url,
        "polymarket_clob_token_id": cfg.polymarket_clob_token_id,
        "clob_depth_levels": cfg.clob_depth_levels,
        "poly_chain_id": cfg.poly_chain_id,
        "poly_signature_type": cfg.poly_signature_type,
        "poly_funder": cfg.poly_funder,
        "pm_order_size_shares": cfg.pm_order_size_shares,
        "pm_max_orders_per_tick": cfg.pm_max_orders_per_tick,
        "pm_odds_test_mode": bool(cfg.pm_odds_test_mode),
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

    # Portal-facing Polymarket status snapshot (lightweight, non-secret)
    p_pm_status = out / "polymarket_status.json"
    pm_status: dict[str, Any] = {
        "generated_at": ts,
        "service": "vps_agent",
        "ok": True,
        "polymarket_clob_base_url": cfg.polymarket_clob_base_url,
        "market_map_path": str(cfg.market_map_path) if cfg.market_map_path else None,
        "notes": [],
    }

    # Additional snapshots to make the jump to real-money easier later.
    # These are read-only/observability and do not place orders.
    sources_health: dict[str, Any] = {
        "generated_at": ts,
        "polymarket": {"clob": {}},
        "kraken": {"futures": {}},
        "options": {"deribit": {}},
    }

    # Prefer real prices if configured; otherwise fall back to stub.
    edge_rows: list[dict[str, Any]] = compute_edge_stub(ts=ts, pm=pm, kraken=kraken)
    p_edge = out / "edge_signals_live.csv"
    write_csv(
        p_edge,
        ["ts", "market", "fair_p", "pm_price", "edge", "sources", "notes"],
        [[r["ts"], r["market"], r["fair_p"], r["pm_price"], r["edge"], r["sources"], r["notes"]] for r in edge_rows],
    )
    files.append(p_edge)

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
        deribit = DeribitOptionsPublic()
        gamma = PolymarketGammaPublic()
        token_cache: dict[str, str] = {}

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
                        cache_key = market_ref
                        if cache_key in token_cache:
                            token_id = token_cache[cache_key]
                        else:
                            try:
                                gm = gamma.get_market_by_slug(slug=market_ref)
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

                                token_id = gamma.resolve_token_id(market=gm, desired_outcome=chosen)
                                token_cache[cache_key] = token_id
                                pm_status.setdefault("gamma_resolved", []).append(
                                    {
                                        "market": market_name,
                                        "slug": gm.slug,
                                        "question": gm.question,
                                        "outcomes": gm.outcomes,
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
        kf_t0 = _now_ms()
        kf_public = KrakenFuturesApi(testnet=cfg.kraken_futures_testnet)
        instruments = kf_public.get_instruments()
        tickers = kf_public.get_tickers()

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
            "generated_at": ts,
            "testnet": cfg.kraken_futures_testnet,
            "base_url": "https://demo-futures.kraken.com" if cfg.kraken_futures_testnet else "https://futures.kraken.com",
            "mapped_symbols": mapped_symbols,
            "instruments_count": len(instruments),
            "tickers_count": len(tickers),
            "tickers_by_symbol": tickers_by_symbol,
            "instruments": instruments,
            "tickers": tickers,
        }
        sources_health["kraken"]["futures"]["public"] = {"ok": True, "ms": _now_ms() - kf_t0}

        p_kf_pub = out / "kraken_futures_public.json"
        write_json(p_kf_pub, kraken_futures_public_snapshot)
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
            kf_priv_t0 = _now_ms()
            try:
                keys = load_kraken_keys(cfg.kraken_keys_path)
                kf_private = KrakenFuturesApi(keys=keys, testnet=cfg.kraken_futures_testnet)
                accounts = kf_private.get_accounts()
                open_positions = kf_private.get_openpositions()
                kraken_futures_private_snapshot.update(
                    {
                        "ok": True,
                        "error": None,
                        "accounts": accounts,
                        "open_positions": open_positions,
                    }
                )
                sources_health["kraken"]["futures"]["private"] = {"ok": True, "ms": _now_ms() - kf_priv_t0}
            except Exception as e:
                kraken_futures_private_snapshot["error"] = str(e)
                sources_health["kraken"]["futures"]["private"] = {"ok": False, "error": str(e)}
        else:
            sources_health["kraken"]["futures"]["private"] = {"ok": False, "error": "keys_missing"}

        write_json(p_kf_priv, kraken_futures_private_snapshot)
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
        for mkt in mkts:
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
                if bid is not None and ask is not None and bid > 0 and ask > 0:
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

    return files


def ftp_upload_files(cfg: Config, files: list[Path]) -> None:
    if not (cfg.ftp_host and cfg.ftp_user and cfg.ftp_pass):
        return

    # Upload only the portal-facing files (not raw debug)
    allow = {
        "live_status.json",
        "sources_health.json",
        "deribit_options_public.json",
        "polymarket_status.json",
        "polymarket_clob_public.json",
        "pm_open_orders.json",
        "pm_scanner_log.csv",
        "edge_signals_live.csv",
        "pm_orders.csv",
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

    run_once = (os.getenv("RUN_ONCE", "0") or "0").strip().lower() in {"1", "true", "yes"}

    print(f"[agent] out_dir={cfg.out_dir}")
    print(f"[agent] interval_s={cfg.interval_s}")
    print(f"[agent] trading_mode={cfg.trading_mode}")

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

        files = write_outputs(cfg, pm=pm, kraken=kraken)

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

        time.sleep(cfg.interval_s)


if __name__ == "__main__":
    main()
