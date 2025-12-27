"C:\Users\lars-\pm_spot_edge_bot\src\runner.py"
from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional

import requests

from .config import Settings
from .models import Tick
from .utils import parse_ts
from .edge_logic import EdgeTrader
from .spot_kraken import KrakenSpotClient
from .pm_gamma import PolymarketGammaClient
from .pm_clob import PolymarketCLOBClient

from tools.pm_orderbook_sizer import PolymarketOrderbookSizer

@dataclass(frozen=True)
class BacktestReport:
    trades: List
    final_open: bool

    def to_text(self) -> str:
        lines = []
        lines.append(f"Trades: {len(self.trades)}")
        if self.trades:
            pnls = [t.pnl_pct for t in self.trades]
            lines.append(f"Avg pnl%: {sum(pnls)/len(pnls):.3f}")
            lines.append(f"Win rate: {sum(1 for p in pnls if p>0)/len(pnls)*100:.1f}%")
            lines.append(f"Min pnl%: {min(pnls):.3f}")
            lines.append(f"Max pnl%: {max(pnls):.3f}")
            lines.append("Last 5 trades:")
            for t in self.trades[-5:]:
                lines.append(f"  {t.side} {t.entry_ts.isoformat()} -> {t.exit_ts.isoformat()} pnl={t.pnl_pct:.3f}%")
        lines.append(f"Open position at end: {self.final_open}")
        return "\n".join(lines)

class BacktestRunner:
    def __init__(self, settings: Settings, spot_csv: str, pm_csv: str) -> None:
        self.s = settings
        self.spot_csv = spot_csv
        self.pm_csv = pm_csv

    def run(self) -> BacktestReport:
        spot = self._load_ticks(self.spot_csv)
        pm = self._load_ticks(self.pm_csv)
        # align by index (simple) - you should align by timestamp in real data
        n = min(len(spot), len(pm))
        spot = spot[:n]
        pm = pm[:n]

        trader = EdgeTrader(
            side=self.s.pm_side,
            lookback_points=self.s.lookback_points,
            spot_move_min_pct=self.s.spot_move_min_pct,
            edge_min_pct=self.s.edge_min_pct,
            edge_exit_pct=self.s.edge_exit_pct,
            max_hold_secs=self.s.max_hold_secs,
            pm_stop_pct=self.s.pm_stop_pct,
            avoid_price_above=self.s.avoid_price_above,
            avoid_price_below=self.s.avoid_price_below,
        )

        for i in range(n):
            trader.on_tick(spot[i].ts, spot[i].price, pm[i].price)

        return BacktestReport(trades=trader.trades, final_open=trader.position is not None)

    @staticmethod
    def _load_ticks(path: str) -> List[Tick]:
        out: List[Tick] = []
        with open(path, "r", encoding="utf-8") as f:
            r = csv.DictReader(f)
            for row in r:
                out.append(Tick(ts=parse_ts(row["ts_iso"]), price=float(row["price"])))
        return out

class LiveRunner:
    def __init__(self, settings: Settings) -> None:
        if not settings.pm_market_slug:
            raise RuntimeError("PM_MARKET_SLUG is required for live mode.")
        self.s = settings
        self.session = requests.Session()
        self.kraken = KrakenSpotClient(session=self.session)
        self.gamma = PolymarketGammaClient(session=self.session)
        self.clob = PolymarketCLOBClient(session=self.session)
        self.sizer = PolymarketOrderbookSizer(session=self.session)

        market = self.gamma.fetch_market_by_slug(self.s.pm_market_slug)
        yes_id, no_id = self.gamma.extract_yes_no_token_ids(market)
        self.yes_token_id = yes_id
        self.no_token_id = no_id

        self.trader = EdgeTrader(
            side=self.s.pm_side,
            lookback_points=self.s.lookback_points,
            spot_move_min_pct=self.s.spot_move_min_pct,
            edge_min_pct=self.s.edge_min_pct,
            edge_exit_pct=self.s.edge_exit_pct,
            max_hold_secs=self.s.max_hold_secs,
            pm_stop_pct=self.s.pm_stop_pct,
            avoid_price_above=self.s.avoid_price_above,
            avoid_price_below=self.s.avoid_price_below,
        )

        print(f"Resolved PM tokens: YES={self.yes_token_id} NO={self.no_token_id}")

    def step(self) -> None:
        ts = self.kraken.now_utc()
        spot = self.kraken.get_ticker_price(self.s.kraken_pair)

        token_id = self.yes_token_id if self.s.pm_side == "YES" else self.no_token_id
        # Orderbook-aware sizing: compute max USDC you should use for THIS tick
        suggested_max_usdc = None
        suggested_max_shares = None
        if self.s.enable_orderbook_sizing:
            try:
                res = self.sizer.size_max_trade(
                    token_id=token_id,
                    side="BUY",
                    slippage_cap=self.s.slippage_cap,
                    max_fraction_of_band_liquidity=self.s.max_fraction_of_band_liquidity,
                    hard_cap_usdc=self.s.hard_cap_usdc,
                )
                suggested_max_usdc = res.suggested_max_usdc
                suggested_max_shares = res.suggested_max_shares
            except Exception:
                suggested_max_usdc = self.s.hard_cap_usdc
                suggested_max_shares = None
        # BUY price is what you'd pay to enter; SELL price approximates exit; we track BUY mid-like proxy
        pm_price_buy = self.clob.get_price(token_id=token_id, side="BUY")
        pm_price_sell = self.clob.get_price(token_id=token_id, side="SELL")
        pm_price = (pm_price_buy + pm_price_sell) / 2.0

        trade = self.trader.on_tick(ts, spot, pm_price)
        pos = self.trader.position
        sizing = ""
        if suggested_max_usdc is not None:
            sizing = f" max_usdc~{suggested_max_usdc:.0f}"
            if suggested_max_shares is not None:
                sizing += f" max_shares~{suggested_max_shares:.0f}"
        msg = f"{ts.isoformat()} spot={spot:.2f} pm={pm_price:.4f} (buy={pm_price_buy:.4f} sell={pm_price_sell:.4f}){sizing}"
        if pos is None:
            print(msg + " state=FLAT")
        else:
            hold = (ts - pos.entry_ts).total_seconds()
            unreal = (pm_price / pos.entry_price - 1.0) * 100.0
            print(msg + f" state=IN side={pos.side} hold={hold:.0f}s unreal={unreal:.3f}%")
        if trade is not None:
            print(f"EXIT trade pnl={trade.pnl_pct:.3f}%")
