"C:\Users\lars-\pm_spot_edge_bot\tools\pm_orderbook_sizer.py"
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Tuple
import requests

Side = Literal["BUY", "SELL"]


@dataclass(frozen=True)
class BookLevel:
    price: float  # dollars per share (0..1)
    size: float   # shares


@dataclass(frozen=True)
class Book:
    bids: List[BookLevel]
    asks: List[BookLevel]
    min_order_size: Optional[float] = None
    tick_size: Optional[float] = None


@dataclass(frozen=True)
class SizingResult:
    best_price: float
    band_limit_price: float
    liquidity_shares_in_band: float
    liquidity_usdc_in_band: float
    suggested_max_usdc: float
    suggested_max_shares: float


class PolymarketOrderbookSizer:
    \"\"\"Reads Polymarket CLOB orderbook and proposes a max trade size.

    Endpoint:
      GET https://clob.polymarket.com/book?token_id=<id>
    \"\"\"
    BASE = "https://clob.polymarket.com"

    def __init__(self, timeout: float = 10.0, session: Optional[requests.Session] = None) -> None:
        self.sess = session or requests.Session()
        self.timeout = timeout

    def get_book(self, token_id: str) -> Book:
        url = f"{self.BASE}/book"
        r = self.sess.get(url, params={"token_id": token_id}, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()

        bids = self._parse_levels(data.get("bids", []))
        asks = self._parse_levels(data.get("asks", []))

        return Book(
            bids=bids,
            asks=asks,
            min_order_size=self._safe_float(data.get("min_order_size")),
            tick_size=self._safe_float(data.get("tick_size")),
        )

    def size_max_trade(
        self,
        token_id: str,
        side: Side,
        slippage_cap: float = 0.01,
        max_fraction_of_band_liquidity: float = 0.10,
        hard_cap_usdc: float = 2000.0,
    ) -> SizingResult:
        \"\"\"Compute how much you can trade without exceeding your slippage band.

        BUY: consume asks in [best_ask, best_ask + slippage_cap]
        SELL: consume bids in [best_bid - slippage_cap, best_bid]
        \"\"\"
        book = self.get_book(token_id)

        if side == "BUY":
            if not book.asks:
                raise RuntimeError("No asks in book (cannot BUY).")
            best = book.asks[0].price
            limit = best + slippage_cap
            in_band = [lv for lv in book.asks if lv.price <= limit]
        else:
            if not book.bids:
                raise RuntimeError("No bids in book (cannot SELL).")
            best = book.bids[0].price
            limit = max(0.0, best - slippage_cap)
            in_band = [lv for lv in book.bids if lv.price >= limit]

        liq_shares, liq_usdc = self._sum_usdc(in_band)

        suggested_max_usdc = min(hard_cap_usdc, liq_usdc * max_fraction_of_band_liquidity)
        suggested_shares = 0.0 if best <= 0 else suggested_max_usdc / best

        if book.min_order_size is not None and suggested_shares < book.min_order_size:
            if liq_shares >= book.min_order_size and (book.min_order_size * best) <= hard_cap_usdc:
                suggested_shares = book.min_order_size
                suggested_max_usdc = suggested_shares * best

        return SizingResult(
            best_price=best,
            band_limit_price=limit,
            liquidity_shares_in_band=liq_shares,
            liquidity_usdc_in_band=liq_usdc,
            suggested_max_usdc=suggested_max_usdc,
            suggested_max_shares=suggested_shares,
        )

    @staticmethod
    def _parse_levels(levels: List[Dict[str, Any]]) -> List[BookLevel]:
        out: List[BookLevel] = []
        for lv in levels:
            p = PolymarketOrderbookSizer._safe_float(lv.get("price"))
            s = PolymarketOrderbookSizer._safe_float(lv.get("size"))
            if p is None or s is None:
                continue
            out.append(BookLevel(price=p, size=s))
        return out

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        try:
            return None if v is None else float(v)
        except Exception:
            return None

    @staticmethod
    def _sum_usdc(levels: List[BookLevel]) -> Tuple[float, float]:
        shares = sum(l.size for l in levels)
        usdc = sum(l.price * l.size for l in levels)
        return shares, usdc


def _demo() -> None:
    import sys
    if len(sys.argv) < 3:
        print("Usage: python pm_orderbook_sizer.py <token_id> BUY|SELL")
        return
    token_id = sys.argv[1]
    side = sys.argv[2].upper()
    if side not in ("BUY", "SELL"):
        raise ValueError("side must be BUY or SELL")

    sizer = PolymarketOrderbookSizer()
    res = sizer.size_max_trade(token_id=token_id, side=side)  # type: ignore[arg-type]
    print(res)


if __name__ == "__main__":
    _demo()
