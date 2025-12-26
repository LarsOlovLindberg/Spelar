from __future__ import annotations

from typing import Any, cast

import requests


class PolymarketClobPublic:
    """Minimal public Polymarket CLOB client.

    This intentionally avoids auth/signing so it can run safely in paper mode.
    You provide the base URL (default points at the public CLOB API).
    """

    def __init__(self, *, base_url: str = "https://clob.polymarket.com", timeout_s: float = 10.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s

    def _get(self, path: str, *, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        resp = requests.get(url, params=params, timeout=self._timeout_s)
        resp.raise_for_status()
        return resp.json()

    def get_market(self, market_id: str) -> dict[str, Any]:
        data = self._get(f"/markets/{market_id}")
        if isinstance(data, dict):
            return cast(dict[str, Any], data)
        return {"data": data}

    def get_orderbook(self, token_id: str) -> dict[str, Any]:
        # token_id is the outcome token identifier used by the CLOB
        data = self._get("/book", params={"token_id": token_id})
        if isinstance(data, dict):
            return cast(dict[str, Any], data)
        return {"data": data}


def best_bid_ask(orderbook: dict[str, Any]) -> tuple[float | None, float | None]:
    """Extract best bid/ask from CLOB /book response if present."""

    bids = orderbook.get("bids")
    asks = orderbook.get("asks")

    def _best(side: Any) -> float | None:
        if not isinstance(side, list):
            return None
        side_list = cast(list[Any], side)
        if len(side_list) == 0:
            return None

        best_any: Any = side_list[0]
        if not isinstance(best_any, dict):
            return None
        best = cast(dict[str, Any], best_any)
        for k in ("price", "p"):
            if k in best:
                try:
                    return float(best[k])
                except Exception:
                    return None
        return None

    bid = _best(bids)
    ask = _best(asks)
    return bid, ask
