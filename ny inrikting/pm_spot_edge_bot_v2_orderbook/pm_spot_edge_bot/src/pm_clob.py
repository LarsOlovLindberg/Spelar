"C:\Users\lars-\pm_spot_edge_bot\src\pm_clob.py"
from __future__ import annotations

import requests
from typing import Optional

class PolymarketCLOBClient:
    """
    CLOB Pricing endpoint:
      GET https://clob.polymarket.com/price?token_id=<id>&side=BUY|SELL
    Docs: https://docs.polymarket.com/api-reference/pricing/get-market-price
    """
    BASE = "https://clob.polymarket.com"

    def __init__(self, session: Optional[requests.Session] = None, timeout: float = 10.0) -> None:
        self.sess = session or requests.Session()
        self.timeout = timeout

    def get_price(self, token_id: str, side: str = "BUY") -> float:
        url = f"{self.BASE}/price"
        r = self.sess.get(url, params={"token_id": token_id, "side": side.upper()}, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        # docs show {"price": "0.1234"} (string)
        if "price" not in data:
            raise RuntimeError(f"Unexpected CLOB /price response: {data}")
        return float(data["price"])
