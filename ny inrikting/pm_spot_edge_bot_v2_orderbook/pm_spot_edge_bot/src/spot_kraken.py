"C:\Users\lars-\pm_spot_edge_bot\src\spot_kraken.py"
from __future__ import annotations

import requests
from datetime import datetime, timezone
from typing import Optional

class KrakenSpotClient:
    BASE = "https://api.kraken.com/0/public"

    def __init__(self, session: Optional[requests.Session] = None, timeout: float = 10.0) -> None:
        self.sess = session or requests.Session()
        self.timeout = timeout

    def get_ticker_price(self, pair: str) -> float:
        # Kraken doc: GET /0/public/Ticker?pair=XBTUSD
        url = f"{self.BASE}/Ticker"
        r = self.sess.get(url, params={"pair": pair}, timeout=self.timeout)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise RuntimeError(f"Kraken error: {data['error']}")
        result = data["result"]
        # result key is not always equal to pair; take first
        key = next(iter(result.keys()))
        last_trade = result[key]["c"][0]
        return float(last_trade)

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)
