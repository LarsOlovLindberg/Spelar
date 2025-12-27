"C:\Users\lars-\pm_spot_edge_bot\src\pm_gamma.py"
from __future__ import annotations

import requests
from typing import Dict, Optional, Tuple

class PolymarketGammaClient:
    """
    Gamma Markets API provides market metadata (including tokens).
    Docs: https://docs.polymarket.com/developers/gamma-markets-api/overview
    Base: https://gamma-api.polymarket.com/
    """
    BASE = "https://gamma-api.polymarket.com"

    def __init__(self, session: Optional[requests.Session] = None, timeout: float = 10.0) -> None:
        self.sess = session or requests.Session()
        self.timeout = timeout

    def fetch_market_by_slug(self, slug: str) -> Dict:
        # Typical gamma endpoint: GET /markets?slug=<slug>
        # Gamma is documented but exact response fields can vary per market type.
        url = f"{self.BASE}/markets"
        r = self.sess.get(url, params={"slug": slug}, timeout=self.timeout)
        r.raise_for_status()
        arr = r.json()
        if not isinstance(arr, list) or len(arr) == 0:
            raise RuntimeError(f"No market found for slug={slug!r}")
        return arr[0]

    @staticmethod
    def extract_yes_no_token_ids(market: Dict) -> Tuple[str, str]:
        """
        Attempts to find YES/NO token ids from Gamma response.

        Commonly, markets include a `tokens` list with objects like:
          { "token_id": "...", "outcome": "Yes" } and { "token_id": "...", "outcome": "No" }

        If your market uses different fields, adapt this mapping.
        """
        tokens = market.get("tokens") or market.get("outcomes") or []
        yes_id = None
        no_id = None
        for t in tokens:
            outcome = (t.get("outcome") or t.get("name") or "").strip().lower()
            tid = t.get("token_id") or t.get("tokenId") or t.get("id")
            if not tid:
                continue
            if outcome in ("yes", "y"):
                yes_id = str(tid)
            elif outcome in ("no", "n"):
                no_id = str(tid)
        if not yes_id or not no_id:
            raise RuntimeError("Could not extract YES/NO token_ids from Gamma response. Inspect market JSON and adapt extract_yes_no_token_ids().")
        return yes_id, no_id
