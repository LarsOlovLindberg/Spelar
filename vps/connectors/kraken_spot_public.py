from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

import requests


@dataclass(frozen=True)
class KrakenSpotTick:
    ts: datetime
    pair: str
    last: float


class KrakenSpotPublic:
    """Minimal Kraken Spot public client.

    Uses: GET https://api.kraken.com/0/public/Ticker?pair=XBTUSD
    """

    BASE = "https://api.kraken.com/0/public"

    def __init__(self, *, base_url: str | None = None, timeout_s: float = 10.0) -> None:
        self.base_url = (base_url or self.BASE).rstrip("/")
        self.timeout_s = timeout_s
        self._sess = requests.Session()

    def now_utc(self) -> datetime:
        return datetime.now(timezone.utc)

    def get_ticker_last(self, *, pair: str) -> KrakenSpotTick:
        url = f"{self.base_url}/Ticker"
        r = self._sess.get(url, params={"pair": pair}, timeout=self.timeout_s)
        r.raise_for_status()
        data: Any = r.json()
        if isinstance(data, dict) and data.get("error"):
            raise RuntimeError(f"Kraken spot error: {data['error']}")
        result = data.get("result")
        if not isinstance(result, dict) or not result:
            raise RuntimeError(f"Unexpected Kraken spot response: {data}")
        key = next(iter(result.keys()))
        last_str = result[key]["c"][0]
        return KrakenSpotTick(ts=self.now_utc(), pair=pair, last=float(last_str))
