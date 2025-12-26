from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class KrakenSnapshot:
    ts_iso: str
    raw: dict[str, Any]


def fetch_public_snapshot(*, base_url: str, timeout_s: float = 10.0) -> dict[str, Any]:
    """Fetch a minimal public snapshot.

    NOTE: This is intentionally generic because Kraken has multiple products
    (spot, futures/derivatives, options). Provide a URL that returns JSON.

    Example (you decide):
      - base_url = "https://api.kraken.com/0/public/Ticker?pair=XXBTZUSD"

    Returns parsed JSON as dict.
    """
    resp = requests.get(base_url, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        raise TypeError("Expected JSON object")
    return data
