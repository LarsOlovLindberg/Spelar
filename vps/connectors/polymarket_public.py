from __future__ import annotations

from typing import Any

import requests


def fetch_public_snapshot(*, base_url: str, timeout_s: float = 10.0) -> dict[str, Any]:
    """Fetch a minimal public Polymarket snapshot.

    Polymarket has multiple APIs (market metadata, prices, etc). This stays
    generic: point base_url at a JSON endpoint you want to ingest.

    Returns parsed JSON as dict.
    """
    resp = requests.get(base_url, timeout=timeout_s)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, dict):
        # Some endpoints return arrays; wrap to keep downstream stable.
        return {"data": data}
    return data
