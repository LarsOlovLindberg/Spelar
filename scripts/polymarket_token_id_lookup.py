from __future__ import annotations

import argparse
import json
import re
from typing import Any, cast

import requests


def _guess_slug(s: str) -> str:
    s = (s or "").strip()
    if not s:
        raise SystemExit("Missing input. Provide a Polymarket market URL or slug.")

    if s.startswith("http://") or s.startswith("https://"):
        # Typical URLs look like https://polymarket.com/market/<slug>
        s = re.sub(r"[?#].*$", "", s)  # drop query/hash
        parts = [p for p in s.split("/") if p]
        if not parts:
            raise SystemExit("Could not parse URL")
        slug = parts[-1]
        if slug in {"market", "markets", "event"} and len(parts) >= 2:
            slug = parts[-2]
        return slug

    return s


def _fetch_json(url: str) -> Any:
    r = requests.get(url, timeout=20)
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code} for {url}")
    return r.json()


def _extract_market_obj(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return cast(dict[str, Any], obj)
    if isinstance(obj, list):
        if not obj:
            raise ValueError("Empty list")
        first: Any = cast(list[Any], obj)[0]
        if isinstance(first, dict):
            return cast(dict[str, Any], first)
    raise ValueError("Unexpected JSON shape")


def main() -> None:
    ap = argparse.ArgumentParser(description="Lookup Polymarket CLOB outcome token ids (YES/NO) for a market.")
    ap.add_argument("market", help="Polymarket market URL or slug")
    ap.add_argument("--gamma", default="https://gamma-api.polymarket.com", help="Gamma API base URL")
    ap.add_argument("--pretty", action="store_true", help="Pretty-print raw market JSON")
    args = ap.parse_args()

    slug = _guess_slug(args.market)

    # We try a couple of common Gamma query shapes. If Polymarket changes this API,
    # the script will fail loudly and tell you what URL it tried.
    candidates = [
        f"{args.gamma.rstrip('/')}/markets?slug={slug}",
        f"{args.gamma.rstrip('/')}/markets?limit=1&slug={slug}",
        f"{args.gamma.rstrip('/')}/markets?search={slug}",
    ]

    last_err: Exception | None = None
    data: Any | None = None
    used_url: str | None = None
    for url in candidates:
        try:
            data = _fetch_json(url)
            used_url = url
            break
        except Exception as e:
            last_err = e

    if data is None:
        raise SystemExit(f"Could not fetch market metadata for slug '{slug}'. Last error: {last_err}")

    market = _extract_market_obj(data)

    # The fields below are what we want; names can vary.
    clob_token_ids = market.get("clobTokenIds") or market.get("clob_token_ids") or market.get("clob_token_id")
    outcomes = market.get("outcomes") or market.get("outcomeNames") or market.get("outcome_names")

    print(f"slug: {slug}")
    if used_url:
        print(f"gamma_url: {used_url}")

    if outcomes is not None:
        print(f"outcomes: {outcomes}")
    if clob_token_ids is not None:
        print(f"clobTokenIds: {clob_token_ids}")

    # Helpful fallback: show keys when fields are not found.
    if clob_token_ids is None:
        print("NOTE: Could not find clobTokenIds in response. Top-level keys:")
        print(sorted(market.keys()))

    if args.pretty:
        print("\nRAW MARKET JSON:")
        print(json.dumps(market, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
