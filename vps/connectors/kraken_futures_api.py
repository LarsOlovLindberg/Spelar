from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import time
import urllib.parse
from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class KrakenFuturesKeys:
    api_key: str
    api_secret: str


class KrakenFuturesApi:
    """Minimal Kraken Futures (derivatives) API client.

    Paper-first intent:
    - Safe to use for public endpoints without keys.
    - Private endpoints require APIKey + Authent signature + Nonce.

    Signature scheme intentionally mirrors the proven Markov implementation.
    """

    def __init__(
        self,
        *,
        keys: KrakenFuturesKeys | None = None,
        testnet: bool = False,
        timeout_s: float = 30.0,
        session: requests.Session | None = None,
    ) -> None:
        self._keys = keys
        self._timeout_s = timeout_s
        self._session = session or requests.Session()
        self._base_url = "https://demo-futures.kraken.com" if testnet else "https://futures.kraken.com"

    @staticmethod
    def _b64decode_secret(secret: str) -> bytes:
        cleaned = "".join(str(secret).split())
        if not cleaned:
            raise ValueError("Empty Kraken Futures API secret")

        # Common mistake: hex string instead of base64
        if cleaned.lower().startswith("0x") and all(c in "0123456789abcdefABCDEF" for c in cleaned[2:]):
            raise ValueError(
                "Kraken Futures API secret looks like a hex string (0x...). "
                "Futures secrets are Base64 strings from Kraken; re-copy the correct secret."
            )

        cleaned += "=" * (-len(cleaned) % 4)
        try:
            return base64.b64decode(cleaned, validate=True)
        except (binascii.Error, ValueError):
            try:
                return base64.urlsafe_b64decode(cleaned)
            except (binascii.Error, ValueError) as e:
                raise ValueError(
                    "Invalid Kraken Futures API secret format. Expected Base64 (often ends with '='). "
                    "Re-copy the Futures API Secret from Kraken and paste it without extra characters."
                ) from e

    def _sign(self, *, endpoint: str, postdata: str, nonce: str) -> str:
        # Official SDK behavior: strip /derivatives prefix
        endpoint_clean = endpoint
        if endpoint_clean.startswith("/derivatives"):
            endpoint_clean = endpoint_clean[len("/derivatives") :]

        message = f"{postdata}{nonce}{endpoint_clean}".encode("utf-8")
        sha256_hash = hashlib.sha256(message).digest()

        if not self._keys:
            raise RuntimeError("Private request requires keys")

        secret = self._b64decode_secret(self._keys.api_secret)
        signature = hmac.new(secret, sha256_hash, hashlib.sha512).digest()
        return base64.b64encode(signature).decode("ascii")

    def _request(
        self,
        *,
        method: str,
        endpoint: str,
        params: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        private: bool = False,
    ) -> dict[str, Any]:
        url = f"{self._base_url}{endpoint}"
        headers: dict[str, str] = {}
        postdata = ""

        if private:
            if not self._keys:
                raise RuntimeError("Private request requires keys")

            nonce = str(int(time.time() * 1000))
            if data:
                postdata = urllib.parse.urlencode(data)
            authent = self._sign(endpoint=endpoint, postdata=postdata, nonce=nonce)
            headers = {
                "APIKey": self._keys.api_key,
                "Nonce": nonce,
                "Authent": authent,
            }

        try:
            if method.upper() == "GET":
                if private and params:
                    query = urllib.parse.urlencode(params)
                    resp = self._session.get(f"{url}?{query}", headers=headers, timeout=self._timeout_s)
                else:
                    resp = self._session.get(url, params=params, headers=headers, timeout=self._timeout_s)
            elif method.upper() == "PUT":
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                resp = self._session.put(url, data=postdata, headers=headers, timeout=self._timeout_s)
            else:
                headers["Content-Type"] = "application/x-www-form-urlencoded"
                resp = self._session.post(url, data=postdata, headers=headers, timeout=self._timeout_s)

            resp.raise_for_status()
            result: dict[str, Any] = resp.json()

            if result.get("result") == "error":
                raise RuntimeError(f"Kraken Futures API error: {result.get('error', 'Unknown error')}")

            return result
        except requests.RequestException as e:
            raise RuntimeError(f"Kraken Futures request failed: {e}") from e

    # Public
    def get_tickers(self) -> list[dict[str, Any]]:
        result = self._request(method="GET", endpoint="/derivatives/api/v3/tickers")
        return list(result.get("tickers", []) or [])

    def get_ticker(self, symbol: str) -> dict[str, Any]:
        tickers = self.get_tickers()
        for t in tickers:
            if t.get("symbol") == symbol:
                return t
        return {}

    def get_instruments(self) -> list[dict[str, Any]]:
        result = self._request(method="GET", endpoint="/derivatives/api/v3/instruments")
        return list(result.get("instruments", []) or [])

    # Private
    def get_accounts(self) -> dict[str, Any]:
        result = self._request(method="GET", endpoint="/derivatives/api/v3/accounts", private=True)
        return dict(result.get("accounts", {}) or {})

    def get_openpositions(self) -> list[dict[str, Any]]:
        result = self._request(method="GET", endpoint="/derivatives/api/v3/openpositions", private=True)
        return list(result.get("openPositions", []) or [])
