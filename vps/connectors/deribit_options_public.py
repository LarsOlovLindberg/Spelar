from __future__ import annotations

import math
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, cast

import requests


@dataclass(frozen=True)
class DeribitOptionRef:
    instrument_name: str
    currency: str
    strike: float
    expiration_timestamp_ms: int
    option_type: str  # call|put


def _utc_now_ms() -> int:
    return int(time.time() * 1000)


def _norm_cdf(x: float) -> float:
    # Standard normal CDF via erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def risk_neutral_prob_above_strike(*, forward: float, strike: float, sigma: float, t_years: float) -> float:
    """Risk-neutral probability P(S_T > K) under lognormal with constant vol.

    Uses d2 from Black-Scholes with forward price F.
    """

    if forward <= 0 or strike <= 0:
        raise ValueError("forward and strike must be > 0")
    if sigma <= 0:
        raise ValueError("sigma must be > 0")
    if t_years <= 0:
        raise ValueError("t_years must be > 0")

    denom = sigma * math.sqrt(t_years)
    d2 = (math.log(forward / strike) - 0.5 * sigma * sigma * t_years) / denom
    return float(_norm_cdf(d2))


def risk_neutral_prob_touch_above_strike(*, spot: float, barrier: float, sigma: float, t_years: float, drift: float = 0.0) -> float:
    """Risk-neutral probability that price *touches* an upper barrier before expiry.

    Models log-price as Brownian motion with drift:
      X_t = ln(S_t) = ln(S0) + (drift - 0.5*sigma^2) t + sigma W_t

    Using the reflection principle for Brownian motion with drift, for a = ln(B/S0) > 0:
      P(max_{t<=T} X_t >= ln B) = Phi(-(a - mT)/(sigma*sqrt(T))) + exp(2 m a / sigma^2) * Phi(-(a + mT)/(sigma*sqrt(T)))
    where m = drift - 0.5*sigma^2.

    Note: drift defaults to 0.0 (reasonable short-horizon approximation when rates are small).
    """

    if spot <= 0 or barrier <= 0:
        raise ValueError("spot and barrier must be > 0")
    if sigma <= 0:
        raise ValueError("sigma must be > 0")
    if t_years <= 0:
        raise ValueError("t_years must be > 0")

    if spot >= barrier:
        return 1.0

    a = math.log(barrier / spot)
    m = float(drift) - 0.5 * sigma * sigma
    denom = sigma * math.sqrt(t_years)

    z1 = -(a - m * t_years) / denom
    z2 = -(a + m * t_years) / denom

    term1 = _norm_cdf(z1)
    term2 = math.exp((2.0 * m * a) / (sigma * sigma)) * _norm_cdf(z2)
    p = float(term1 + term2)
    # Numerical guard
    return max(0.0, min(1.0, p))


def risk_neutral_prob_touch_below_strike(*, spot: float, barrier: float, sigma: float, t_years: float, drift: float = 0.0) -> float:
    """Risk-neutral probability that price *touches* a lower barrier before expiry.

    For B < S0, this computes:
      P(min_{t<=T} S_t <= B)

    Uses reflection principle on log-price, analogous to the upper-barrier formula.
    """

    if spot <= 0 or barrier <= 0:
        raise ValueError("spot and barrier must be > 0")
    if sigma <= 0:
        raise ValueError("sigma must be > 0")
    if t_years <= 0:
        raise ValueError("t_years must be > 0")

    if spot <= barrier:
        return 1.0

    a = math.log(spot / barrier)
    m = float(drift) - 0.5 * sigma * sigma
    denom = sigma * math.sqrt(t_years)

    # Derived by applying the upper-barrier formula to Z_t = ln(S0) - ln(S_t)
    # which has drift -m and barrier a.
    z1 = -(a + m * t_years) / denom
    z2 = -(a - m * t_years) / denom

    term1 = _norm_cdf(z1)
    term2 = math.exp((-2.0 * m * a) / (sigma * sigma)) * _norm_cdf(z2)
    p = float(term1 + term2)
    return max(0.0, min(1.0, p))


def _parse_iso_utc_to_ms(iso_utc: str) -> int:
    s = (iso_utc or "").strip()
    if not s:
        raise ValueError("expiry_iso is empty")
    # Accept Z suffix
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


class DeribitOptionsPublic:
    def __init__(self, *, base_url: str = "https://www.deribit.com/api/v2", timeout_s: float = 30.0, session: requests.Session | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._session = session or requests.Session()
        self._instruments_cache: dict[str, list[dict[str, Any]]] = {}

    def _get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        r = self._session.get(url, params=params or {}, timeout=self._timeout_s)
        r.raise_for_status()
        j_any: Any = r.json()
        if not isinstance(j_any, dict) or "result" not in j_any:
            raise ValueError("Unexpected Deribit response")
        j = cast(dict[str, Any], j_any)
        return j["result"]

    def list_option_instruments(self, *, currency: str) -> list[dict[str, Any]]:
        cur = currency.strip().upper()
        if cur in self._instruments_cache:
            return self._instruments_cache[cur]
        res_any = self._get(
            "/public/get_instruments",
            params={"currency": cur, "kind": "option", "expired": "false"},
        )
        if not isinstance(res_any, list):
            raise ValueError("Deribit get_instruments returned non-list")
        res_list = cast(list[Any], res_any)
        out: list[dict[str, Any]] = [cast(dict[str, Any], x) for x in res_list if isinstance(x, dict)]
        self._instruments_cache[cur] = out
        return out

    def get_book_summary_by_instrument(self, *, instrument_name: str) -> dict[str, Any]:
        res_any = self._get("/public/get_book_summary_by_instrument", params={"instrument_name": instrument_name})
        if not isinstance(res_any, list) or not res_any:
            raise ValueError("Deribit get_book_summary_by_instrument returned empty")
        res_list = cast(list[Any], res_any)
        first_any: Any = res_list[0]
        if not isinstance(first_any, dict):
            raise ValueError("Deribit book summary returned non-object")
        return cast(dict[str, Any], first_any)

    def find_option(self, *, currency: str, strike: float, expiry_ms: int, option_type: str = "call") -> DeribitOptionRef:
        instruments = self.list_option_instruments(currency=currency)

        want_type = option_type.strip().lower()
        if want_type not in {"call", "put"}:
            raise ValueError("option_type must be call or put")

        best: dict[str, Any] | None = None
        best_score: tuple[float, float] | None = None

        for ins in instruments:
            try:
                if str(ins.get("option_type") or "").strip().lower() != want_type:
                    continue
                strike_any = ins.get("strike")
                exp_any = ins.get("expiration_timestamp")
                if strike_any is None or exp_any is None:
                    continue
                ins_strike = float(strike_any)
                ins_exp = int(exp_any)
                # Score: (expiry distance, strike distance)
                score = (abs(ins_exp - expiry_ms), abs(ins_strike - float(strike)))
                if best_score is None or score < best_score:
                    best = ins
                    best_score = score
            except Exception:
                continue

        if best is None:
            raise ValueError("No matching Deribit option instruments found")

        return DeribitOptionRef(
            instrument_name=str(best.get("instrument_name") or ""),
            currency=str(best.get("currency") or currency).upper(),
            strike=float(best.get("strike") or strike),
            expiration_timestamp_ms=int(best.get("expiration_timestamp") or expiry_ms),
            option_type=str(best.get("option_type") or want_type),
        )

    def compute_rn_probability_from_model(self, *, model: dict[str, Any]) -> dict[str, Any]:
        """Compute risk-neutral probability from a simple model dict.

        Expected keys:
        - currency: BTC|ETH
        - strike: number
        - expiry_iso: ISO string (UTC) OR expiry_ts_ms: int
        - instrument_name: optional Deribit instrument override
        - direction: above|below (default above)
        """

        currency = str(model.get("currency") or "BTC").strip().upper()
        strike_any = model.get("strike")
        if strike_any is None:
            raise ValueError("deribit_rn model requires 'strike'")
        strike = float(strike_any)

        expiry_ms: int
        if "expiry_ts_ms" in model:
            expiry_any = model.get("expiry_ts_ms")
            if expiry_any is None:
                raise ValueError("expiry_ts_ms is None")
            expiry_ms = int(expiry_any)
        else:
            expiry_ms = _parse_iso_utc_to_ms(str(model.get("expiry_iso") or ""))

        direction = str(model.get("direction") or "above").strip().lower()
        if direction not in {"above", "below"}:
            raise ValueError("direction must be above or below")

        instrument_name = str(model.get("instrument_name") or "").strip() or None
        ref: DeribitOptionRef
        if instrument_name:
            # We can still pull summary; strike/expiry are for the RN math.
            ref = DeribitOptionRef(
                instrument_name=instrument_name,
                currency=currency,
                strike=strike,
                expiration_timestamp_ms=expiry_ms,
                option_type="call",
            )
        else:
            ref = self.find_option(currency=currency, strike=strike, expiry_ms=expiry_ms, option_type="call")

        summary = self.get_book_summary_by_instrument(instrument_name=ref.instrument_name)

        forward = float(summary.get("underlying_price") or summary.get("underlying_index") or summary.get("index_price") or 0.0)
        mark_iv = summary.get("mark_iv")
        if mark_iv is None:
            raise ValueError("Deribit summary missing mark_iv")
        iv = float(mark_iv)
        # mark_iv is typically in percent (e.g. 55.2) -> convert to decimal
        sigma = iv / 100.0 if iv > 3.0 else iv

        now_ms = _utc_now_ms()
        t_years = max((expiry_ms - now_ms) / 1000.0 / (365.0 * 24.0 * 3600.0), 1e-6)

        p_above = risk_neutral_prob_above_strike(forward=forward, strike=strike, sigma=sigma, t_years=t_years)
        p = p_above if direction == "above" else 1.0 - p_above

        return {
            "ok": True,
            "currency": currency,
            "direction": direction,
            "strike": strike,
            "expiry_ts_ms": expiry_ms,
            "instrument_name": ref.instrument_name,
            "forward": forward,
            "mark_iv": iv,
            "sigma": sigma,
            "t_years": t_years,
            "rn_prob": p,
            "rn_prob_above": p_above,
            "summary": {
                # keep it small and portal-safe
                "mark_price": summary.get("mark_price"),
                "bid_price": summary.get("bid_price"),
                "ask_price": summary.get("ask_price"),
                "underlying_price": summary.get("underlying_price"),
                "mark_iv": summary.get("mark_iv"),
                "volume": summary.get("volume"),
                "open_interest": summary.get("open_interest"),
            },
        }

    def compute_touch_probability_from_model(self, *, model: dict[str, Any]) -> dict[str, Any]:
        """Compute touch/no-touch probability for an upper/lower barrier using Deribit mark IV.

        Expected keys:
        - currency: BTC|ETH
        - barrier (or strike): number
        - expiry_iso: ISO string (UTC) OR expiry_ts_ms: int
        - instrument_name: optional Deribit instrument override used to source IV
        - direction: touch_above | no_touch_above | touch_below | no_touch_below (default touch_above)
        - drift: optional (annualized) drift in log-space model; default 0.0
        """

        currency = str(model.get("currency") or "BTC").strip().upper()
        barrier_any = model.get("barrier")
        if barrier_any is None:
            barrier_any = model.get("strike")
        if barrier_any is None:
            raise ValueError("deribit_touch model requires 'barrier' (or 'strike')")
        barrier = float(barrier_any)

        expiry_ms: int
        if "expiry_ts_ms" in model:
            expiry_any = model.get("expiry_ts_ms")
            if expiry_any is None:
                raise ValueError("expiry_ts_ms is None")
            expiry_ms = int(expiry_any)
        else:
            expiry_ms = _parse_iso_utc_to_ms(str(model.get("expiry_iso") or ""))

        direction = str(model.get("direction") or "touch_above").strip().lower()
        if direction not in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"}:
            raise ValueError("direction must be touch_above/no_touch_above/touch_below/no_touch_below")

        drift = float(model.get("drift") or 0.0)

        instrument_name = str(model.get("instrument_name") or "").strip() or None
        # Use the nearest call option as IV source by default.
        if not instrument_name:
            ref = self.find_option(currency=currency, strike=barrier, expiry_ms=expiry_ms, option_type="call")
            instrument_name = ref.instrument_name

        summary = self.get_book_summary_by_instrument(instrument_name=instrument_name)

        spot = float(summary.get("underlying_price") or summary.get("underlying_index") or summary.get("index_price") or 0.0)
        mark_iv = summary.get("mark_iv")
        if mark_iv is None:
            raise ValueError("Deribit summary missing mark_iv")
        iv = float(mark_iv)
        sigma = iv / 100.0 if iv > 3.0 else iv

        now_ms = _utc_now_ms()
        t_years = max((expiry_ms - now_ms) / 1000.0 / (365.0 * 24.0 * 3600.0), 1e-6)

        if direction in {"touch_above", "no_touch_above"}:
            p_touch = risk_neutral_prob_touch_above_strike(spot=spot, barrier=barrier, sigma=sigma, t_years=t_years, drift=drift)
            p = p_touch if direction == "touch_above" else 1.0 - p_touch
        else:
            p_touch = risk_neutral_prob_touch_below_strike(spot=spot, barrier=barrier, sigma=sigma, t_years=t_years, drift=drift)
            p = p_touch if direction == "touch_below" else 1.0 - p_touch

        return {
            "ok": True,
            "currency": currency,
            "direction": direction,
            "barrier": barrier,
            "expiry_ts_ms": expiry_ms,
            "instrument_name": instrument_name,
            "spot": spot,
            "mark_iv": iv,
            "sigma": sigma,
            "t_years": t_years,
            "drift": drift,
            "touch_prob": p_touch,
            "event_prob": p,
            "summary": {
                "mark_price": summary.get("mark_price"),
                "bid_price": summary.get("bid_price"),
                "ask_price": summary.get("ask_price"),
                "underlying_price": summary.get("underlying_price"),
                "mark_iv": summary.get("mark_iv"),
                "volume": summary.get("volume"),
                "open_interest": summary.get("open_interest"),
            },
        }
