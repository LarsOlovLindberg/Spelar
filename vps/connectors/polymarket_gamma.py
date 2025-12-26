from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any, cast

import requests


def _normalize(s: Any) -> str:
    return str(s or "").strip().lower()


def _coerce_str_or_list_to_list(value: Any) -> list[Any] | None:
    if isinstance(value, list):
        return cast(list[Any], value)
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return []
        # Gamma sometimes returns JSON-encoded lists as strings, e.g. '["Yes","No"]'.
        if (s.startswith("[") and s.endswith("]")) or (s.startswith("{") and s.endswith("}")):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return cast(list[Any], parsed)
            except Exception:
                return None
        # Last-resort: comma-separated tokens.
        if "," in s:
            return [p.strip().strip('"\'') for p in s.split(",") if p.strip()]
        return [s]
    return None


def _extract_slug(market_url_or_slug: str) -> str:
    s = (market_url_or_slug or "").strip()
    if not s:
        raise ValueError("Empty market_url_or_slug")

    if s.startswith("http://") or s.startswith("https://"):
        s = re.sub(r"[?#].*$", "", s)
        parts = [p for p in s.split("/") if p]
        if not parts:
            raise ValueError("Could not parse market URL")
        slug = parts[-1]
        if slug in {"market", "markets", "event"} and len(parts) >= 2:
            slug = parts[-2]
        return slug

    return s


def _has_negation(question: str) -> bool:
    q = (question or "").lower()
    # Simple, practical heuristic for English Polymarket questions.
    # We only use it for binary YES/NO markets.
    needles = [" not ", "n't", " never ", " won't ", " cannot ", " can't "]
    return any(n in q for n in needles)


@dataclass(frozen=True)
class GammaMarket:
    slug: str
    question: str | None
    outcomes: list[str]
    clob_token_ids: list[str]


class PolymarketGammaPublic:
    def __init__(self, *, base_url: str = "https://gamma-api.polymarket.com", timeout_s: float = 20.0, session: requests.Session | None = None) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout_s = timeout_s
        self._session = session or requests.Session()

    def _get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self._base_url}{path}"
        r = self._session.get(url, params=params or {}, timeout=self._timeout_s)
        r.raise_for_status()
        return r.json()

    def get_market_by_slug(self, *, slug: str) -> GammaMarket:
        s = _extract_slug(slug)

        # Gamma is not formally versioned in this repo; try a couple of common query shapes.
        candidates: list[tuple[str, dict[str, Any]]] = [
            ("/markets", {"slug": s}),
            ("/markets", {"limit": 1, "slug": s}),
            ("/markets", {"search": s}),
        ]

        last_err: Exception | None = None
        data: Any | None = None
        for path, params in candidates:
            try:
                data = self._get_json(path, params=params)
                break
            except Exception as e:
                last_err = e

        if data is None:
            raise RuntimeError(f"Could not fetch gamma market for slug '{s}': {last_err}")

        market_obj: dict[str, Any]
        if isinstance(data, list):
            if not data:
                raise ValueError(f"Gamma returned empty list for slug '{s}'")
            first: Any = cast(list[Any], data)[0]
            if not isinstance(first, dict):
                raise ValueError("Gamma response shape unexpected")
            market_obj = cast(dict[str, Any], first)
        elif isinstance(data, dict):
            market_obj = cast(dict[str, Any], data)
        else:
            raise ValueError("Gamma response shape unexpected")

        outcomes_any = market_obj.get("outcomes") or market_obj.get("outcomeNames") or market_obj.get("outcome_names")
        token_ids_any = market_obj.get("clobTokenIds") or market_obj.get("clob_token_ids")
        question_any = market_obj.get("question") or market_obj.get("title") or market_obj.get("name")

        outcomes: list[str] = []
        outcomes_list = _coerce_str_or_list_to_list(outcomes_any)
        if outcomes_list is not None:
            outcomes = [str(x) for x in outcomes_list]

        clob_token_ids: list[str] = []
        token_ids_list = _coerce_str_or_list_to_list(token_ids_any)
        if token_ids_list is not None:
            clob_token_ids = [str(x) for x in token_ids_list]

        if not outcomes or not clob_token_ids or len(outcomes) != len(clob_token_ids):
            raise ValueError(
                f"Gamma market missing outcomes/clobTokenIds (or mismatched lengths). "
                f"outcomes={len(outcomes)} token_ids={len(clob_token_ids)}"
            )

        return GammaMarket(
            slug=str(market_obj.get("slug") or s),
            question=str(question_any) if question_any is not None else None,
            outcomes=outcomes,
            clob_token_ids=clob_token_ids,
        )

    def resolve_token_id(self, *, market: GammaMarket, desired_outcome: str) -> str:
        want = _normalize(desired_outcome)
        if not want:
            raise ValueError("desired_outcome is empty")

        for i, name in enumerate(market.outcomes):
            if _normalize(name) == want:
                return market.clob_token_ids[i]

        # Convenience for YES/NO
        if want in {"yes", "y"}:
            for i, name in enumerate(market.outcomes):
                if _normalize(name) == "yes":
                    return market.clob_token_ids[i]
        if want in {"no", "n"}:
            for i, name in enumerate(market.outcomes):
                if _normalize(name) == "no":
                    return market.clob_token_ids[i]

        raise ValueError(f"Could not match desired_outcome='{desired_outcome}' to outcomes={market.outcomes}")

    def infer_yes_no_for_touch_event(self, *, market: GammaMarket, event: str) -> str:
        """Infer whether the event corresponds to YES or NO for a typical 'reach/touch' question.

                event:
                    - 'touch_above' -> event is 'reaches barrier'
                    - 'no_touch_above' -> event is 'does not reach barrier'
                    - 'touch_below' -> event is 'dips to barrier'
                    - 'no_touch_below' -> event is 'does not dip to barrier'

        Only works reliably for binary markets with outcomes ['Yes','No'].
        """

        outs = [_normalize(x) for x in market.outcomes]
        if len(outs) != 2 or "yes" not in outs or "no" not in outs:
            raise ValueError("Inference requires a binary YES/NO market")

        q = market.question or ""
        neg = _has_negation(q)

        # If question is negated ("Will BTC NOT reach ..."), YES corresponds to 'no_touch'.
        # If question is normal ("Will BTC reach ..."), YES corresponds to 'touch'.
        ev = _normalize(event)
        if ev not in {"touch_above", "no_touch_above", "touch_below", "no_touch_below"}:
            raise ValueError("event must be touch_above/no_touch_above/touch_below/no_touch_below")

        # For typical (non-negated) questions:
        # - 'Will BTC reach X?' -> YES corresponds to touch_above
        # - 'Will BTC dip to X?' -> YES corresponds to touch_below
        # For negated variants, the mapping flips.
        is_touch = ev in {"touch_above", "touch_below"}
        if not neg:
            return "yes" if is_touch else "no"
        return "no" if is_touch else "yes"
