from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


def normalize_outcome_label(s: str) -> str:
    s2 = str(s or "").strip().lower()
    # Light normalization: keep letters/numbers only.
    out: list[str] = []
    for ch in s2:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


_DRAW_ALIASES = {
    "draw",
    "tie",
    "x",
    "oavgjort",
    "oavgjortx",
}


def is_draw_outcome(label: str) -> bool:
    n = normalize_outcome_label(label)
    return n in _DRAW_ALIASES


def is_draw_market_question(question: str | None) -> bool:
    """Heuristic: market question indicates a draw/tie proposition.

    This supports binary markets like "Will the match end in a draw?" where outcomes are Yes/No.
    """

    n = normalize_outcome_label(question or "")
    return ("draw" in n) or ("tie" in n) or ("oavgjort" in n)


def is_likely_match_question(question: str | None) -> bool:
    """Heuristic: question looks like a sports match between two sides.

    We use this to avoid selecting unrelated multi-outcome markets that happen
    to include an outcome named "Draw".
    """

    q = str(question or "").strip().lower()
    if not q:
        return False

    # Common match formatting.
    if any(sep in q for sep in [" vs ", " vs. ", " v ", " v. ", " @ "]):
        return True

    # Fallback: draw/tie proposition phrasing.
    n = normalize_outcome_label(q)
    if ("draw" in n or "tie" in n or "oavgjort" in n) and any(w in q for w in ["match", "game", "fixture", "ends", "end"]):
        return True

    return False


def resolve_token_id_from_listing(*, outcomes: list[str], token_ids: list[str], desired_outcome: str) -> str | None:
    if len(outcomes) != len(token_ids) or not outcomes:
        return None

    desired_norm = normalize_outcome_label(desired_outcome)
    if not desired_norm:
        return None

    # 1) exact normalized match
    for i, out in enumerate(outcomes):
        if normalize_outcome_label(out) == desired_norm:
            tok = str(token_ids[i]).strip()
            return tok or None

    # 2) draw aliases
    if desired_norm in _DRAW_ALIASES:
        for i, out in enumerate(outcomes):
            if is_draw_outcome(out):
                tok = str(token_ids[i]).strip()
                return tok or None

    return None


@dataclass(frozen=True)
class DrawBaseline:
    """Baseline probability for draw per market slug (bookmaker implied or manual).

    Values are probabilities in [0, 1].
    """

    by_slug: dict[str, float]

    def get(self, slug: str) -> float | None:
        k = str(slug or "").strip()
        if not k:
            return None
        v = self.by_slug.get(k)
        if v is None:
            return None
        try:
            fv = float(v)
        except Exception:
            return None
        if fv < 0:
            return 0.0
        if fv > 1:
            return 1.0
        return fv


def _prob_from_row(row: dict[str, Any]) -> float | None:
    def _f(key: str) -> float | None:
        raw = row.get(key)
        if raw is None:
            return None
        try:
            return float(str(raw).strip())
        except Exception:
            return None

    p = _f("draw_prob")
    if p is not None:
        return p

    odds = _f("draw_odds")
    if odds is None:
        odds = _f("odds")
    if odds is not None and odds > 0:
        return 1.0 / float(odds)

    return None


def load_draw_baseline(path: Path) -> DrawBaseline:
    """Load baseline draw probabilities.

    Supported formats:
    - CSV with columns: slug (or market_ref), and one of draw_odds / odds / draw_prob
    - JSON:
      - dict mapping slug -> {draw_odds|draw_prob}
      - list of objects with {slug|market_ref, draw_odds|draw_prob}
      - dict with key "items" as list
    """

    by_slug: dict[str, float] = {}

    if not path.exists():
        return DrawBaseline(by_slug={})

    if path.suffix.lower() == ".json":
        raw: Any = json.loads(path.read_text(encoding="utf-8"))
        items: list[dict[str, Any]] = []
        if isinstance(raw, dict):
            raw_dict = cast(dict[str, Any], raw)
            raw_items: Any = raw_dict.get("items")
            if isinstance(raw_items, list):
                raw_items_list = cast(list[Any], raw_items)
                items = [cast(dict[str, Any], x) for x in raw_items_list if isinstance(x, dict)]
            else:
                # dict keyed by slug
                for k, v in raw_dict.items():
                    if isinstance(v, dict):
                        v_dict = cast(dict[str, Any], v)
                        row: dict[str, Any] = dict(v_dict)
                        row.setdefault("slug", str(k))
                        items.append(row)
        elif isinstance(raw, list):
            raw_list = cast(list[Any], raw)
            items = [cast(dict[str, Any], x) for x in raw_list if isinstance(x, dict)]

        for row in items:
            slug = str(row.get("slug") or row.get("market_ref") or "").strip()
            if not slug:
                continue
            p = _prob_from_row(row)
            if p is None:
                continue
            by_slug[slug] = float(p)

        return DrawBaseline(by_slug=by_slug)

    # CSV
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            slug = str(row.get("slug") or row.get("market_ref") or "").strip()
            if not slug:
                continue
            p = _prob_from_row(row)
            if p is None:
                continue
            by_slug[slug] = float(p)

    return DrawBaseline(by_slug=by_slug)
