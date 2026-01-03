#!/usr/bin/env python
"""Find Polymarket "deadline ladder" inconsistencies from local scan snapshots.

This scans `web/data/pm_scan_candidates.csv` and looks for pairs of markets that
appear to represent the *same underlying event* but with different resolution
(end) dates (e.g. "Will X happen by DATE?").

If event A resolves earlier than event B and A  B (A implies B), then we
expect P(A) <= P(B). When the market violates that (roughly p_yes_early > p_yes_late)
there can be a riskless structure:

- Buy (early) NO
- Buy (late) YES

Payoffs:
- event happens before early deadline: 0 + 1 = 1
- event happens between deadlines:   1 + 1 = 2  ("double win")
- event never happens by late:       1 + 0 = 1

Cost (worst-case, executable): no_ask_early + yes_ask_late
Guaranteed profit exists iff: 1 - cost > 0.

This is purely a scanner; it does not place orders.
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
import re
from typing import Iterable


_RE_BY = re.compile(r"\bby\b", re.IGNORECASE)
_RE_YEAR = re.compile(r"\b20\d{2}\b")
_RE_TIME_HINT = re.compile(r"\b(by|before|after|in|on|during|until|through)\b", re.IGNORECASE)
_RE_ISO_DATE = re.compile(r"\b20\d{2}-\d{2}-\d{2}\b")


def _normalize_key(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9\s\-]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _parse_iso_dt(s: str) -> datetime | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        # Handles trailing 'Z'
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _safe_float(v: str | None) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def _looks_like_deadline_market(*, slug: str, question: str) -> bool:
    slug_l = (slug or "").lower()
    q_l = (question or "").lower()
    if "-by-" in slug_l:
        return True
    if _RE_BY.search(q_l):
        return True
    if " before " in q_l:
        return True
    # Many maturity ladders are phrased as "... in 2025?" (or similar) and
    # differ only by the year / end_date.
    if "-in-20" in slug_l:
        return True
    if _RE_YEAR.search(q_l) and _RE_TIME_HINT.search(q_l):
        return True
    # Broad fallback: any explicit year/date hint is worth considering for
    # maturity-ladder grouping (we'll require multi-group matches later).
    if _RE_ISO_DATE.search(q_l) or _RE_YEAR.search(q_l):
        return True
    if re.search(r"-20\d{2}(-\d{2}-\d{2})?$", slug_l):
        return True
    return False


def _base_key(*, slug: str, question: str) -> str:
    """Best-effort grouping key.

    We primarily use slug because PM slugs for deadline markets commonly contain
    `-by-<date...>`.
    """

    s = _normalize_key(slug)
    if "-by-" in s:
        return s.split("-by-", 1)[0]

    # Common ladder patterns in slugs.
    s2 = re.sub(r"-(in|by|before|after|until|through)-20\d{2}$", "", s)
    s2 = re.sub(r"-(in|by|before|after|until|through)-20\d{2}s$", "", s2)
    s2 = re.sub(r"-(in|by|before|after|until|through)-\d{4}-\d{2}-\d{2}$", "", s2)

    # Aggressive grouping: remove any explicit year/date tokens anywhere in the
    # slug. This is okay because we only act on bases that appear >=2 times.
    s2 = re.sub(r"20\d{2}-\d{2}-\d{2}", "", s2)
    s2 = re.sub(r"20\d{2}", "", s2)
    s2 = re.sub(r"-{2,}", "-", s2).strip("-")
    if s2:
        return s2

    # Fallback: strip a trailing time clause from the question.
    q = _normalize_key(question)
    # Remove explicit years/dates anywhere.
    q = re.sub(r"20\d{2}-\d{2}-\d{2}", "", q)
    q = re.sub(r"20\d{2}", "", q)
    q = re.sub(r"\s+(by|before|after|in|on|during|until|through)\s+([a-z]{3,9}\s+\d{1,2}(,\s*20\d{2})?|20\d{2}(-\d{2}-\d{2})?)\s*\??$", "", q)
    q = re.sub(r"\s+\?$", "", q)
    q = re.sub(r"\s+", " ", q).strip()
    return q


@dataclass(frozen=True)
class MarketRow:
    ts: str
    slug: str
    question: str
    end_date: datetime
    yes_bid: float
    yes_ask: float
    no_bid: float | None
    no_ask: float | None
    volume_usd: float | None
    liquidity_usd: float | None


@dataclass(frozen=True)
class DeadlineEdge:
    base: str
    early: MarketRow
    late: MarketRow
    # Executable calendar spread:
    #   sell early YES @ early_yes_bid
    #   buy  late  YES @ late_yes_ask
    # Guaranteed profit in worst case:
    guaranteed_profit: float
    # Profit if event happens between deadlines (A=false, B=true)
    between_deadlines_profit: float
    # Optional equivalent "double-win" form when early NO ask is known:
    #   buy early NO @ early_no_ask
    #   buy late  YES @ late_yes_ask
    double_win_cost: float | None
    double_win_guaranteed_profit: float | None
    double_win_profit: float | None


def _read_candidates(path: Path) -> list[MarketRow]:
    rows: list[MarketRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for r in reader:
            slug = (r.get("slug") or "").strip()
            question = (r.get("question") or "").strip()
            if not _looks_like_deadline_market(slug=slug, question=question):
                continue

            end_dt = _parse_iso_dt(r.get("end_date") or "")
            if end_dt is None:
                continue

            yes_bid = _safe_float(r.get("yes_bid"))
            yes_ask = _safe_float(r.get("yes_ask"))
            if yes_bid is None or yes_ask is None:
                continue

            no_bid = _safe_float(r.get("no_bid"))
            no_ask = _safe_float(r.get("no_ask"))

            rows.append(
                MarketRow(
                    ts=(r.get("ts") or "").strip(),
                    slug=slug,
                    question=question,
                    end_date=end_dt,
                    yes_bid=float(yes_bid),
                    yes_ask=float(yes_ask),
                    no_bid=float(no_bid) if no_bid is not None else None,
                    no_ask=float(no_ask) if no_ask is not None else None,
                    volume_usd=_safe_float(r.get("volume_usd")),
                    liquidity_usd=_safe_float(r.get("liquidity_usd")),
                )
            )
    return rows


def _find_edges(rows: Iterable[MarketRow], *, min_guaranteed_profit: float) -> list[DeadlineEdge]:
    by_base: dict[str, list[MarketRow]] = {}
    for r in rows:
        base = _base_key(slug=r.slug, question=r.question)
        if not base:
            continue
        by_base.setdefault(base, []).append(r)

    found: list[DeadlineEdge] = []
    for base, group in by_base.items():
        if len(group) < 2:
            continue
        group_sorted = sorted(group, key=lambda x: x.end_date)
        # Compare adjacent maturities (most plausible to be the same underlying)
        for i in range(len(group_sorted) - 1):
            early = group_sorted[i]
            late = group_sorted[i + 1]

            # Must be a true maturity ladder (different end dates).
            if late.end_date <= early.end_date:
                continue

            # Executable calendar spread (does not require NO token quotes)
            # Worst case: event happens by early (both settle 1) OR never happens by late (both 0)
            # => profit = early_yes_bid - late_yes_ask
            guaranteed_profit = float(early.yes_bid) - float(late.yes_ask)
            if guaranteed_profit <= float(min_guaranteed_profit):
                continue

            # If event happens between deadlines: A=false (short pays 0), B=true (long pays 1)
            between_profit = guaranteed_profit + 1.0

            dw_cost = None
            dw_g = None
            dw_between = None
            if early.no_ask is not None:
                dw_cost = float(early.no_ask) + float(late.yes_ask)
                dw_g = 1.0 - float(dw_cost)
                dw_between = 2.0 - float(dw_cost)
            found.append(
                DeadlineEdge(
                    base=base,
                    early=early,
                    late=late,
                    guaranteed_profit=guaranteed_profit,
                    between_deadlines_profit=between_profit,
                    double_win_cost=dw_cost,
                    double_win_guaranteed_profit=dw_g,
                    double_win_profit=dw_between,
                )
            )

    found.sort(key=lambda e: e.guaranteed_profit, reverse=True)
    return found


def _fmt_dt(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> int:
    ap = argparse.ArgumentParser(description="Find Polymarket deadline ladder edges from local CSV snapshots.")
    ap.add_argument(
        "--csv",
        default=str(Path("web/data/pm_scan_candidates.csv")),
        help="Path to pm_scan_candidates.csv (default: web/data/pm_scan_candidates.csv)",
    )
    ap.add_argument(
        "--min-profit",
        type=float,
        default=0.002,
        help="Minimum guaranteed profit to report (default 0.002 = 0.2%)",
    )
    ap.add_argument(
        "--out",
        default=str(Path("out/pm_deadline_edges.csv")),
        help="Optional output CSV path (default: out/pm_deadline_edges.csv)",
    )
    args = ap.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    rows = _read_candidates(csv_path)

    # Diagnostics: how many multi-maturity groups exist?
    by_base: dict[str, int] = {}
    for r in rows:
        b = _base_key(slug=r.slug, question=r.question)
        if not b:
            continue
        by_base[b] = by_base.get(b, 0) + 1
    multi = sum(1 for c in by_base.values() if c >= 2)
    print(f"candidate_groups={len(by_base)} multi_groups={multi}")

    edges = _find_edges(rows, min_guaranteed_profit=float(args.min_profit))

    out_path = Path(args.out) if args.out else None
    if out_path is not None:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "base",
                    "early_slug",
                    "early_end",
                    "early_yes_bid",
                    "early_yes_ask",
                    "early_no_ask",
                    "late_slug",
                    "late_end",
                    "late_yes_bid",
                    "late_yes_ask",
                    "calendar_spread_profit(sell_early_yes@bid - buy_late_yes@ask)",
                    "between_deadlines_profit(above+1)",
                    "double_win_cost(buy_early_no@ask + buy_late_yes@ask)",
                    "double_win_guaranteed_profit(1-cost)",
                    "double_win_between_profit(2-cost)",
                ]
            )
            for e in edges:
                w.writerow(
                    [
                        e.base,
                        e.early.slug,
                        _fmt_dt(e.early.end_date),
                        f"{e.early.yes_bid:.6f}",
                        f"{e.early.yes_ask:.6f}",
                        f"{float(e.early.no_ask):.6f}" if e.early.no_ask is not None else "",
                        e.late.slug,
                        _fmt_dt(e.late.end_date),
                        f"{e.late.yes_bid:.6f}",
                        f"{e.late.yes_ask:.6f}",
                        f"{e.guaranteed_profit:.6f}",
                        f"{e.between_deadlines_profit:.6f}",
                        f"{float(e.double_win_cost):.6f}" if e.double_win_cost is not None else "",
                        f"{float(e.double_win_guaranteed_profit):.6f}" if e.double_win_guaranteed_profit is not None else "",
                        f"{float(e.double_win_profit):.6f}" if e.double_win_profit is not None else "",
                    ]
                )

    print(f"scanned_rows={len(rows)}")
    print(f"edges_found={len(edges)} (min_profit={float(args.min_profit)})")
    if out_path is not None:
        print(f"wrote={out_path}")

    # Print top 15 to stdout
    for e in edges[:15]:
        print(
            f"profit={e.guaranteed_profit:.4f} (between={e.between_deadlines_profit:.4f}) | "
            f"early={e.early.slug} ({_fmt_dt(e.early.end_date)}) | "
            f"late={e.late.slug} ({_fmt_dt(e.late.end_date)})"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
