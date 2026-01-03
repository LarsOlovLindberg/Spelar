from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
import threading
import time


@dataclass(frozen=True)
class FillEvent:
    trade_id: str
    token_id: str
    side: str  # buy|sell
    size: float
    price: float | None = None
    ts_ms: int | None = None


def _norm_side(side: str) -> str:
    s = (side or "").strip().lower()
    if s in {"b", "buy", "bid"}:
        return "buy"
    if s in {"s", "sell", "ask"}:
        return "sell"
    return s or "unknown"


def _to_float(x: Any) -> float:
    try:
        return float(x)
    except Exception:
        return float("nan")


def _to_int_or_none(x: Any) -> int | None:
    try:
        if x is None:
            return None
        return int(float(x))
    except Exception:
        return None


@dataclass
class PolymarketPositionStore:
    """Tracks net position per token based on fills.

    This is intentionally simple: it de-duplicates trades by trade_id and maintains
    a net shares balance per token_id.
    """

    _lock: threading.Lock = field(default_factory=threading.Lock, init=False)
    _seen_trade_ids: set[str] = field(default_factory=lambda: set(), init=False)
    _net_shares_by_token: dict[str, float] = field(default_factory=lambda: {}, init=False)
    _fills_total: int = field(default=0, init=False)
    _last_update_ms: int | None = field(default=None, init=False)

    # Reconcile bookkeeping
    _last_reconcile_mono: float = field(default=0.0, init=False)

    def apply_fill(self, fill: FillEvent) -> bool:
        trade_id = str(fill.trade_id or "").strip()
        token_id = str(fill.token_id or "").strip()
        if not trade_id or not token_id:
            return False

        side = _norm_side(fill.side)
        size = float(fill.size)
        if not (size > 0):
            return False

        delta = size if side == "buy" else (-size if side == "sell" else 0.0)
        if delta == 0.0:
            return False

        with self._lock:
            if trade_id in self._seen_trade_ids:
                return False
            self._seen_trade_ids.add(trade_id)
            self._net_shares_by_token[token_id] = float(self._net_shares_by_token.get(token_id, 0.0)) + float(delta)
            self._fills_total += 1
            self._last_update_ms = int(fill.ts_ms) if fill.ts_ms is not None else int(time.time() * 1000)
        return True

    def should_reconcile(self, *, interval_s: float) -> bool:
        if interval_s <= 0:
            return False
        return (time.monotonic() - float(self._last_reconcile_mono or 0.0)) >= float(interval_s)

    def mark_reconciled(self) -> None:
        self._last_reconcile_mono = time.monotonic()

    def snapshot(self, *, ts_iso: str | None = None) -> dict[str, Any]:
        with self._lock:
            items = sorted(self._net_shares_by_token.items(), key=lambda kv: (-abs(float(kv[1])), str(kv[0])))
            return {
                "ts": ts_iso,
                "fills_total": int(self._fills_total),
                "unique_trade_ids": int(len(self._seen_trade_ids)),
                "last_update_ms": self._last_update_ms,
                "positions": [{"token_id": k, "net_shares": v} for k, v in items],
            }


def fill_from_loose_dict(d: dict[str, Any]) -> FillEvent | None:
    """Best-effort conversion from an unknown JSON payload shape to FillEvent."""

    # Common keys / variants
    trade_id = d.get("trade_id") or d.get("tradeId") or d.get("id")
    token_id = d.get("token_id") or d.get("tokenId") or d.get("asset_id") or d.get("assetId")
    side = d.get("side") or d.get("taker_side") or d.get("takerSide")
    size = d.get("size") or d.get("amount") or d.get("shares")
    price = d.get("price") or d.get("rate")
    ts = d.get("timestamp") or d.get("ts") or d.get("created_at") or d.get("createdAt")

    if trade_id is None or token_id is None or side is None or size is None:
        return None

    size_f = _to_float(size)
    if not (size_f > 0):
        return None

    price_f: float | None = None
    if price is not None:
        try:
            price_f = float(price)
        except Exception:
            price_f = None

    ts_ms = _to_int_or_none(ts)

    return FillEvent(
        trade_id=str(trade_id),
        token_id=str(token_id),
        side=str(side),
        size=float(size_f),
        price=price_f,
        ts_ms=ts_ms,
    )
