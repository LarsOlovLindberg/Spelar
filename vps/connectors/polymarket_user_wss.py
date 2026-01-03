from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, cast

from vps.connectors.polymarket_position_store import FillEvent, fill_from_loose_dict


@dataclass(frozen=True)
class PolymarketUserWssAuth:
    api_key: str
    api_secret: str
    api_passphrase: str


@dataclass(frozen=True)
class PolymarketUserWssConfig:
    wss_url: str  # e.g. wss://ws-subscriptions-clob.polymarket.com/ws/
    auth: PolymarketUserWssAuth


def _require_websocket_client() -> Any:
    try:
        import websocket  # type: ignore

        return websocket
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency 'websocket-client'. Install it on the VPS (pip install websocket-client). "
            "This is required for Polymarket user-channel WebSocket reconciliation."
        ) from e


def _join_user_url(base: str) -> str:
    b = (base or "").strip()
    if not b:
        return ""
    # Accept both base like .../ws/ and already-targeted .../ws/user
    b = b.rstrip("/")
    if b.endswith("/user"):
        return b
    if b.endswith("/ws"):
        return b + "/user"
    if b.endswith("/ws-subscriptions"):
        # Not expected, but avoid double slashes.
        return b + "/ws/user"
    # If someone supplies domain only, append /ws/user.
    if "/ws" not in b:
        return b + "/ws/user"
    # Otherwise, just append /user.
    return b + "/user"


class PolymarketUserWssClient:
    """Background user-channel websocket.

    Implementation is intentionally defensive: payload formats can vary.
    It extracts fill-like events and forwards them to an on_fill callback.
    """

    def __init__(
        self,
        *,
        cfg: PolymarketUserWssConfig,
        on_fill: Callable[[FillEvent], None],
        status_sink: dict[str, Any] | None = None,
    ) -> None:
        self._cfg = cfg
        self._on_fill = on_fill
        self._status = status_sink if status_sink is not None else {}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="pm_user_wss", daemon=True)
        self._thread.start()

    def stop(self, *, timeout_s: float = 2.0) -> None:
        self._stop.set()
        t = self._thread
        if t:
            t.join(timeout=float(timeout_s))

    def _run(self) -> None:
        websocket = _require_websocket_client()

        url = _join_user_url(self._cfg.wss_url)
        if not url:
            self._status.update({"ok": False, "error": "missing_wss_url"})
            return

        backoff_s = 1.0
        while not self._stop.is_set():
            try:
                self._status.update({"ok": False, "url": url, "state": "connecting", "error": None})

                def on_open(ws: Any) -> None:
                    self._status.update({"ok": True, "state": "open", "connected_at": time.time()})
                    # Subscribe/auth message shape based on existing PM user-channel examples.
                    msg: dict[str, Any] = {
                        "type": "user",
                        "auth": {
                            "apiKey": self._cfg.auth.api_key,
                            "secret": self._cfg.auth.api_secret,
                            "passphrase": self._cfg.auth.api_passphrase,
                        },
                    }
                    try:
                        ws.send(json.dumps(msg))
                        self._status.update({"subscribed": True, "subscribe_error": None})
                    except Exception as e:
                        self._status.update({"subscribed": False, "subscribe_error": str(e)})

                def on_message(ws: Any, message: str) -> None:
                    try:
                        payload: Any = json.loads(message)
                    except Exception:
                        return

                    fills = _extract_fills(payload)
                    if fills:
                        self._status["last_fill_at"] = time.time()
                        self._status["fills_seen"] = int(self._status.get("fills_seen") or 0) + int(len(fills))
                        for f in fills:
                            try:
                                self._on_fill(f)
                            except Exception:
                                # never crash the ws thread on fill callback
                                pass

                def on_error(ws: Any, error: Any) -> None:
                    self._status.update({"ok": False, "state": "error", "error": str(error)})

                def on_close(ws: Any, status_code: Any, msg: Any) -> None:
                    self._status.update({"ok": False, "state": "closed", "close_code": status_code, "close_msg": str(msg)})

                app = websocket.WebSocketApp(url, on_open=on_open, on_message=on_message, on_error=on_error, on_close=on_close)

                # Keep-alives
                app.run_forever(ping_interval=30, ping_timeout=10)

            except Exception as e:
                self._status.update({"ok": False, "state": "exception", "error": str(e)})

            if self._stop.is_set():
                break

            # simple reconnect backoff
            time.sleep(backoff_s)
            backoff_s = min(backoff_s * 1.7, 30.0)


def _extract_fills(payload: Any) -> list[FillEvent]:
    out: list[FillEvent] = []

    def handle_obj(o: Any) -> None:
        if not isinstance(o, dict):
            return

        o = cast(dict[str, Any], o)

        # Sometimes nested
        if "data" in o and isinstance(o.get("data"), dict):
            fe = fill_from_loose_dict(cast(dict[str, Any], o["data"]))
            if fe:
                out.append(fe)
                return

        # Common shapes
        event_type = str(o.get("event_type") or o.get("eventType") or o.get("type") or "").strip().lower()
        if event_type in {"fill", "filled", "trade"}:
            fe = fill_from_loose_dict(o)
            if fe:
                out.append(fe)
                return

        # Sometimes wrapped like {event_type: 'fill', payload: {...}}
        if isinstance(o.get("payload"), dict):
            inner = o.get("payload")
            if isinstance(inner, dict):
                fe = fill_from_loose_dict(cast(dict[str, Any], inner))
                if fe:
                    out.append(fe)
                    return

        # Or {fills: [...]} / {events: [...]} lists
        for k in ("fills", "events", "data"):
            v = o.get(k)
            if isinstance(v, list):
                for it in cast(list[Any], v):
                    handle_obj(it)

    if isinstance(payload, list):
        for it in cast(list[Any], payload):
            handle_obj(it)
    elif isinstance(payload, dict):
        handle_obj(payload)

    return out
