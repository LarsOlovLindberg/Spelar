from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class PolymarketClobApiCreds:
    api_key: str
    api_secret: str
    api_passphrase: str


@dataclass(frozen=True)
class PolymarketClobLiveConfig:
    host: str
    chain_id: int
    private_key: str
    creds: PolymarketClobApiCreds
    signature_type: int = 0
    funder: str | None = None


def _require_py_clob_client() -> Any:
    try:
        import py_clob_client  # type: ignore

        return py_clob_client
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "Missing dependency 'py-clob-client'. Install it on the VPS (pip install py-clob-client). "
            "This is required for Polymarket live trading (order signing + L2 headers)."
        ) from e


def make_live_client(cfg: PolymarketClobLiveConfig) -> Any:
    py_clob_client = _require_py_clob_client()

    ClobClient = py_clob_client.client.ClobClient
    ApiCreds = py_clob_client.clob_types.ApiCreds

    creds = ApiCreds(api_key=cfg.creds.api_key, api_secret=cfg.creds.api_secret, api_passphrase=cfg.creds.api_passphrase)

    # Note: for real-money order placement you need BOTH:
    # - L1 auth (private key) to sign orders
    # - L2 auth (api creds) to post/cancel/list orders
    return ClobClient(
        cfg.host,
        chain_id=cfg.chain_id,
        key=cfg.private_key,
        creds=creds,
        signature_type=cfg.signature_type,
        funder=cfg.funder,
    )


def cancel_token_orders(client: Any, *, token_id: str) -> dict[str, Any]:
    # Uses DELETE /cancel-market-orders with asset_id.
    return client.cancel_market_orders(asset_id=token_id)


def cancel_all_orders(client: Any) -> dict[str, Any]:
    return client.cancel_all()


def get_open_orders(client: Any, *, market: str | None = None) -> list[dict[str, Any]]:
    py_clob_client = _require_py_clob_client()
    OpenOrderParams = py_clob_client.clob_types.OpenOrderParams

    params = None
    if market:
        params = OpenOrderParams(market=market)

    results = client.get_orders(params)
    # library returns list[dict]
    return list(results or [])


def post_limit_order(
    client: Any,
    *,
    token_id: str,
    side: str,
    price: float,
    size: float,
    order_type: str = "GTC",
) -> dict[str, Any]:
    py_clob_client = _require_py_clob_client()

    OrderArgs = py_clob_client.clob_types.OrderArgs
    OrderType = py_clob_client.clob_types.OrderType

    order_args = OrderArgs(token_id=token_id, price=float(price), size=float(size), side=side)

    # Create+sign (L1) then post (L2)
    signed = client.create_order(order_args)

    # OrderType enum by name
    try:
        ot = getattr(OrderType, str(order_type).upper())
    except Exception:
        ot = OrderType.GTC

    return client.post_order(signed, orderType=ot)
