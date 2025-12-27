# Polymarket lead–lag bot (spot → pm)

This project implements the core idea you described:

1) Use **spot price (Kraken)** as the leader signal.
2) Detect a short-lived **edge window** when spot moves but **pm is lagging**.
3) Enter pm (BUY YES or BUY NO) during the edge window.
4) Exit when pm has repriced (edge is gone) or max-hold-time is reached.

## What this is / isn't
- This is **lead–lag repricing** logic, not "hold to resolution".
- It is **not** a ready-to-deploy money printer. pm markets are thin and binary; risk remains.

## Quick start (backtest mode)

```bash
python -m venv .venv
. .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt

python run_backtest.py --spot sample_data/spot.csv --pm sample_data/pm.csv
```

## Quick start (live polling mode)

This polls:
- Kraken spot ticker (`/0/public/Ticker`) for BTC/USD (pair configurable).
- Polymarket Gamma API to resolve a market slug to token_ids.
- Polymarket CLOB Pricing API (`/price`) to get BUY/SELL price for a token_id.

Environment variables:
- `PM_MARKET_SLUG` (required)
- `PM_SIDE` = `YES` or `NO` (default YES)
- `KRAKEN_PAIR` (default `XBTUSD`)
- `POLL_SECS` (default `5`)

Run:
```bash
PM_MARKET_SLUG="will-bitcoin-hit-100000-by-december-31-2025" python run_live.py
```

## Machine spec (state machine)

States:
- `FLAT`: no pm position.
- `IN_POSITION`: holding pm shares.

Key signals computed every tick:
- `spot_ret`: percent change in spot price over a lookback window.
- `pm_ret`: percent change in pm price (for chosen side) over same window.
- `edge = spot_ret - pm_ret` for YES-bias (or inverted for NO-bias).

Transitions:
- `FLAT` -> `IN_POSITION` when:
  - `abs(spot_ret) >= SPOT_MOVE_MIN_PCT`
  - `edge >= EDGE_MIN_PCT`
  - and `pm_price` is not in the "too-late" zone (avoid extreme 0.9+ / 0.1- regions)
- `IN_POSITION` -> `FLAT` when:
  - `edge <= EDGE_EXIT_PCT` (pm caught up), OR
  - `hold_time >= MAX_HOLD_SECS`, OR
  - `pm_move_against >= PM_STOP_PCT` (optional stop)

See `src/config.py` for parameters.

## Notes on Polymarket endpoints
This project uses official Polymarket docs endpoints:
- CLOB REST: `https://clob.polymarket.com/`
- Gamma Markets API: `https://gamma-api.polymarket.com/`
See docs:
- Endpoints: https://docs.polymarket.com/quickstart/introduction/endpoints
- Pricing: https://docs.polymarket.com/api-reference/pricing/get-market-price
- Kraken Ticker: https://docs.kraken.com/api/docs/rest-api/get-ticker-information/


## Orderbook-aware max size (recommended)

Environment variables:
- `ENABLE_ORDERBOOK_SIZING` = `1` or `0` (default 1)
- `SLIPPAGE_CAP` (default 0.01)
- `MAX_FRACTION_OF_BAND_LIQUIDITY` (default 0.10)
- `HARD_CAP_USDC` (default 2000)

The live runner prints `max_usdc~...` based on visible liquidity inside the band.
