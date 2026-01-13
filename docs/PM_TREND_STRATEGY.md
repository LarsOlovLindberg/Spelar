# PM Trend strategy (pm_trend)

Note (2026-01-13): `pm_trend` is still supported, but the current primary strategy described on spelar.eu is `pm_draw`.
See: `docs/PM_DRAW_STRATEGY.md`.

Date: 2026-01-03

This repo now supports a Polymarket-only trend-following strategy mode: `STRATEGY_MODE=pm_trend`.

## Goal

Trade short-term momentum in Polymarket outcome tokens (YES/NO) using only Polymarket CLOB prices (bid/ask/mid). No external reference feed (no Kraken/spot) is used in decision-making.

## Core behavior

Per market/token:

- Read Polymarket CLOB best bid/ask, compute `pm_mid = (bid+ask)/2`.
- Maintain rolling history per token.
- Compute trend return:
  - `pm_ret_pct` over `PM_TREND_LOOKBACK_POINTS` samples.
- Entry condition (trend):
  - Enter only when `pm_ret_pct >= PM_TREND_MOVE_MIN_PCT`.
- Exit condition (trend gone):
  - Exit when `pm_ret_pct <= PM_TREND_EXIT_MOVE_MIN_PCT` (reason `trend_gone`).
- Scaling:
  - Can scale-in when price moves in our favor (same scaling logic as existing engine).

## Auto-side selection (YES/NO)

When `PM_TREND_AUTO_SIDE=1` and the market reference (slug) is available, the engine resolves both YES and NO token IDs via Gamma and evaluates trend for both sides.

- Only the best trending side (highest `pm_ret_pct`) is allowed to enter.
- The other side is suppressed with reason `not_best_side`.
- If the other side is already open for the same market group, entry is suppressed with `other_side_open`.
- If a best side cannot be determined yet (insufficient history), entry is suppressed with `no_best_side`.

## Risk / microstructure gates (legacy names)

Several generic gates still use `LEAD_LAG_*` environment variable names (legacy prefix), but they are applied in `pm_trend` too as general cost/liquidity filters, e.g.

- `LEAD_LAG_SPREAD_COST_CAP_PCT`
- `LEAD_LAG_NET_EDGE_MIN_PCT`
- `LEAD_LAG_MIN_TRADE_NOTIONAL_USDC`
- slippage-band liquidity caps

These are **not** external-reference logic; they are execution quality/risk constraints.

## Portal outputs (snapshots)

The portal reads from `web/data/` snapshots, typically:

- `live_status.json` (includes `pm_trend_*` fields)
- `pm_paper_candidates.csv` (decision + reason per candidate)
- `pm_paper_positions.csv`, `pm_paper_trades.csv`, `pm_paper_portfolio.json`
- `edge_signals_live.csv`, `edge_calculator_live.csv`

## Code locations

- Strategy engine: `vps/strategies/pm_trend.py`
- Main loop + wiring: `vps/vps_agent.py`

## Quick run (local)

Example (paper, run once):

- Set `STRATEGY_MODE=pm_trend`
- Set `TRADING_MODE=paper`
- Set `OUT_DIR=.\web\data`

Then run `python -m vps.vps_agent`.
