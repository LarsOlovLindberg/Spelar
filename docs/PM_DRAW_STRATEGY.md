# PM Draw strategy (pm_draw)

Date: 2026-01-13

This repo supports a Polymarket-focused draw/value strategy mode: `STRATEGY_MODE=pm_draw`.

The idea: compare a bookmaker-implied baseline draw probability (external reference) to Polymarket’s Draw token price (≈ probability). If Draw is cheap versus baseline by a sufficient margin, buy Draw. Exit when the value edge fades.

## Core signal

For each selected market (match), for the Draw outcome:

- `pm_mid_draw` = (best_bid + best_ask) / 2 from Polymarket CLOB
- `baseline_p_draw` = baseline probability from file or fallback, optionally adjusted

Edge in percentage points:

- `edge_pct = (baseline_p_draw - pm_mid_draw) * 100`

Entry is allowed when:
- `edge_pct >= PM_DRAW_EDGE_MIN_PCT`
- `pm_mid_draw <= PM_DRAW_MAX_PRICE`
- generic cost/liquidity/freshness gates pass

Exit when:
- `edge_pct <= PM_DRAW_EDGE_EXIT_PCT`

## Baseline input

Preferred:
- `PM_DRAW_BASELINE_FILE` pointing to a CSV/JSON mapping market slug → draw probability or odds.

Fallback:
- `PM_DRAW_BASELINE_P` (default 0.28)

Conservatism knob:
- `PM_DRAW_BOOK_PROB_MULT` (default 0.95) scales baseline down: `baseline = clamp01(baseline * mult)`.

### Baseline file format

CSV (recommended):

- Required: `slug` (or `market_ref`)
- Provide one of:
  - `draw_prob` (0..1)
  - `draw_odds` (decimal odds)
  - `odds` (decimal odds)

JSON (supported):
- dict keyed by slug → {draw_prob|draw_odds}
- list of objects with {slug|market_ref, draw_prob|draw_odds}
- dict with key `items` as list

Implementation: `vps/strategies/pm_draw.py` (`load_draw_baseline`).

## Market selection (avoid "Draw" noise)

Key problem learned: many markets contain the string “Draw” but are not 1X2 match-draw markets (e.g., esports map draws or unrelated props). The agent uses heuristics to prefer real match questions.

- `is_likely_match_question(question)`: prefers match-like strings (" vs ", " v ", " @ ")
- `PM_DRAW_REQUIRE_3WAY=1`: require 3 outcomes (Home/Draw/Away)
- If not requiring 3-way, the agent can also accept binary “Will it end in a draw?” markets, and then uses the YES token.

Optional quality gate (favorite window):
- `PM_DRAW_FAV_MIN`, `PM_DRAW_FAV_MAX`
  - For 3-way markets, estimate favorite probability as max(mid(home), mid(away)).
  - Reject markets where favorite is outside the configured range.

## Gamma scan / universe discovery

When scan is enabled, the agent can auto-discover new markets via the Gamma API and optionally use scan-selected markets for trading.

- Enable: `PM_SCAN_ENABLE=1`
- Use for trading: `PM_SCAN_USE_FOR_TRADING=1`

For `pm_draw`, the agent:
- defaults `PM_SCAN_BINARY_ONLY` to **0** (so it can find 3-way markets)
- uses multiple search terms when `PM_SCAN_SEARCH` is not set: `" vs "`, `"draw"`, `"tie"`, and a no-search fallback
- scans multiple offset blocks (default 3) via `PM_SCAN_EXTRA_BLOCKS`
- dedupes by slug

Safety learned/fix:
- If `PM_DRAW_REQUIRE_3WAY=1` and `PM_SCAN_BINARY_ONLY=1`, discovery becomes impossible; the agent overrides binary-only to false and logs a warning.

Related code: `vps/vps_agent.py`.

## Observability (portal snapshots)

Key portal-facing files:
- `live_status.json` includes `strategy_mode` and `pm_draw_*` parameters.
- `pm_paper_candidates.csv` shows per-market decision and the `reason` when skipped.
- `edge_signals_live.csv` and `edge_calculator_live.csv` show the latest edge calculations.
- `pm_scan_candidates.csv` and `pm_markets_index.json` show what the scan discovered.

Portal copy alignment:
- `web/index.html` and `web/pages/*` were updated on 2026-01-13 to describe `pm_draw`.

## Practical defaults (good starting point)

- `PM_DRAW_EDGE_MIN_PCT=2.0`
- `PM_DRAW_EDGE_EXIT_PCT=0.5`
- `PM_DRAW_MAX_PRICE=0.45`
- `PM_DRAW_REQUIRE_3WAY=1`
- `PM_DRAW_BOOK_PROB_MULT=0.95`

## Known limitations

- Gamma metadata (`category/topic`) is often missing or unhelpful; don’t rely on it for filtering.
- Discovery quality is the main performance lever: the cleaner the draw-universe, the better the signal behaves.
