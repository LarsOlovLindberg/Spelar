# Changelog: pm_draw rollout (2026-01-13)

## Summary

- Added `pm_draw`: a draw-focused value/edge strategy comparing external baseline probability vs Polymarket CLOB mid price.
- Updated Polymarket Gamma scan/universe selection to better discover and select match-draw markets.
- Updated portal copy + live-status display so spelar.eu describes and surfaces what we run now.

## Agent changes

- New strategy module: `vps/strategies/pm_draw.py`
  - Detects Draw outcomes (aliases: draw/tie/x/oavgjort)
  - Loads baseline from `PM_DRAW_BASELINE_FILE` (CSV/JSON) with `PM_DRAW_BASELINE_P` fallback
  - Computes `edge_pct = (baseline_p - pm_mid) * 100`
  - Entry/exit thresholds: `PM_DRAW_EDGE_MIN_PCT`, `PM_DRAW_EDGE_EXIT_PCT`
  - Guardrail: `PM_DRAW_MAX_PRICE`
  - Optional strictness: `PM_DRAW_REQUIRE_3WAY`, `PM_DRAW_FAV_MIN/MAX`

- `vps/vps_agent.py`
  - Integrates `pm_draw` end-to-end through the same decision pipeline and portal outputs.
  - Exposes `pm_draw_*` parameters in `live_status.json`.
  - Gamma scan improvements for draw discovery:
    - multiple search terms in pm_draw mode
    - multiple offset blocks via `PM_SCAN_EXTRA_BLOCKS`
    - dedupe by slug
    - safety override: if `PM_DRAW_REQUIRE_3WAY=1` then force `PM_SCAN_BINARY_ONLY=0`
  - Adds draw-specific skip reasons (e.g. `draw_edge_too_small`, `draw_too_expensive`).

- `vps/connectors/polymarket_gamma.py`
  - Sends multiple search parameter aliases (`search`, `searchTerm`, `query`, `q`) for robustness across Gamma deployments.

## Portal changes

- `web/index.html`
  - Updated meta description to reflect draw-edge trading.
  - Engine Room file list includes scan artifacts (`pm_scan_candidates.csv`, `pm_markets_index.json`).
  - Live status card renders `pm_draw_*` parameters (keeps fallback to `pm_trend`/legacy).

- `web/pages/*.html`
  - Strategy/about/flow/tech/risk/transparency pages updated from `pm_trend` wording to `pm_draw`.

## Verification

- Deployed `web/` via `scripts/deploy-ftp.ps1`.
- Verified live pages contain `pm_draw` copy and have updated `Last-Modified`.
- Verified `scripts/check-live-headers.ps1` returns 200 for key `/data/*` artifacts.
