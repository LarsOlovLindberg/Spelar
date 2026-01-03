# Portal copy/status alignment

Date: 2026-01-03

Purpose: the static portal (`web/index.html` + `web/pages/*.html`) should describe what the system actually does today: Polymarket-only trend trading (`pm_trend`) with optional YES/NO auto-side selection.

## What changed

### Portal shell

File: `web/index.html`

- Removed lead–lag / spot / reference wording from UI labels.
- Updated reason mapping to include:
  - `trend_move_too_small`
  - `trend_gone`
  - `not_best_side`, `no_best_side`, `other_side_open`
- Kept a compatibility alias:
  - legacy `spot_move_too_small` is displayed as “Trend too small”.
- Live-status panel now prefers displaying `pm_trend_*` parameters when present.

### Page fragments

Files:

- `web/pages/strategy_about.html`
- `web/pages/strategy_flow.html`
- `web/pages/strategy_risk.html`
- `web/pages/strategy_tech.html`
- `web/pages/transparency_data.html`

All pages were updated to remove the old lead–lag/spot/reference narrative and describe:

- Trend detection on Polymarket CLOB only
- Auto-side selection (YES/NO)
- Entry/scale/exit logic in trend terms
- Observability via CSV/JSON snapshots

## Notes on legacy filenames

Some snapshot filenames still contain legacy naming (e.g. `lead_lag_health.json`). The portal copy treats these as “strategy health” and notes the filename may be historical.

## Verify

- Start local server: `python -m http.server 5173 --directory .\web`
- Open: `http://localhost:5173/`
- Grep: ensure no remaining `lead–lag`, `spot`, `Kraken`, `reference` wording exists in portal pages.
