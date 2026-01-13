# Portal copy/status alignment

Date: 2026-01-13

Purpose: the static portal (`web/index.html` + `web/pages/*.html`) should describe what the system actually does today: Draw value/edge trading (`pm_draw`) based on baseline probability vs Polymarket CLOB price.

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
- Live-status panel now prefers displaying `pm_draw_*` parameters when present (keeps `pm_trend_*`/legacy fallback).
- Engine Room file list includes scan artifacts used by the current run (`pm_scan_candidates.csv`, `pm_markets_index.json`).

### Page fragments

Files:

- `web/pages/strategy_about.html`
- `web/pages/strategy_flow.html`
- `web/pages/strategy_risk.html`
- `web/pages/strategy_tech.html`
- `web/pages/transparency_data.html`

All pages were updated to describe:

- Draw value/edge: baseline probability vs PM mid price
- Entry/exit in “edge in → edge out” terms
- Observability via CSV/JSON snapshots

## Notes on legacy filenames

Some snapshot filenames still contain legacy naming (e.g. `lead_lag_health.json`). The portal copy treats these as “strategy health” and notes the filename may be historical.

## Verify

- Start local server: `python -m http.server 5173 --directory .\web`
- Open: `http://localhost:5173/`
- Live check: `scripts/check-live-headers.ps1` plus spot-check one page contains `pm_draw` wording.
