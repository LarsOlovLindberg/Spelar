# Changelog: pm_trend rollout (2026-01-03)

Note (2026-01-13): This is a historical changelog for the `pm_trend` rollout. The portal and agent have since added `pm_draw` and the portal copy now reflects `pm_draw` as the primary mode.

## Summary

- Added `pm_trend`: Polymarket-only trend-following strategy mode.
- Added auto YES/NO side selection for `pm_trend`.
- Ensured `pm_trend` runs through the same per-market decision pipeline so the portal updates (`pm_paper_candidates.csv`).
- Updated portal copy to match current behavior.

## Code changes

- Added `vps/strategies/pm_trend.py`
- Updated `vps/vps_agent.py`:
  - `STRATEGY_MODE=pm_trend`
  - New env fields: `PM_TREND_LOOKBACK_POINTS`, `PM_TREND_MOVE_MIN_PCT`, `PM_TREND_EXIT_MOVE_MIN_PCT`, `PM_TREND_AUTO_SIDE`
  - New reasons: `trend_gone`, `trend_move_too_small`, `not_best_side`, `no_best_side`, `other_side_open`
  - `live_status.json` includes `pm_trend_*` fields

## Portal changes

- Updated `web/index.html` and strategy pages under `web/pages/` to remove lead–lag/spot wording and describe `pm_trend`.

## Deploy

- Full site deploy uses `scripts/deploy-ftp.ps1` (static `web/`).
- Backup uses `scripts/backup.ps1`.
