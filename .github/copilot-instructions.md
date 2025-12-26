# spelar.eu – Copilot instructions

## Big picture
- Static site content is deployed as-is from `web/` (no build step).
- `web/pages/*.html` are HTML fragments (not full documents) that are intended to be injected into a portal shell.
- `web/data/` holds optional CSV/JSON snapshots rendered inside pages.

## Repo state (important)
- The docs reference `web/index.html` as the portal entry, but it is currently missing in this repo snapshot.
- `web/index.php` is a fallback entrypoint that **only** serves `index.html`; without `web/index.html` it returns 404.
- If you’re working on the portal shell/router, you likely need to restore/create `web/index.html` (and its inline CSS/JS) to make the site runnable.

## Page/data conventions
- Page fragments frequently include “data blocks” that the shell JS is expected to hydrate:
  - CSV: `<div data-csv="executed_trades.csv" data-max-rows="50"></div>` (see `web/pages/live_status.html`)
  - JSON: `<div data-json="live_status.json"></div>` (see `web/pages/live_status.html`)
- Data files live under `web/data/` and are referenced by filename only (e.g. `live_status.json`, `edge_signals_live.csv`).

## Local dev
```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
python -m http.server 5173 --directory .\web
```
Open `http://localhost:5173/`.

## Deploy & ops (PowerShell)
- FTP deploy: `scripts/deploy-ftp.ps1` uploads `./web` to `remote_path` (ignores `.git`, `node_modules`, `.vscode`).
- Local-only config: copy `ftp_config.example.json` -> `ftp_config.local.json` (gitignored); supports `host` (or legacy `server`) + `remote_path`.
- Safer check: `./scripts/deploy-ftp.ps1 -DryRun`.
- If ExecutionPolicy blocks: `powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-ftp.ps1`.
- Backup: `scripts/backup.ps1` zips `web/` + `README.md` into `backups/` (never includes `ftp_config.local.json`; optional `-IncludeDocs`).
- VPS data sync: `scripts/sync-vps-stats.ps1` uses `scp` and `scripts/vps_sync_map.json` to pull files into `web/data/`, then (unless `-SkipDeploy`) triggers FTP deploy.

## VPS pipeline (real data)
- Server-side scaffold lives under `vps/` and is designed to run on a VPS (keeps secrets off the static site).
- The agent writes “latest” snapshots and (optionally) FTP-uploads them into `web/data/`:
  - `live_status.json`, `edge_signals_live.csv`, `pm_orders.csv`, `kraken_futures_signals.csv`, `kraken_futures_fills.csv`, `executed_trades.csv`
- Example VPS sync mapping for these files: `scripts/vps_sync_map_pm.json` (use with `scripts/sync-vps-stats.ps1 -MappingFile ...`).

## Hosting gotcha
- Apache often prefers `index.php` over `index.html`; `web/.htaccess` sets `DirectoryIndex index.html index.php`, disables HTML/PHP caching, and forces UTF-8.
