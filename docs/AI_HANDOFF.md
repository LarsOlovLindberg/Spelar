# AI handoff – spelar.eu

Det här dokumentet är skrivet för nästa AI (eller nästa person) som tar över repot.

## Big picture
- `web/` är en **statisk** portal som deployas “as-is” (ingen build-step).
- Portalen visar snapshot-data från `web/data/` (CSV/JSON) och laddar sidor som HTML-fragment från `web/pages/`.
- “Riktig” data produceras på en VPS av Python-agenten i `vps/` och synkas till `web/data/` med PowerShell-scripts.

## Repo-struktur (det viktigaste)
- `web/index.html`: portalens shell + router + all JS/CSS (inline).
- `web/pages/*.html`: sidfragment (inte fulla HTML-dokument) som laddas in i shell:en.
- `web/data/`: snapshots (CSV/JSON). **Inte tänkt att committas** – synkas från VPS och deployas data-only.
- `scripts/`: PowerShell för deploy (FTP) och VPS-sync (SCP).
- `vps/`: Python-agent + connectors som skriver snapshots till `vps/out/` på VPS.

## Portalen (UI)
### Router
- Hash-baserad routing (t.ex. `#/start/overview`).
- Varje route pekar på en fil i `web/pages/`.

### Data-hydrering (CSV/JSON)
Sidor kan innehålla “data blocks” som shell:en hydratiserar:
- CSV:
  - Exempel: `<div data-csv="executed_trades.csv" data-max-rows="50"></div>`
- JSON:
  - Exempel: `<div data-json="live_status.json"></div>`

Konvention:
- Attributen refererar **endast filnamn** (inte path). Shell:en läser från `/data/<filnamn>`.

### Landing / paper
- Landing visar paper-saldo stort + öppna positioner + senaste trades.
- Paper-filer som UI läser:
  - `pm_paper_portfolio.json`
  - `pm_paper_positions.csv`
  - `pm_paper_trades.csv`
  - `pm_paper_candidates.csv` (debug: varför trades inte triggar)

## Data pipeline: VPS → web/data → FTP
### VPS agent
- Kör som systemd service: `spelar-agent`.
- Config (env): `/etc/spelar-agent.env`.
- Output-dir: `/opt/spelar_eu/vps/out` (synkas till `web/data/`).

Vanliga operativa kommandon på VPS:
- Status: `sudo systemctl status spelar-agent`
- Loggar: `sudo journalctl -u spelar-agent -n 200 --no-pager`
- Start/stop: `sudo systemctl restart spelar-agent`

### Sync + deploy
- Synk: [scripts/sync-vps-stats.ps1](../scripts/sync-vps-stats.ps1)
- FTP deploy: [scripts/deploy-ftp.ps1](../scripts/deploy-ftp.ps1)
- One-command (web + data): [scripts/deploy-all.ps1](../scripts/deploy-all.ps1)

Normalt flöde:
1) VPS skriver snapshots i `/opt/spelar_eu/vps/out`.
2) Windows-maskinen kör `scripts/sync-vps-stats.ps1` (SCP) → lägger filer i `web/data/`.
3) Scriptet deployar **data-only** (FTP) till webbservern.

Obs:
- UI-ändringar kräver **full deploy av `web/`**.
- Data-ändringar kan deployas **data-only**.

## Säkerhetsgates (live trading)
All live-handel ska vara av som default och kräver explicit arming.

Designprincip:
- Paper-flöde ska alltid kunna köras utan hemligheter.
- Live-flöde kräver både env-flags och secrets.

Nyckel-variabler (översikt):
- `TRADING_MODE=paper|live` (default ska vara paper)
- `POLY_LIVE_CONFIRM=YES` krävs för live
- `PM_MAX_ORDERS_PER_TICK` cap per tick (kan sättas till `0` för “0-risk live verification”)
- `EDGE_THRESHOLD` (högt värde + `PM_MAX_ORDERS_PER_TICK=0` är säkert testläge)
- `KILLSWITCH_FILE` om finns/är aktiverad ska stoppa live

## Viktiga output-filer (för health och “mapping green”)
För att sync-mappen ska vara “grön” även utan secrets skriver agenten vissa snapshots som stubbar när den saknar credentials:
- `pm_open_orders.json` (stub: `{ ok:false, error:"..." }`)
- `kraken_futures_private.json` (stub: `{ ok:false, error:"..." }`)

Health/sammanfattning:
- `sources_health.json`

## Kända gotchas / felsökning
- 404 på `/data/pm_paper_*.csv|.json` på live-siten betyder oftast att:
  - VPS inte producerar filerna, eller
  - sync/deploy data-only inte körts, eller
  - mappingen saknar filen.

- Apache index-prio:
  - Server kan föredra `index.php` före `index.html`.
  - `web/.htaccess` sätter `DirectoryIndex index.html index.php`.

- Polymarket Gamma parsing:
  - Gamma kan ibland returnera `outcomes`/`clobTokenIds` som JSON-encodade strängar.
  - Connectorn behöver tåla både list och sträng.

## Git-policy för det här repot
- Commita: `web/index.html`, `web/pages/`, `scripts/`, `vps/`, `docs/`.
- Commita inte:
  - `web/data/` (snapshots)
  - `out/`, `deploy.log`, `deploy_run.log`
  - lokala secrets (`ftp_config.local.json`, `kraken_keys.local.json`, `.env*`)

## Var finns mer dokumentation?
- Drift/ops: [docs/DRIFT.md](DRIFT.md)
- Autosync VPS→FTP: [docs/DEPLOY_AUTOSYNC_VPS_TO_FTP.md](DEPLOY_AUTOSYNC_VPS_TO_FTP.md)
- Paper-runbook: [docs/RUNBOOK_PM_KRAKEN_PAPER.md](RUNBOOK_PM_KRAKEN_PAPER.md)
