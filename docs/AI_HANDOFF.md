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

Nyare strategi-moduler:
- `vps/strategies/lead_lag.py`: lead–lag edge (Kraken spot leder, PM CLOB släpar).
- `vps/connectors/kraken_spot_public.py`: minimal Kraken spot ticker-klient (public).

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

Kod till VPS:
- Upload + restart (default): [scripts/upload-to-vps.ps1](../scripts/upload-to-vps.ps1)
  - Om du vill ladda upp utan restart: kör med `-NoRestart`

Normalt flöde:
1) VPS skriver snapshots i `/opt/spelar_eu/vps/out`.
2) Windows-maskinen kör `scripts/sync-vps-stats.ps1` (SCP) → lägger filer i `web/data/`.
3) Scriptet deployar **data-only** (FTP) till webbservern.

Alternativt (om du vill lägga FTP creds på VPS):
- Agenten kan själv FTP-ladda upp portal-filer direkt från VPS om `FTP_HOST/FTP_USER/FTP_PASS` är satta.

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
- `EDGE_THRESHOLD` (gäller främst fair_model-läget)
- `KILLSWITCH_FILE` om finns/är aktiverad ska stoppa live

Strategival (edge-definition):
- `STRATEGY_MODE=lead_lag|fair_model`
  - `lead_lag` är nu default om env-var saknas.
  - `fair_model` är den äldre vägen (pm_price vs fair_p från Deribit/Kraken futures).

Lead–lag (kraken spot → pm clob) – viktiga env (översikt):
- `KRAKEN_SPOT_PAIR=XBTUSD`
- `LEAD_LAG_SIDE=YES|NO`
- `LEAD_LAG_LOOKBACK_POINTS`, `LEAD_LAG_SPOT_MOVE_MIN_PCT`, `LEAD_LAG_EDGE_MIN_PCT`, `LEAD_LAG_EDGE_EXIT_PCT`, `LEAD_LAG_MAX_HOLD_SECS`
- Orderbok-sizing: `LEAD_LAG_ENABLE_ORDERBOOK_SIZING`, `LEAD_LAG_SLIPPAGE_CAP`, `LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY`, `LEAD_LAG_HARD_CAP_USDC`
- Freshness-gate: `FRESHNESS_MAX_AGE_SECS`

## Viktiga output-filer (för health och “mapping green”)
För att sync-mappen ska vara “grön” även utan secrets skriver agenten vissa snapshots som stubbar när den saknar credentials:
- `pm_open_orders.json` (stub: `{ ok:false, error:"..." }`)
- `kraken_futures_private.json` (stub: `{ ok:false, error:"..." }`)

Health/sammanfattning:
- `sources_health.json`

Lead–lag/paper (portal):
- `pm_paper_portfolio.json`, `pm_paper_positions.csv`, `pm_paper_trades.csv`, `pm_paper_candidates.csv`
- `pm_orders.csv` (paper-ordrar loggas även när live-client saknas)

## Kända gotchas / felsökning
- 404 på `/data/pm_paper_*.csv|.json` på live-siten betyder oftast att:
  - VPS inte producerar filerna, eller
  - sync/deploy data-only inte körts, eller
  - mappingen saknar filen.

- Lead–lag “inga trades” är ofta inte en bug:
  - warmup (för få ticks för `LEAD_LAG_LOOKBACK_POINTS`)
  - `FRESHNESS_MAX_AGE_SECS` för strikt
  - trösklar för höga (`LEAD_LAG_EDGE_MIN_PCT`, `LEAD_LAG_SPOT_MOVE_MIN_PCT`)
  - se `pm_paper_candidates.csv` för `decision/reason`.

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
