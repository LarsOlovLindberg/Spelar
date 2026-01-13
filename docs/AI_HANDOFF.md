# AI handoff – spelar.eu

Det här dokumentet är skrivet för nästa AI (eller nästa person) som tar över repot.

## Big picture
- `web/` är en **statisk** portal som deployas “as-is” (ingen build-step).
- Portalen visar snapshot-data från `web/data/` (CSV/JSON) och laddar sidor som HTML-fragment från `web/pages/`.
- “Riktig” data produceras på en VPS av Python-agenten i `vps/` och synkas till `web/data/` med PowerShell-scripts.

## Status (2026-01-13)
Vi kör nu primärt `STRATEGY_MODE=pm_draw`: en Draw-fokuserad value/edge-strategi där vi jämför
bookmaker-implied baseline-sannolikhet för oavgjort mot Polymarkets Draw-pris (mid i CLOB).
Portalen (copy + UI) är uppdaterad för att beskriva `pm_draw`.

## Repo-struktur (det viktigaste)
- `web/index.html`: portalens shell + router + all JS/CSS (inline).
- `web/pages/*.html`: sidfragment (inte fulla HTML-dokument) som laddas in i shell:en.
- `web/data/`: snapshots (CSV/JSON). **Inte tänkt att committas** – synkas från VPS och deployas data-only.
- `scripts/`: PowerShell för deploy (FTP) och VPS-sync (SCP).
- `vps/`: Python-agent + connectors som skriver snapshots till `vps/out/` på VPS.

Nyare strategi-moduler:
- `vps/strategies/lead_lag.py`: lead–lag edge (extern referens/spot leder, PM CLOB släpar).
- `vps/connectors/kraken_spot_public.py`: nuvarande implementation av referens/spot (Kraken public).
- `vps/strategies/pm_draw.py`: Draw value/edge (baseline vs Polymarket CLOB mid). Nyckel: undvik “Draw” i irrelevanta marknader via heuristik.

Obs: UI/portalen är numera **Polymarket-only i text/diagnostik** (inga “Kraken …”-labels i webben), men backend kan fortfarande använda extern referens under huven.

### pm_draw i en mening
Handla Draw när $\text{edge} = (p_{\text{baseline}} - p_{\text{PM}})\cdot 100$ är stor nog, och gå ur när edgen försvinner.

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

### Scale-in (paper) på odds/prisrörelse
Lead–lag-loopen kan “skala in” i en redan öppnad position om PM-mid rör sig i rätt riktning.

- Triggar bara när man redan är i position.
- Respekterar cooldown + max antal “adds” + max total shares.
- Loggas tydligt i:
  - `pm_paper_trades.csv` (notering innehåller `scale_in`)
  - `pm_orders.csv`
  - `edge_calculator_live.csv` (reason = `scale_in`)

Nya env-vars (defaults i parentes):
- `LEAD_LAG_SCALE_ON_ODDS_CHANGE_PCT` (0.40)
- `LEAD_LAG_SCALE_COOLDOWN_S` (20)
- `LEAD_LAG_SCALE_MAX_ADDS` (3)
- `LEAD_LAG_SCALE_SIZE_MULT` (0.50)
- `LEAD_LAG_SCALE_MAX_TOTAL_SHARES` (50)

Schema-notis:
- `pm_paper_positions.csv` har extra kolumner för scale-state: `adds`, `last_mid`, `last_scale_at`.

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

Live-URL:er / paths (viktigt)
- På prod ligger filerna i webbrooten (t.ex. `https://spelar.eu/index.html`, `https://spelar.eu/pages/strategy_about.html`).
- `https://spelar.eu/web/...` kan vara 404 och är inte en garanti för deploy-status.
- Vid misstänkt cache: testa med querystring, t.ex. `.../pages/strategy_about.html?x=TIMESTAMP`.

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
- `STRATEGY_MODE=lead_lag|pm_trend|pm_draw|fair_model`
  - `lead_lag`: extern referens (Kraken spot) → Polymarket CLOB lag.
  - `pm_trend`: Polymarket-only trend (YES/NO auto-side kan användas för binära marknader).
  - `pm_draw`: Polymarket-only draw-value (baseline vs Draw-pris).
  - `fair_model`: äldre väg (pm_price vs fair_p från Deribit/Kraken futures).

pm_draw – viktiga env (översikt):
- `PM_DRAW_BASELINE_FILE` (valfri): CSV/JSON mapping `slug -> draw_prob|draw_odds`.
- `PM_DRAW_BASELINE_P` fallback (default 0.28)
- `PM_DRAW_BOOK_PROB_MULT` (default 0.95) för konservativ baseline
- `PM_DRAW_EDGE_MIN_PCT` / `PM_DRAW_EDGE_EXIT_PCT`
- `PM_DRAW_MAX_PRICE` (guard mot för dyr Draw)
- `PM_DRAW_REQUIRE_3WAY` (kräv 1X2 Home/Draw/Away)
- `PM_DRAW_FAV_MIN` / `PM_DRAW_FAV_MAX` (favorit-range gate baserat på de två icke-draw mids)

Gamma scan (universe discovery) – viktiga env (översikt):
- `PM_SCAN_ENABLE=1`
- `PM_SCAN_USE_FOR_TRADING=1` (om scan ska kunna driva universe)
- `PM_SCAN_SEARCH` (valfri). I `pm_draw` används annars en multi-term fallback (" vs ", "draw", "tie", None).
- `PM_SCAN_ORDER`, `PM_SCAN_DIRECTION`, `PM_SCAN_OFFSET`, `PM_SCAN_LIMIT`, `PM_SCAN_PAGES`
- `PM_SCAN_EXTRA_BLOCKS` (pm_draw default 3) för fler offset-block
- `PM_SCAN_BINARY_ONLY` default är 0 i `pm_draw` (för att kunna hitta 3-way). Säkerhetsregel: om `PM_DRAW_REQUIRE_3WAY=1` så tvingas binary-only av.

Lead–lag (kraken spot → pm clob) – viktiga env (översikt):
- `KRAKEN_SPOT_PAIR=XBTUSD`
- `LEAD_LAG_SIDE=YES|NO`
- `LEAD_LAG_LOOKBACK_POINTS`, `LEAD_LAG_SPOT_MOVE_MIN_PCT`, `LEAD_LAG_EDGE_MIN_PCT`, `LEAD_LAG_EDGE_EXIT_PCT`, `LEAD_LAG_MAX_HOLD_SECS`
- Orderbok-sizing: `LEAD_LAG_ENABLE_ORDERBOOK_SIZING`, `LEAD_LAG_SLIPPAGE_CAP`, `LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY`, `LEAD_LAG_HARD_CAP_USDC`
- Freshness-gate: `FRESHNESS_MAX_AGE_SECS`

För “mer action” i paper (risk-on för att se aktivitet) har vi ibland kört extremt permissiva gates (obs: bara paper):
- `LEAD_LAG_EDGE_MIN_PCT=0.0`
- `LEAD_LAG_SPOT_MOVE_MIN_PCT=0.0`
- `LEAD_LAG_NET_EDGE_MIN_PCT=-100.0`
- `LEAD_LAG_SPREAD_COST_CAP_PCT=100.0`
- `LEAD_LAG_MIN_TRADE_NOTIONAL_USDC=0.0`

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

- Upload API key incident (lärdom):
  - `web/trading/api/upload_api_key.local` ska **aldrig** vara publik/serve:ad.
  - Skydd finns på två nivåer:
    - `scripts/deploy-ftp.ps1` exkluderar `.local`/hemligheter från uppladdning.
    - `web/.htaccess` blockerar servering av lokala/secret-filer.
  - Om fil ändå råkat läcka på server: kör `scripts/ftp-delete.ps1 -RemoteFile "trading/api/upload_api_key.local"` och rotera nyckeln.

- Polymarket Gamma parsing:
  - Gamma kan ibland returnera `outcomes`/`clobTokenIds` som JSON-encodade strängar.
  - Connectorn behöver tåla både list och sträng.

- Lokalt körläge (Python-import gotcha):
  - Kör helst agenten som modul så att package-importer fungerar:
    - `python -m vps.vps_agent --once`
  - Om du kör `python .\vps\vps_agent.py` kan du få `ModuleNotFoundError: No module named 'vps'` beroende på CWD/PYTHONPATH.

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
