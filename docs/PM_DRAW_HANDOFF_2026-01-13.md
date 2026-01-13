# PM_DRAW handoff (2026-01-13)

Det här dokumentet är en konkret “vad funkade / vad var buggen / hur verifierar man snabbt”-guide för `STRATEGY_MODE=pm_draw`.

## Målet
- Hitta match-liknande marknader där Draw är "billig" relativt en baseline-sannolikhet.
- Handla i **paper** och se att följande fylls:
  - `pm_paper_trades.csv`
  - `pm_paper_positions.csv`
  - `pm_paper_portfolio.json`

## Viktig insikt: Gamma discovery
### Varför `/markets` inte räckte
- Gamma `/markets` uppförde sig inkonsekvent med `search` (i praktiken ignorerad i vissa listningar) och missade ofta sports/match-marknader.

### Vad som fungerade: `/events`
- Gamma `/events` innehåller events med en embedded lista av markets.
- För fotbollsmatcher finns ofta tre markets under samma event:
  - Home win?
  - Away win?
  - **End in a draw?** (ofta binär YES/NO prop)

I agenten används därför `/events` i `pm_scan` när `strategy_mode == "pm_draw"` och embedded markets “flattenas”.

### Viktig prestandagrej
- `/events` är tyngre än `/markets`.
- Agenten cap:ar därför event-scan även om `PM_SCAN_LIMIT/PAGES` är stora (för att undvika att scanning stallar hela tick-loopen).

## Paper trading: vad du ska titta på
### 1) Trades
- På VPS: `/opt/spelar_eu/vps/out/pm_paper_trades.csv`
- På webben: `https://spelar.eu/data/pm_paper_trades.csv`

### 2) Portfolio
- `/opt/spelar_eu/vps/out/pm_paper_portfolio.json`
- `https://spelar.eu/data/pm_paper_portfolio.json`

### 3) Varför blev det inga trades?
- `pm_orders.csv` innehåller “skipped” + orsak (t.ex. `blocked:throttled`).
- `pm_paper_candidates.csv` kan innehålla kandidater + decision/reason.

## Bug fix: “local variable 'p'”
### Symptom
- `pm_scanner_log.csv`: `status=error` med texten:
  - `cannot access local variable 'p' where it is not associated with a value`

### Orsak
- I paper mark-to-market-loopen användes fel variabelnamn (`p`) istället för positionsdict (`pos_any`).

### Fix
- Uppdaterat i `vps/vps_agent.py` så MTM använder `pos_any.get(...)`.

## Deploy / verifiering
### Snabbaste vägen (Windows)
- Kör VS Code task: `Sync VPS snapshots -> web/data + FTP deploy (data-only)`.

### Verifiera att live verkligen uppdaterats
- Kör task: `Verify live headers (HEAD)` och bekräfta att `Last-Modified` rör sig framåt.

## Känd störning: HTTP upload API key
- VPS kan logga: `server_missing_api_key` (HTTP 500) för stats-upload.
- Detta är separat från FTP sync/deploy och påverkar inte att webben uppdateras via data-only FTP.
- Åtgärd: sätt korrekt `UPLOAD_API_KEY` i `/etc/spelar-agent.env` eller disable:a upload genom att ta bort `UPLOAD_URL`/API key.
