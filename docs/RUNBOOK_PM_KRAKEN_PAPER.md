# Runbook: PM (CLOB) ↔ Kraken Futures (paper-first)

Det här dokumentet beskriver den minsta "paper"-kedjan:
1) hämta Polymarket CLOB orderbook (public)
2) hämta Kraken Futures ticker (public)
3) om edge passerar tröskel: skriv en signal + en “paper order log” till CSV
4) portalen renderar CSV/JSON från `web/data/`

## Konfig
På VPS (eller lokalt) skapas env baserat på `vps/systemd/spelar-agent.env.example`.

Minst:
Rekommenderat (för att få en meningsfull koppling PM ↔ Kraken):
- Skapa en mapping-fil på VPS, t.ex. `/etc/spelar_eu/market_map.json` baserat på `vps/market_map.example.json`.
- Sätt `MARKET_MAP_PATH=/etc/spelar_eu/market_map.json`

`fair_model` stöds i två enkla lägen:
- `constant`: `{"mode":"constant","p":0.5}`
- `linear_range`: mappar Kraken ref-pris linjärt till sannolikhet $[0,1]$ via `min_ref` och `max_ref`.

Valfritt (för att verifiera privata keys utan att skicka order):

## Output-filer
Agenten skriver (latest + rullande logg i vissa):
- `live_status.json`
- `edge_signals_live.csv`
- `kraken_futures_signals.csv` (append, keep-last)
- `pm_orders.csv` (append, keep-last)
- `kraken_futures_fills.csv` (tom i paper)
- `executed_trades.csv` (tom i paper)

## Kör lokalt (Windows)
- `python -m http.server 5173 --directory .\web`
- starta agent separat (om du vill testa lokalt):
  - sätt env i terminal
  - kör `python -m vps.vps_agent`

## Kör via VPS (Markov-style)
- Upload: `scripts/upload-to-vps.ps1`
- Install på VPS: `vps/install.sh`
- Deploy env + restart: `scripts/deploy-vps-env-and-restart.ps1`
- Sync data → deploy portal: `scripts/sync-vps-stats.ps1`
