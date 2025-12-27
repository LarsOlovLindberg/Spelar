# Runbook: PM (CLOB) ↔ Kraken Futures (paper-first)

Det här dokumentet beskriver den minsta "paper"-kedjan:
1) hämta Polymarket CLOB orderbook (public)
2) hämta Kraken Futures ticker (public)
3) om edge passerar tröskel: skriv en signal + en “paper order log” till CSV
4) portalen renderar CSV/JSON från `web/data/`

Obs: Agenten stödjer nu även `lead_lag`-strategi där Kraken spot leder och Polymarket (CLOB) släpar. Den skriver samma portal-filer (`pm_paper_*`, `edge_signals_live.csv`, `pm_orders.csv`) så att spelar.eu kan visa paper-saldo, trades och edge i realtid.

## Konfig
På VPS (eller lokalt) skapas env baserat på `vps/systemd/spelar-agent.env.example`.

Minst:
Rekommenderat (för att få en meningsfull koppling PM ↔ Kraken):
- Skapa en mapping-fil på VPS, t.ex. `/etc/spelar_eu/market_map.json` baserat på `vps/market_map.example.json`.
- Sätt `MARKET_MAP_PATH=/etc/spelar_eu/market_map.json`

`fair_model` stöds i två enkla lägen:
- `constant`: `{"mode":"constant","p":0.5}`
- `linear_range`: mappar Kraken ref-pris linjärt till sannolikhet $[0,1]$ via `min_ref` och `max_ref`.

### Lead–lag (Kraken spot → PM CLOB) – rekommenderad paper-kedja

Sätt:
- `STRATEGY_MODE=lead_lag`
- `MARKET_MAP_PATH=/etc/spelar_eu/market_map.json` (rekommenderat)

Viktiga env-var (minsta):
- `KRAKEN_SPOT_PAIR=XBTUSD`
- `LEAD_LAG_SIDE=YES` (eller `NO`)
- `LEAD_LAG_LOOKBACK_POINTS=6` (t.ex. 6 ticks)
- `LEAD_LAG_SPOT_MOVE_MIN_PCT=0.25`
- `LEAD_LAG_EDGE_MIN_PCT=0.20`
- `LEAD_LAG_EDGE_EXIT_PCT=0.05`
- `LEAD_LAG_MAX_HOLD_SECS=180`

Risk/sizing (orderbok-band):
- `LEAD_LAG_ENABLE_ORDERBOOK_SIZING=1`
- `LEAD_LAG_SLIPPAGE_CAP=0.01`
- `LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY=0.10`
- `LEAD_LAG_HARD_CAP_USDC=2000`

Stabilitet/säkerhet:
- `FRESHNESS_MAX_AGE_SECS=60` (krav på fräscha inputs innan entry/exit)

Minsta market_map (exempel):
```json
{
  "version": 1,
  "markets": [
    {
      "name": "BTC reach 100k (example)",
      "polymarket": {
        "market_url": "will-bitcoin-hit-100000-by-december-31-2025",
        "outcome": "Yes"
      },
      "kraken_spot": { "pair": "XBTUSD" }
    }
  ]
}
```

Notera:
- Du kan även sätta `polymarket.clob_token_id` direkt istället för `market_url`.
- Utan `polymarket.outcome` väljer agenten default utifrån `LEAD_LAG_SIDE`.

Valfritt (för att verifiera privata keys utan att skicka order):

## Output-filer
Agenten skriver (latest + rullande logg i vissa):
- `live_status.json`
- `edge_signals_live.csv`
- `kraken_futures_signals.csv` (append, keep-last)
- `pm_orders.csv` (append, keep-last)
- `kraken_futures_fills.csv` (tom i paper)
- `executed_trades.csv` (tom i paper)

Lead–lag skriver även paper-portfolio som portalen visar:
- `pm_paper_portfolio.json`
- `pm_paper_positions.csv`
- `pm_paper_trades.csv`
- `pm_paper_candidates.csv` (debug: varför trades triggar/inte triggar)

## Kör lokalt (Windows)
- `python -m http.server 5173 --directory .\web`
- starta agent separat (om du vill testa lokalt):
  - sätt env i terminal
  - kör `python -m vps.vps_agent`

## Kör via VPS (Markov-style)
- Upload + restart (default): `scripts/upload-to-vps.ps1`
- Install på VPS: `vps/install.sh`
- Deploy env + restart: `scripts/deploy-vps-env-and-restart.ps1`
- Sync data → deploy portal: `scripts/sync-vps-stats.ps1`

Tips:
- Om du bara vill ladda upp kod utan restart: kör `scripts/upload-to-vps.ps1 -NoRestart`

### Verifiera i portalen (spelar.eu)
- Öppna `#/start/overview` och kontrollera att `pm_paper_portfolio.json`, `pm_paper_positions.csv`, `pm_paper_trades.csv` får låg ålder.
- Öppna `#/pm/edge` och kontrollera att `edge_signals_live.csv` uppdateras.
- Om inga trades sker: titta i `pm_paper_candidates.csv` (decision/reason) och justera `LEAD_LAG_*` trösklar.
