# Runbook: PM (CLOB) ↔ Kraken Futures (paper-first)

Det här dokumentet beskriver den minsta "paper"-kedjan:
1) hämta Polymarket CLOB orderbook (public)
2) hämta Kraken Futures ticker (public)
3) om edge passerar tröskel: skriv en signal + en “paper order log” till CSV
4) portalen renderar CSV/JSON från `web/data/`

Obs: Agenten stödjer flera strategier:
- `lead_lag`: extern referens (Kraken spot) leder, Polymarket (CLOB) släpar.
- `pm_trend`: Polymarket-only trend.
- `pm_draw`: Polymarket-only draw-value (baseline vs Draw-pris).

Alla skriver samma portal-filer (`pm_paper_*`, `edge_signals_live.csv`, `pm_orders.csv`) så spelar.eu kan visa paper-saldo, trades och beslut i realtid.

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

Valfritt: scale-in (mer “aktion” i paper)
- `LEAD_LAG_SCALE_ON_ODDS_CHANGE_PCT=0.40`
- `LEAD_LAG_SCALE_COOLDOWN_S=20`
- `LEAD_LAG_SCALE_MAX_ADDS=3`
- `LEAD_LAG_SCALE_SIZE_MULT=0.50`
- `LEAD_LAG_SCALE_MAX_TOTAL_SHARES=50`

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

Schema-notis:
- `pm_paper_positions.csv` har extra kolumner för scale-state: `adds`, `last_mid`, `last_scale_at`.

## Kör lokalt (Windows)
- `python -m http.server 5173 --directory .\web`
- starta agent separat (om du vill testa lokalt):
  - sätt env i terminal
  - kör `python -m vps.vps_agent` (helst som modul; undvik `python .\vps\vps_agent.py`)

### pm_draw (Draw value) – snabbstart (paper)

Minsta:
- `STRATEGY_MODE=pm_draw`
- `TRADING_MODE=paper`

Baseline:
- Rekommenderat: `PM_DRAW_BASELINE_FILE=/etc/spelar_eu/pm_draw_baseline.csv` (på VPS) med kolumner `slug` och `draw_odds` eller `draw_prob`.
- Fallback: `PM_DRAW_BASELINE_P=0.28`

Trösklar (rimlig start):
- `PM_DRAW_EDGE_MIN_PCT=2.0`
- `PM_DRAW_EDGE_EXIT_PCT=0.5`
- `PM_DRAW_MAX_PRICE=0.45`
- `PM_DRAW_REQUIRE_3WAY=1`

Universe discovery (om du inte vill hårdkoda market_map):
- `PM_SCAN_ENABLE=1`
- `PM_SCAN_USE_FOR_TRADING=1`
- Låt `PM_SCAN_BINARY_ONLY` vara av (default i pm_draw), annars hittar du inga 3-way.

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
