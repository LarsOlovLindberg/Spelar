# VPS agent (pm ↔ Kraken) – minimal scaffold

Mål: kör en liten agent på VPS som
1) hämtar marknadsdata (Polymarket + Kraken)
2) räknar edge (stub först)
3) skriver snapshots till CSV/JSON
4) (valfritt) laddar upp `web/data/` via FTP till spelar.eu

## Viktigt
- Lägg aldrig API-nycklar i repot eller i `web/`.
- Portalen är statisk: den läser bara exporterade filer från `web/data/`.
- Default är **paper** (ingen live-exekvering). För live krävs explicit `TRADING_MODE=live`.

## Output-filer (det portalen kan visa)
Agenten kan generera dessa “latest”-filer:
- `live_status.json`
- `sources_health.json`
- `deribit_options_public.json` (om någon marknad använder `fair_model.mode=deribit_rn`)
- `edge_signals_live.csv`
- `polymarket_status.json`
- `polymarket_clob_public.json`
- `pm_open_orders.json` (om Polymarket L2 auth är konfigurerad)
- `pm_orders.csv`
- `pm_paper_portfolio.json` (paper-saldo/equity + PnL)
- `pm_paper_positions.csv` (paper-öppna positioner, mark-to-market)
- `pm_paper_trades.csv` (paper-trades)
- `kraken_futures_public.json`
- `kraken_futures_private.json` (om API-nycklar finns)
- `kraken_futures_signals.csv`
- `kraken_futures_fills.csv`
- `executed_trades.csv`

## Polymarket live trading (förberett, men hårt gated)

För att kunna lägga riktiga ordrar på Polymarket krävs:
- `py-clob-client` installerat på VPS (se `vps/requirements.txt`)
- L1 (private key) + L2 (apiKey/secret/passphrase)
- `TRADING_MODE=live` och `POLY_LIVE_CONFIRM=YES`

Utan dessa gates kör agenten paper (loggar signaler men skickar inga ordrar).

## Options → risk-neutral fair_p (Deribit, public)

Om du vill att edge ska baseras på options istället för en enkel price-proxy, använd någon av:
- `fair_model.mode=deribit_rn` – terminal-sannolikhet (t.ex. BTC > 100k *vid expiry*)
- `fair_model.mode=deribit_touch` – touch/barrier (t.ex. BTC *når* 100k eller *dippar till* 86k någon gång före expiry)
Detta gör inga trades och kräver inga nycklar (Deribit public API).

Paper-portfolio:
- Default start: `PAPER_START_BALANCE_USD=1000`
- Agenten gör endast paper-trades när `TRADING_MODE=paper` (eller när live-client saknas) och uppdaterar snapshots ovan.

Testläge (för att snabbt se trades i portalen):
- Sätt `PM_ODDS_TEST_MODE=1` för att tillfälligt widen:a oddsbandet till ca `1.01–10.0`.
- Kombinera ofta med en lägre `EDGE_THRESHOLD` (t.ex. `0` eller `0.005`) om du bara vill verifiera pipeline/UI.

## Polymarket clob_token_id (automatiskt)

Du kan antingen sätta `polymarket.clob_token_id` direkt, eller låta agenten slå upp den automatiskt via
Polymarkets publika metadata (Gamma API):
- Sätt `polymarket.market_url` (t.ex. `https://polymarket.com/market/<slug>`) eller `polymarket.market_slug`.
- Valfritt: sätt `polymarket.outcome` till `Yes`/`No` (eller exakt outcome-label) för att välja sida.

Auto-val (utan `polymarket.outcome`) stöds bara för vanliga binary YES/NO-marknader när `fair_model.mode=deribit_touch`:
- Om frågan är av typen “Will BTC reach … before …?” väljer agenten `Yes` för `touch_above` och `No` för `no_touch_above`.
- Om frågan är negerad (“Will BTC NOT reach …?”) vänds logiken.

Om agenten inte kan inferera outcome kommer den kräva att du sätter `polymarket.outcome`.

## Install (VPS)
```bash
cd /opt/spelar_eu/vps
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

## Konfig (env)
Skapa en env-fil på VPS (ex `/etc/spelar-agent.env`) baserat på `vps/systemd/spelar-agent.env.example`.

## VPS quickstart (Markov-style)

Detta är den snabbaste “Markov-lika” vägen att få agenten igång som en systemd service.

På Windows (laddar upp env + systemd unit och restartar):
```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-vps-env-and-restart.ps1 -VpsIp 77.42.42.124 -VpsUser root
```

På VPS (edita env och restart):
```bash
sudo nano /etc/spelar-agent.env
sudo systemctl daemon-reload
sudo systemctl restart spelar-agent
sudo systemctl --no-pager --full status spelar-agent
```

Kolla loggar (om något inte skriver snapshots):
```bash
sudo journalctl -u spelar-agent -n 200 --no-pager
```

Verifiera att agenten faktiskt skriver output (default enligt env-mallen):
```bash
ls -la /opt/spelar_eu/vps/out | head
cat /opt/spelar_eu/vps/out/live_status.json
cat /opt/spelar_eu/vps/out/sources_health.json
```

Killswitch (stoppar live-actions och kan trigga cancel-all när live-client finns):
```bash
sudo touch /opt/spelar_eu/vps/out/KILLSWITCH
```
Ta bort för att släppa igen:
```bash
sudo rm -f /opt/spelar_eu/vps/out/KILLSWITCH
```

## Kör manuellt
```bash
cd /opt/spelar_eu
. .venv/bin/activate
python -m vps.vps_agent
```

## systemd
Se mallar i `vps/systemd/`.

## Markov-style workflow (Windows → VPS → portal)
Om du vill köra samma typ av flöde som i Markov:
1) Ladda upp agenten till VPS: `scripts/upload-to-vps.ps1`
2) På VPS: kör `vps/install.sh`
3) Skicka env + (valfritt) `kraken_keys.local.json` och starta service: `scripts/deploy-vps-env-and-restart.ps1`
4) Hämta “latest” filer från VPS till `web/data/`: `scripts/sync-vps-stats.ps1`
5) Deploy statiska siten: `scripts/deploy-ftp.ps1`
