# Markov reference notes (for spelar_eu)

Syfte: spara “allt användbart” jag hittade i `C:\Users\lars-\OneDrive\Markov` som vi återanvänder i det här projektet.
Inga hemligheter/nycklar ska läggas i git – detta är bara en runbook + tekniska mönster.

## Markov top-level struktur (OneDrive)
- `C:\Users\lars-\OneDrive\Markov\MarkovTrading` – huvudsaklig kodbas
- `C:\Users\lars-\OneDrive\Markov\MarkovTrading_full_backup_...` – backup
- `C:\Users\lars-\OneDrive\Markov\scripts` – små Windows-scripts
- `C:\Users\lars-\OneDrive\Markov\ssh_keys` – SSH-nycklar (ska inte kopieras in i git)

## Markovs Kraken Futures API (det vi återanvänder)
Markov har en fungerande Kraken Futures-implementation i:
- `MarkovTrading/src/brokers/kraken_futures_client.py`

Viktiga observationer (för futures/derivatives API):
- Base URL:
  - live: `https://futures.kraken.com`
  - testnet: `https://demo-futures.kraken.com`
- Private auth headers:
  - `APIKey`, `Nonce`, `Authent`
- Signering (Markov-mönster):
  1. Ta endpoint och **strippa** prefix `/derivatives` från endpoint vid signering.
  2. Skapa sträng: `postdata + nonce + endpoint_clean`.
  3. SHA256 på den strängen.
  4. HMAC-SHA512 på SHA256-digest med **Base64-dekodat** API secret.
  5. Base64-encoda signaturen → `Authent`.
- Markov använder `application/x-www-form-urlencoded` för POST/PUT.

Endpoints Markov använder (v3):
- Public:
  - `/derivatives/api/v3/instruments`
  - `/derivatives/api/v3/tickers`
  - `/derivatives/api/v3/orderbook?symbol=...`
- Private:
  - `/derivatives/api/v3/accounts`
  - `/derivatives/api/v3/openpositions`
  - `/derivatives/api/v3/openorders`
  - `/derivatives/api/v3/fills`
  - `/derivatives/api/v3/sendorder` (vi kör paper först – inga live orders)

## Markov VPS-deploy flow (mönstret)
Markov har ett Windows→VPS-flow i:
- `MarkovTrading/deploy/upload_to_vps.ps1` – skapar tar.gz + `scp` till `/opt/markov-trading`
- `MarkovTrading/deploy/install.sh` – installerar dependencies på VPS
- `MarkovTrading/deploy/deploy_vps_env_and_restart.ps1` – laddar upp env + keys till `/etc/markovtrading/` och restartar systemd service
- `MarkovTrading/deploy/README_VPS.md` – runbook

Nyckelfiler/paths i Markov (konceptuellt):
- keys på VPS: `/etc/markovtrading/kraken_keys.local.json`
- env på VPS: `/etc/markovtrading/markov.env`
- services: `markov-futures` (systemd)

## Hur detta mappas till spelar_eu
Vi gör samma sak men med våra paths:
- kod på VPS: `/opt/spelar_eu/vps`
- output: `/opt/spelar_eu/vps/out` (synkas till `web/data/`)
- keys på VPS: `/etc/spelar_eu/kraken_keys.local.json`
- env på VPS: `/etc/spelar-agent.env`
- systemd service: `spelar-agent`

Se även `vps/README.md` i spelar_eu.
