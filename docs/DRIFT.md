# Drift / kom-ihåg – spelar.eu (Trading Portal)

## Struktur

- Publika filer ligger i `web/`:
  - `web/index.html` (portal/SPA)
  - `web/pages/*` (sidfragment som laddas via `fetch`)
  - `web/data/*` (valfria datafiler som portalen kan läsa)
  - `web/.htaccess` (prioriterar `index.html`)
  - `web/index.php` (fallback om servern alltid kör PHP först)

## Vanliga problem

### "Tom sida" fast filer är uppladdade

Orsak: många Apache-konfigurationer laddar `index.php` före `index.html`.

Åtgärder:
- Vi har lagt `web/.htaccess`:
  - `DirectoryIndex index.html index.php`
- Vi har lagt `web/index.php` som bara skickar vidare till `index.html`.

Om webbhotellet inte tillåter `.htaccess` (AllowOverride avstängt), måste du byta namn på/ta bort en gammal `index.php` eller se till att vår `index.php` ligger i samma mapp.

### Rätt webroot på servern

I FileZilla: gå till mappen där webben faktiskt körs (där `index.php`/`index.html` ska ligga).
Sätt `remote_path` i `ftp_config.local.json` till den mappen.

Exempel (enligt din vy):
- Om du står i `/web` och där finns `index.php`, då ska `remote_path` vara `/web`.

## Deploy via FTP

- Konfig:
  - Kopiera `ftp_config.example.json` -> `ftp_config.local.json`
  - Fyll i `username` och `password`
  - Sätt `remote_path` (vanligen `/web`)

- Deploy (Windows PowerShell kan blocka scripts):

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
# Om scripts är tillåtna:
.\scripts\deploy-ftp.ps1

# Om ExecutionPolicy stoppar script:
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-ftp.ps1
```

Scriptet laddar upp innehållet i `web/` till `remote_path`.

## VPS → web/data (snapshots)

- VPS-agenten skriver “latest” snapshots till `/opt/spelar_eu/vps/out/`.
- Windows-scriptet `scripts/sync-vps-stats.ps1` kopierar ner dessa till `web/data/` enligt mapping-filen `scripts/vps_sync_map_pm.json`.
- Nya viktiga snapshots (utöver CSV:erna) som nu också kan syncas:
  - `sources_health.json`
  - `polymarket_clob_public.json`
  - `kraken_futures_public.json`
  - `kraken_futures_private.json` (om API-nycklar finns på VPS)

Notera: UI-ändringar (`web/index.html`, `web/pages/*`) kräver full deploy av `web/` för att synas på sajten.

## Lokal test

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
python -m http.server 5173 --directory .\web
```

Öppna `http://localhost:5173/`.

## Säkerhet (viktigt)

- Lagra aldrig lösenord i git.
- `ftp_config.local.json` ska vara lokalt och ignoreras av `.gitignore`.
- Om ett lösenord råkat exponeras: byt/rotera det direkt.

## GitHub (commit + push)

Repo: `https://github.com/LarsOlovLindberg/Spelar`

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
git status
git add -A
git commit -m "Uppdatera trading-portalen"
git branch -M main
git push -u origin main
```
