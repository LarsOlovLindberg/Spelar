# spelar.eu – Trading Portal

Statisk portal (utan build-step) för strategi, backtest, live, risk, data och dokumentation.

- UI: `web/index.html` (router som laddar sidfragment från `web/pages/`)
- Datafiler: `web/data/` (valfritt; kan laddas via `data-csv` / `data-json` block)

För drift/FTP-felsökning och kom-ihåg: se `docs/DRIFT.md`.
Projektets inriktning: se `docs/INRIKTNING.md`.

## Kör lokalt

Sidan ligger i `web/`.

Du kan dubbelklicka på `web\index.html`, men vissa webbläsare kan vara striktare med lokala filer.

Rekommenderat: starta en liten lokal server:

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
python -m http.server 5173 --directory .\web
```

Öppna sedan `http://localhost:5173`.

## Viktigt: för att se ändringar på spelar.eu måste du deploya

När Copilot (eller du) ändrar filer i den här repot så syns det **bara lokalt** tills du laddar upp dem till webbhotellet.

- Ändringar i UI / sidor (t.ex. `web/index.html` eller `web/pages/*.html`) kräver **full deploy av `web/`**.
- Ändringar i snapshots (`web/data/*`) kan deployas som **data-only**.

Exempel: deploya hela `web/` (så att UI-ändringar syns):

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-ftp.ps1 -ConfigPath .\ftp_config.local.json
```

Exempel: sync från VPS + deploya bara `web/data/*`:

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\sync-vps-stats.ps1 -HostName "root@77.42.42.124" -DeployDataOnly -FtpConfigPath .\ftp_config.local.json
```

### One-command: deploy allt

Kör både full deploy av `web/` och VPS sync + data-only deploy i ett kommando:

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-all.ps1
```

Vill du bara köra en del:

```powershell
# Bara web/
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-all.ps1 -DeployWeb

# Bara VPS->data + deploy data-only
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-all.ps1 -DeployData
```

## Deploy via FTP

1. Skapa din lokala konfig (den ska inte committas):
   - Kopiera `ftp_config.example.json` -> `ftp_config.local.json`
   - Fyll i `username` och `password`

2. Kör deploy-scriptet:

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
.\scripts\deploy-ftp.ps1
```

Om du får fel om att script-körning är spärrad (ExecutionPolicy), kör så här:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\deploy-ftp.ps1
```

Scriptet laddar upp innehållet i `web/` till `remote_path`.

### Vanlig orsak till "tom sida"

- Om server-mappen du laddar upp till redan innehåller `index.php`, så visar Apache ofta **index.php före index.html**.
   - Den här repot inkluderar `web\.htaccess` som sätter `DirectoryIndex index.html index.php` (måste ligga i samma fjärrmapp som `index.php`/`index.html`).
   - Om din server inte tillåter `.htaccess`, behöver du istället byta namn på/ta bort `index.php` på servern.

### Hitta rätt `remote_path`

- I FileZilla: gå till den mapp som faktiskt är webroot (där din nuvarande `index.php` ligger).
- Sätt `remote_path` i `ftp_config.local.json` till just den mappen.
   - Exempel: om du är i `/web` och där finns `index.php`, då ska `remote_path` vara `/web`.

Testkörning utan uppladdning:

```powershell
.\scripts\deploy-ftp.ps1 -DryRun
```

## Backup (zip)

Skapar en zip i `backups/` med `web/` + `README.md` (ingen `ftp_config.local.json`).

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\backup.ps1
```

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
