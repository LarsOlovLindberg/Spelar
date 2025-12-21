# Spelar.eu – Småspel (MVP)

En enkel statisk sida med små spel och ett gemensamt reglage för svårighetsgrad.

Spel just nu:
- Gissa talet
- Reaktion
- Snabb matte
- Snake (pilar/WASD)
- Schack (klicka för att flytta)
- Bilbana (pilar/A/D)
- Space Invaders (pilar/A/D + mellanslag)

För drift/FTP-felsökning och kom-ihåg: se `docs/DRIFT.md`.

## Kör lokalt

Sidan ligger i `web/`.

Du kan dubbelklicka på `web\index.html`, men vissa webbläsare kan vara striktare med lokala filer.

Rekommenderat: starta en liten lokal server:

```powershell
Set-Location "c:\Users\lars-\OneDrive\spelar_eu"
python -m http.server 5173 --directory .\web
```

Öppna sedan `http://localhost:5173`.

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
git commit -m "Lägg till grafiska spel (Snake/Schack/Bilbana/Invaders)"
git branch -M main
git push -u origin main
```
