# Auto-sync: VPS → web/data → FTP

## Alternativ (VPS-only, ingen FTP/SFTP): VPS → HTTPS POST → web/data

Om webbhotellet blockerar FTP/SFTP från VPS (vanligt), kan du istället låta VPS:en POST:a snapshots till en PHP-endpoint på sajten.

1) Deploya endpointen till webbhotellet:
   - [web/trading/api/upload_stats.php](web/trading/api/upload_stats.php)

2) Skapa en hemlig nyckel på webbhotellet (server-side):
   - Antingen env-var `SPELAR_UPLOAD_API_KEY` (eller `MARKOV_UPLOAD_API_KEY`), eller
   - en fil bredvid endpointen: `web/trading/api/upload_api_key.local` (innehåll = nyckeln)

3) Sätt detta på VPS i `/etc/spelar-agent.env` och restart:a tjänsten:
   - `UPLOAD_URL=https://spelar.eu/trading/api/upload_stats.php`
   - `UPLOAD_API_KEY=...` (samma som server-side)

Valfritt (rekommenderat): skicka alla uppdaterade filer i en enda POST (zip-bundle):
- `UPLOAD_BUNDLE_ZIP=1`

Agenten kommer då att ladda upp allowlistade filer (t.ex. `live_status.json`, `edge_signals_live.csv`) direkt till `web/data/` via HTTPS.

## Rekommenderat (ingen lokal dator): VPS → FTP direkt

Om du vill att allt ska gå "via VPS" (utan att din Windows-dator kör SCP/FTP), så kan agenten på VPS:en ladda upp portal-snapshots direkt till webbhotellets FTP.

Detta kräver att FTP-credentials finns på VPS:en (i `/etc/spelar-agent.env`) och att `spelar-agent` är startad.

### Hosting-gotcha (viktigt)

Även om allt är korrekt konfigurerat kan vissa webbhotell **blockera FTP-inloggning från “server-IP”** (VPS) men tillåta FTP från din egen dator.

Symptom:
- `spelar-agent` loggar `ftp upload failed` och ofta `EOFError` eller att anslutningen “droppas” vid login.
- Filer i `/opt/spelar_eu/vps/out/` uppdateras, men `/data/*` på sajten blir kvar på gamla timestamps.

Åtgärd:
- Rekommenderat: byt till **SFTP (port 22)** om det finns (spelar.eu svarar med `SSH-2.0-mod_sftp`). Det undviker ofta “FTP droppas vid login”-problemet.
- Obs: SFTP kan kräva att webbhotellet **aktiverar SSH/SFTP** för kontot eller att du använder en **separat SFTP/SSH-användare** (inte alltid samma som FTP-user).
- Alternativt: be webbhotellet att **whitelista VPS:ens utgående IP** (t.ex. `77.42.42.124`) för FTP-kontot.
- Sista fallback: använd “Windows → VPS → FTP”-flödet (SCP + `deploy-ftp.ps1`).

### Aktivera (engångssetup)

Kör från repo-root (på din dator) för att kopiera in FTP-credentials till VPS:en och restart:a tjänsten:

```powershell
./scripts/vps-enable-ftp-upload.ps1 -VpsIp "77.42.42.124" -VpsUser "root" -ServiceName "spelar-agent"
```

Som standard sätter scriptet `FTP_PROTOCOL=sftp` och `FTP_PORT=22`. Om du vill tvinga klassisk FTP:

```powershell
./scripts/vps-enable-ftp-upload.ps1 -Protocol ftp -Port 21
```

### Verifiera

```powershell
./scripts/vps-check-ftp-upload.ps1 -VpsIp "77.42.42.124" -VpsUser "root" -ServiceName "spelar-agent"
```

Tips: Portalen visar färskhet via `Last-Modified` på `/data/*`.

### Viktigt

- När VPS-direkt-FTP är aktivt ska du INTE köra lokal autosync/Task Scheduler. Annars kan det bli "dragkamp" om vilka snapshots som senast laddats upp.

Målet: få portalen att alltid visa färska (LIVE) siffror genom att:
1) hämta snapshot-filer från VPS (SCP),
2) lägga dem i `web/data/`,
3) ladda upp bara `web/data/` till webbhotellet (FTP).

## Förutsättningar
- Windows OpenSSH Client (för `scp`). Testa i PowerShell: `scp -V`
- `ftp_config.local.json` korrekt ifyllt (host + remote_path + username/password).
- VPS skriver snapshots i `/opt/spelar_eu/vps/out` (default i script).

## En gång (manuell körning)
Kör i repo-root:

```powershell
# Om du vill ange användare separat
./scripts/sync-vps-stats.ps1 -HostName "77.42.42.124" -SshUser "root" -DeployDataOnly

# Alternativt kan HostName innehålla user@host
./scripts/sync-vps-stats.ps1 -HostName "root@77.42.42.124" -DeployDataOnly
```

Default mapping för PM↔Kraken ligger i `scripts/vps_sync_map_pm.json` och syncar bl.a:
- `live_status.json`
- `polymarket_status.json`
- `pm_scanner_log.csv`
- `edge_signals_live.csv`
- `pm_orders.csv`
- `kraken_futures_signals.csv`
- `kraken_futures_fills.csv`
- `executed_trades.csv`

## Deploya kod till VPS (agenten)
Autosync-scriptet flyttar bara snapshot-filer. Om du har ändrat agentkoden under `vps/` (t.ex. connectors/strategier), deploya den till VPS och restart:a tjänsten:

```powershell
./scripts/upload-to-vps.ps1 -VpsIp "77.42.42.124" -VpsUser "root" -RemoteRoot "/opt/spelar_eu" -ServiceName "spelar-agent"
```

- Scriptet gör upload av hela `vps/` och restartar `spelar-agent` som default.
- Om du vill ladda upp utan restart: lägg till `-NoRestart`.

## Kontinuerligt (watch/loop)

```powershell
./scripts/sync-vps-stats.ps1 -HostName "root@77.42.42.124" -DeployDataOnly -Watch -IntervalSeconds 60
```

- Stoppa med `Ctrl+C`.
- `-DeployDataOnly` laddar upp endast de syncade snapshot-filerna (snabbt).
- Utan `-DeployDataOnly` laddas hela `web/` upp (långsammare).

## Windows Task Scheduler (rekommenderat)
Skapa en schemalagd task som kör varje minut.

### Snabbast: skapa task via script

```powershell
./scripts/register-autosync-task.ps1 -HostName "root@77.42.42.124" -EveryMinutes 1 -RunNow
```

För att köra även när du är utloggad (kräver admin):

```powershell
./scripts/register-autosync-task.ps1 -HostName "root@77.42.42.124" -EveryMinutes 1 -RunAsSystem -Highest
```

Ta bort task:

```powershell
./scripts/register-autosync-task.ps1 -Delete
```

1) Öppna Task Scheduler → “Create Task…”
2) **General**
   - Run whether user is logged on or not
   - Run with highest privileges (om din maskin kräver det)
3) **Triggers**
   - New… → Begin the task: “On a schedule” → Daily → Repeat task every: 1 minute → for a duration of: Indefinitely
4) **Actions**
   - New… → Action: Start a program
   - Program/script: `powershell.exe`
   - Add arguments:
     ```
     -NoProfile -ExecutionPolicy Bypass -File "C:\Users\lars-\OneDrive\spelar_eu\scripts\sync-vps-stats.ps1" -HostName "root@77.42.42.124" -DeployDataOnly
     ```
   - Start in:
     ```
     C:\Users\lars-\OneDrive\spelar_eu
     ```

Tips:
- Om du vill köra “watch”-läge i task, använd `-Watch -IntervalSeconds 60` och sätt triggern att köra “At startup” istället.

## Felsökning
- Logg: `./.autosync/autosync.log` (skrivs även när tasken kör som SYSTEM).
- SCP auth: se till att du kan köra `scp root@77.42.42.124:/opt/spelar_eu/vps/out/live_status.json .` utan prompt (SSH-key rekommenderas).
- FTP fel: kör `./scripts/deploy-ftp.ps1 -DryRun` för att verifiera anslutning.
- Portal visar “LIVE: …”: statusraden baseras på webbserverns `Last-Modified` för filerna i `/data/`.

Vanliga fel:
- `No such file or directory` i loggen betyder att `RemoteRoot` eller `MappingFile` inte matchar vad som faktiskt finns på VPS.
   - Om VPS:en kör en annan pipeline, registrera tasken med rätt mapping, t.ex.:
      - `./scripts/register-autosync-task.ps1 -HostName "root@77.42.42.124" -EveryMinutes 1 -RunAsSystem -Highest -MappingFile "C:\Users\lars-\OneDrive\spelar_eu\scripts\vps_sync_map.json" -RunNow`
