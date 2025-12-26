# Auto-sync: VPS → web/data → FTP

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
