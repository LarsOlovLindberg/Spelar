param(
  [string]$VpsIp = "77.42.42.124",
  [string]$VpsUser = "root"
)

$ErrorActionPreference = "Stop"

$target = "$VpsUser@$VpsIp"

$remoteCmd = @'
set -e
ls -lah /opt/spelar_eu/vps/out | egrep 'pm_(markets_index|scan_candidates|scanner_log)|sources_health|live_status' || true

echo '--- candidates head ---'
(head -n 5 /opt/spelar_eu/vps/out/pm_scan_candidates.csv 2>/dev/null || true)

echo '--- markets index head ---'
(head -n 20 /opt/spelar_eu/vps/out/pm_markets_index.json 2>/dev/null || true)
'@

$remoteCmd = $remoteCmd -replace "`r", ""

ssh $target $remoteCmd
