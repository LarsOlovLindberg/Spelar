param(
  [string]$VpsIp = "77.42.42.124",
  [string]$VpsUser = "root"
)

$ErrorActionPreference = "Stop"

$target = "$VpsUser@$VpsIp"

$remoteCmd = @'
set -e

echo '--- out dir (paper + draw + scan) ---'
ls -lah /opt/spelar_eu/vps/out | egrep 'pm_paper_(portfolio|positions|trades)|pm_(markets_index|scan_candidates|scanner_log)|sources_health|live_status' || true

echo '--- live_status (subset) ---'
(egrep '"strategy_mode"|"trading_mode"|pm_draw_|pm_scan_' /opt/spelar_eu/vps/out/live_status.json 2>/dev/null || true)

echo '--- paper portfolio ---'
(cat /opt/spelar_eu/vps/out/pm_paper_portfolio.json 2>/dev/null || true)

echo '--- paper positions head ---'
(head -n 15 /opt/spelar_eu/vps/out/pm_paper_positions.csv 2>/dev/null || true)

echo '--- paper trades tail ---'
(tail -n 25 /opt/spelar_eu/vps/out/pm_paper_trades.csv 2>/dev/null || true)

echo '--- sources_health ---'
(cat /opt/spelar_eu/vps/out/sources_health.json 2>/dev/null | head -n 60 || true)
'@

$remoteCmd = $remoteCmd -replace "`r", ""
ssh $target $remoteCmd | Out-Host
