param(
  [string]$VpsIp = "77.42.42.124",
  [string]$VpsUser = "root",
  [int]$JournalLines = 120
)

$ErrorActionPreference = "Stop"

$target = "$VpsUser@$VpsIp"

$remoteCmd = @'
set -e

echo '--- systemd status (head) ---'
sudo systemctl --no-pager --full status spelar-agent | sed -n '1,120p' || true

echo '--- recent journal (tail) ---'
sudo journalctl -u spelar-agent -n JOURNAL_LINES --no-pager | tail -n JOURNAL_LINES || true

echo '--- out dir (pm + health) ---'
ls -lah /opt/spelar_eu/vps/out | egrep 'pm_(markets_index|scan_candidates|scanner_log|paper_portfolio|paper_positions|paper_trades)|sources_health|live_status|lead_lag_health|polymarket_(status|clob_public)' || true

echo '--- latest out mtimes ---'
for f in /opt/spelar_eu/vps/out/sources_health.json /opt/spelar_eu/vps/out/live_status.json /opt/spelar_eu/vps/out/lead_lag_health.json /opt/spelar_eu/vps/out/polymarket_status.json /opt/spelar_eu/vps/out/polymarket_clob_public.json; do
  if [ -f "$f" ]; then
    stat -c '%y  %s  %n' "$f" || true
  else
    echo "MISSING  $f"
  fi
done
'@

$remoteCmd = $remoteCmd -replace "JOURNAL_LINES", "$JournalLines"
$remoteCmd = $remoteCmd -replace "`r", ""

ssh -o BatchMode=yes -o ConnectTimeout=10 $target $remoteCmd
