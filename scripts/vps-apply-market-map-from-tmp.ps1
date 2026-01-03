param(
  [string]$VpsIp = "77.42.42.124",
  [string]$VpsUser = "root",
  [string]$TmpPath = "/tmp/market_map.current.generated.json",
  [string]$RemotePath = "/opt/spelar_eu/vps/market_map.current.generated.json"
)

$ErrorActionPreference = "Stop"

$target = "$VpsUser@$VpsIp"

$remoteCmd = @'
set -e

if [ ! -f "TMP_PATH" ]; then
  echo "missing tmp file: TMP_PATH"
  exit 2
fi

sudo mkdir -p /opt/spelar_eu/vps
sudo mv "TMP_PATH" "REMOTE_PATH"
sudo chmod 0644 "REMOTE_PATH" || true

echo '--- restart ---'
sudo systemctl restart spelar-agent
sleep 2
sudo systemctl --no-pager --full status spelar-agent | sed -n '1,80p'
'@

$remoteCmd = $remoteCmd -replace "`r", ""
$remoteCmd = $remoteCmd -replace "TMP_PATH", $TmpPath
$remoteCmd = $remoteCmd -replace "REMOTE_PATH", $RemotePath

ssh -o BatchMode=yes -o ConnectTimeout=10 $target $remoteCmd | Out-Host
