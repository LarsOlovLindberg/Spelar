param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $true)]
  [string]$MarketMapLocalPath,

  [Parameter(Mandatory = $false)]
  [string]$MarketMapRemotePath = "/opt/spelar_eu/vps/market_map.current.generated.json",

  [Parameter(Mandatory = $false)]
  [switch]$ResetPaperState
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $MarketMapLocalPath)) {
  throw "Market map not found: $MarketMapLocalPath"
}

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}
if (-not (Get-Command "scp" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'scp'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  VPS set market map"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "Local: $MarketMapLocalPath"
Write-Host "Remote: $MarketMapRemotePath"
Write-Host "ResetPaperState: $($ResetPaperState.IsPresent)"

# Upload to a tmp path then atomically move into place.
$tmp = "$MarketMapRemotePath.tmp"

# Note: In PowerShell, "$target:$tmp" can be parsed like drive syntax; use ${} to disambiguate.
$dest = "${target}:$tmp"
scp -o BatchMode=yes -o ConnectTimeout=10 $MarketMapLocalPath $dest | Out-Host

$remoteCmd = @'
set -e

# Ensure destination directory exists
sudo mkdir -p /opt/spelar_eu/vps

# Move into place
sudo mv 'TMP_PATH' 'REMOTE_PATH'

# Make readable
sudo chmod 0644 'REMOTE_PATH' || true

if [ "RESET_PAPER" = "1" ]; then
  echo '--- reset paper state ---'
  sudo rm -f /opt/spelar_eu/vps/out/pm_paper_portfolio.json || true
  sudo rm -f /opt/spelar_eu/vps/out/pm_paper_positions.csv || true
  sudo rm -f /opt/spelar_eu/vps/out/pm_paper_trades.csv || true
  sudo rm -f /opt/spelar_eu/vps/out/pm_paper_candidates.csv || true
fi

echo '--- restart ---'
sudo systemctl restart spelar-agent
sleep 2
sudo systemctl --no-pager --full status spelar-agent | sed -n '1,80p'
'@

$remoteCmd = $remoteCmd -replace "`r", ""
$remoteCmd = $remoteCmd -replace "REMOTE_PATH", $MarketMapRemotePath
$remoteCmd = $remoteCmd -replace "TMP_PATH", $tmp
$remoteCmd = $remoteCmd -replace "RESET_PAPER", $(if ($ResetPaperState) { "1" } else { "0" })

ssh -o BatchMode=yes -o ConnectTimeout=10 $target $remoteCmd | Out-Host

Write-Host "[OK] Market map uploaded and agent restarted."
