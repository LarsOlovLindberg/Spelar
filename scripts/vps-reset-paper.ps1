param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$OutDir = "/opt/spelar_eu/vps/out",

  [Parameter(Mandatory = $false)]
  [string]$ServiceName = "spelar-agent",

  [Parameter(Mandatory = $false)]
  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  VPS reset paper state"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "OutDir: $OutDir"
Write-Host "Service: $ServiceName"

$remoteCmd = @"
set -e
sudo mkdir -p '$OutDir'
echo '--- before ---'
ls -lah '$OutDir' | egrep 'pm_paper_(portfolio|positions|trades)' || true

# Remove paper state so next ticks write clean pm_draw-only history
sudo rm -f '$OutDir/pm_paper_portfolio.json' '$OutDir/pm_paper_positions.csv' '$OutDir/pm_paper_trades.csv' || true

echo '--- after rm ---'
ls -lah '$OutDir' | egrep 'pm_paper_(portfolio|positions|trades)' || true
"@

$remoteCmd = $remoteCmd -replace "`r", ""
ssh $target $remoteCmd | Out-Host

if (-not $NoRestart) {
  ssh $target "sudo systemctl restart '$ServiceName'; sleep 2; sudo systemctl --no-pager --full status '$ServiceName' | head -80" | Out-Host
}

Write-Host "[OK] Paper state reset."