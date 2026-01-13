param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  [Parameter(Mandatory = $false)]
  [ValidateSet("lead_lag", "pm_trend", "pm_draw", "fair_model")]
  [string]$StrategyMode = "pm_draw",

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
Write-Host "  VPS set strategy mode"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "STRATEGY_MODE=$StrategyMode"

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"
$lines += "if sudo grep -qE '^STRATEGY_MODE=' '$EnvPath'; then sudo sed -i 's/^STRATEGY_MODE=.*/STRATEGY_MODE=$StrategyMode/' '$EnvPath'; else echo 'STRATEGY_MODE=$StrategyMode' | sudo tee -a '$EnvPath' >/dev/null; fi"
$lines += "echo '--- env (strategy + trading subset, masked) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(STRATEGY_MODE=|TRADING_MODE=|PM_SCAN_|PM_DRAW_)' '$EnvPath' | sed -E 's/(PASS|KEY|SECRET)=.*/\\1=***MASKED***/g' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart '$ServiceName'"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status '$ServiceName' | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] Updated STRATEGY_MODE on VPS."