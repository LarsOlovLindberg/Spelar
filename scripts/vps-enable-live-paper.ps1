param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  [Parameter(Mandatory = $false)]
  [string]$ServiceName = "spelar-agent",

  # Paper execution controls (still uses live market data)
  [Parameter(Mandatory = $false)]
  [int]$MaxOrdersPerTick = 3,

  [Parameter(Mandatory = $false)]
  [double]$OrderSizeShares = 5,

  [Parameter(Mandatory = $false)]
  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  VPS enable live-data paper mode"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "TRADING_MODE=paper (never live orders)"
Write-Host "PM_ODDS_TEST_MODE=0 (real odds band)"
Write-Host "PM_MAX_ORDERS_PER_TICK=$MaxOrdersPerTick  PM_ORDER_SIZE_SHARES=$OrderSizeShares"

$vars = @{}
$vars["TRADING_MODE"] = "paper"
$vars["POLY_LIVE_CONFIRM"] = "NO"
$vars["PM_ODDS_TEST_MODE"] = "0"
$vars["PM_MAX_ORDERS_PER_TICK"] = "$MaxOrdersPerTick"
$vars["PM_ORDER_SIZE_SHARES"] = "$OrderSizeShares"

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"

foreach ($k in ($vars.Keys | Sort-Object)) {
  $v = [string]$vars[$k]
  # Escape special chars for sed replacement + bash single-quoted echo.
  $vSed = ($v -replace "\\", "\\\\" -replace "&", "\\&" -replace "\|", "\\|")
  $vEcho = ("$k=$v" -replace "'", "'\\''")
  $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's|^${k}=.*|${k}=${vSed}|' '$EnvPath'; else echo '$vEcho' | sudo tee -a '$EnvPath' >/dev/null; fi"
}

$lines += "echo '--- env (paper safety subset) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(TRADING_MODE=|POLY_LIVE_CONFIRM=|PM_ODDS_TEST_MODE=|PM_MAX_ORDERS_PER_TICK=|PM_ORDER_SIZE_SHARES=)' '$EnvPath' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart '$ServiceName'"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status '$ServiceName' | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] VPS is now running live-data + paper execution (service: $ServiceName)."
