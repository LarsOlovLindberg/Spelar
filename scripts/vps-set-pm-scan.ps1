param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  # Scan behavior
  [Parameter(Mandatory = $false)]
  [string]$Search = "bitcoin",

  [Parameter(Mandatory = $false)]
  [int]$Limit = 200,

  [Parameter(Mandatory = $false)]
  [int]$Pages = 5,

  [Parameter(Mandatory = $false)]
  [int]$IntervalSeconds = 300,

  [Parameter(Mandatory = $false)]
  [int]$OrderbookSample = 200,

  [Parameter(Mandatory = $false)]
  [string]$Order = "createdAt",

  [Parameter(Mandatory = $false)]
  [ValidateSet("asc", "desc")]
  [string]$Direction = "desc",

  [Parameter(Mandatory = $false)]
  [int]$Offset = 0,

  [Parameter(Mandatory = $false)]
  [switch]$ActiveOnly,

  [Parameter(Mandatory = $false)]
  [switch]$BinaryOnly,

  # For safety: keep trading usage off by default
  [Parameter(Mandatory = $false)]
  [switch]$UseForTrading,

  [Parameter(Mandatory = $false)]
  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

# Clamp safety
$Limit = [Math]::Max(1, [Math]::Min($Limit, 500))
$Pages = [Math]::Max(1, [Math]::Min($Pages, 20))
$IntervalSeconds = [Math]::Max(10, [Math]::Min($IntervalSeconds, 3600))
$OrderbookSample = [Math]::Max(0, [Math]::Min($OrderbookSample, 2500))

$vars = @{}
$vars["PM_SCAN_ENABLE"] = "1"
$vars["PM_SCAN_INTERVAL_S"] = "$IntervalSeconds"
$vars["PM_SCAN_LIMIT"] = "$Limit"
$vars["PM_SCAN_PAGES"] = "$Pages"
$vars["PM_SCAN_ORDERBOOK_SAMPLE"] = "$OrderbookSample"
$vars["PM_SCAN_ORDER"] = "$Order"
$vars["PM_SCAN_DIRECTION"] = "$Direction"
$vars["PM_SCAN_OFFSET"] = "$([Math]::Max(0, $Offset))"
$vars["PM_SCAN_ACTIVE_ONLY"] = $(if ($ActiveOnly) { "1" } else { "1" })
$vars["PM_SCAN_BINARY_ONLY"] = $(if ($BinaryOnly) { "1" } else { "1" })
$vars["PM_SCAN_USE_FOR_TRADING"] = $(if ($UseForTrading) { "1" } else { "0" })
$vars["PM_SCAN_TRADE_MAX_MARKETS"] = "20"

if ($Search -and $Search.Trim().Length -gt 0) {
  $vars["PM_SCAN_SEARCH"] = $Search.Trim()
} else {
  # Clear search when empty
  $vars["PM_SCAN_SEARCH"] = ""
}

Write-Host "==============================="
Write-Host "  VPS set PM scan"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "Search: $Search"
Write-Host "Limit: $Limit  Pages: $Pages  OrderbookSample: $OrderbookSample"
Write-Host "UseForTrading: $($UseForTrading.IsPresent)"

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"
foreach ($k in ($vars.Keys | Sort-Object)) {
  $v = $vars[$k]
  # If v is empty, delete the var line (so default behavior applies)
  if ($v -eq "") {
    $lines += "sudo sed -i '/^${k}=.*/d' '$EnvPath' || true"
  } else {
    $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's/^${k}=.*/${k}=${v}/' '$EnvPath'; else echo '${k}=${v}' | sudo tee -a '$EnvPath' >/dev/null; fi"
  }
}
$lines += "echo '--- env (pm_scan subset, masked) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(PM_SCAN_|TRADING_MODE=|STRATEGY_MODE=)' '$EnvPath' | sed -E 's/(PASS|KEY|SECRET)=.*/\\1=***MASKED***/g' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart spelar-agent"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status spelar-agent | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] Updated PM_SCAN_* on VPS."