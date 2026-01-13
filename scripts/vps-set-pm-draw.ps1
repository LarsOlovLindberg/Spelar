param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  [Parameter(Mandatory = $false)]
  [double]$EdgeMinPct = 2.0,

  [Parameter(Mandatory = $false)]
  [double]$EdgeExitPct = 0.5,

  [Parameter(Mandatory = $false)]
  [double]$MaxPrice = 0.45,

  [Parameter(Mandatory = $false)]
  [switch]$Require3Way,

  [Parameter(Mandatory = $false)]
  [double]$FavMin = 0.35,

  [Parameter(Mandatory = $false)]
  [double]$FavMax = 0.65,

  [Parameter(Mandatory = $false)]
  [double]$BaselineP = 0.28,

  [Parameter(Mandatory = $false)]
  [double]$BookProbMult = 0.95,

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
Write-Host "  VPS set pm_draw params"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "PM_DRAW_EDGE_MIN_PCT=$EdgeMinPct"
Write-Host "PM_DRAW_EDGE_EXIT_PCT=$EdgeExitPct"
Write-Host "PM_DRAW_MAX_PRICE=$MaxPrice"
Write-Host "PM_DRAW_REQUIRE_3WAY=$($Require3Way.IsPresent)"
Write-Host "PM_DRAW_FAV_MIN=$FavMin  PM_DRAW_FAV_MAX=$FavMax"
Write-Host "PM_DRAW_BASELINE_P=$BaselineP  PM_DRAW_BOOK_PROB_MULT=$BookProbMult"

$vars = @{
  "PM_DRAW_EDGE_MIN_PCT"     = "$EdgeMinPct"
  "PM_DRAW_EDGE_EXIT_PCT"    = "$EdgeExitPct"
  "PM_DRAW_MAX_PRICE"        = "$MaxPrice"
  "PM_DRAW_REQUIRE_3WAY"     = $(if ($Require3Way) { "1" } else { "0" })
  "PM_DRAW_FAV_MIN"          = "$FavMin"
  "PM_DRAW_FAV_MAX"          = "$FavMax"
  "PM_DRAW_BASELINE_P"       = "$BaselineP"
  "PM_DRAW_BOOK_PROB_MULT"   = "$BookProbMult"
}

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"
foreach ($k in ($vars.Keys | Sort-Object)) {
  $v = [string]$vars[$k]
  $vSed = ($v -replace "\\", "\\\\" -replace "&", "\\&" -replace "\|", "\\|")
  $vEcho = ("$k=$v" -replace "'", "'\\''")
  $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's|^${k}=.*|${k}=${vSed}|' '$EnvPath'; else echo '$vEcho' | sudo tee -a '$EnvPath' >/dev/null; fi"
}

$lines += "echo '--- env (pm_draw subset) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(STRATEGY_MODE=|TRADING_MODE=|PM_DRAW_)' '$EnvPath' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart '$ServiceName'"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status '$ServiceName' | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] Updated PM_DRAW_* on VPS."