param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  [Parameter(Mandatory = $false)]
  [ValidateSet("balanced","profit","proof")]
  [string]$Profile = "profit",

  # Optional overrides (numeric env values)
  [double]$LookbackPoints = [double]::NaN,
  [double]$SpotMoveMinPct = [double]::NaN,
  [double]$EdgeMinPct = [double]::NaN,
  [double]$EdgeExitPct = [double]::NaN,
  [int]$MaxHoldSecs = -1,
  [double]$PmStopPct = [double]::NaN,
  [double]$AvoidPriceBelow = [double]::NaN,
  [double]$AvoidPriceAbove = [double]::NaN,

  [double]$NetEdgeMinPct = [double]::NaN,
  [double]$SpreadCostCapPct = [double]::NaN,
  [double]$MinTradeNotionalUsdc = [double]::NaN,

  [double]$SlippageCap = [double]::NaN,
  [double]$MaxFractionOfBandLiquidity = [double]::NaN,
  [double]$HardCapUsdc = [double]::NaN,

  [double]$FreshnessMaxAgeSecs = [double]::NaN,

  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

# Profiles
$profileVars = @{}

switch ($Profile) {
  "balanced" {
    # Close to docs/RUNBOOK defaults: trades happen, but not in 'proof-mode'.
    $profileVars["LEAD_LAG_LOOKBACK_POINTS"] = "6"
    $profileVars["LEAD_LAG_SPOT_MOVE_MIN_PCT"] = "0.25"
    $profileVars["LEAD_LAG_EDGE_MIN_PCT"] = "0.20"
    $profileVars["LEAD_LAG_EDGE_EXIT_PCT"] = "0.05"
    $profileVars["LEAD_LAG_MAX_HOLD_SECS"] = "180"
    $profileVars["LEAD_LAG_PM_STOP_PCT"] = "0.25"
    $profileVars["LEAD_LAG_AVOID_PRICE_BELOW"] = "0.02"
    $profileVars["LEAD_LAG_AVOID_PRICE_ABOVE"] = "0.90"

    $profileVars["LEAD_LAG_NET_EDGE_MIN_PCT"] = "0.08"
    $profileVars["LEAD_LAG_SPREAD_COST_CAP_PCT"] = "0.75"
    $profileVars["LEAD_LAG_MIN_TRADE_NOTIONAL_USDC"] = "10"

    $profileVars["LEAD_LAG_SLIPPAGE_CAP"] = "0.01"
    $profileVars["LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY"] = "0.10"
    $profileVars["LEAD_LAG_HARD_CAP_USDC"] = "2000"

    # Adaptive move gating (recommended)
    $profileVars["LEAD_LAG_SPOT_NOISE_WINDOW_POINTS"] = "40"
    $profileVars["LEAD_LAG_SPOT_NOISE_MULT"] = "2.0"
    $profileVars["LEAD_LAG_SPREAD_MOVE_MULT"] = "1.0"

    # Demo-only odds widening should be off in prod
    $profileVars["PM_ODDS_TEST_MODE"] = "0"

    $profileVars["FRESHNESS_MAX_AGE_SECS"] = "60"
  }
  "profit" {
    # More selective: fewer trades, higher quality after costs.
    $profileVars["LEAD_LAG_LOOKBACK_POINTS"] = "8"
    $profileVars["LEAD_LAG_SPOT_MOVE_MIN_PCT"] = "0.35"
    $profileVars["LEAD_LAG_EDGE_MIN_PCT"] = "0.30"
    $profileVars["LEAD_LAG_EDGE_EXIT_PCT"] = "0.10"
    $profileVars["LEAD_LAG_MAX_HOLD_SECS"] = "240"
    $profileVars["LEAD_LAG_PM_STOP_PCT"] = "0.30"
    $profileVars["LEAD_LAG_AVOID_PRICE_BELOW"] = "0.03"
    $profileVars["LEAD_LAG_AVOID_PRICE_ABOVE"] = "0.85"

    $profileVars["LEAD_LAG_NET_EDGE_MIN_PCT"] = "0.12"
    $profileVars["LEAD_LAG_SPREAD_COST_CAP_PCT"] = "0.50"
    $profileVars["LEAD_LAG_MIN_TRADE_NOTIONAL_USDC"] = "25"

    $profileVars["LEAD_LAG_SLIPPAGE_CAP"] = "0.01"
    $profileVars["LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY"] = "0.08"
    $profileVars["LEAD_LAG_HARD_CAP_USDC"] = "1500"

    # Adaptive move gating (recommended)
    $profileVars["LEAD_LAG_SPOT_NOISE_WINDOW_POINTS"] = "40"
    $profileVars["LEAD_LAG_SPOT_NOISE_MULT"] = "2.0"
    $profileVars["LEAD_LAG_SPREAD_MOVE_MULT"] = "1.0"

    # Demo-only odds widening should be off in prod
    $profileVars["PM_ODDS_TEST_MODE"] = "0"

    $profileVars["FRESHNESS_MAX_AGE_SECS"] = "45"
  }
  "proof" {
    # Extremely sensitive / demo-only. Avoid for production.
    $profileVars["LEAD_LAG_LOOKBACK_POINTS"] = "2"
    $profileVars["LEAD_LAG_SPOT_MOVE_MIN_PCT"] = "0.00"
    $profileVars["LEAD_LAG_EDGE_MIN_PCT"] = "0.00"
    $profileVars["LEAD_LAG_EDGE_EXIT_PCT"] = "0.00"
    $profileVars["LEAD_LAG_MAX_HOLD_SECS"] = "180"
    $profileVars["LEAD_LAG_PM_STOP_PCT"] = "100.0"
    $profileVars["LEAD_LAG_AVOID_PRICE_BELOW"] = "0.0"
    $profileVars["LEAD_LAG_AVOID_PRICE_ABOVE"] = "1.0"

    $profileVars["LEAD_LAG_NET_EDGE_MIN_PCT"] = "-100.0"
    $profileVars["LEAD_LAG_SPREAD_COST_CAP_PCT"] = "100.0"
    $profileVars["LEAD_LAG_MIN_TRADE_NOTIONAL_USDC"] = "0"

    $profileVars["LEAD_LAG_SLIPPAGE_CAP"] = "1.0"
    $profileVars["LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY"] = "1.0"
    $profileVars["LEAD_LAG_HARD_CAP_USDC"] = "2000"

    # Disable adaptive gating to maximize demo trade frequency
    $profileVars["LEAD_LAG_SPOT_NOISE_WINDOW_POINTS"] = "2"
    $profileVars["LEAD_LAG_SPOT_NOISE_MULT"] = "0.0"
    $profileVars["LEAD_LAG_SPREAD_MOVE_MULT"] = "0.0"

    # Demo-only odds widening
    $profileVars["PM_ODDS_TEST_MODE"] = "1"

    $profileVars["FRESHNESS_MAX_AGE_SECS"] = "120"
  }
}

# Apply overrides if provided
function SetIfProvided([string]$key, $value) {
  if ($value -is [double]) {
    if (-not [double]::IsNaN($value)) { $profileVars[$key] = ("{0}" -f $value) }
  } elseif ($value -is [int]) {
    if ($value -ge 0) { $profileVars[$key] = ("{0}" -f $value) }
  }
}

SetIfProvided "LEAD_LAG_LOOKBACK_POINTS" $LookbackPoints
SetIfProvided "LEAD_LAG_SPOT_MOVE_MIN_PCT" $SpotMoveMinPct
SetIfProvided "LEAD_LAG_EDGE_MIN_PCT" $EdgeMinPct
SetIfProvided "LEAD_LAG_EDGE_EXIT_PCT" $EdgeExitPct
SetIfProvided "LEAD_LAG_MAX_HOLD_SECS" $MaxHoldSecs
SetIfProvided "LEAD_LAG_PM_STOP_PCT" $PmStopPct
SetIfProvided "LEAD_LAG_AVOID_PRICE_BELOW" $AvoidPriceBelow
SetIfProvided "LEAD_LAG_AVOID_PRICE_ABOVE" $AvoidPriceAbove

SetIfProvided "LEAD_LAG_NET_EDGE_MIN_PCT" $NetEdgeMinPct
SetIfProvided "LEAD_LAG_SPREAD_COST_CAP_PCT" $SpreadCostCapPct
SetIfProvided "LEAD_LAG_MIN_TRADE_NOTIONAL_USDC" $MinTradeNotionalUsdc

SetIfProvided "LEAD_LAG_SLIPPAGE_CAP" $SlippageCap
SetIfProvided "LEAD_LAG_MAX_FRACTION_OF_BAND_LIQUIDITY" $MaxFractionOfBandLiquidity
SetIfProvided "LEAD_LAG_HARD_CAP_USDC" $HardCapUsdc

SetIfProvided "FRESHNESS_MAX_AGE_SECS" $FreshnessMaxAgeSecs

Write-Host "==============================="
Write-Host "  VPS set sensitivity profile"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "Profile: $Profile"

# Build remote bash script (numeric values only; safe to inline)
$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"
foreach ($k in ($profileVars.Keys | Sort-Object)) {
  $v = $profileVars[$k]
  $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's/^${k}=.*/${k}=${v}/' '$EnvPath'; else echo '${k}=${v}' | sudo tee -a '$EnvPath' >/dev/null; fi"
}
$lines += "echo '--- env (lead_lag subset, masked) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(LEAD_LAG_|FRESHNESS_MAX_AGE_SECS=|TRADING_MODE=|STRATEGY_MODE=|PM_ODDS_TEST_MODE=|PM_MIN_ODDS=|PM_MAX_ODDS=)' '$EnvPath' | sed -E 's/(PASS|KEY|SECRET)=.*/\\1=***MASKED***/g' || true"
if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart spelar-agent"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status spelar-agent | head -80"
}

$remote = $lines -join "; "

ssh $target $remote | Out-Host

Write-Host "[OK] Updated sensitivity env vars on VPS."