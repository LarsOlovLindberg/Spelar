param(
	[double]$IntervalS = 5,
	[int]$OrderbookWorkers = 8,
	[int]$MaxOrdersPerTick = 3,
	[int]$OrderSizeShares = 5,

	[int]$TradeMaxMarkets = 20,
	[double]$ScanIntervalS = 60,
	[int]$ScanLimit = 120,
	[int]$ScanPages = 1,
	[int]$OrderbookSample = 10,

	[switch]$OddsTestMode
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
	throw "Python not found at: $python (expected venv at .venv)"
}

$outDir = Join-Path $repoRoot "web\data"

# Live data, but never live trading.
$env:TRADING_MODE = "paper"
$env:POLY_LIVE_CONFIRM = "NO"

# Run continuously (stop with Ctrl+C).
Remove-Item Env:RUN_ONCE -ErrorAction SilentlyContinue
Remove-Item Env:RUN_TICKS -ErrorAction SilentlyContinue

$env:OUT_DIR = $outDir
$env:STRATEGY_MODE = "lead_lag"

# Use real/default thresholds unless you explicitly override them elsewhere.
# (This script intentionally does NOT force ultra-sensitive settings.)

# Ticking cadence
$env:INTERVAL_S = "$IntervalS"

# PM scanning: drives the universe and paper trading candidates
$env:PM_ORDERBOOK_WORKERS = "$OrderbookWorkers"
$env:PM_SCAN_ENABLE = "1"
$env:PM_SCAN_USE_FOR_TRADING = "1"
$env:PM_SCAN_TRADE_MAX_MARKETS = "$TradeMaxMarkets"
$env:PM_SCAN_INTERVAL_S = "$ScanIntervalS"
$env:PM_SCAN_LIMIT = "$ScanLimit"
$env:PM_SCAN_PAGES = "$ScanPages"
$env:PM_SCAN_ORDERBOOK_SAMPLE = "$OrderbookSample"

if ($OddsTestMode) {
	$env:PM_ODDS_TEST_MODE = "1"
} else {
	$env:PM_ODDS_TEST_MODE = "0"
}

Write-Host "[run-local-live-paper] outDir=$outDir" -ForegroundColor Cyan
Write-Host "[run-local-live-paper] TRADING_MODE=paper (live data, paper execution)" -ForegroundColor Cyan
Write-Host "[run-local-live-paper] interval_s=$IntervalS, pm_scan_interval_s=$ScanIntervalS, scan_limit=$ScanLimit, orderbook_sample=$OrderbookSample" -ForegroundColor Cyan
Write-Host "[run-local-live-paper] Stop: Ctrl+C" -ForegroundColor DarkGray

& $python -u -m vps.vps_agent
