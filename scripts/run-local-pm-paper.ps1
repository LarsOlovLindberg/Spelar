param(
	[int]$Ticks = 20,
	[double]$IntervalS = 2,
	[int]$TradeMaxMarkets = 8,
	[int]$OrderbookWorkers = 8,
	[int]$MaxOrdersPerTick = 5,
	[int]$OrderSizeShares = 5,
	[int]$ScanIntervalS = 30,
	[int]$ScanLimit = 120,
	[int]$ScanPages = 1,
	[int]$OrderbookSample = 10
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$python = Join-Path $repoRoot ".venv\Scripts\python.exe"

if (-not (Test-Path -LiteralPath $python)) {
	throw "Python not found at: $python (expected venv at .venv)"
}

$outDir = Join-Path $repoRoot "web\data"

$env:TRADING_MODE = "paper"
$env:POLY_LIVE_CONFIRM = "NO"
$env:RUN_TICKS = "$Ticks"
$env:OUT_DIR = $outDir

$env:STRATEGY_MODE = "lead_lag"
$env:PM_ODDS_TEST_MODE = "1"

$env:PM_MAX_ORDERS_PER_TICK = "$MaxOrdersPerTick"
$env:PM_ORDER_SIZE_SHARES = "$OrderSizeShares"

$env:LEAD_LAG_EDGE_MIN_PCT = "0.0"
$env:LEAD_LAG_EDGE_EXIT_PCT = "0.0"
$env:LEAD_LAG_SPOT_MOVE_MIN_PCT = "0.0"
$env:LEAD_LAG_NET_EDGE_MIN_PCT = "-100.0"
$env:LEAD_LAG_SPREAD_COST_CAP_PCT = "100.0"
$env:LEAD_LAG_SLIPPAGE_CAP = "1.0"
$env:LEAD_LAG_MIN_TRADE_NOTIONAL_USDC = "0.0"

$env:INTERVAL_S = "$IntervalS"

$env:PM_ORDERBOOK_WORKERS = "$OrderbookWorkers"
$env:PM_SCAN_ENABLE = "1"
$env:PM_SCAN_USE_FOR_TRADING = "1"
$env:PM_SCAN_TRADE_MAX_MARKETS = "$TradeMaxMarkets"
$env:PM_SCAN_INTERVAL_S = "$ScanIntervalS"
$env:PM_SCAN_LIMIT = "$ScanLimit"
$env:PM_SCAN_PAGES = "$ScanPages"
$env:PM_SCAN_ORDERBOOK_SAMPLE = "$OrderbookSample"

& $python -m vps.vps_agent
