param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$OutDir = "/opt/spelar_eu/vps/out",

  [Parameter(Mandatory = $false)]
  [int]$HeadLines = 8
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$target = "$VpsUser@$VpsIp"

$remoteLines = @(
  "set +e",
  "echo '--- out dir listing (filtered) ---'",
  "ls -lah $OutDir | egrep 'pm_(markets_index|scan_candidates|scanner_log|open_orders|paper_)|sources_health|live_status' || true",
  "",
  "echo '--- pm_scan_candidates.csv head ---'",
  "head -n $HeadLines $OutDir/pm_scan_candidates.csv 2>/dev/null || true",
  "",
  "echo '--- pm_scanner_log.csv tail ---'",
  "tail -n $HeadLines $OutDir/pm_scanner_log.csv 2>/dev/null || true",
  "",
  "echo '--- pm_markets_index.json head ---'",
  "head -n $HeadLines $OutDir/pm_markets_index.json 2>/dev/null || true"
)

$remote = $remoteLines -join "`n"

ssh $target $remote | Out-Host
