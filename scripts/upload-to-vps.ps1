param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$RemoteRoot = "/opt/spelar_eu",

  [Parameter(Mandatory = $false)]
  [string]$ServiceName = "spelar-agent",

  [switch]$NoRestart,

  [switch]$SkipArchiveCleanup
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}
if (-not (Get-Command "scp" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'scp'. Install Windows OpenSSH Client and ensure it is in PATH."
}
if (-not (Get-Command "tar" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'tar'. Ensure tar is available (Windows 10/11 usually has bsdtar)."
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$vpsDir = Join-Path $repoRoot "vps"
if (-not (Test-Path -LiteralPath $vpsDir)) { throw "Missing vps/ folder at: $vpsDir" }

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  Upload spelar_eu/vps -> VPS"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "RemoteRoot: $RemoteRoot"

$tempDir = Join-Path $env:TEMP ("spelar-vps-deploy-" + (Get-Date -Format 'yyyyMMddHHmmss'))
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null

try {
  Write-Host "[1/4] Staging files..."
  Copy-Item -LiteralPath $vpsDir -Destination (Join-Path $tempDir "vps") -Recurse -Force

  Write-Host "[2/4] Creating archive..."
  $archive = Join-Path $env:TEMP "spelar_eu_vps.tar.gz"
  if (Test-Path -LiteralPath $archive) { Remove-Item -Force $archive }

  Push-Location $tempDir
  tar -czvf $archive vps | Out-Null
  Pop-Location

  Write-Host "[3/4] Uploading archive..."
  ssh $target "mkdir -p $RemoteRoot" | Out-Host
  scp $archive "$target`:$RemoteRoot/spelar_eu_vps.tar.gz" | Out-Host

  Write-Host "[4/4] Extracting on VPS..."
  ssh $target "cd $RemoteRoot; tar -xzvf spelar_eu_vps.tar.gz; rm spelar_eu_vps.tar.gz" | Out-Host

  if (-not $NoRestart) {
    Write-Host "[VPS] Restarting systemd service: $ServiceName ..."
    ssh $target "sudo systemctl restart $ServiceName; sudo systemctl --no-pager --full status $ServiceName | head -120" | Out-Host
  } else {
    Write-Host "[VPS] NoRestart set; skipping systemd restart."
  }

  Write-Host "[OK] Uploaded vps/ to $RemoteRoot/vps"
} finally {
  if (-not $SkipArchiveCleanup) {
    if (Test-Path -LiteralPath $tempDir) { Remove-Item -Recurse -Force $tempDir }
    $archive = Join-Path $env:TEMP "spelar_eu_vps.tar.gz"
    if (Test-Path -LiteralPath $archive) { Remove-Item -Force $archive }
  }
}
