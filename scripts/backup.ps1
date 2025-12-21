param(
  [string]$OutDir = "./backups",
  [string]$Root = "./",
  [switch]$IncludeDocs
)

$ErrorActionPreference = "Stop"

$rootFull = (Resolve-Path -LiteralPath $Root).Path
$outFull = Join-Path $rootFull $OutDir

if (-not (Test-Path -LiteralPath $outFull)) {
  New-Item -ItemType Directory -Path $outFull | Out-Null
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$zipPath = Join-Path $outFull "spelar_eu-backup-$stamp.zip"

$tempDir = Join-Path $env:TEMP "spelar_eu_backup_$stamp"
New-Item -ItemType Directory -Path $tempDir | Out-Null

try {
  Copy-Item -LiteralPath (Join-Path $rootFull "web") -Destination (Join-Path $tempDir "web") -Recurse -Force
  Copy-Item -LiteralPath (Join-Path $rootFull "README.md") -Destination (Join-Path $tempDir "README.md") -Force
  if ($IncludeDocs -and (Test-Path -LiteralPath (Join-Path $rootFull "docs"))) {
    Copy-Item -LiteralPath (Join-Path $rootFull "docs") -Destination (Join-Path $tempDir "docs") -Recurse -Force
  }

  # Never include secrets
  $secret = Join-Path $tempDir "ftp_config.local.json"
  if (Test-Path -LiteralPath $secret) { Remove-Item -LiteralPath $secret -Force }

  if (Test-Path -LiteralPath $zipPath) { Remove-Item -LiteralPath $zipPath -Force }
  Compress-Archive -LiteralPath (Join-Path $tempDir "*") -DestinationPath $zipPath

  Write-Host "Created backup: $zipPath"
} finally {
  if (Test-Path -LiteralPath $tempDir) {
    Remove-Item -LiteralPath $tempDir -Recurse -Force
  }
}
