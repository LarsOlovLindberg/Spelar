param(
  [string]$FtpConfigPath = "./ftp_config.local.json",
  [switch]$DeployWeb,
  [switch]$DeployData,
  [string]$HostName = "root@77.42.42.124",
  [string]$MappingFile = "",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$deployScript = Join-Path $repoRoot "scripts\deploy-ftp.ps1"
$syncScript = Join-Path $repoRoot "scripts\sync-vps-stats.ps1"

if (-not (Test-Path -LiteralPath $deployScript)) { throw "Missing: $deployScript" }
if (-not (Test-Path -LiteralPath $syncScript)) { throw "Missing: $syncScript" }

# Default behavior: do both, unless user explicitly asked for only one.
if (-not $DeployWeb -and -not $DeployData) {
  $DeployWeb = $true
  $DeployData = $true
}

if ($DeployWeb) {
  Write-Host "== Deploying web/ via FTP ==" 
  $args = @{
    ConfigPath = $FtpConfigPath
    LocalRoot  = (Join-Path $repoRoot "web")
  }
  if ($DryRun) { $args["DryRun"] = $true }
  & $deployScript @args
}

if ($DeployData) {
  Write-Host "== Syncing VPS out -> web/data + deploying data-only ==" 

  $syncArgs = @{
    HostName       = $HostName
    DeployDataOnly = $true
    FtpConfigPath  = $FtpConfigPath
  }
  if ($MappingFile -and $MappingFile.Trim() -ne "") {
    $syncArgs["MappingFile"] = $MappingFile
  }

  if ($DryRun) {
    Write-Host "[DRY] Would run: $syncScript" 
    Write-Host "[DRY] HostName=$HostName DeployDataOnly=True FtpConfigPath=$FtpConfigPath MappingFile=$MappingFile" 
  } else {
    & $syncScript @syncArgs
  }
}
