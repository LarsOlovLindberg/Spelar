param(
  [string]$TaskName = "spelar_eu_autosync",
  [string]$HostName = "77.42.42.124",
  [string]$SshUser = "",
  [string]$RemoteRoot = "/opt/spelar_eu/vps/out",
  [string]$MappingFile = "",
  [string]$FtpConfigPath = "",
  [int]$EveryMinutes = 1,
  [switch]$DeployDataOnly,
  [switch]$SkipDeploy,
  [switch]$Strict,
  [switch]$RunAsSystem,
  [switch]$Highest,
  [switch]$RunNow,
  [switch]$Delete
)

$ErrorActionPreference = "Stop"

function Test-IsAdmin {
  try {
    $currentIdentity = [Security.Principal.WindowsIdentity]::GetCurrent()
    $principal = New-Object Security.Principal.WindowsPrincipal($currentIdentity)
    return $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)
  } catch {
    return $false
  }
}

function Invoke-NativeOrThrow {
  param(
    [Parameter(Mandatory = $true)][string]$Exe,
    [Parameter(Mandatory = $true)][string[]]$Args,
    [Parameter(Mandatory = $true)][string]$What
  )

  $out = & $Exe @Args 2>&1 | Out-String
  if ($out -and $out.Trim() -ne "") { $out.TrimEnd() | Out-Host }
  if ($LASTEXITCODE -ne 0) {
    $hint = "Re-run in an elevated PowerShell (Run as Administrator)."
    if ($out -match "(?i)access\s+is\s+denied|\xC5tkomst\s+nekad") {
      $hint = "Access denied from Task Scheduler. $hint"
    }
    throw "Failed: $What (exitCode=$LASTEXITCODE). $hint"
  }
}

function Resolve-RepoRoot {
  $root = Resolve-Path (Join-Path $PSScriptRoot "..")
  return $root.Path
}

function ConvertTo-CmdQuoted([string]$s) {
  $q = [char]34
  return ($q + ($s -replace $q, ($q + $q)) + $q)
}

$repoRoot = Resolve-RepoRoot
$syncScript = Join-Path $repoRoot "scripts\sync-vps-stats.ps1"
if (-not (Test-Path -LiteralPath $syncScript)) {
  throw "Missing script: $syncScript"
}

if ($Delete) {
  Invoke-NativeOrThrow -Exe "schtasks" -Args @("/Delete", "/TN", $TaskName, "/F") -What "Delete scheduled task '$TaskName'"
  Write-Host "Deleted task: $TaskName"
  return
}

if ($EveryMinutes -lt 1) {
  throw "EveryMinutes must be >= 1"
}

if (-not $DeployDataOnly.IsPresent) {
  $DeployDataOnly = $true
}

$isAdmin = Test-IsAdmin
if (($RunAsSystem -or $Highest) -and -not $isAdmin) {
  Write-Warning "You requested -RunAsSystem and/or -Highest but this PowerShell session is not elevated. schtasks may return 'Access denied'. Re-run in an elevated PowerShell (Run as Administrator)."
}

if ($RunAsSystem -and ($repoRoot -match "\\OneDrive\\")) {
  Write-Warning "RepoRoot appears to be under OneDrive. SYSTEM tasks often cannot access OneDrive paths. If the task fails, move the repo to a non-OneDrive path (e.g. C:\\spelar_eu) or run the task as your user instead of SYSTEM."
}

$mappingArg = $MappingFile
if (-not $mappingArg -or $mappingArg.Trim() -eq "") {
  $mappingArg = Join-Path $repoRoot "scripts\vps_sync_map_pm.json"
}

# Build a short /TR command line (schtasks has a 261 char limit).
$cmdLine = "powershell.exe -NoProfile -ExecutionPolicy Bypass -File " + (ConvertTo-CmdQuoted $syncScript) + " -HostName " + (ConvertTo-CmdQuoted $HostName)
if ($SshUser -and $SshUser.Trim() -ne "") {
  $cmdLine += " -SshUser " + (ConvertTo-CmdQuoted $SshUser.Trim())
}
if ($RemoteRoot -and $RemoteRoot.Trim() -ne "/opt/spelar_eu/vps/out") {
  $cmdLine += " -RemoteRoot " + (ConvertTo-CmdQuoted $RemoteRoot)
}
if ($MappingFile -and $MappingFile.Trim() -ne "") {
  $cmdLine += " -MappingFile " + (ConvertTo-CmdQuoted $mappingArg)
}
if ($DeployDataOnly) { $cmdLine += " -DeployDataOnly" }
if ($SkipDeploy) { $cmdLine += " -SkipDeploy" }
if ($Strict) { $cmdLine += " -Strict" }
if ($FtpConfigPath -and $FtpConfigPath.Trim() -ne "") {
  $cmdLine += " -FtpConfigPath " + (ConvertTo-CmdQuoted $FtpConfigPath)
}

# Create/update task.
# Default runs in current user context (Interactive only). Use -RunAsSystem for unattended mode.
$createArgs = @(
  "/Create",
  "/F",
  "/TN", $TaskName,
  "/SC", "MINUTE",
  "/MO", "$EveryMinutes",
  "/TR", $cmdLine,
  "/ST", "00:00"
)
if ($RunAsSystem) { $createArgs += @("/RU", "SYSTEM") }
if ($Highest) { $createArgs += @("/RL", "HIGHEST") }

Invoke-NativeOrThrow -Exe "schtasks" -Args $createArgs -What "Create/update scheduled task '$TaskName'"

Write-Host "Installed task: $TaskName"
Write-Host "Schedule: every $EveryMinutes minute(s)"
Write-Host "Command: $cmdLine"

if ($RunNow) {
  Invoke-NativeOrThrow -Exe "schtasks" -Args @("/Run", "/TN", $TaskName) -What "Run scheduled task '$TaskName'"
  Write-Host "Triggered run: $TaskName"
}
