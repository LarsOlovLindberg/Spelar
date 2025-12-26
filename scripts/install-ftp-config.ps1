param(
  [string]$SourceConfig = "./ftp_config.local.json",
  [string]$DestDir = "$env:ProgramData\\spelar_eu",
  [string]$DestFileName = "ftp_config.local.json",
  [switch]$Force
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

$src = Resolve-Path -LiteralPath $SourceConfig -ErrorAction Stop
$destPath = Join-Path $DestDir $DestFileName

if (-not (Test-Path -LiteralPath $DestDir)) {
  New-Item -ItemType Directory -Force -Path $DestDir | Out-Null
}

if ((Test-Path -LiteralPath $destPath) -and -not $Force) {
  throw "Destination already exists: $destPath (use -Force to overwrite)"
}

Copy-Item -LiteralPath $src.Path -Destination $destPath -Force

# Try to restrict ACL (best-effort; only when elevated)
if (Test-IsAdmin) {
  try {
    & icacls $DestDir /inheritance:r | Out-Null
    & icacls $DestDir /grant:r "SYSTEM:(OI)(CI)F" "BUILTIN\Administrators:(OI)(CI)F" | Out-Null
    & icacls $destPath /inheritance:r | Out-Null
    & icacls $destPath /grant:r "SYSTEM:F" "BUILTIN\Administrators:F" | Out-Null
  } catch {
    Write-Warning "Could not tighten ACLs. Re-run in elevated PowerShell to lock down $destPath."
  }
} else {
  Write-Warning "Not elevated; copied config but did not change ACLs. If you plan to run as SYSTEM, re-run this script in an elevated PowerShell to lock down and ensure SYSTEM access."
}

Write-Host "Installed FTP config to: $destPath"