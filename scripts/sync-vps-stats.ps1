param(
  [string]$HostName = "77.42.42.124",
  [string]$SshUser = "",
  [string]$RemoteRoot = "/opt/spelar_eu/vps/out",
  [string]$MappingFile = "$PSScriptRoot\vps_sync_map_pm.json",
  [string]$FtpConfigPath = "",
  [string]$ScpPath = "",
  [string]$KnownHostsPath = "",
  [string]$IdentityFile = "",
  [string]$LogPath = "",
  [switch]$DeployDataOnly,
  [switch]$SkipDeploy,
  [switch]$Strict,
  [switch]$Watch,
  [int]$IntervalSeconds = 60
)

$ErrorActionPreference = "Stop"

function Resolve-FtpConfigPath([string]$repoRoot, [string]$FtpConfigPath) {
  if ($FtpConfigPath -and $FtpConfigPath.Trim() -ne "") {
    return $FtpConfigPath.Trim()
  }
  if ($env:SPELAR_FTP_CONFIG -and $env:SPELAR_FTP_CONFIG.Trim() -ne "") {
    return $env:SPELAR_FTP_CONFIG.Trim()
  }
  $repoLocal = Join-Path $repoRoot "ftp_config.local.json"
  if (Test-Path -LiteralPath $repoLocal) {
    return $repoLocal
  }
  return ""
}

function Test-RequiredCommand([string]$Name) {
  $cmd = Get-Command $Name -ErrorAction SilentlyContinue
  if (-not $cmd) { throw "Missing required command '$Name'. Install Windows OpenSSH Client or ensure it is in PATH." }
}

function Resolve-ScpExe([string]$ScpPath) {
  if ($ScpPath -and (Test-Path -LiteralPath $ScpPath)) { return (Resolve-Path -LiteralPath $ScpPath).Path }

  $cmd = Get-Command scp -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }

  $fallback = "C:\\Windows\\System32\\OpenSSH\\scp.exe"
  if (Test-Path -LiteralPath $fallback) { return $fallback }

  throw "Missing required command 'scp'. Install Windows OpenSSH Client or ensure it is in PATH."
}

if (-not (Test-Path $MappingFile)) { throw "Mapping file not found: $MappingFile" }

$mapping = Get-Content $MappingFile -Raw | ConvertFrom-Json
if (-not $mapping.files -or $mapping.files.Count -lt 1) { throw "Mapping file has no 'files' entries: $MappingFile" }

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$dataDir = Join-Path $repoRoot "web\data"
New-Item -ItemType Directory -Force -Path $dataDir | Out-Null

$autosyncDir = Join-Path $repoRoot ".autosync"
New-Item -ItemType Directory -Force -Path $autosyncDir | Out-Null

$scpExe = Resolve-ScpExe -ScpPath $ScpPath

if (-not $KnownHostsPath -or $KnownHostsPath.Trim() -eq "") {
  $KnownHostsPath = Join-Path $autosyncDir "known_hosts"
}

if (-not $LogPath -or $LogPath.Trim() -eq "") {
  $LogPath = Join-Path $autosyncDir "autosync.log"
}

function Write-Log([string]$msg) {
  $ts = (Get-Date).ToString("s")
  $line = "[$ts] $msg"
  Write-Host $line
  try {
    Add-Content -LiteralPath $LogPath -Value $line
  } catch {
    # ignore log write failures
  }
}

$failed = New-Object System.Collections.Generic.List[string]

function Resolve-RemoteHostSpec([string]$HostName, [string]$SshUser) {
  if ($HostName -match "@") { return $HostName }
  if ($SshUser -and $SshUser.Trim() -ne "") { return ("{0}@{1}" -f $SshUser.Trim(), $HostName) }
  return $HostName
}

function Invoke-SyncOnce {
  # Ensure per-run state (important in -Watch mode)
  $failed.Clear()

  # Prevent overlapping runs (common with Task Scheduler minute triggers + FTP deploy time).
  # Keep the lock outside OneDrive-backed folders to avoid sync clients holding exclusive handles.
  $lockDir = Join-Path $env:LOCALAPPDATA "spelar_eu"
  New-Item -ItemType Directory -Force -Path $lockDir | Out-Null
  $lockPath = Join-Path $lockDir "autosync.lock"
  $lockStream = $null
  try {
    $lockStream = [System.IO.File]::Open($lockPath, [System.IO.FileMode]::OpenOrCreate, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
  } catch {
    Write-Log "Another autosync appears to be running; exiting this run."
    return
  }

  try {
    $remoteHost = Resolve-RemoteHostSpec -HostName $HostName -SshUser $SshUser
    $syncedCount = 0
    $includeForDeploy = New-Object System.Collections.Generic.List[string]

    Write-Log "Sync start: remoteHost=$remoteHost RemoteRoot=$RemoteRoot mapping=$MappingFile deployDataOnly=$DeployDataOnly skipDeploy=$SkipDeploy scpExe=$scpExe"
    Write-Log "KnownHostsPath=$KnownHostsPath IdentityFile=$IdentityFile"
    Write-Host "Syncing" $mapping.files.Count "file(s) from" $remoteHost

  function Invoke-ScpOnce([string[]]$scpArgs) {
    $oldEap = $ErrorActionPreference
    try {
      # Avoid native stderr/nonzero exit becoming a terminating error under SYSTEM/task.
      $ErrorActionPreference = "Continue"
      $outText = & $scpExe @scpArgs 2>&1 | Out-String
    } finally {
      $ErrorActionPreference = $oldEap
    }
    return $outText
  }

  foreach ($item in $mapping.files) {
    $remotePath = $item.remote
    $localName  = $item.local

    if (-not $remotePath -or -not $localName) {
      throw "Invalid mapping entry (need 'remote' and 'local'): $($item | ConvertTo-Json -Compress)"
    }

    $remoteFull = $remotePath
    if (-not ($remoteFull.StartsWith("/"))) { $remoteFull = "$RemoteRoot/$remotePath" }

    $localPath = Join-Path $dataDir $localName
    $tmpPath   = "$localPath.tmp"

    $remoteSpec = "${remoteHost}:$remoteFull"
    Write-Host "SCP" $remoteSpec "->" (Split-Path $localPath -Leaf)
    Write-Log "SCP: $remoteSpec -> $tmpPath"

    if (Test-Path $tmpPath) { Remove-Item -Force $tmpPath }

    $scpArgs = @(
      "-q",
      "-o", "BatchMode=yes",
      "-o", "StrictHostKeyChecking=accept-new",
      "-o", ("UserKnownHostsFile=" + $KnownHostsPath),
      $remoteSpec,
      $tmpPath
    )
    if ($IdentityFile -and $IdentityFile.Trim() -ne "") {
      $scpArgs = @(
        "-q",
        "-i", $IdentityFile,
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=accept-new",
        "-o", ("UserKnownHostsFile=" + $KnownHostsPath),
        $remoteSpec,
        $tmpPath
      )
    }

    $attempt = 0
    $outText = ""
    $exitCode = 0
    while ($true) {
      $attempt++
      $outText = Invoke-ScpOnce -scpArgs $scpArgs
      $exitCode = $LASTEXITCODE
      if ($outText -and $outText.Trim() -ne "") {
        Write-Log ("scp output (attempt ${attempt}): " + $outText.Trim())
      }

      # Retry transient disconnects.
      $isConnectionClosed = $false
      if ($exitCode -eq 255) { $isConnectionClosed = $true }
      if ($outText -match "Connection closed") { $isConnectionClosed = $true }

      if ($isConnectionClosed -and $attempt -lt 3) {
        Start-Sleep -Milliseconds (500 * $attempt)
        continue
      }
      break
    }
    if ($exitCode -ne 0 -or -not (Test-Path $tmpPath)) {
      $msg = "Missing/unreadable on VPS: $remoteFull"
      if ($Strict) { throw $msg }
      Write-Warning $msg
      Write-Log ("scp failed exit=${exitCode}: $msg")
      $failed.Add($remoteFull) | Out-Null

      # Prevent a handled scp failure from leaking out as the script process exit code.
      $global:LASTEXITCODE = 0
      continue
    }

    # Replace safely: try to rename existing aside, then move tmp into place.
    if (Test-Path -LiteralPath $localPath) {
      $backupPath = ($localPath + ".prev")
      try {
        if (Test-Path -LiteralPath $backupPath) {
          Remove-Item -Force -LiteralPath $backupPath -ErrorAction SilentlyContinue
        }
        Move-Item -Force -LiteralPath $localPath -Destination $backupPath
      } catch {
        # If the destination is locked (e.g., FTP upload reading), skip updating this file this run.
        $msg = "Local file is in use; skipping update: $localName"
        if ($Strict) { throw $msg }
        Write-Warning $msg
        Write-Log $msg
        Remove-Item -Force -LiteralPath $tmpPath -ErrorAction SilentlyContinue
        $failed.Add($localPath) | Out-Null
        $global:LASTEXITCODE = 0
        continue
      }
    }
    Move-Item -Force -LiteralPath $tmpPath -Destination $localPath
    $syncedCount++
    $includeForDeploy.Add(("data/{0}" -f $localName)) | Out-Null
    Write-Log ("synced: $localName")
  }

  Write-Host "OK: synced to" $dataDir
  if ($failed.Count -gt 0) {
    Write-Warning ("Some files failed (" + $failed.Count + "): " + ($failed -join ", "))
  }

  if (-not $SkipDeploy) {
    if ($syncedCount -le 0 -and $failed.Count -gt 0) {
      Write-Warning "No files were synced; skipping FTP deploy."
      return
    }

    $deploy = Join-Path $repoRoot "scripts\deploy-ftp.ps1"
    $effectiveFtpConfigPath = Resolve-FtpConfigPath -repoRoot $repoRoot -FtpConfigPath $FtpConfigPath
    if ($DeployDataOnly) {
      Write-Host "Deploying web/data via FTP (data-only)..."
      if ($includeForDeploy.Count -le 0) {
        Write-Warning "No files to deploy (data-only)."
        return
      }
      Write-Log ("FTP deploy (data-only) files=" + ($includeForDeploy.ToArray() -join ","))
      $deployParams = @{
        LocalRoot = (Join-Path $repoRoot "web")
        IncludeFiles = $includeForDeploy.ToArray()
      }
      if ($effectiveFtpConfigPath -and $effectiveFtpConfigPath.Trim() -ne "") {
        $deployParams["ConfigPath"] = $effectiveFtpConfigPath
      }
      try {
        & $deploy @deployParams
      } catch {
        if ($Strict) { throw }
        Write-Warning ("FTP deploy failed (data-only): " + $_.Exception.Message)
        Write-Log ("FTP deploy failed (data-only): " + $_.Exception.Message)
        $global:LASTEXITCODE = 0
      }
    } else {
      Write-Host "Deploying web/ via FTP..."
      Write-Log "FTP deploy (full web/)"
      try {
        if ($effectiveFtpConfigPath -and $effectiveFtpConfigPath.Trim() -ne "") {
          & $deploy -ConfigPath $effectiveFtpConfigPath
        } else {
          & $deploy
        }
      } catch {
        if ($Strict) { throw }
        Write-Warning ("FTP deploy failed (full): " + $_.Exception.Message)
        Write-Log ("FTP deploy failed (full): " + $_.Exception.Message)
        $global:LASTEXITCODE = 0
      }
    }
  }

  Write-Log ("Sync done: syncedCount=$syncedCount failedCount=" + $failed.Count)

    # If we successfully synced at least one file, we consider the run successful unless -Strict.
    # PowerShell will otherwise often exit with the last native command's code (scp), even if we handled it.
    if (-not $Strict) {
      if ($syncedCount -gt 0) {
        $global:LASTEXITCODE = 0
      }
    }
  } finally {
    if ($lockStream) {
      try { $lockStream.Dispose() } catch { }
    }
  }
}

if ($Watch) {
  Write-Host "Watch mode enabled. IntervalSeconds=$IntervalSeconds (Ctrl+C to stop)."
  while ($true) {
    try {
      Invoke-SyncOnce
    } catch {
      if ($Strict) { throw }
      Write-Warning ("Sync loop error: " + $_.Exception.Message)
    }
    Start-Sleep -Seconds $IntervalSeconds
  }
} else {
  Invoke-SyncOnce
}
