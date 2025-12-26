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
  $remoteHost = Resolve-RemoteHostSpec -HostName $HostName -SshUser $SshUser
  $syncedCount = 0
  $includeForDeploy = New-Object System.Collections.Generic.List[string]

  Write-Log "Sync start: remoteHost=$remoteHost RemoteRoot=$RemoteRoot mapping=$MappingFile deployDataOnly=$DeployDataOnly skipDeploy=$SkipDeploy scpExe=$scpExe"
  Write-Log "KnownHostsPath=$KnownHostsPath IdentityFile=$IdentityFile"
  Write-Host "Syncing" $mapping.files.Count "file(s) from" $remoteHost

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

    $oldEap = $ErrorActionPreference
    try {
      # Avoid native stderr/nonzero exit becoming a terminating error under SYSTEM/task.
      $ErrorActionPreference = "Continue"
      $outText = & $scpExe @scpArgs 2>&1 | Out-String
    } finally {
      $ErrorActionPreference = $oldEap
    }
    if ($outText -and $outText.Trim() -ne "") {
      Write-Log ("scp output: " + $outText.Trim())
    }
    if ($LASTEXITCODE -ne 0 -or -not (Test-Path $tmpPath)) {
      $msg = "Missing/unreadable on VPS: $remoteFull"
      if ($Strict) { throw $msg }
      Write-Warning $msg
      Write-Log ("scp failed exit=${LASTEXITCODE}: $msg")
      $failed.Add($remoteFull) | Out-Null
      continue
    }

    Move-Item -Force $tmpPath $localPath
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
      if ($FtpConfigPath -and $FtpConfigPath.Trim() -ne "") {
        $deployParams["ConfigPath"] = $FtpConfigPath
      }
      & $deploy @deployParams
    } else {
      Write-Host "Deploying web/ via FTP..."
      Write-Log "FTP deploy (full web/)"
      if ($FtpConfigPath -and $FtpConfigPath.Trim() -ne "") {
        & $deploy -ConfigPath $FtpConfigPath
      } else {
        & $deploy
      }
    }
  }

  Write-Log ("Sync done: syncedCount=$syncedCount failedCount=" + $failed.Count)
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
