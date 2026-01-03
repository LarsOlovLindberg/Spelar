param(
  [string]$ConfigPath = "./ftp_config.local.json",
  [string]$LocalRoot = "./web",
  [string]$RemoteSubdir = "",
  [string[]]$IncludeFiles = @(),
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

$programDataFallback = Join-Path $env:ProgramData "spelar_eu\ftp_config.local.json"

# Allow override via environment variable (useful for Task Scheduler / SYSTEM).
if ($env:SPELAR_FTP_CONFIG -and $env:SPELAR_FTP_CONFIG.Trim() -ne "") {
  $ConfigPath = $env:SPELAR_FTP_CONFIG.Trim()
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  try {
    if (Test-Path -LiteralPath $programDataFallback) {
      $ConfigPath = $programDataFallback
    }
  } catch {
    # If access is denied probing ProgramData (e.g., locked-down ACL), ignore and keep original path.
  }
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Config not found: $ConfigPath (copy ftp_config.example.json -> ftp_config.local.json)"
}

if (-not (Test-Path -LiteralPath $LocalRoot)) {
  throw "LocalRoot not found: $LocalRoot (expected site files under ./web)"
}

$config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json

$ftpHost = if ($config.host) { [string]$config.host } elseif ($config.server) { [string]$config.server } else { throw "Missing host/server in config" }
$remotePathRaw = if ($null -ne $config.remote_path -and [string]$config.remote_path -ne "") { [string]$config.remote_path } else { "/" }
$remotePath = $remotePathRaw.TrimEnd("/")
$username = $config.username
$password = $config.password

if (-not $username -or -not $password) {
  throw "Missing username/password in config"
}

$cred = New-Object System.Net.NetworkCredential($username, $password)

function Join-FtpPath([string]$a, [string]$b) {
  ($a.TrimEnd("/") + "/" + $b.TrimStart("/")).Replace("\\", "/").Replace("//", "/")
}

function Ensure-FtpDirectory([string]$ftpDirUrl) {
  try {
    $req = [System.Net.FtpWebRequest]::Create($ftpDirUrl)
    $req.Credentials = $cred
    $req.Method = [System.Net.WebRequestMethods+Ftp]::MakeDirectory
    $req.UseBinary = $true
    $req.UsePassive = $true
    $req.KeepAlive = $false
    if ($DryRun) { Write-Host "[DRY] MKDIR $ftpDirUrl"; return }
    $resp = $req.GetResponse(); $resp.Close()
  } catch {
    # ignore "already exists" and similar
  }
}

function Upload-File([string]$localFile, [string]$remoteFileUrl) {
  $dirUrl = $remoteFileUrl.Substring(0, $remoteFileUrl.LastIndexOf("/"))
  Ensure-FtpDirectory $dirUrl

  Write-Host "UPLOAD $localFile -> $remoteFileUrl"
  if ($DryRun) { return }

  $wc = New-Object System.Net.WebClient
  $wc.Credentials = $cred
  $wc.UploadFile($remoteFileUrl, "STOR", $localFile) | Out-Null
  $wc.Dispose()
}

$localRootFull = (Resolve-Path -LiteralPath $LocalRoot).Path

$files = @()
if ($IncludeFiles -and $IncludeFiles.Count -gt 0) {
  foreach ($relPath in $IncludeFiles) {
    if (-not $relPath -or $relPath.Trim() -eq "") { continue }
    $p = Join-Path $localRootFull $relPath
    if (-not (Test-Path -LiteralPath $p)) {
      throw "IncludeFiles path not found: $relPath (resolved: $p)"
    }
    $item = Get-Item -LiteralPath $p -Force
    if ($item.PSIsContainer) {
      throw "IncludeFiles must point to files, not directories: $relPath"
    }
    $files += $item
  }
} else {
  $files = Get-ChildItem -LiteralPath $localRootFull -Recurse -File -Force |
    Where-Object {
      # Only deploy site content
      $_.FullName -notmatch "\\\.git\\" -and
      $_.FullName -notmatch "\\node_modules\\" -and
      $_.FullName -notmatch "\\\.vscode\\" -and
      # Never deploy secrets (e.g. upload_api_key.local)
      $_.Name -notlike "*.local" -and
      $_.Name -notlike "*.local.*"
    }
}

# Ensure remote base directories
$baseUrl = "ftp://$ftpHost"
$remoteBase = $remotePath
if ($RemoteSubdir -and $RemoteSubdir.Trim() -ne "") {
  $remoteBase = Join-FtpPath $remoteBase $RemoteSubdir.Trim()
}
Ensure-FtpDirectory ("$baseUrl$remoteBase")

foreach ($f in $files) {
  $rel = $f.FullName.Substring($localRootFull.Length).TrimStart("\\")
  $relUrl = $rel.Replace("\", "/")

  $remoteUrl = "$baseUrl" + (Join-FtpPath $remoteBase $relUrl)

  # Ensure nested directories exist
  $parts = $relUrl.Split("/")
  if ($parts.Length -gt 1) {
    $acc = $remoteBase
    for ($i = 0; $i -lt $parts.Length - 1; $i++) {
      $acc = Join-FtpPath $acc $parts[$i]
      Ensure-FtpDirectory ("$baseUrl$acc")
    }
  }

  Upload-File $f.FullName $remoteUrl
}

Write-Host "Done. Uploaded $($files.Count) file(s)."
