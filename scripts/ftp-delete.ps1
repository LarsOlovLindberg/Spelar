param(
  [string]$ConfigPath = "./ftp_config.local.json",
  [string]$RemoteFile,
  [string]$RemoteSubdir = "",
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not $RemoteFile -or $RemoteFile.Trim() -eq "") {
  throw "RemoteFile is required (e.g. 'trading/api/upload_api_key.local')"
}

$programDataFallback = Join-Path $env:ProgramData "spelar_eu\ftp_config.local.json"
if ($env:SPELAR_FTP_CONFIG -and $env:SPELAR_FTP_CONFIG.Trim() -ne "") {
  $ConfigPath = $env:SPELAR_FTP_CONFIG.Trim()
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  try {
    if (Test-Path -LiteralPath $programDataFallback) {
      $ConfigPath = $programDataFallback
    }
  } catch {
  }
}

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Config not found: $ConfigPath (copy ftp_config.example.json -> ftp_config.local.json)"
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

$baseUrl = "ftp://$ftpHost"
$remoteBase = $remotePath
if ($RemoteSubdir -and $RemoteSubdir.Trim() -ne "") {
  $remoteBase = Join-FtpPath $remoteBase $RemoteSubdir.Trim()
}

$remoteRel = $RemoteFile.TrimStart("/")
$remoteUrl = "$baseUrl" + (Join-FtpPath $remoteBase $remoteRel)

Write-Host "DELETE $remoteUrl"
if ($DryRun) { return }

try {
  $req = [System.Net.FtpWebRequest]::Create($remoteUrl)
  $req.Credentials = $cred
  $req.Method = [System.Net.WebRequestMethods+Ftp]::DeleteFile
  $req.UseBinary = $true
  $req.UsePassive = $true
  $req.KeepAlive = $false

  $resp = $req.GetResponse()
  $resp.Close()
  Write-Host "Deleted."
} catch {
  throw "Failed to delete ${remoteUrl}: $($_.Exception.Message)"
}
