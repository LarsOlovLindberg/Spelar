param(
  [string]$ConfigPath = "./ftp_config.local.json",
  [string[]]$PathsToTry = @(),
  [int]$MaxItems = 50
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Config not found: $ConfigPath"
}

$config = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$ftpHost = if ($config.host) { [string]$config.host } elseif ($config.server) { [string]$config.server } else { throw "Missing host/server in config" }
$username = [string]$config.username
$password = [string]$config.password

if (-not $username -or -not $password) {
  throw "Missing username/password in config"
}

$cred = New-Object System.Net.NetworkCredential($username, $password)

function Normalize-Path([string]$p) {
  if (-not $p) { return "/" }
  $p = $p.Trim()
  if ($p -eq "") { return "/" }
  if (-not $p.StartsWith("/")) { $p = "/" + $p }
  return $p.TrimEnd("/")
}

function Try-List([string]$path) {
  $path = Normalize-Path $path
  $url = "ftp://$ftpHost$path"

  Write-Host "==== LIST $url ===="
  try {
    $req = [System.Net.FtpWebRequest]::Create($url)
    $req.Credentials = $cred
    $req.Method = [System.Net.WebRequestMethods+Ftp]::ListDirectory
    $req.UseBinary = $true
    $req.UsePassive = $true
    $req.KeepAlive = $false

    $resp = $req.GetResponse()
    $stream = $resp.GetResponseStream()
    $reader = New-Object System.IO.StreamReader($stream)
    $text = $reader.ReadToEnd()
    $reader.Dispose(); $stream.Dispose(); $resp.Close()

    $lines = $text -split "`r?`n" | Where-Object { $_ -and $_.Trim() -ne "" }
    if ($lines.Count -gt $MaxItems) {
      $lines = $lines | Select-Object -First $MaxItems
      Write-Host "(showing first $MaxItems items)"
    }

    if ($lines.Count -eq 0) {
      Write-Host "(empty or no listing returned)"
    } else {
      $lines | ForEach-Object { Write-Host " - $_" }
    }

    return $true
  } catch {
    $msg = $_.Exception.Message
    if ($_.Exception.InnerException) { $msg += " | " + $_.Exception.InnerException.Message }
    Write-Warning $msg
    return $false
  }
}

$remotePath = if ($null -ne $config.remote_path) { [string]$config.remote_path } else { "/" }

if (-not $PathsToTry -or $PathsToTry.Count -eq 0) {
  $PathsToTry = @(
    $remotePath,
    "/",
    "/web",
    "/public_html",
    "/public_html/web",
    "/httpdocs",
    "/www",
    "/htdocs"
  )
}

$seen = New-Object System.Collections.Generic.HashSet[string]
foreach ($p in $PathsToTry) {
  $np = Normalize-Path $p
  if ($seen.Add($np)) {
    [void](Try-List $np)
    Write-Host ""
  }
}

Write-Host "Done."