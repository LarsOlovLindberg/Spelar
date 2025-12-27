param(
  [string]$ConfigPath = "./ftp_config.local.json",
  [string[]]$Keep = @(
    "start_home.html",
    "strategy_about.html",
    "strategy_flow.html",
    "strategy_risk.html",
    "strategy_tech.html",
    "transparency_data.html"
  ),
  [switch]$DryRun
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $ConfigPath)) {
  throw "Config not found: $ConfigPath"
}

$cfg = Get-Content -LiteralPath $ConfigPath -Raw | ConvertFrom-Json
$ftpHost = if ($cfg.host) { [string]$cfg.host } elseif ($cfg.server) { [string]$cfg.server } else { throw "Missing host/server in config" }
$remotePath = if ($null -ne $cfg.remote_path -and [string]$cfg.remote_path -ne "") { [string]$cfg.remote_path } else { "/" }
$remotePath = $remotePath.TrimEnd("/")
$username = [string]$cfg.username
$password = [string]$cfg.password

if (-not $username -or -not $password) {
  throw "Missing username/password in config"
}

$cred = New-Object System.Net.NetworkCredential($username, $password)

function New-FtpRequest([string]$url, [string]$method) {
  $req = [System.Net.FtpWebRequest]::Create($url)
  $req.Credentials = $cred
  $req.Method = $method
  $req.UseBinary = $true
  $req.UsePassive = $true
  $req.KeepAlive = $false
  return $req
}

$baseUrl = "ftp://$ftpHost"
$pagesDir = ("$remotePath/pages").Replace("//", "/")
$listUrl = "$baseUrl$pagesDir/"

Write-Host "Listing $listUrl"
$req = New-FtpRequest $listUrl ([System.Net.WebRequestMethods+Ftp]::ListDirectory)
$resp = $req.GetResponse()
$sr = New-Object System.IO.StreamReader($resp.GetResponseStream())
$text = $sr.ReadToEnd()
$sr.Close()
$resp.Close()

$files = $text -split "`r?`n" | Where-Object { $_ -and $_.Trim() -ne "" } | ForEach-Object { $_.Trim() }
$toDelete = $files | Where-Object { $Keep -notcontains $_ }

Write-Host ("Remote pages found: {0}" -f $files.Count)
Write-Host ("Keeping: {0}" -f ($Keep -join ", "))
Write-Host ("Deleting: {0}" -f $toDelete.Count)

foreach ($f in $toDelete) {
  $url = "$baseUrl$pagesDir/$f"
  Write-Host "DELETE $url"
  if ($DryRun) { continue }
  try {
    $d = New-FtpRequest $url ([System.Net.WebRequestMethods+Ftp]::DeleteFile)
    $dr = $d.GetResponse()
    $dr.Close()
  } catch {
    Write-Host "WARN delete failed: $f :: $($_.Exception.Message)"
  }
}

Write-Host "Done pruning remote pages."