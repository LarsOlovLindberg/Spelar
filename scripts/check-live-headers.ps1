param(
  [string]$BaseUrl = "https://spelar.eu",
  [string[]]$Paths = @(
    "/data/live_status.json",
    "/data/sources_health.json",
    "/data/edge_signals_live.csv",
    "/data/pm_markets_index.json",
    "/data/pm_scan_candidates.csv",
    "/data/pm_scanner_log.csv",
    "/data/polymarket_status.json"
  )
)

$ErrorActionPreference = "Stop"

function Get-HeadInfo {
  param([Parameter(Mandatory=$true)][string]$Url)

  try {
    $resp = Invoke-WebRequest -Uri $Url -Method Head -MaximumRedirection 5 -UseBasicParsing
    $headers = $resp.Headers

    [PSCustomObject]@{
      Url          = $Url
      StatusCode   = $resp.StatusCode
      LastModified = $headers["Last-Modified"]
      CacheControl = $headers["Cache-Control"]
      ETag         = $headers["ETag"]
      ContentType  = $headers["Content-Type"]
      Length       = $headers["Content-Length"]
    }
  }
  catch {
    $msg = $_.Exception.Message
    [PSCustomObject]@{
      Url          = $Url
      StatusCode   = "ERR"
      LastModified = ""
      CacheControl = ""
      ETag         = ""
      ContentType  = ""
      Length       = ""
      Error        = $msg
    }
  }
}

$base = $BaseUrl.TrimEnd("/")
$results = foreach ($p in $Paths) {
  $path = if ($p.StartsWith("/")) { $p } else { "/$p" }
  Get-HeadInfo -Url ($base + $path)
}

$results | Format-Table -AutoSize

$errors = $results | Where-Object { $_.StatusCode -eq "ERR" -or ($_.StatusCode -as [int]) -ge 400 }
if ($errors) {
  Write-Host ""
  Write-Host "Some requests failed (see above)." -ForegroundColor Yellow
  exit 1
}
