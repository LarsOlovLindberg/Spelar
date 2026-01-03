param(
  [Parameter(Mandatory = $true)]
  [string]$VpsIp,

  [Parameter(Mandatory = $false)]
  [string]$VpsUser = "root",

  [Parameter(Mandatory = $false)]
  [string]$EnvPath = "/etc/spelar-agent.env",

  [Parameter(Mandatory = $false)]
  [string]$ServiceName = "spelar-agent",

  [Parameter(Mandatory = $false)]
  [string]$UploadUrl = "https://spelar.eu/trading/api/upload_stats.php",

  [Parameter(Mandatory = $false)]
  [string]$UploadApiKeyFile = "web/trading/api/upload_api_key.local",

  [Parameter(Mandatory = $false)]
  [switch]$BundleZip,

  [Parameter(Mandatory = $false)]
  [double]$UploadIntervalS = 60,

  [Parameter(Mandatory = $false)]
  [switch]$DisableFtp,

  [Parameter(Mandatory = $false)]
  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

if (-not (Test-Path -LiteralPath $UploadApiKeyFile)) {
  throw "UploadApiKeyFile not found: $UploadApiKeyFile"
}

function ConvertTo-SedReplacement([string]$s) {
  $s = $s -replace "\\", "\\\\"
  $s = $s -replace "&", "\\&"
  $s = $s -replace "\|", "\\|"
  return $s
}

function ConvertTo-BashSingleQuoted([string]$s) {
  return ($s -replace "'", "'\\''")
}

$apiKey = (Get-Content -LiteralPath $UploadApiKeyFile -Raw).Trim()
if (-not $apiKey) {
  throw "UploadApiKeyFile is empty: $UploadApiKeyFile"
}

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  VPS enable HTTPS upload"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "UploadUrl: $UploadUrl"
Write-Host "BundleZip: $($BundleZip.IsPresent)"
Write-Host "UploadIntervalS: $UploadIntervalS"
Write-Host "(api key masked)"

$vars = @{}
$vars["UPLOAD_URL"] = $UploadUrl
$vars["UPLOAD_API_KEY"] = $apiKey
$vars["UPLOAD_INTERVAL_S"] = "$UploadIntervalS"
$vars["UPLOAD_BUNDLE_ZIP"] = $(if ($BundleZip) { "1" } else { "0" })

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"

foreach ($k in ($vars.Keys | Sort-Object)) {
  $v = [string]$vars[$k]
  $vSed = ConvertTo-SedReplacement $v
  $vEcho = ConvertTo-BashSingleQuoted "$k=$v"
  $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's|^${k}=.*|${k}=${vSed}|' '$EnvPath'; else echo '$vEcho' | sudo tee -a '$EnvPath' >/dev/null; fi"
}

if ($DisableFtp) {
  foreach ($k in @("FTP_HOST","FTP_USER","FTP_PASS","FTP_REMOTE_DIR","FTP_PROTOCOL","FTP_PORT")) {
    $lines += "sudo sed -i '/^${k}=.*/d' '$EnvPath' || true"
  }
}

$lines += "echo '--- env (upload subset, masked) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(UPLOAD_|FTP_)' '$EnvPath' | sed -E 's/(UPLOAD_API_KEY|FTP_PASS)=.*/\\1=***MASKED***/g' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart '$ServiceName'"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status '$ServiceName' | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] Enabled HTTPS upload on VPS."
