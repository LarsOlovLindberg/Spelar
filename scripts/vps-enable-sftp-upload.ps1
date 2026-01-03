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
  [string]$FtpConfigPath = "ftp_config.local.json",

  [Parameter(Mandatory = $false)]
  [ValidateSet("ftp", "sftp")]
  [string]$Protocol = "sftp",

  [Parameter(Mandatory = $false)]
  [int]$Port = 0,

  [Parameter(Mandatory = $false)]
  [string]$RemoteDir = "",

  [Parameter(Mandatory = $false)]
  [switch]$NoRestart
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}

if (-not (Test-Path -LiteralPath $FtpConfigPath)) {
  throw "FtpConfigPath not found: $FtpConfigPath"
}

function ConvertTo-BashSingleQuoted([string]$s) {
  # Wrap value in single quotes in bash; escape embedded single quotes.
  return ($s -replace "'", "'\\''")
}

function ConvertTo-SedReplacement([string]$s) {
  # Escape characters that are special in sed replacement.
  $s = $s -replace "\\", "\\\\"
  $s = $s -replace "&", "\\&"
  $s = $s -replace "\|", "\\|"
  return $s
}

$cfg = Get-Content -LiteralPath $FtpConfigPath -Raw | ConvertFrom-Json

$ftpHost = ""
if ($cfg.host) { $ftpHost = [string]$cfg.host }
elseif ($cfg.server) { $ftpHost = [string]$cfg.server }

$user = [string]$cfg.username
$pass = [string]$cfg.password
$remotePath = [string]$cfg.remote_path

if (-not $ftpHost) { throw "Missing host/server in $FtpConfigPath" }
if (-not $user) { throw "Missing username in $FtpConfigPath" }
if (-not $pass) { throw "Missing password in $FtpConfigPath" }
if (-not $remotePath) { throw "Missing remote_path in $FtpConfigPath" }

if (-not $RemoteDir) {
  $RemoteDir = ($remotePath.TrimEnd('/') + "/data")
}

if ($Port -le 0) {
  $Port = if ($Protocol -eq "sftp") { 22 } else { 21 }
}

$target = "$VpsUser@$VpsIp"

Write-Host "==============================="
Write-Host "  VPS enable $Protocol upload"
Write-Host "==============================="
Write-Host "VPS: $target"
Write-Host "EnvPath: $EnvPath"
Write-Host "Host: $ftpHost"
Write-Host "User: $user"
Write-Host "RemoteDir: $RemoteDir"
Write-Host "Protocol: $Protocol  Port: $Port"
Write-Host "(password masked)"

$vars = @{}
$vars["FTP_HOST"] = $ftpHost
$vars["FTP_USER"] = $user
$vars["FTP_PASS"] = $pass
$vars["FTP_REMOTE_DIR"] = $RemoteDir
$vars["FTP_PROTOCOL"] = $Protocol
$vars["FTP_PORT"] = "$Port"

# Ensure we don't accidentally prefer HTTP upload over FTP/SFTP.
$clearVars = @("UPLOAD_URL", "UPLOAD_API_KEY")

$lines = @()
$lines += "set -e"
$lines += "sudo touch '$EnvPath'"

foreach ($k in ($vars.Keys | Sort-Object)) {
  $v = [string]$vars[$k]
  $vSed = ConvertTo-SedReplacement $v
  $vEcho = ConvertTo-BashSingleQuoted "$k=$v"
  $lines += "if sudo grep -qE '^${k}=' '$EnvPath'; then sudo sed -i 's|^${k}=.*|${k}=${vSed}|' '$EnvPath'; else echo '$vEcho' | sudo tee -a '$EnvPath' >/dev/null; fi"
}

foreach ($k in $clearVars) {
  $lines += "sudo sed -i '/^${k}=.*/d' '$EnvPath' || true"
}

$lines += "echo '--- env (upload subset, masked) ---'"
$lines += "sudo test -f '$EnvPath' && egrep '^(FTP_|UPLOAD_)' '$EnvPath' | sed -E 's/(PASS|KEY)=.*/\\1=***MASKED***/g' || true"

if (-not $NoRestart) {
  $lines += "echo '--- restart ---'"
  $lines += "sudo systemctl restart '$ServiceName'"
  $lines += "sleep 2"
  $lines += "sudo systemctl --no-pager --full status '$ServiceName' | head -80"
}

$remote = $lines -join "; "
ssh $target $remote | Out-Host

Write-Host "[OK] Enabled $Protocol upload on VPS."
