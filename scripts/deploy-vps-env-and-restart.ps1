param(
  [string]$VpsIp = "77.42.42.124",
  [string]$VpsUser = "root",
  [string]$RemoteRoot = "/opt/spelar_eu",
  [string]$ServiceName = "spelar-agent",

  # Local files
  [string]$EnvTemplatePath = "./vps/systemd/spelar-agent.env.example",
  [string]$ServiceUnitPath = "./vps/systemd/spelar-agent.service",

  # Optional local-only secrets file; gitignored by default
  [string]$LocalKrakenKeysPath = "./kraken_keys.local.json"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command "ssh" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'ssh'. Install Windows OpenSSH Client and ensure it is in PATH."
}
if (-not (Get-Command "scp" -ErrorAction SilentlyContinue)) {
  throw "Missing required command 'scp'. Install Windows OpenSSH Client and ensure it is in PATH."
}

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..")
$envTemplate = Join-Path $repoRoot $EnvTemplatePath
$serviceUnit = Join-Path $repoRoot $ServiceUnitPath

if (-not (Test-Path -LiteralPath $envTemplate)) { throw "Env template not found: $envTemplate" }
if (-not (Test-Path -LiteralPath $serviceUnit)) { throw "Service unit not found: $serviceUnit" }

$target = "$VpsUser@$VpsIp"

# Create a VPS env file with sane defaults. Users can edit /etc/spelar-agent.env later.
$envLines = Get-Content -LiteralPath $envTemplate -Raw

# Force OUT_DIR and service paths to match our systemd unit
$envLines = $envLines -replace "(?m)^OUT_DIR=.*$", "OUT_DIR=$RemoteRoot/vps/out"
$envLines = $envLines -replace "(?m)^KILLSWITCH_FILE=.*$", "KILLSWITCH_FILE=$RemoteRoot/vps/out/KILLSWITCH"

$tmpEnv = Join-Path $env:TEMP "spelar-agent.env"
$envLines | Out-File $tmpEnv -Encoding ASCII -Force

Write-Host "[VPS] Uploading env + systemd unit..."
ssh $target "sudo mkdir -p /etc/spelar_eu; sudo chmod 700 /etc/spelar_eu; sudo mkdir -p $RemoteRoot" | Out-Host

scp $tmpEnv "$target`:/tmp/spelar-agent.env" | Out-Host
ssh $target "sudo mv /tmp/spelar-agent.env /etc/spelar-agent.env; sudo chmod 600 /etc/spelar-agent.env" | Out-Host

scp $serviceUnit "$target`:/tmp/spelar-agent.service" | Out-Host
ssh $target "sudo mv /tmp/spelar-agent.service /etc/systemd/system/$ServiceName.service; sudo chmod 644 /etc/systemd/system/$ServiceName.service" | Out-Host

# Upload Kraken keys if present
$keysPath = Join-Path $repoRoot $LocalKrakenKeysPath
if (Test-Path -LiteralPath $keysPath) {
  Write-Host "[VPS] Uploading Kraken keys to /etc/spelar_eu/kraken_keys.local.json..."
  scp $keysPath "$target`:/tmp/kraken_keys.local.json" | Out-Host
  ssh $target "sudo mv /tmp/kraken_keys.local.json /etc/spelar_eu/kraken_keys.local.json; sudo chmod 600 /etc/spelar_eu/kraken_keys.local.json" | Out-Host
} else {
  Write-Warning "No Kraken keys found at: $keysPath (skipping)."
}

Write-Host "[VPS] Reloading systemd and restarting service..."
ssh $target "sudo systemctl daemon-reload; sudo systemctl enable $ServiceName; sudo systemctl restart $ServiceName" | Out-Host

Write-Host "[VPS] Status:"
ssh $target "sudo systemctl --no-pager --full status $ServiceName | head -120" | Out-Host

Write-Host "[OK] VPS env + systemd updated. Edit /etc/spelar-agent.env on VPS as needed."
