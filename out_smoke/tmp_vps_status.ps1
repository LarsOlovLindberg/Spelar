$ErrorActionPreference = 'Stop'
$ip = '77.42.42.124'

Write-Host '--- systemd status ---'
ssh "root@$ip" "sudo systemctl --no-pager --full status spelar-agent | sed -n '1,160p'"

Write-Host '--- journal (tail) ---'
ssh "root@$ip" "sudo journalctl -u spelar-agent -n 160 --no-pager"

Write-Host '--- /etc/spelar-agent.env (tail) ---'
ssh "root@$ip" "sudo tail -n 140 /etc/spelar-agent.env"
