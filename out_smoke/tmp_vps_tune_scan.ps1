$ErrorActionPreference = 'Stop'
$ip = '77.42.42.124'

Write-Host '--- patch /etc/spelar-agent.env ---'
$cmd = "f=/etc/spelar-agent.env; " +
	"sudo sed -i 's/^PM_SCAN_ORDERBOOK_SAMPLE=.*/PM_SCAN_ORDERBOOK_SAMPLE=25/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_INTERVAL_S=.*/PM_SCAN_INTERVAL_S=300/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_LIMIT=.*/PM_SCAN_LIMIT=200/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_PAGES=.*/PM_SCAN_PAGES=3/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_OFFSET=.*/PM_SCAN_OFFSET=0/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_ORDER=.*/PM_SCAN_ORDER=createdAt/' `$f; " +
	"sudo sed -i 's/^PM_SCAN_DIRECTION=.*/PM_SCAN_DIRECTION=desc/' `$f; " +
	"echo '--- effective PM_SCAN lines ---'; grep -E '^PM_SCAN_' `$f | sort"

ssh "root@$ip" "bash -lc \"$cmd\""

Write-Host '--- restart service ---'
ssh "root@$ip" "sudo systemctl restart spelar-agent; sleep 2; sudo systemctl --no-pager --full status spelar-agent | sed -n '1,60p'"
