#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
if ! command -v crontab >/dev/null 2>&1; then
   echo 'Chua co cron. Ubuntu: sudo apt install cron && sudo systemctl enable --now cron'
   exit 1
fi
chmod +x run_daily.sh
mkdir -p data/logs
project="$(pwd)"
case "$project" in *' '*) echo 'Project path contains spaces; use a path without spaces.'; exit 1;; esac
marker='# SE363_DAILY_COLLECTOR'
entry="0 2 * * * $project/run_daily.sh >> $project/data/logs/cron.log 2>&1 $marker"
{ crontab -l 2>/dev/null | grep -vF "$marker" || true; echo "$entry"; } | crontab -
echo 'Installed daily at 02:00 SYSTEM local timezone, while computer is on:'
echo "$entry"
echo "Current local time: $(date '+%Y-%m-%d %H:%M:%S %Z %z')"
echo 'Please verify the local timezone is Asia/Ho_Chi_Minh. Cron does not run when laptop is shut down.'
