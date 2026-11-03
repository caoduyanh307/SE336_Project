#!/usr/bin/env bash
set -euo pipefail
marker='# SE363_DAILY_COLLECTOR'
(crontab -l 2>/dev/null || true) | grep -vF "$marker" | crontab -
echo 'Removed daily SE363 cron entry.'
