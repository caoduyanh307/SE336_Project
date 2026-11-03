#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$(readlink -f "$0")")"
mkdir -p data/logs
# flock prevents cron executions from overlapping if a previous run is still busy.
exec 9> data/logs/daily.lock
if ! flock -n 9; then
    printf '[SKIP] Previous SE363 job still running: %s\n' "$(date --iso-8601=seconds)"
    exit 0
fi
if [[ -x .venv/bin/python ]]; then
    PYTHON=.venv/bin/python
else
    PYTHON=python3
fi
"$PYTHON" daily_se363.py --once
