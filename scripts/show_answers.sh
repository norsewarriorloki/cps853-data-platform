#!/usr/bin/env bash
set -euo pipefail

echo "========================================="
echo "CPS853 Environmental Data Platform"
echo "2024 PM2.5 Analytical Results"
echo "========================================="

docker compose run --rm --no-deps -T \
    --entrypoint psql db-init \
    -X -v ON_ERROR_STOP=1 \
    -f /sql/queries/answers.sql