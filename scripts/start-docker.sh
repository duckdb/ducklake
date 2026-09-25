#!/usr/bin/env bash
if test ! -f "./scripts/docker-compose.yml"
then
  # in CI
  echo "Please run from duckdb root."
  exit 1
fi

# cd into scripts where docker-compose file is.
cd scripts

set -ex

docker compose down --volumes --timeout 0

# Starts SeaweedFS and blocks until the bucket is writable
docker compose run --rm s3-init || { docker compose logs; exit 1; }
