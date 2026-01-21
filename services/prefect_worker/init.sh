#!/bin/sh
set -e

echo "Starting prefect_worker application..."
python3 -m ingestion.deployments
