#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Usage: ./run-executor.sh <executor-name>"
    echo "Available executors:"
    echo "  QueryExtractExecutor"
    echo "  InstallerVerQueryExtractExecutor"
    echo "  CustomQueryExtractExecutor"
    echo "  OpenStepConcurrentSimulationExecutor"
    echo "  ClosedStepConcurrentSimulationExecutor"
    echo "  OpenStepSequentialSimulationExecutor"
    echo "  ClosedStepSequentialSimulationExecutor"
    echo "  ArchiveJdbcToSnowflake"
    echo "  ArchiveXmlaToSnowflake"
    exit 1
fi

EXECUTOR=$1

echo "🐳 Running $EXECUTOR via Docker..."
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  rwidjaja/atscale-gatling:latest \
  $EXECUTOR working_dir/config/systems.properties