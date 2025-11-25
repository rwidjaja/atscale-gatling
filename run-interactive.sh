#!/bin/bash

echo "🎯 Starting AtScale Gatling Interactive Mode"
echo "Using Docker image: rwidjaja/atscale-gatling:latest"
echo ""

# Check if systems.properties exists
if [ ! -f "working_dir/config/systems.properties" ]; then
    echo "⚠️  No configuration found. Please run the Python script first."
    echo "   python atscale-gatling.py"
    exit 1
fi

python atscale-gatling.py