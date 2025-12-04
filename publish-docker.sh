#!/bin/bash

echo "🚀 Publishing AtScale Gatling Docker image..."

# Build the JAR first
echo "Step 1: Building JAR..."
./mvnw clean package -DskipExec

if [ $? -ne 0 ]; then
    echo "❌ Maven build failed"
    exit 1
fi

# Build Docker image
echo "Step 2: Building Docker image..."
docker build --platform linux/amd64 -t rwidjaja/atscale-gatling:latest .

if [ $? -ne 0 ]; then
    echo "❌ Docker build failed"
    exit 1
fi

# Push to Docker Hub
echo "Step 3: Pushing to Docker Hub..."
docker push rwidjaja/atscale-gatling:latest

if [ $? -eq 0 ]; then
    echo "✅ Successfully published: rwidjaja/atscale-gatling:latest"
    echo ""
    echo "🎉 Now others can run using:"
    echo "   python atscale-gatling-gui.py"
    echo "   OR"
    echo "   docker run --rm -v \$(pwd)/working_dir:/app/working_dir rwidjaja/atscale-gatling:latest <executor>"
else
    echo "❌ Docker push failed"
    exit 1
fi