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
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t rwidjaja/atscale-gatling:latest --push .

if [ $? -ne 0 ]; then
    echo "❌ Docker build failed"
    exit 1
fi

if [ $? -eq 0 ]; then
    echo "✅ Successfully published: rwidjaja/atscale-gatling:latest"
    echo ""
    echo "🎉 Now others can run using:"
    echo "   python atscale-gatling-gui.py"
    echo "   OR"
    echo "   docker run --rm -v \$(pwd)/working_dir:/app/working_dir rwidjaja/atscale-gatling:latest <executor>"
    echo ""
    echo "🔐 For PostgreSQL SSL connections, you may need to:"
    echo "   1. Mount your certificate: docker run --rm -v \$(pwd)/root.crt:/root/.postgresql/root.crt ..."
    echo "   2. Or use the built-in certificate if root.crt was present during build"
else
    echo "❌ Docker push failed"
    exit 1
fi