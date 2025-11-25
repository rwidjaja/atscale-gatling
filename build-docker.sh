#!/bin/bash

echo "🔨 Building AtScale Gatling Docker image..."

# Install Hive JDBC to local Maven repository with CORRECT groupId
echo "Installing Hive JDBC driver to local Maven repository..."
./mvnw install:install-file \
  -Dfile=lib/hive-jdbc-uber-2.6.3.0-235.jar \
  -DgroupId=veil.hdp.hive \
  -DartifactId=hive-jdbc-uber \
  -Dversion=2.6.3.0-235 \
  -Dpackaging=jar \
  -DgeneratePom=true

# Build the JAR first
echo "Building JAR with Maven wrapper..."
./mvnw clean package -DskipTests

if [ $? -ne 0 ]; then
    echo "❌ Maven build failed"
    exit 1
fi

# Build Docker image
echo "Building Docker image..."
docker build -t atscale-gatling:latest .

if [ $? -eq 0 ]; then
    echo "✅ Docker image built successfully: atscale-gatling:latest"
    echo ""
    echo "Usage:"
    echo "  python atscale-gatling.py"
    echo "  OR"
    echo "  docker run --rm -v \$(pwd)/working_dir:/app/working_dir atscale-gatling:latest <executor>"
else
    echo "❌ Docker build failed"
    exit 1
fi