#!/bin/bash

set -e  # Exit on error

echo "🔨 Building AtScale Gatling Docker image..."
echo "=========================================="

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "pom.xml" ]; then
    print_error "pom.xml not found. Are you in the correct directory?"
    exit 1
fi

# Check for required files
REQUIRED_FILES=("config.json" "requirements.txt")
for file in "${REQUIRED_FILES[@]}"; do
    if [ ! -f "$file" ]; then
        print_warn "$file not found. Creating from template..."
        case "$file" in
            "config.json")
                echo '{
  "host": "your-atscale-host",
  "username": "your-username",
  "password": "your-password",
  "token": "your-token",
  "postgres_host": "your-postgres-host",
  "aws": {
    "region": "us-east-1",
    "secrets-key": "your-secrets-key"
  },
  "snowflake": {
    "archive": {
      "account": "your-account",
      "warehouse": "your-warehouse",
      "database": "your-database",
      "schema": "your-schema",
      "role": "your-role",
      "username": "your-username",
      "token": "your-token"
    }
  }
}' > config.json
                print_info "Created config.json template. Please edit it with your credentials."
                ;;
            "requirements.txt")
                echo "requests>=2.28.0
urllib3>=1.26.0" > requirements.txt
                print_info "Created requirements.txt"
                ;;
        esac
    fi
done

# Install Hive JDBC to local Maven repository
if [ -f "lib/hive-jdbc-uber-2.6.3.0-235.jar" ]; then
    print_info "Installing Hive JDBC driver to local Maven repository..."
    ./mvnw install:install-file \
      -Dfile=lib/hive-jdbc-uber-2.6.3.0-235.jar \
      -DgroupId=veil.hdp.hive \
      -DartifactId=hive-jdbc-uber \
      -Dversion=2.6.3.0-235 \
      -Dpackaging=jar \
      -DgeneratePom=true
else
    print_warn "Hive JDBC driver not found in lib/ directory. Skipping installation."
fi

# Run dependency check (Python)
print_info "Checking Python dependencies..."
python3 -m pip install -r requirements.txt

# Build the JAR
print_info "Building JAR with Maven wrapper..."
./mvnw clean package -DskipTests -DskipExec

if [ $? -ne 0 ]; then
    print_error "Maven build failed"
    exit 1
fi

# Build Docker image
print_info "Building Docker image..."
docker buildx create --use
docker buildx build --platform linux/amd64,linux/arm64 -t rwidjaja/atscale-gatling:latest --push .


if [ $? -eq 0 ]; then
    echo ""
    echo -e "${GREEN}✅ Docker image built successfully: atscale-gatling:latest${NC}"
    echo ""
    echo "📦 Image Details:"
    echo "  docker images | grep atscale-gatling"
    echo ""
    echo "🚀 Usage Examples:"
    echo "  GUI Mode (with display):"
    echo "    python main.py"
    echo ""
    echo "  CLI Mode:"
    echo "    python main.py --mode cli --all-models --executor QueryExtractExecutor"
    echo ""
    echo "  Dependency Check:"
    echo "    python main.py --check"
    echo "    # or"
    echo "    docker run --rm atscale-gatling:latest --check"
    echo ""
    echo "  Docker Run Examples:"
    echo "    # Run with volume mounts for persistence:"
    echo "    docker run --rm -v \$(pwd)/working_dir:/app/working_dir \\"
    echo "      -v \$(pwd)/config.json:/app/config.json \\"
    echo "      atscale-gatling:latest --mode cli --all-models"
    echo ""
    echo "    # Run with GUI (requires X11 forwarding):"
    echo "    docker run --rm -v \$(pwd)/working_dir:/app/working_dir \\"
    echo "      -v /tmp/.X11-unix:/tmp/.X11-unix -e DISPLAY=\$DISPLAY \\"
    echo "      atscale-gatling:latest --mode gui"
    echo ""
    echo "🔧 Quick Test:"
    echo "  docker run --rm atscale-gatling:latest --check"
else
    print_error "Docker build failed"
    exit 1
fi