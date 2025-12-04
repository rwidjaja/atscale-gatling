FROM eclipse-temurin:21-jdk

WORKDIR /app

# Install Maven and Python
RUN apt-get update && \
    apt-get install -y maven python3 python3-pip python3-venv python3-tk && \
    rm -rf /var/lib/apt/lists/*

# Create virtual environment
RUN python3 -m venv /opt/venv

# Ensure venv python/pip are used automatically
ENV PATH="/opt/venv/bin:$PATH"

# Copy requirements and install inside venv
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Generate Maven Wrapper
RUN mvn wrapper:wrapper && chmod +x /app/mvnw

# Copy Java project files
COPY pom.xml /app/
COPY src /app/src/
COPY lib /app/lib/
COPY settings.xml /app/
RUN mkdir -p working_dir/config working_dir/run_logs working_dir/app_logs working_dir/queries working_dir/control working_dir/ingest
COPY example_systems.properties working_dir/config/systems.properties

# Install Hive JDBC driver (optional - uncomment if needed)
# RUN mvn install:install-file \
#   -Dfile=lib/hive-jdbc-uber-2.6.3.0-235.jar \
#   -DgroupId=veil.hdp.hive \
#   -DartifactId=hive-jdbc-uber \
#   -Dversion=2.6.3.0-235 \
#   -Dpackaging=jar \
#   -DgeneratePom=true

# Build the Java application
RUN ./mvnw clean package -DskipTests -DskipExec

# Copy the pre-built JAR (from local build)
COPY target/atscale-gatling-1.0-SNAPSHOT.jar /app/atscale-gatling.jar

# Set the entrypoint
ENTRYPOINT ["java", "-jar", "atscale-gatling.jar"]