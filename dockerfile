FROM eclipse-temurin:21-jdk

WORKDIR /app

# Install Maven
RUN apt-get update && \
    apt-get install -y maven && \
    rm -rf /var/lib/apt/lists/*

# Generate Maven Wrapper
RUN mvn wrapper:wrapper && chmod +x /app/mvnw

# Copy essential files
COPY pom.xml /app/
COPY src /app/src/
COPY lib /app/lib/
COPY settings.xml /app/

# Install Hive JDBC driver
#RUN mvn install:install-file \
#  -Dfile=lib/hive-jdbc-uber-2.6.3.0-235.jar \
#  -DgroupId=org.apache.hive \
#  -DartifactId=hive-jdbc-uber \
#  -Dversion=2.6.3.0-235 \
#  -Dpackaging=jar \
#  -DgeneratePom=true

# Copy the pre-built JAR (from local build)
COPY target/atscale-gatling-1.0-SNAPSHOT.jar /app/atscale-gatling.jar

# Create working directories
RUN mkdir -p working_dir/config working_dir/run_logs working_dir/app_logs working_dir/queries

# Set the entrypoint
ENTRYPOINT ["java", "-jar", "atscale-gatling.jar"]