import requests
import xml.etree.ElementTree as ET
import json
import sys
import subprocess
import os
import urllib3
from InquirerPy import inquirer

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Load configuration
with open("config.json") as f:
    cfg = json.load(f)

USERNAME = cfg["username"]
PASSWORD = cfg["password"]
HOST = cfg["host"]  # e.g. "ubuntu-atscale.atscaledomain.com"
TOKEN = cfg["token"]  # e.g. "37022d31-a848-4e89-71de-7047cc69ee77"
PROXY = cfg.get("proxy", "")
PROXYPORT = cfg.get("proxyport", "")
PROXY_USERNAME = cfg.get("proxy_username", "")
PROXY_PASSWORD = cfg.get("proxy_password", "")    
POSTGRES_HOST = cfg["postgres_host"]
AWS_REGION = cfg.get("aws.region", "")
AWS_SECRETS_KEY = cfg.get("aws.secrets-key", "")
SNOWFLAKE_ACCOUNT = cfg.get("snowflake.archive.account", "")
SNOWFLAKE_WAREHOUSE = cfg.get("snowflake.archive.warehouse", "")
SNOWFLAKE_DATABASE = cfg.get("snowflake.archive.database", "")
SNOWFLAKE_SCHEMA = cfg.get("snowflake.archive.schema", "")
SNOWFLAKE_ROLE = cfg.get("snowflake.archive.role", "")
SNOWFLAKE_USERNAME = cfg.get("snowflake.archive.username", "")
SNOWFLAKE_PASSWORD = cfg.get("snowflake.archive.password", "")


# SOAP Templates
CATALOG_QUERY = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement>
              SELECT [CATALOG_NAME] from $system.DBSCHEMA_CATALOGS
        </Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>Default</Catalog>
          <Cube>Default</Cube>
        </PropertyList>
      </Properties>
    </Execute>
  </soap:Body>
</soap:Envelope>"""

CUBE_QUERY_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
               xmlns:xsd="http://www.w3.org/2001/XMLSchema">
  <soap:Body>
    <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
      <Command>
        <Statement>
              SELECT [CUBE_NAME] from $system.MDSCHEMA_CUBES
        </Statement>
      </Command>
      <Properties>
        <PropertyList>
          <Catalog>{catalog}</Catalog>
          <Cube>Default</Cube>
        </PropertyList>
      </Properties>
    </Execute>
  </soap:Body>
</soap:Envelope>"""

# Base SQL query template
BASE_SQL_QUERY = """SELECT
    q.service,
    q.query_language,
    q.query_text as inbound_text,
    MAX(s.subquery_text) as outbound_text,
    p.cube_name,
    p.project_id,
    case when MAX(s.subquery_text) like '%as_agg_%' then true else false end as used_agg,
    COUNT(*)                             AS num_times,
    AVG(r.finished - p.planning_started) AS elasped_time_in_seconds,
    AVG(r.result_size)                   AS avg_result_size
FROM
    atscale.queries q
INNER JOIN
    atscale.query_results r
ON
    q.query_id=r.query_id
INNER JOIN
    atscale.queries_planned p
ON
    q.query_id=p.query_id
INNER JOIN
    atscale.subqueries s
ON
    q.query_id=s.query_id
WHERE
    q.query_language = ? AND
    p.planning_started > current_timestamp - interval '60 day'
    and p.cube_name = ?
    AND q.service = 'user-query'
    AND r.succeeded = true
    AND LENGTH(q.query_text) > 100
    AND q.query_text NOT LIKE '/* Virtual query to get the members of a level */%'
    AND q.query_text NOT LIKE '-- statement does not return rows%'
GROUP BY
    1,
    2,
    3,
    5,
    6
ORDER BY 3"""

# ----------------------------
# Proxy Configuration
# ----------------------------
def should_use_proxy(executor):
    """Determine if proxy should be used for this executor"""
    if not PROXY or not PROXYPORT:
        return False
    
    # Only use proxy for archive to snowflake steps
    archive_executors = [
        'ArchiveJdbcToSnowflake',
        'ArchiveXmlaToSnowflake'
    ]
    
    return executor in archive_executors

def get_proxy_java_properties():
    """Get Java system properties for proxy configuration"""
    if not PROXY or not PROXYPORT:
        return []
    
    return [
        f'-Dhttp.proxyHost={PROXY}',
        f'-Dhttp.proxyPort={PROXYPORT}',
        f'-Dhttps.proxyHost={PROXY}',
        f'-Dhttps.proxyPort={PROXYPORT}',
        f'-Dhttp.nonProxyHosts=localhost|127.0.0.1|{HOST}'
    ]


# Docker image name
DOCKER_IMAGE = "rwidjaja/atscale-gatling:latest"

def create_base_query_file():
    """Create base_query.sql file if it doesn't exist"""
    config_dir = "working_dir/config"
    base_query_path = os.path.join(config_dir, "base_query.sql")
    
    # Create config directory if it doesn't exist
    os.makedirs(config_dir, exist_ok=True)
    
    # Create base_query.sql if it doesn't exist
    if not os.path.exists(base_query_path):
        print(f"📝 Creating base_query.sql in {config_dir}")
        with open(base_query_path, "w") as f:
            f.write(BASE_SQL_QUERY)
        print("✅ base_query.sql created successfully")
    else:
        print(f"✅ base_query.sql already exists in {config_dir}")

def check_docker_image():
    """Check if Docker image exists"""
    result = subprocess.run(
        ["docker", "image", "inspect", DOCKER_IMAGE],
        capture_output=True, text=True
    )
    return result.returncode == 0

def pull_docker_image():
    """Pull the Docker image"""
    print(f"📥 Pulling Docker image: {DOCKER_IMAGE}")
    result = subprocess.run(
        ["docker", "pull", DOCKER_IMAGE],
        capture_output=True, text=True
    )
    if result.returncode == 0:
        print("✅ Docker image pulled successfully")
        return True
    else:
        print(f"❌ Failed to pull Docker image: {result.stderr}")
        return False

def ensure_docker_image():
    """Ensure Docker image is available, pull if not"""
    if check_docker_image():
        return True
    
    print(f"🐳 Docker image '{DOCKER_IMAGE}' not found locally")
    return pull_docker_image()

def run_xmla_query(xml_body: str):
    url = f"https://{HOST}:10502/xmla/default"
    resp = requests.post(
        url,
        data=xml_body.encode("utf-8"),
        headers={"Content-Type": "text/xml"},
        auth=(USERNAME, PASSWORD),
        verify=False
    )
    resp.raise_for_status()
    return resp.text

def parse_catalogs(xml_text: str):
    root = ET.fromstring(xml_text)
    return [el.text for el in root.findall(".//{urn:schemas-microsoft-com:xml-analysis:rowset}CATALOG_NAME")]

def parse_cubes(xml_text: str):
    root = ET.fromstring(xml_text)
    return [el.text for el in root.findall(".//{urn:schemas-microsoft-com:xml-analysis:rowset}CUBE_NAME")]

def write_systems_properties(selected_pairs):
    os.makedirs("working_dir/config", exist_ok=True)
    filepath = "working_dir/config/systems.properties"

    cubes = [pair.split("::")[1].strip() for pair in selected_pairs]
    catalogs = [pair.split("::")[0].strip() for pair in selected_pairs]

    with open(filepath, "w") as f:
        f.write("atscale.schema.type=installer\n")
        f.write("atscale.models=" + ", ".join(catalogs) + "\n")

        for pair in selected_pairs:
            catalog, cube = [p.strip() for p in pair.split("::")]
            cube_key = cube.replace(" ", "_")
            catalog_jdbc_name = catalog.replace(" ", "%20")

            f.write(f"atscale.{cube_key}.jdbc.url=jdbc:postgresql://{HOST}:15432/{catalog_jdbc_name}\n")
            f.write(f"atscale.{cube_key}.jdbc.username={USERNAME}\n")
            f.write(f"atscale.{cube_key}.jdbc.password={PASSWORD}\n")
            f.write(f"atscale.{cube_key}.jdbc.maxPoolSize=10\n")
            f.write(f"atscale.{cube_key}.jdbc.log.resultset.rows=true\n")
            f.write(f"atscale.{cube_key}.xmla.auth.url=https://{HOST}:10500/default/auth\n")
            f.write(f"atscale.{cube_key}.xmla.url=https://{HOST}:10502/xmla/default/{TOKEN}\n")
            f.write(f"atscale.{cube_key}.xmla.cube={cube}\n")
            f.write(f"atscale.{cube_key}.xmla.catalog={catalog}\n")
            f.write(f"atscale.{cube_key}.xmla.log.responsebody=true\n")
            f.write(f"atscale.{cube_key}.xmla.auth.username={USERNAME}\n")
            f.write(f"atscale.{cube_key}.xmla.auth.password={PASSWORD}\n")
            f.write("# \n")

        f.write(f"atscale.postgres.jdbc.url=jdbc:postgresql://{POSTGRES_HOST}:10520/atscale\n")
        f.write("atscale.postgres.jdbc.username=atscale\n")
        f.write("atscale.postgres.jdbc.password=atscale\n")
        f.write("#System Parameter\n")
        f.write("atscale.gatling.throttle.ms=5\n")
        f.write("atscale.xmla.maxConnectionsPerHost=20\n")
        f.write("atscale.xmla.useAggregates=true\n")
        f.write("atscale.xmla.generateAggregates=false\n")
        f.write("atscale.xmla.useQueryCache=false\n")
        f.write("atscale.xmla.useAggregateCache=true\n")
        f.write("atscale.jdbc.useAggregates=true\n")
        f.write("atscale.jdbc.generateAggregates=false\n")
        f.write("atscale.jdbc.useLocalCache=false\n")
        # Add AWS Secret Manager configuration only if values are present
        if AWS_REGION or AWS_SECRETS_KEY:
            f.write("#AWS Secret Manager\n")
            if AWS_REGION:
                f.write(f"aws.region={AWS_REGION}\n")
            if AWS_SECRETS_KEY:
                f.write(f"aws.secrets-key={AWS_SECRETS_KEY}\n")
            f.write("\n")

        # Add Snowflake Properties for Archiving Logs to Snowflake only if account is present
        if SNOWFLAKE_ACCOUNT:
            f.write("#Snowflake Properties for Archiving Logs to Snowflake\n")
            f.write(f"snowflake.archive.account={SNOWFLAKE_ACCOUNT}\n")
            if SNOWFLAKE_WAREHOUSE:
                f.write(f"snowflake.archive.warehouse={SNOWFLAKE_WAREHOUSE}\n")
            if SNOWFLAKE_DATABASE:
                f.write(f"snowflake.archive.database={SNOWFLAKE_DATABASE}\n")
            if SNOWFLAKE_SCHEMA:
                f.write(f"snowflake.archive.schema={SNOWFLAKE_SCHEMA}\n")
            if SNOWFLAKE_ROLE:
                f.write(f"snowflake.archive.role={SNOWFLAKE_ROLE}\n")
            if SNOWFLAKE_USERNAME:
                f.write(f"snowflake.archive.username={SNOWFLAKE_USERNAME}\n")
            if SNOWFLAKE_PASSWORD:
                f.write(f"snowflake.archive.password={SNOWFLAKE_PASSWORD}\n")
                
    print(f"✅ systems.properties written to {filepath}")

def build_docker_command(executor_name, docker_image):
    """Build Docker command with optional cacerts and base_query.sql mounting"""
    cmd = [
        "docker", "run", "--rm",
        "-v", f"{os.getcwd()}/working_dir:/app/working_dir",
        "-v", f"{os.getcwd()}/config.json:/app/config.json",
    ]
    
    # Add cacerts if available
    cacerts_path = os.path.join(os.getcwd(), "cacerts")
    if os.path.isfile(cacerts_path):
        cmd.extend([
            "-v", f"{cacerts_path}:/app/cacerts",
            "-e", "JAVA_TOOL_OPTIONS=-Djavax.net.ssl.trustStore=/app/cacerts -Djavax.net.ssl.trustStorePassword=changeit"
        ])
    
        
    # ADD PROXY SETTINGS FOR SNOWFLAKE ARCHIVE EXECUTORS
    if executor_name in ["ArchiveJdbcToSnowflake", "ArchiveXmlaToSnowflake"]:
        print(f"🔧 Configuring proxy settings for {executor_name}")
        
        # Add proxy environment variables (use your actual proxy settings)
        proxy_host = cfg.get("proxy", "your-proxy-server.com")
        proxy_port = cfg.get("proxyport", "8080")
        proxy_user = cfg.get("proxy_username", "")
        proxy_pass = cfg.get("proxy_password", "")
        
        cmd.extend([
            "-e", f"HTTP_PROXY=http://{proxy_host}:{proxy_port}",
            "-e", f"HTTPS_PROXY=http://{proxy_host}:{proxy_port}",
            "-e", f"http.proxyHost={proxy_host}",
            "-e", f"http.proxyPort={proxy_port}",
            "-e", f"https.proxyHost={proxy_host}", 
            "-e", f"https.proxyPort={proxy_port}",
        ])
        
        # Add proxy authentication if provided
        if proxy_user and proxy_pass:
            cmd.extend([
                "-e", f"http.proxyUser={proxy_user}",
                "-e", f"http.proxyPassword={proxy_pass}",
                "-e", f"https.proxyUser={proxy_user}",
                "-e", f"https.proxyPassword={proxy_pass}",
            ])
    
    # Add image and arguments
    cmd.extend([docker_image, executor_name, "working_dir/config/systems.properties"])
    
    return cmd

def run_executor(executor_name):
    """Run executor using Docker container"""
    if not ensure_docker_image():
        print("❌ Cannot run without Docker image")
        return False

    # Create log directory
    os.makedirs("working_dir/run_logs", exist_ok=True)
    log_path = f"working_dir/run_logs/{executor_name}.log"

    # Docker command
    docker_cmd = build_docker_command(executor_name, DOCKER_IMAGE)

    print(f"🐳 Running {executor_name}...")
    
    try:
        with open(log_path, "w") as log_file:
            result = subprocess.run(
                docker_cmd,
                stdout=log_file,
                stderr=log_file,
                text=True
            )
        
        if result.returncode == 0:
            print(f"✅ {executor_name} completed successfully")
            print(f"📄 Logs: {log_path}")
            return True
        else:
            print(f"❌ {executor_name} failed (see {log_path})")
            # Show last few lines for context
            try:
                with open(log_path, "r") as f:
                    lines = f.readlines()
                    if lines:
                        print("Last few lines of log:")
                        for line in lines[-10:]:
                            print(f"  {line.strip()}")
            except:
                pass
            return False
            
    except Exception as e:
        print(f"❌ Error running {executor_name}: {e}")
        return False

def select_and_run_executor():
    """Executor selection menu"""
    executors = [
        "InstallerVerQueryExtractExecutor",
        "CustomQueryExtractExecutor",
        "QueryExtractExecutor",
        "OpenStepConcurrentSimulationExecutor",
        "ClosedStepConcurrentSimulationExecutor",
        "OpenStepSequentialSimulationExecutor", 
        "ClosedStepSequentialSimulationExecutor",
        "ArchiveJdbcToSnowflake",
        "ArchiveXmlaToSnowflake",
        "Exit"
    ]

    try:
        selected = inquirer.select(
            message="Select Executor to run:",
            choices=executors
        ).execute()
    except KeyboardInterrupt:
        return "exit"

    if selected == "Exit":
        return "exit"
    
    success = run_executor(selected)
    return "continue"

def ensure_control_directory():
    """Ensure control directory exists for stop signals"""
    control_dir = os.path.join("working_dir", "control")
    os.makedirs(control_dir, exist_ok=True)
    return control_dir

def create_stop_simulation_file():
    """Create stop simulation file for graceful termination"""
    control_dir = ensure_control_directory()
    stop_file = os.path.join(control_dir, "stop_simulation")
    
    with open(stop_file, 'w') as f:
        f.write("Stop signal created at: " + str(datetime.now()))
    
    print(f"🛑 Stop signal created: {stop_file}")
    print("Simulations will stop gracefully after current task")
    
    
def main():
    # Ensure Docker is available first
    try:
        subprocess.run(["docker", "--version"], capture_output=True, check=True)
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("❌ Docker is not installed or not available")
        print("   Please install Docker first: https://docs.docker.com/get-docker/")
        sys.exit(1)
    ensure_control_directory()
    # Create base_query.sql file
    create_base_query_file()

    # Check and pull Docker image if needed
    print("🔍 Checking for Docker image...")
    if not ensure_docker_image():
        print("❌ Cannot continue without Docker image")
        sys.exit(1)

    # Discover catalogs and cubes
    print("🔍 Discovering AtScale catalogs and cubes...")
    try:
        cat_xml = run_xmla_query(CATALOG_QUERY)
        catalogs = parse_catalogs(cat_xml)
    except Exception as e:
        print(f"❌ Failed to connect to AtScale: {e}")
        print("   Please check your config.json credentials and network connectivity")
        sys.exit(1)

    results = []
    for cat in catalogs:
        try:
            cube_xml = run_xmla_query(CUBE_QUERY_TEMPLATE.format(catalog=cat))
            cubes = parse_cubes(cube_xml)
            for cube in cubes:
                results.append(f"{cat} :: {cube}")
        except Exception as e:
            print(f"⚠️  Failed to get cubes for catalog {cat}: {e}")

    if not results:
        print("❌ No cubes found. Please check your AtScale instance and credentials.")
        sys.exit(1)

    try:
        selected_pairs = inquirer.checkbox(
            message="Select Catalog/Cube pairs:",
            choices=results
        ).execute()
    except KeyboardInterrupt:
        sys.exit(0)

    if not selected_pairs:
        print("No Catalog/Cube selected. Exiting.")
        sys.exit(0)

    # Generate configuration
    write_systems_properties(selected_pairs)
    
    print("\n" + "="*50)
    print("Configuration complete! Ready to run executors.")
    print("="*50 + "\n")

    # Main executor loop
    while True:
        result = select_and_run_executor()
        if result == "exit":
            break
        
        # Continue?
        try:
            continue_running = inquirer.select(
                message="Run another executor?",
                choices=["Yes", "No"],
                default="Yes"
            ).execute()
            if continue_running == "No":
                break
        except KeyboardInterrupt:
            break

    print("👋 Exiting...")

if __name__ == "__main__":
    main()