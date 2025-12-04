import requests
import xml.etree.ElementTree as ET
import json
import sys
import subprocess
import os
import urllib3
from InquirerPy import inquirer

# Suppress SSL warnings since verify=False is used
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ----------------------------
# Load credentials & host
# ----------------------------
with open("config.json") as f:
    cfg = json.load(f)

USERNAME = cfg["username"]
PASSWORD = cfg["password"]
HOST = cfg["host"]  # e.g. "ubuntu-atscale.atscaledomain.com"
TOKEN = cfg["token"]  # e.g. "37022d31-a848-4e89-71de-7047cc69ee77"
PROXY = cfg.get("proxy", "")
PROXYPORT = cfg.get("proxyport", "")    
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

# ----------------------------
# SOAP Templates
# ----------------------------
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

def get_proxy_environment():
    """Get proxy environment variables if proxy is configured"""
    if not PROXY or not PROXYPORT:
        return {}
    
    return {
        'HTTP_PROXY': f'http://{PROXY}:{PROXYPORT}',
        'HTTPS_PROXY': f'http://{PROXY}:{PROXYPORT}',
        'http_proxy': f'http://{PROXY}:{PROXYPORT}',
        'https_proxy': f'http://{PROXY}:{PROXYPORT}',
        'NO_PROXY': f'localhost,127.0.0.1,{HOST}',
        'no_proxy': f'localhost,127.0.0.1,{HOST}'
    }

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

# ----------------------------
# Helpers
# ----------------------------
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

    # Print summary of AWS/Snowflake configuration
    print("\nAWS/Snowflake Configuration Summary:")
    if AWS_REGION:
        print(f"  AWS Region: {AWS_REGION}")
    if AWS_SECRETS_KEY:
        print(f"  AWS Secrets Key: {AWS_SECRETS_KEY}")
    
    if SNOWFLAKE_ACCOUNT:
        print(f"  Snowflake Account: {SNOWFLAKE_ACCOUNT}")
        if SNOWFLAKE_WAREHOUSE:
            print(f"  Snowflake Warehouse: {SNOWFLAKE_WAREHOUSE}")
        if SNOWFLAKE_DATABASE:
            print(f"  Snowflake Database: {SNOWFLAKE_DATABASE}")
        if SNOWFLAKE_SCHEMA:
            print(f"  Snowflake Schema: {SNOWFLAKE_SCHEMA}")
        if SNOWFLAKE_ROLE:
            print(f"  Snowflake Role: {SNOWFLAKE_ROLE}")
        if SNOWFLAKE_USERNAME:
            print(f"  Snowflake Username: {SNOWFLAKE_USERNAME}")
        if SNOWFLAKE_PASSWORD:
            print(f"  Snowflake Password: {'*' * len(SNOWFLAKE_PASSWORD)}")
    else:
        print("  Snowflake: Not configured")

def run_docker(selected_executor):
    docker_cmd = [
        "docker", "run", "--rm",
        "--env-file", ".env",
        "--platform", "linux/amd64",
        "-v", "$(pwd)/working_dir/config/systems.properties:/app/target/classes/systems.properties:ro",
        "-v", "$(pwd)/working_dir/run_logs:/app/run_logs",
        "-v", "$(pwd)/working_dir/app_logs:/app/app_logs",
        "-v", "$(pwd)/working_dir/queries:/app/queries",
        "rwidjaja/atscale-gatling:latest"
    ]

    print("\nExecuting Docker command...")
    os.makedirs("working_dir/run_logs", exist_ok=True)
    log_path = os.path.join("working_dir", "run_logs", "docker_output.log")

    try:
        with open(log_path, "w") as log_file:
            subprocess.run(
                " ".join(docker_cmd),
                shell=True,
                check=True,
                stdout=log_file,
                stderr=log_file
            )
        print(f"Docker run completed. Logs written to {log_path}")
    except subprocess.CalledProcessError as e:
        print(f"Docker run failed (see {log_path} for details): {e}")
        sys.exit(1)

def run_local(selected_executor):
    """Run the selected executor locally using Maven"""
    
    # Map executor names to their Java classes
    executor_to_class = {
        "InstallerVerQueryExtractExecutor": "executors.InstallerVerQueryExtractExecutor",
        "CustomQueryExtractExecutor": "executors.CustomQueryExtractExecutor", 
        "QueryExtractExecutor": "executors.QueryExtractExecutor",
        "OpenStepConcurrentSimulationExecutor": "executors.OpenStepConcurrentSimulationExecutor",
        "ClosedStepConcurrentSimulationExecutor": "executors.ClosedStepConcurrentSimulationExecutor",
        "OpenStepSequentialSimulationExecutor": "executors.OpenStepSequentialSimulationExecutor",
        "ClosedStepSequentialSimulationExecutor": "executors.ClosedStepSequentialSimulationExecutor",
        "ArchiveJdbcToSnowflake": "executors.ArchiveJdbcToSnowflakeExecutor",
        "ArchiveXmlaToSnowflake": "executors.ArchiveXmlaToSnowflakeExecutor"
    }
    
    java_class = executor_to_class.get(selected_executor)
    
    if not java_class:
        print(f"❌ No Java class found for executor: {selected_executor}")
        print(f"Available executors: {list(executor_to_class.keys())}")
        sys.exit(1)
    
    print(f"🚀 Running {selected_executor} locally...")
    print(f"📝 Java class: {java_class}")
    
    # Create necessary directories
    os.makedirs("working_dir/run_logs", exist_ok=True)
    os.makedirs("working_dir/app_logs", exist_ok=True)
    os.makedirs("working_dir/queries", exist_ok=True)
    
    # Ensure target/classes exists and copy properties file
    os.makedirs("target/classes", exist_ok=True)
    properties_source = "working_dir/config/systems.properties"
    properties_target = "target/classes/systems.properties"
    
    if os.path.exists(properties_source):
        import shutil
        shutil.copy2(properties_source, properties_target)
        print(f"📁 Copied {properties_source} to {properties_target}")
    else:
        print(f"⚠️  Warning: {properties_source} not found")
    
    log_path = os.path.join("working_dir", "run_logs", f"{selected_executor}.log")
    
    try:
        # Check if we need to use proxy
        use_proxy = should_use_proxy(selected_executor)
        proxy_env = get_proxy_environment()
        proxy_java_props = get_proxy_java_properties()
        
        if use_proxy:
            print(f"🔌 Using proxy: {PROXY}:{PROXYPORT}")
        
        # Option 1: Use java command directly with simple Log4j2 configuration
        java_cmd = [
            "java",
            "-Dlog4j.configurationFile=log4j2-simple.xml",  # Use simple config
        ]
        
        # Add proxy properties if needed
        if use_proxy:
            java_cmd.extend(proxy_java_props)
            
        java_cmd.extend([
            "-cp",
            "target/classes:target/dependency/*",
            java_class
        ])
        
        # Alternative: Use Maven to run with full classpath
        maven_cmd = [
            "./mvnw",
            "exec:java",
            f"-Dexec.mainClass={java_class}",
            "-Dexec.classpathScope=test",  # Use test scope for full dependencies
            "-Dexec.includeProjectDependencies=true",
            "-Dexec.includePluginDependencies=true"
        ]
        
        # Add proxy properties to Maven if needed
        if use_proxy:
            maven_cmd.extend(proxy_java_props)
        
        # Let user choose which method to use
        run_method = inquirer.select(
            message="Select execution method:",
            choices=[
                {"name": "Java command (direct)", "value": "java"},
                {"name": "Maven exec (recommended)", "value": "maven"}
            ],
            default="maven"
        ).execute()
        
        if run_method == "maven":
            print(f"🔧 Running with Maven: {' '.join(maven_cmd)}")
            cmd_to_run = maven_cmd
            env = None  # Maven will handle proxy via system properties
        else:
            print(f"🔧 Running with Java: {' '.join(java_cmd)}")
            cmd_to_run = java_cmd
            # For direct Java execution, we need to set both system properties and environment
            env = os.environ.copy()
            if use_proxy:
                env.update(proxy_env)
        
        with open(log_path, "w") as log_file:
            if run_method == "maven":
                result = subprocess.run(
                    cmd_to_run,
                    stdout=log_file,
                    stderr=log_file,
                    text=True
                )
            else:
                result = subprocess.run(
                    cmd_to_run,
                    stdout=log_file,
                    stderr=log_file,
                    text=True,
                    env=env
                )
        
        if result.returncode == 0:
            print(f"✅ {selected_executor} completed successfully")
            print(f"📄 Logs written to {log_path}")
        else:
            print(f"❌ {selected_executor} failed with exit code {result.returncode}")
            print(f"📄 See {log_path} for details")
            
            # Show last few lines of log for quick debugging
            try:
                with open(log_path, "r") as f:
                    lines = f.readlines()
                    if lines:
                        print("Last few lines of log:")
                        for line in lines[-10:]:
                            print(f"  {line.strip()}")
            except:
                pass
            
    except subprocess.CalledProcessError as e:
        print(f"❌ Execution failed: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        sys.exit(1)

def select_and_run_executor():
    """Allow user to select and run an executor, then return to menu"""
    
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
        "Exit"  # Add Exit option
    ]

    try:
        selected_executor = inquirer.select(
            message="Select Executor to run:",
            choices=executors,
            instruction="(Use arrow keys, enter to confirm, ESC to quit)"
        ).execute()
    except KeyboardInterrupt:
        print("\nCancelled by user (ESC pressed).")
        return "exit"

    if selected_executor == "Exit":
        print("👋 Exiting...")
        return "exit"
    
    # Only update .env for non-archive executors (archive executors don't use it)
    if selected_executor not in ["ArchiveJdbcToSnowflake", "ArchiveXmlaToSnowflake"]:
        with open(".env", "w") as f:
            f.write(f"EXECUTOR_CLASS=executors.{selected_executor}\n")
        print(f"\n.env file created with EXECUTOR_CLASS=executors.{selected_executor}")
    
    # Run the selected executor locally
    run_local(selected_executor)
    
    # Return to allow running another executor
    return "continue"

# ----------------------------
# Main
# ----------------------------
def main():
    # First, select catalog/cube pairs and generate systems.properties
    cat_xml = run_xmla_query(CATALOG_QUERY)
    catalogs = parse_catalogs(cat_xml)
    create_base_query_file()

    results = []
    for cat in catalogs:
        cube_xml = run_xmla_query(CUBE_QUERY_TEMPLATE.format(catalog=cat))
        cubes = parse_cubes(cube_xml)
        for cube in cubes:
            results.append(f"{cat} :: {cube}")

    try:
        selected_pairs = inquirer.checkbox(
            message="Select Catalog/Cube pairs:",
            choices=results,
            instruction="(Use space to select, enter to confirm, ESC to quit)"
        ).execute()
    except KeyboardInterrupt:
        print("\nCancelled by user (ESC pressed).")
        sys.exit(0)

    if not selected_pairs:
        print("No Catalog/Cube selected. Exiting.")
        sys.exit(0)

    # Generate systems.properties once at the beginning
    write_systems_properties(selected_pairs)
    print("\n" + "="*50)
    print("Configuration setup complete!")
    print("You can now run multiple executors with the same configuration.")
    
    # Show proxy status
    if PROXY and PROXYPORT:
        print(f"🔌 Proxy configured: {PROXY}:{PROXYPORT}")
        print("   (Will be used for ArchiveJdbcToSnowflake and ArchiveXmlaToSnowflake)")
    else:
        print("🔌 No proxy configured")
        
    print("="*50 + "\n")

    # Main loop to run multiple executors
    while True:
        result = select_and_run_executor()
        
        if result == "exit":
            break
        
        # Ask if user wants to run another executor
        print("\n" + "="*50)
        try:
            continue_running = inquirer.select(
                message="Do you want to run another executor?",
                choices=["Yes", "No"],
                default="Yes"
            ).execute()
            
            if continue_running == "No":
                print("👋 Exiting...")
                break
                
        except KeyboardInterrupt:
            print("\n👋 Exiting...")
            break
        
        print("="*50 + "\n")

if __name__ == "__main__":
    main()