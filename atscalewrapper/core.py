"""Core wrapper to create configured `AtScaleGatlingCore` instances."""
import os

def create_core(config_path=None):
    """Create and return an `AtScaleGatlingCore` instance.

    If `config_path` is not provided the wrapper will look for a
    `config.json` next to this package, and fall back to a top-level
    `config.json` in the repository root.
    """
    if config_path is None:
        # Prefer package-local config if present
        pkg_config = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(pkg_config):
            config_path = pkg_config
        elif os.path.exists("config.json"):
            config_path = "config.json"
        else:
            config_path = "config.json"

    return AtScaleGatlingCore(config_path=config_path)

__all__ = ["create_core"]
"""Core functionality that works for both GUI and CLI"""
import os
import subprocess
import requests
import xml.etree.ElementTree as ET
import json
import urllib3
from datetime import datetime

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class AtScaleGatlingCore:
    def __init__(self, config_path="config.json"):
        with open(config_path) as f:
            self.cfg = json.load(f)
            
        self.working_dir = "working_dir"
        self.control_dir = os.path.join(self.working_dir, "control")
        self.run_logs_dir = os.path.join(self.working_dir, "run_logs")
        self.config_dir = os.path.join(self.working_dir, "config")
        self.ingest_dir = os.path.join(self.working_dir, "ingest")
        
        # Create directories if they don't exist
        os.makedirs(self.control_dir, exist_ok=True)
        os.makedirs(self.run_logs_dir, exist_ok=True)
        os.makedirs(self.config_dir, exist_ok=True)
        os.makedirs(self.ingest_dir, exist_ok=True)
        
        self.DOCKER_IMAGE = "rwidjaja/atscale-gatling:latest"
        
        # Simulation executors
        self.executors = [
            "InstallerVerQueryExtractExecutor",
            "CustomQueryExtractExecutor", 
            "QueryExtractExecutor",
            "OpenStepConcurrentSimulationExecutor",
            "ClosedStepConcurrentSimulationExecutor",
            "OpenStepSequentialSimulationExecutor",
            "ClosedStepSequentialSimulationExecutor",
            "ArchiveJdbcToSnowflake",
            "ArchiveXmlaToSnowflake"
        ]
        
        self.current_process = None
        self.is_running = False
        self.current_executor = None
        self.catalog_cube_pairs = []

    def discover_and_setup(self):
        """Discover catalogs/cubes"""
        try:
            print("Starting catalog and cube discovery...")
            
            catalogs = self.discover_catalogs()
            if not catalogs:
                print("❌ No catalogs found")
                return False
                
            pairs = []
            for cat in catalogs:
                cubes = self.discover_cubes(cat)
                for cube in cubes:
                    pairs.append(f"{cat} :: {cube}")
                    
            self.catalog_cube_pairs = pairs
            print(f"✅ Discovery complete: {len(pairs)} catalog/cube pairs")
            return True
            
        except Exception as e:
            print(f"❌ Discovery failed: {e}")
            return False

    def discover_catalogs(self):
        """Discover catalogs using XMLA"""
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
        
        xml_response = self.run_xmla_query(CATALOG_QUERY)
        return self.parse_catalogs(xml_response)
        
    def discover_cubes(self, catalog):
        """Discover cubes for a catalog"""
        CUBE_QUERY = f"""<?xml version="1.0" encoding="utf-8"?>
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
        
        xml_response = self.run_xmla_query(CUBE_QUERY)
        return self.parse_cubes(xml_response)
        
    def run_xmla_query(self, xml_body):
        """Run XMLA query"""
        url = f"https://{self.cfg['host']}:10502/xmla/default"
        resp = requests.post(
            url,
            data=xml_body.encode("utf-8"),
            headers={"Content-Type": "text/xml"},
            auth=(self.cfg["username"], self.cfg["password"]),
            verify=False
        )
        resp.raise_for_status()
        return resp.text
        
    def parse_catalogs(self, xml_text):
        """Parse catalogs from XML response"""
        root = ET.fromstring(xml_text)
        return [el.text for el in root.findall(".//{urn:schemas-microsoft-com:xml-analysis:rowset}CATALOG_NAME")]
        
    def parse_cubes(self, xml_text):
        """Parse cubes from XML response"""
        root = ET.fromstring(xml_text)
        return [el.text for el in root.findall(".//{urn:schemas-microsoft-com:xml-analysis:rowset}CUBE_NAME")]
        
    def write_systems_properties_with_csv(self, selected_pairs, file_assignments=None):
        """Write systems.properties file with optional CSV file assignments"""
        if not selected_pairs:
            raise ValueError("No catalog/cube pairs selected")
            
        cubes = [pair.split("::")[1].strip() for pair in selected_pairs]
        catalogs = [pair.split("::")[0].strip() for pair in selected_pairs]

        filepath = os.path.join(self.config_dir, "systems.properties")

        with open(filepath, "w") as f:
            # Determine schema type based on whether CSV files are provided
            if file_assignments:
                f.write("# CSV Mode - Executors will read from CSV files\n")
                f.write("atscale.schema.type=ingestion\n")
            else:
                f.write("# Live Mode - Executors will make live JDBC/XMLA calls\n")
                f.write("atscale.schema.type=installer\n")
                
            f.write("atscale.models=" + ", ".join(catalogs) + "\n")

            for pair in selected_pairs:
                catalog, cube = [p.strip() for p in pair.split("::")]
                cube_key = cube.replace(" ", "_")
                catalog_jdbc_name = catalog.replace(" ", "%20")

                f.write(f"# {catalog} :: {cube}\n")
                f.write(f"atscale.{cube_key}.jdbc.url=jdbc:postgresql://{self.cfg['host']}:15432/{catalog_jdbc_name}\n")
                f.write(f"atscale.{cube_key}.jdbc.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.jdbc.password={self.cfg['password']}\n")
                f.write(f"atscale.{cube_key}.jdbc.maxPoolSize=10\n")
                f.write(f"atscale.{cube_key}.jdbc.log.resultset.rows=true\n")
                
                # Add CSV configuration if provided
                if file_assignments and pair in file_assignments:
                    assignment = file_assignments[pair]
                    jdbc_file = assignment.get('jdbc_file', '')
                    xmla_file = assignment.get('xmla_file', '')
                    jdbc_has_header = assignment.get('jdbc_has_header', True)
                    xmla_has_header = assignment.get('xmla_has_header', True)
                    
                    if jdbc_file:
                        f.write(f"atscale.{cube_key}.jdbc.setIngestionFileName={jdbc_file}\n")
                        f.write(f"atscale.{cube_key}.jdbc.setIngestionFileHasHeader={str(jdbc_has_header).lower()}\n")
                
                f.write(f"atscale.{cube_key}.xmla.auth.url=https://{self.cfg['host']}:10500/default/auth\n")
                f.write(f"atscale.{cube_key}.xmla.url=https://{self.cfg['host']}:10502/xmla/default/{self.cfg['token']}\n")
                f.write(f"atscale.{cube_key}.xmla.cube={cube}\n")
                f.write(f"atscale.{cube_key}.xmla.catalog={catalog}\n")
                f.write(f"atscale.{cube_key}.xmla.log.responsebody=true\n")
                f.write(f"atscale.{cube_key}.xmla.auth.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.xmla.auth.password={self.cfg['password']}\n")
                
                # Add CSV configuration if provided
                if file_assignments and pair in file_assignments:
                    assignment = file_assignments[pair]
                    xmla_file = assignment.get('xmla_file', '')
                    xmla_has_header = assignment.get('xmla_has_header', True)
                    
                    if xmla_file:
                        f.write(f"atscale.{cube_key}.xmla.setIngestionFileName={xmla_file}\n")
                        f.write(f"atscale.{cube_key}.xmla.setIngestionFileHasHeader={str(xmla_has_header).lower()}\n")
                
                f.write("# \n")

            f.write(f"atscale.postgres.jdbc.url=jdbc:postgresql://{self.cfg['postgres_host']}:10520/atscale\n")
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
            
            # Add AWS config if present
            if self.cfg.get("aws.region"):
                f.write(f"aws.region={self.cfg['aws.region']}\n")
            if self.cfg.get("aws.secrets-key"):
                f.write(f"aws.secrets-key={self.cfg['aws.secrets-key']}\n")
                
            # Add Snowflake config if present
            if self.cfg.get("snowflake.archive.account"):
                f.write(f"snowflake.archive.account={self.cfg['snowflake.archive.account']}\n")
            if self.cfg.get("snowflake.archive.warehouse"):
                f.write(f"snowflake.archive.warehouse={self.cfg['snowflake.archive.warehouse']}\n")
            if self.cfg.get("snowflake.archive.database"):
                f.write(f"snowflake.archive.database={self.cfg['snowflake.archive.database']}\n")
            if self.cfg.get("snowflake.archive.schema"):
                f.write(f"snowflake.archive.schema={self.cfg['snowflake.archive.schema']}\n")
            if self.cfg.get("snowflake.archive.role"):
                f.write(f"snowflake.archive.role={self.cfg['snowflake.archive.role']}\n")
            if self.cfg.get("snowflake.archive.username"):
                f.write(f"snowflake.archive.username={self.cfg['snowflake.archive.username']}\n")
            if self.cfg.get("snowflake.archive.password"):
                f.write(f"snowflake.archive.password={self.cfg['snowflake.archive.token']}\n")
            if self.cfg.get("snowflake.archive.token"):
                f.write(f"snowflake.archive.token={self.cfg['snowflake.archive.token']}\n")
                
        mode = "CSV" if file_assignments else "Live"
        print(f"✅ systems.properties regenerated for {mode} mode with {len(selected_pairs)} selected pairs")
            
    def build_docker_command(self, executor_name):
        """Build Docker command"""
        cmd = [
            "docker", "run", "--rm", "--platform", "linux/amd64",
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
        
        # Add proxy for archive executors
        if executor_name in ["ArchiveJdbcToSnowflake", "ArchiveXmlaToSnowflake"]:
            proxy_host = self.cfg.get("proxy", "")
            proxy_port = self.cfg.get("proxyport", "")
            if proxy_host and proxy_port:
                cmd.extend([
                    "-e", f"HTTP_PROXY=http://{proxy_host}:{proxy_port}",
                    "-e", f"HTTPS_PROXY=http://{proxy_host}:{proxy_port}",
                ])
        
        cmd.extend([self.DOCKER_IMAGE, executor_name, "working_dir/config/systems.properties"])
        return cmd
        
    def ensure_docker_image(self):
        """Check and pull Docker image if needed"""
        try:
            result = subprocess.run(["docker", "image", "inspect", self.DOCKER_IMAGE], 
                                  capture_output=True)
            if result.returncode == 0:
                return True
                
            print("Pulling Docker image...")
            result = subprocess.run(["docker", "pull", self.DOCKER_IMAGE])
            return result.returncode == 0
        except:
            return False
            
    def run_executor(self, executor_name, selected_pairs, follow_logs=False):
        """Run an executor with selected catalog/cube pairs"""
        if self.is_running:
            print("Already running an executor!")
            return False
            
        self.is_running = True
        self.current_executor = executor_name
        
        try:
            # Regenerate systems.properties with selected pairs
            self.write_systems_properties(selected_pairs)
            
            if not self.ensure_docker_image():
                print("❌ Docker image not available")
                return False
                
            log_file = os.path.join(self.run_logs_dir, f"{executor_name}.log")
            
            # Build Docker command
            cmd = self.build_docker_command(executor_name)
            
            print(f"🐳 Running {executor_name} with {len(selected_pairs)} selected models...")
            print(f"Command: {' '.join(cmd)}")
            
            with open(log_file, "w") as f:
                self.current_process = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
                
            if follow_logs:
                # Tail logs in real-time for CLI
                self.tail_logs_cli(executor_name)
            else:
                # Wait for completion
                self.current_process.wait()
                
            if self.current_process.returncode == 0:
                print(f"✅ {executor_name} completed successfully")
                return True
            else:
                print(f"❌ {executor_name} failed with exit code {self.current_process.returncode}")
                return False
                
        except Exception as e:
            print(f"❌ Error running {executor_name}: {e}")
            return False
        finally:
            self.is_running = False
            
    def tail_logs_cli(self, executor_name):
        """Tail logs for CLI mode"""
        log_file = os.path.join(self.run_logs_dir, f"{executor_name}.log")
        
        if not os.path.exists(log_file):
            print(f"Log file not found: {log_file}")
            return
            
        print(f"\n--- Tailing logs for {executor_name} ---")
        print(f"Log file: {log_file}")
        print("Press Ctrl+C to stop tailing (executor will continue running)\n")
        
        try:
            # Use tail -f to follow logs
            tail_process = subprocess.Popen(['tail', '-f', log_file], 
                                          stdout=subprocess.PIPE, 
                                          stderr=subprocess.PIPE,
                                          text=True)
            
            while self.current_process.poll() is None and tail_process.poll() is None:
                line = tail_process.stdout.readline()
                if line:
                    print(line, end='', flush=True)
                time.sleep(0.1)
                
            tail_process.terminate()
            
        except KeyboardInterrupt:
            print("\n⏹️  Stopped tailing logs (executor continues running)")
            if tail_process:
                tail_process.terminate()
        except Exception as e:
            print(f"Error tailing logs: {e}")
            
    def stop_simulation(self):
        """Create stop signal file"""
        stop_file = os.path.join(self.control_dir, "stop_simulation")
        try:
            with open(stop_file, 'w') as f:
                f.write(f"Stop signal at {datetime.now()}")
            print("🛑 Stop signal sent to all simulations")
            return True
        except Exception as e:
            print(f"❌ Failed to create stop signal: {e}")
            return False
            
    def cancel_stop_signal(self):
        """Remove stop signal file"""
        stop_file = os.path.join(self.control_dir, "stop_simulation")
        try:
            if os.path.exists(stop_file):
                os.remove(stop_file)
                print("✅ Stop signal cancelled")
                return True
        except Exception as e:
            print(f"❌ Failed to cancel stop signal: {e}")
            return False
        return False
    
    def write_csv_systems_properties(self, selected_pairs, file_assignments):
        """Write systems.properties file for CSV mode"""
        if not selected_pairs:
            raise ValueError("No catalog/cube pairs selected")
            
        cubes = [pair.split("::")[1].strip() for pair in selected_pairs]
        catalogs = [pair.split("::")[0].strip() for pair in selected_pairs]

        filepath = os.path.join(self.config_dir, "systems.properties")

        with open(filepath, "w") as f:
            f.write("# CSV Mode Configuration\n")
            f.write("atscale.schema.type=ingestion\n")
            f.write("atscale.models=" + ", ".join(catalogs) + "\n")

            for pair in selected_pairs:
                catalog, cube = [p.strip() for p in pair.split("::")]
                cube_key = cube.replace(" ", "_")
                catalog_jdbc_name = catalog.replace(" ", "%20")

                # Get file assignments for this pair
                assignment = file_assignments.get(pair, {})
                jdbc_file = assignment.get('jdbc_file', '')
                xmla_file = assignment.get('xmla_file', '')
                jdbc_has_header = assignment.get('jdbc_has_header', True)
                xmla_has_header = assignment.get('xmla_has_header', True)

                f.write(f"atscale.{cube_key}.jdbc.url=jdbc:postgresql://{self.cfg['host']}:15432/{catalog_jdbc_name}\n")
                f.write(f"atscale.{cube_key}.jdbc.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.jdbc.password={self.cfg['password']}\n")
                f.write(f"atscale.{cube_key}.jdbc.maxPoolSize=10\n")
                f.write(f"atscale.{cube_key}.jdbc.log.resultset.rows=true\n")
                
                if jdbc_file:
                    f.write(f"atscale.{cube_key}.jdbc.setIngestionFileName={jdbc_file}\n")
                    f.write(f"atscale.{cube_key}.jdbc.setIngestionFileHasHeader={str(jdbc_has_header).lower()}\n")
                
                f.write(f"atscale.{cube_key}.xmla.auth.url=https://{self.cfg['host']}:10500/default/auth\n")
                f.write(f"atscale.{cube_key}.xmla.url=https://{self.cfg['host']}:10502/xmla/default/{self.cfg['token']}\n")
                f.write(f"atscale.{cube_key}.xmla.cube={cube}\n")
                f.write(f"atscale.{cube_key}.xmla.catalog={catalog}\n")
                f.write(f"atscale.{cube_key}.xmla.log.responsebody=true\n")
                f.write(f"atscale.{cube_key}.xmla.auth.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.xmla.auth.password={self.cfg['password']}\n")
                
                if xmla_file:
                    f.write(f"atscale.{cube_key}.xmla.setIngestionFileName={xmla_file}\n")
                    f.write(f"atscale.{cube_key}.xmla.setIngestionFileHasHeader={str(xmla_has_header).lower()}\n")
                
                f.write("# \n")

            f.write(f"atscale.postgres.jdbc.url=jdbc:postgresql://{self.cfg['postgres_host']}:10520/atscale\n")
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
            
            # ... (rest of the existing config writing code)
            
        print(f"✅ CSV systems.properties generated for {len(selected_pairs)} selected pairs")