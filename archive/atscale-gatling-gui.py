import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import subprocess
import os
import time
import queue
import requests
import xml.etree.ElementTree as ET
import json
import sys
import urllib3
import argparse
from datetime import datetime

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class AtScaleGatlingCore:
    """Core functionality that works for both GUI and CLI"""
    
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
        
    def write_systems_properties(self, selected_pairs):
        """Write systems.properties file with selected catalog/cube pairs"""
        if not selected_pairs:
            raise ValueError("No catalog/cube pairs selected")
            
        cubes = [pair.split("::")[1].strip() for pair in selected_pairs]
        catalogs = [pair.split("::")[0].strip() for pair in selected_pairs]

        filepath = os.path.join(self.config_dir, "systems.properties")

        with open(filepath, "w") as f:
            f.write("atscale.schema.type=installer\n")
            f.write("atscale.models=" + ", ".join(catalogs) + "\n")

            for pair in selected_pairs:
                catalog, cube = [p.strip() for p in pair.split("::")]
                cube_key = cube.replace(" ", "_")
                catalog_jdbc_name = catalog.replace(" ", "%20")

                f.write(f"atscale.{cube_key}.jdbc.url=jdbc:postgresql://{self.cfg['host']}:15432/{catalog_jdbc_name}\n")
                f.write(f"atscale.{cube_key}.jdbc.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.jdbc.password={self.cfg['password']}\n")
                f.write(f"atscale.{cube_key}.jdbc.maxPoolSize=10\n")
                f.write(f"atscale.{cube_key}.jdbc.log.resultset.rows=true\n")
                f.write(f"atscale.{cube_key}.xmla.auth.url=https://{self.cfg['host']}:10500/default/auth\n")
                f.write(f"atscale.{cube_key}.xmla.url=https://{self.cfg['host']}:10502/xmla/default/{self.cfg['token']}\n")
                f.write(f"atscale.{cube_key}.xmla.cube={cube}\n")
                f.write(f"atscale.{cube_key}.xmla.catalog={catalog}\n")
                f.write(f"atscale.{cube_key}.xmla.log.responsebody=true\n")
                f.write(f"atscale.{cube_key}.xmla.auth.username={self.cfg['username']}\n")
                f.write(f"atscale.{cube_key}.xmla.auth.password={self.cfg['password']}\n")
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
                
        print(f"✅ systems.properties regenerated with {len(selected_pairs)} selected pairs")
            
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

class AtScaleGatlingGUI:
    """GUI version - Left: Model selection, Right: Executor selection"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("AtScale Gatling Controller")
        self.root.geometry("1200x800")
        
        self.core = AtScaleGatlingCore()
        self.log_queue = queue.Queue()
        self.tail_process = None
        self.current_executor = None
        self.is_running = False
        
        self.setup_gui()
        self.start_log_monitor()
        
        # Start discovery in background
        threading.Thread(target=self.discover_and_setup_gui, daemon=True).start()
        
    def setup_gui(self):
        # Main container
        main_frame = tk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Top section (2/3 of window)
        top_frame = tk.Frame(main_frame)
        top_frame.pack(fill=tk.BOTH, expand=True)
        
        # Bottom section (1/3 of window) - Logs
        bottom_frame = tk.Frame(main_frame, height=300)
        bottom_frame.pack(fill=tk.BOTH, expand=False)
        bottom_frame.pack_propagate(False)
        
        # Split top frame into left and right
        left_frame = tk.Frame(top_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        right_frame = tk.Frame(top_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True, padx=(5, 0))
        
        # Setup frames
        self.setup_model_selection_frame(left_frame)
        self.setup_executor_selection_frame(right_frame)
        self.setup_log_frame(bottom_frame)
        
    def setup_model_selection_frame(self, parent):
        """Left panel: Catalog/Cube pair selection"""
        frame = tk.LabelFrame(parent, text="Model Selection (Catalog/Cube Pairs)", font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Instructions
        tk.Label(frame, text="Select one or more catalog/cube pairs:", font=('Arial', 10)).pack(anchor=tk.W, pady=(0, 5))
        
        # Listbox for catalog/cube pairs
        list_frame = tk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.model_listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, font=('Arial', 10), exportselection=False)
        self.model_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        # Bind selection events
        self.model_listbox.bind('<<ListboxSelect>>', self.on_model_selection_change)
        
        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.model_listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.model_listbox.yview)
        
        # Selection controls
        control_frame = tk.Frame(frame)
        control_frame.pack(fill=tk.X, pady=5)
        
        tk.Button(control_frame, text="Select All", command=self.select_all_models,
                 bg='#E3F2FD', font=('Arial', 9)).pack(side=tk.LEFT, padx=5)
        
        tk.Button(control_frame, text="Clear Selection", command=self.clear_model_selection,
                 bg='#FFEBEE', font=('Arial', 9)).pack(side=tk.LEFT, padx=5)
        
        # Selection info
        self.selection_info = tk.Label(frame, text="No models selected", font=('Arial', 9), fg='blue')
        self.selection_info.pack(anchor=tk.W, pady=5)

    def setup_executor_selection_frame(self, parent):
        """Right panel: Executor selection and controls"""
        frame = tk.LabelFrame(parent, text="Simulation Executors", font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Instructions
        tk.Label(frame, text="Select simulation executor:", font=('Arial', 10)).pack(anchor=tk.W, pady=(0, 5))
        
        # Listbox for executors
        list_frame = tk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.executor_listbox = tk.Listbox(list_frame, font=('Arial', 10), height=12, exportselection=False, selectmode=tk.SINGLE)
        self.executor_listbox.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        scrollbar = tk.Scrollbar(list_frame)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.executor_listbox.config(yscrollcommand=scrollbar.set)
        scrollbar.config(command=self.executor_listbox.yview)
        
        # Populate executors
        for executor in self.core.executors:
            self.executor_listbox.insert(tk.END, executor)
        
        # Control buttons frame
        button_frame = tk.Frame(frame)
        button_frame.pack(fill=tk.X, pady=10)
        
        # Start button - light blue background, dark text
        self.start_button = tk.Button(button_frame, text="Start Selected Simulation", 
                                    command=self.start_simulation,
                                    bg='#E3F2FD', fg='black', font=('Arial', 11, 'bold'),
                                    height=2, width=20)
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # Stop button - red background, white text (good as is)
        self.stop_button = tk.Button(button_frame, text="Stop Running Simulation", 
                                   command=self.stop_running_simulation,
                                   bg='#F44336', fg='white', font=('Arial', 11, 'bold'),
                                   height=2, width=20)
        self.stop_button.pack(side=tk.LEFT, padx=5)
        
        # Stop signal controls
        stop_frame = tk.Frame(frame)
        stop_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(stop_frame, text="Global Stop Control:", font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        
        stop_button_frame = tk.Frame(stop_frame)
        stop_button_frame.pack(fill=tk.X, pady=5)
        
        # Orange button - dark text for better contrast
        tk.Button(stop_button_frame, text="Send Stop Signal to All", 
                 command=self.create_stop_signal,
                 bg='#FF9800', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=2)
        
        # Gray button - dark text for better contrast
        tk.Button(stop_button_frame, text="Cancel Stop Signal", 
                 command=self.cancel_stop_signal,
                 bg='#9E9E9E', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=2)
        
        # Status
        self.status_label = tk.Label(frame, text="Status: Ready", relief=tk.SUNKEN, bd=1, 
                                   font=('Arial', 10), bg='#E8F5E8')
        self.status_label.pack(fill=tk.X, pady=5)
        
    def setup_log_frame(self, parent):
        """Bottom panel: Log display"""
        frame = tk.LabelFrame(parent, text="Live Simulation Logs", font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Log file info
        info_frame = tk.Frame(frame)
        info_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.log_file_label = tk.Label(info_frame, text="No active simulation", fg='blue', font=('Arial', 9))
        self.log_file_label.pack(side=tk.LEFT)
        
        tk.Button(info_frame, text="Clear Logs", command=self.clear_logs,
                 font=('Arial', 8)).pack(side=tk.RIGHT)
        
        # Log text area
        self.log_text = scrolledtext.ScrolledText(frame, font=('Courier', 9), height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
        
    def discover_and_setup_gui(self):
        """GUI wrapper for discovery"""
        self.log_activity("Starting catalog and cube discovery...")
        if self.core.discover_and_setup():
            self.update_model_list()
            self.log_activity(f"Discovery completed: {len(self.core.catalog_cube_pairs)} models found")
        else:
            self.log_activity("Discovery failed - using empty model list")
            
    def update_model_list(self):
        """Update the model listbox with discovered catalog/cube pairs"""
        self.model_listbox.delete(0, tk.END)
        for pair in self.core.catalog_cube_pairs:
            self.model_listbox.insert(tk.END, pair)
        
    def select_all_models(self):
        """Select all models in the list"""
        self.model_listbox.select_set(0, tk.END)
        self.update_selection_info()
        
    def clear_model_selection(self):
        """Clear all model selections"""
        self.model_listbox.select_clear(0, tk.END)
        self.update_selection_info()
        
    def on_model_selection_change(self, event):
        """Handle listbox selection changes"""
        self.update_selection_info()

    def update_selection_info(self):
        """Update the selection info label"""
        selected = self.model_listbox.curselection()
        count = len(selected)
        if count == 0:
            self.selection_info.config(text="No models selected", fg='red')
        else:
            self.selection_info.config(text=f"{count} model(s) selected", fg='green')
        
    def start_simulation(self):
        """Start simulation with selected models and executor"""
        if self.is_running:
            messagebox.showwarning("Warning", "A simulation is already running!")
            return
            
        # Get selected models
        selected_model_indices = self.model_listbox.curselection()
        if not selected_model_indices:
            messagebox.showwarning("Warning", "Please select at least one catalog/cube pair!")
            return
            
        selected_models = [self.core.catalog_cube_pairs[i] for i in selected_model_indices]
        
        # Get selected executor
        selected_executor_indices = self.executor_listbox.curselection()
        if not selected_executor_indices:
            messagebox.showwarning("Warning", "Please select an executor!")
            return
            
        executor = self.core.executors[selected_executor_indices[0]]
        self.current_executor = executor
        
        # Update UI
        self.status_label.config(text=f"Running: {executor} with {len(selected_models)} models", 
                               bg='#FFEBEE', fg='red')
        self.log_file_label.config(text=f"Tailing: {executor}.log - Models: {len(selected_models)}")
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        
        # Disable model and executor selection during simulation
        self.model_listbox.config(state=tk.DISABLED)
        self.executor_listbox.config(state=tk.DISABLED)
        
        # Clear log display
        self.clear_logs()
        
        # Start simulation in background
        self.is_running = True
        threading.Thread(target=self.run_simulation_gui, 
                        args=(executor, selected_models), daemon=True).start()
        
        # Start tailing logs
        self.start_tail_logs(executor)
        
        self.log_activity(f"Started {executor} with {len(selected_models)} selected models")
        self.log_activity(f"Selected models: {', '.join([m.split('::')[1].strip() for m in selected_models])}")
        
    def run_simulation_gui(self, executor, selected_models):
        """Run simulation for GUI mode"""
        try:
            success = self.core.run_executor(executor, selected_models, follow_logs=False)
            if success:
                self.log_activity(f"✅ {executor} completed successfully")
            else:
                self.log_activity(f"❌ {executor} failed")
                
        except Exception as e:
            self.log_activity(f"❌ Error: {e}")
        finally:
            self.is_running = False
            self.root.after(0, self.on_simulation_complete)
            
    def on_simulation_complete(self):
        """Update UI when simulation completes"""
        self.status_label.config(text="Status: Ready", bg='#E8F5E8', fg='black')
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        
        # Re-enable model and executor selection
        self.model_listbox.config(state=tk.NORMAL)
        self.executor_listbox.config(state=tk.NORMAL)
        
        # Stop tailing logs
        self.stop_tail_logs()

    def stop_running_simulation(self):
        """Stop the currently running simulation"""
        if not self.is_running:
            messagebox.showinfo("Info", "No simulation is currently running")
            return
            
        if self.core.current_process:
            self.core.current_process.terminate()
            self.is_running = False
            self.status_label.config(text="Status: Stopped by user", bg='#FFF3E0')
            self.log_activity(f"🛑 Manually stopped {self.current_executor}")
            self.on_simulation_complete()  # Clean up UI
            
    def start_tail_logs(self, executor):
        """Start tailing the log file"""
        log_file = os.path.join(self.core.run_logs_dir, f"{executor}.log")
        
        # Stop any existing tail process
        if self.tail_process:
            self.tail_process.terminate()
            self.tail_process = None
            
        # Create file if doesn't exist
        if not os.path.exists(log_file):
            open(log_file, 'w').close()
            
        # Use system tail -F command for efficient tailing
        def tail_with_system_command():
            try:
                self.tail_process = subprocess.Popen(
                    ['tail', '-F', log_file],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    universal_newlines=True
                )
                
                while self.tail_process and self.tail_process.poll() is None and self.is_running:
                    line = self.tail_process.stdout.readline()
                    if line:
                        self.log_queue.put(line.strip())
                    time.sleep(0.01)
                        
            except Exception as e:
                self.log_queue.put(f"Tail error: {e}")
            finally:
                if self.tail_process:
                    self.tail_process.terminate()
                    self.tail_process = None
                
        threading.Thread(target=tail_with_system_command, daemon=True).start()
        
    def stop_tail_logs(self):
        """Stop the tail process"""
        if self.tail_process:
            self.tail_process.terminate()
            self.tail_process = None
            
    def create_stop_signal(self):
        """Create stop_simulation file"""
        if self.core.stop_simulation():
            self.log_activity("🛑 GLOBAL STOP SIGNAL SENT to all simulations")
            messagebox.showinfo("Stop Signal", "Stop signal sent to all running simulations!")
        else:
            messagebox.showerror("Error", "Failed to create stop signal")
            
    def cancel_stop_signal(self):
        """Remove stop_simulation file"""
        if self.core.cancel_stop_signal():
            self.log_activity("✅ Global stop signal cancelled")
        else:
            messagebox.showerror("Error", "Failed to cancel stop signal")
            
    def start_log_monitor(self):
        """Monitor log queue and update display"""
        def check_queue():
            try:
                while True:
                    line = self.log_queue.get_nowait()
                    self.append_log(line)
            except queue.Empty:
                pass
            finally:
                self.root.after(100, check_queue)
                
        self.root.after(100, check_queue)
        
    def append_log(self, line):
        """Append line to log display"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.insert(tk.END, line + '\n')
        
        # Limit log size to prevent memory issues (keep last 1000 lines)
        lines = int(self.log_text.index('end-1c').split('.')[0])
        if lines > 1000:
            self.log_text.delete(1.0, f"{lines-1000}.0")
            
        self.log_text.see(tk.END)
        self.log_text.config(state=tk.DISABLED)
        
    def clear_logs(self):
        """Clear log display"""
        self.log_text.config(state=tk.NORMAL)
        self.log_text.delete(1.0, tk.END)
        self.log_text.config(state=tk.DISABLED)
        
    def log_activity(self, message):
        """Log activity message"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        self.append_log(f"[{timestamp}] {message}")
        
    def on_closing(self):
        """Clean up when closing the application"""
        self.stop_tail_logs()
        if self.core.current_process:
            self.core.current_process.terminate()
        self.root.destroy()

def run_cli_mode(args):
    """Run in CLI mode with command line arguments"""
    core = AtScaleGatlingCore()
    
    print("🚀 AtScale Gatling CLI Mode")
    print("=" * 50)
    
    # Discover models
    print("Discovering catalogs and cubes...")
    if not core.discover_and_setup():
        print("❌ Failed to discover models")
        return 1
    
    if not core.catalog_cube_pairs:
        print("❌ No catalog/cube pairs found")
        return 1
    
    # Handle model selection
    selected_models = []
    
    if args.models:
        # Use specified models
        specified_models = args.models.split(',')
        for model in specified_models:
            model = model.strip()
            # Try to find matching catalog::cube pair
            found = False
            for pair in core.catalog_cube_pairs:
                if model in pair:
                    selected_models.append(pair)
                    found = True
                    break
            if not found:
                print(f"❌ Model '{model}' not found in discovered pairs")
                return 1
    elif args.all_models:
        # Use all models
        selected_models = core.catalog_cube_pairs
    else:
        # Interactive selection
        print("\nAvailable catalog/cube pairs:")
        for i, pair in enumerate(core.catalog_cube_pairs, 1):
            print(f"  {i}. {pair}")
        
        try:
            selection = input(f"\nSelect models (comma-separated numbers 1-{len(core.catalog_cube_pairs)}, or 'all'): ").strip()
            if selection.lower() == 'all':
                selected_models = core.catalog_cube_pairs
            else:
                indices = [int(x.strip()) - 1 for x in selection.split(',')]
                selected_models = [core.catalog_cube_pairs[i] for i in indices if 0 <= i < len(core.catalog_cube_pairs)]
        except (ValueError, IndexError):
            print("❌ Invalid selection")
            return 1
    
    if not selected_models:
        print("❌ No models selected")
        return 1
    
    print(f"\n✅ Selected {len(selected_models)} models:")
    for model in selected_models:
        print(f"  - {model}")
    
    # Handle executor selection
    if args.executor:
        executor = args.executor
        if executor not in core.executors:
            print(f"❌ Executor '{executor}' not found")
            print(f"Available executors: {', '.join(core.executors)}")
            return 1
    else:
        # Interactive executor selection
        print("\nAvailable executors:")
        for i, executor in enumerate(core.executors, 1):
            print(f"  {i}. {executor}")
        
        try:
            selection = int(input(f"\nSelect executor (1-{len(core.executors)}): ")) - 1
            if 0 <= selection < len(core.executors):
                executor = core.executors[selection]
            else:
                print("❌ Invalid executor selection")
                return 1
        except ValueError:
            print("❌ Invalid selection")
            return 1
    
    print(f"\n🎯 Starting simulation with:")
    print(f"  Executor: {executor}")
    print(f"  Models: {len(selected_models)} selected")
    print(f"  Follow logs: {args.follow}")
    print("=" * 50)
    
    # Run the simulation
    try:
        success = core.run_executor(executor, selected_models, follow_logs=args.follow)
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⏹️  Simulation interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

def run_gui_mode():
    """Run in GUI mode"""
    # Check if we have display (for SSH with X11 forwarding)
    if sys.platform != "darwin" and not os.getenv('DISPLAY'):
        print("No display available. This application requires a GUI display.")
        print("Use the CLI version or run on a system with graphical display.")
        return 1
        
    root = tk.Tk()
    app = AtScaleGatlingGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
    return 0

def main():
    parser = argparse.ArgumentParser(description='AtScale Gatling Controller')
    parser.add_argument('--mode', choices=['gui', 'cli'], default='gui',
                       help='Run in GUI or CLI mode (default: GUI)')
    
    # CLI-specific arguments
    parser.add_argument('--executor', help='Executor to run (required for CLI mode)')
    parser.add_argument('--models', help='Comma-separated list of models to run')
    parser.add_argument('--all-models', action='store_true', help='Run with all discovered models')
    parser.add_argument('--follow', action='store_true', help='Follow logs in real-time')
    
    args = parser.parse_args()
    
    if args.mode == 'cli':
        return run_cli_mode(args)
    else:
        return run_gui_mode()

if __name__ == "__main__":
    sys.exit(main())