"""GUI version - Left: Model selection, Right: Executor selection"""
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import threading
import subprocess
import os
import time
import queue
from datetime import datetime
from .core import AtScaleGatlingCore
from .csv_handler import CSVConfigWindow
from .config_manager import ConfigManager

class AtScaleGatlingGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("AtScale Gatling Controller")
        self.root.geometry("1200x800")
        
        self.core = AtScaleGatlingCore()
        self.config_manager = ConfigManager()  # Add this line
        self.log_queue = queue.Queue()
        self.tail_process = None
        self.current_executor = None
        self.is_running = False
        self.csv_file_assignments = None
        self.csv_selected_models = None
        
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
        frame = tk.LabelFrame(parent, text="Model Selection (Catalog/Cube Pairs)", 
                            font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Instructions
        tk.Label(frame, text="Select one or more catalog/cube pairs:", 
                font=('Arial', 10)).pack(anchor=tk.W, pady=(0, 5))
        
        # Listbox for catalog/cube pairs
        list_frame = tk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.model_listbox = tk.Listbox(list_frame, selectmode=tk.MULTIPLE, 
                                    font=('Arial', 10), exportselection=False)
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
                bg='#E3F2FD', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=5)
        
        tk.Button(control_frame, text="Clear Selection", command=self.clear_model_selection,
                bg='#FFEBEE', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=5)
        
        # NEW: Load From CSV button (white with black text)
        self.csv_button = tk.Button(control_frame, text="Load From CSV", 
                                command=self.load_from_csv,
                                bg='white', fg='black', font=('Arial', 9))
        self.csv_button.pack(side=tk.LEFT, padx=5)
        self.csv_button.config(state=tk.DISABLED)  # Initially disabled
        
        # NEW: Edit Config button (white with black text)
        self.edit_config_button = tk.Button(control_frame, text="Edit Config", 
                                        command=self.edit_config,
                                        bg='white', fg='black', font=('Arial', 9))
        self.edit_config_button.pack(side=tk.LEFT, padx=5)
        
        # Selection info
        self.selection_info = tk.Label(frame, text="No models selected", 
                                    font=('Arial', 9), fg='blue')
        self.selection_info.pack(anchor=tk.W, pady=5)
        
        # Mode indicator
        self.mode_label = tk.Label(frame, text="Mode: Live (will make JDBC/XMLA calls)", 
                                font=('Arial', 9), fg='blue')
        self.mode_label.pack(anchor=tk.W, pady=5)

    def edit_config(self):
        """Open configuration editor"""
        try:
            if not self.config_manager.config_exists():
                response = messagebox.askyesno(
                    "Configuration Not Found",
                    "Configuration file not found. Would you like to create a new one?"
                )
                if response:
                    # Create new configuration
                    if self.config_manager.create_config_gui(parent_window=self.root):
                        self.log_activity("✅ Configuration created successfully")
                        # Optionally, check and create certificates
                        self.check_and_create_certificates()
                    else:
                        self.log_activity("⚠️ Configuration creation cancelled")
                return
            
            # Edit existing configuration
            if self.config_manager.edit_config_gui(parent_window=self.root):
                self.log_activity("✅ Configuration updated successfully")
                # Optionally, check and create certificates after config update
                self.check_and_create_certificates()
            else:
                self.log_activity("⚠️ Configuration editing cancelled")
                
        except Exception as e:
            messagebox.showerror("Error", f"Failed to edit configuration: {e}")
            self.log_activity(f"❌ Error editing configuration: {e}")

    def check_and_create_certificates(self):
        """Check and prompt for certificate creation if needed"""
        try:
            # Check if certificates exist
            root_crt = os.path.join(str(Path.home()), 'root.crt')
            cacerts = os.path.join(str(Path.home()), 'cacerts')
            
            if not os.path.exists(root_crt) or not os.path.exists(cacerts):
                response = messagebox.askyesno(
                    "Missing Certificates",
                    "Certificate files (root.crt and/or cacerts) are missing. "
                    "These are required for secure connections.\n\n"
                    "Would you like to create them now?"
                )
                if response:
                    # Use the ConfigManager to create certificates
                    success = self.config_manager.check_and_create_certificates(auto_create=True)
                    if success:
                        self.log_activity("✅ Certificates created successfully")
                        messagebox.showinfo("Success", "Certificates created successfully!")
                    else:
                        self.log_activity("❌ Failed to create certificates")
                        messagebox.showerror("Error", 
                            "Failed to create certificates. "
                            "Please check your configuration and network connectivity.")
        except Exception as e:
            self.log_activity(f"⚠️ Certificate check error: {e}")


    # Add this class at the end of gui.py, before the closing brace:
    class RuntimeConfigWindow:
        """Window for configuring runtime simulation settings"""
        
        def __init__(self, parent, core):
            self.parent = parent
            self.core = core
            self.window = tk.Toplevel(parent)
            self.window.title("Runtime Configuration")
            self.window.geometry("800x600")
            
            # Make window modal
            self.window.transient(parent)
            self.window.grab_set()
            
            # Set up the GUI
            self.setup_gui()
            
            # Load existing config if available
            self.load_config()
            
            # Center window
            self.center_window()
            
        def setup_gui(self):
            """Set up the runtime configuration GUI"""
            # Main container with scroll
            main_frame = tk.Frame(self.window, padx=10, pady=10)
            main_frame.pack(fill=tk.BOTH, expand=True)
            
            # Title
            tk.Label(main_frame, text="Runtime Simulation Configuration", 
                    font=("Arial", 14, "bold")).pack(anchor=tk.W, pady=(0, 10))
            
            tk.Label(main_frame, text="Configure injection steps for each simulation executor:", 
                    font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 20))
            
            # Create notebook for tabs
            notebook = ttk.Notebook(main_frame)
            notebook.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
            
            # Create tabs for each executor type
            self.executor_frames = {}
            self.entries = {}
            
            # Default configurations for each executor type
            self.default_configs = {
                "ClosedStepSequentialSimulationExecutor": {
                    "title": "Closed Step - Sequential",
                    "step_type": "ConstantConcurrentUsersClosedInjectionStep",
                    "fields": [
                        {"name": "users", "label": "Number of Users", "type": "int", "default": 10},
                        {"name": "durationMinutes", "label": "Duration (Minutes)", "type": "int", "default": 60}
                    ]
                },
                "OpenStepSequentialSimulationExecutor": {
                    "title": "Open Step - Sequential",
                    "step_type": "AtOnceUsersOpenInjectionStep",
                    "fields": [
                        {"name": "users", "label": "Number of Users", "type": "int", "default": 20}
                    ]
                },
                "ClosedStepConcurrentSimulationExecutor": {
                    "title": "Closed Step - Concurrent",
                    "step_type": "ConstantConcurrentUsersClosedInjectionStep",
                    "fields": [
                        {"name": "users", "label": "Number of Users", "type": "int", "default": 15},
                        {"name": "durationMinutes", "label": "Duration (Minutes)", "type": "int", "default": 45}
                    ]
                },
                "OpenStepConcurrentSimulationExecutor": {
                    "title": "Open Step - Concurrent",
                    "step_type": "AtOnceUsersOpenInjectionStep",
                    "fields": [
                        {"name": "users", "label": "Number of Users", "type": "int", "default": 25}
                    ]
                }
            }
            
            for executor, config in self.default_configs.items():
                # Create frame for this executor
                frame = tk.Frame(notebook, padx=10, pady=10)
                notebook.add(frame, text=config["title"])
                self.executor_frames[executor] = frame
                
                # Store step type
                self.entries[executor] = {"type": config["step_type"]}
                
                # Add fields
                tk.Label(frame, text=f"{config['title']} Configuration", 
                        font=("Arial", 12, "bold")).pack(anchor=tk.W, pady=(0, 15))
                
                tk.Label(frame, text=f"Injection Step Type: {config['step_type']}", 
                        font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 10))
                
                # Create entry fields
                for field in config["fields"]:
                    field_frame = tk.Frame(frame)
                    field_frame.pack(fill=tk.X, pady=5)
                    
                    tk.Label(field_frame, text=f"{field['label']}:", 
                            width=25, anchor=tk.W).pack(side=tk.LEFT)
                    
                    var = tk.StringVar(value=str(field["default"]))
                    entry = tk.Entry(field_frame, textvariable=var, width=15)
                    entry.pack(side=tk.LEFT)
                    
                    self.entries[executor][field["name"]] = var
            
            # Buttons frame
            button_frame = tk.Frame(main_frame)
            button_frame.pack(fill=tk.X, pady=10)
            
            # Save button
            tk.Button(button_frame, text="Save Configuration", 
                    command=self.save_config,
                    bg="#FBFEFB", fg='black', font=("Arial", 10, "bold"),
                    width=20).pack(side=tk.LEFT, padx=5)
            
            # Cancel button
            tk.Button(button_frame, text="Cancel", 
                    command=self.window.destroy,
                    bg="#f9f7f7", fg='black', font=("Arial", 10),
                    width=20).pack(side=tk.LEFT, padx=5)
            
            # Reset to defaults button
            tk.Button(button_frame, text="Reset to Defaults", 
                    command=self.reset_to_defaults,
                    bg='#FF9800', fg='black', font=("Arial", 10),
                    width=20).pack(side=tk.LEFT, padx=5)
            
            # Info label
            self.info_label = tk.Label(main_frame, text="", fg='blue')
            self.info_label.pack(anchor=tk.W, pady=5)
        
        def load_config(self):
            """Load existing runtime configuration"""
            import json
            import os
            
            runtime_file = os.path.join("working_dir", "config", "runtime.json")
            
            # Check if file exists
            if not os.path.exists(runtime_file):
                self.info_label.config(
                    text="No existing runtime configuration found. Using default values.", 
                    fg='orange'
                )
                return
            
            # File exists, try to load it
            try:
                with open(runtime_file, 'r') as f:
                    config_data = json.load(f)
                
                # Update UI with loaded values
                loaded_entries = 0
                for executor, settings in config_data.items():
                    if executor in self.entries:
                        injection_steps = settings.get("injectionSteps", [{}])
                        if injection_steps:
                            step = injection_steps[0]
                            for key, var in self.entries[executor].items():
                                if key != "type" and key in step:
                                    var.set(str(step[key]))
                                    loaded_entries += 1
                
                if loaded_entries > 0:
                    self.info_label.config(
                        text=f"✅ Loaded existing configuration from {runtime_file}", 
                        fg='green'
                    )
                else:
                    self.info_label.config(
                        text="⚠️ Configuration file exists but contains no matching data", 
                        fg='orange'
                    )
                    
            except json.JSONDecodeError as e:
                self.info_label.config(
                    text=f"❌ Error: Invalid JSON in {runtime_file}: {e}", 
                    fg='red'
                )
            except Exception as e:
                self.info_label.config(
                    text=f"❌ Error loading configuration: {e}", 
                    fg='red'
                )
        
        def save_config(self):
            """Save runtime configuration to file"""
            import json
            import os
            
            # Build configuration structure
            config_data = {}
            
            # Validate all inputs before saving
            for executor, entries in self.entries.items():
                # Create injection step based on type
                step = {"type": entries["type"]}
                
                # Add other fields
                for key, var in entries.items():
                    if key != "type":
                        value_str = var.get().strip()
                        if not value_str:
                            self.info_label.config(
                                text=f"❌ Error: {key} in {executor} cannot be empty", 
                                fg='red'
                            )
                            return
                        
                        try:
                            # Convert to appropriate type
                            if key == "users" or key == "durationMinutes":
                                value = int(value_str)
                                if value <= 0:
                                    raise ValueError("must be positive")
                            else:
                                value = value_str
                            step[key] = value
                        except ValueError as e:
                            self.info_label.config(
                                text=f"❌ Error: {key} in {executor} must be a positive integer", 
                                fg='red'
                            )
                            return
                
                config_data[executor] = {
                    "injectionSteps": [step]
                }
            
            # Create directory if it doesn't exist
            config_dir = os.path.join("working_dir", "config")
            try:
                os.makedirs(config_dir, exist_ok=True)
            except Exception as e:
                self.info_label.config(
                    text=f"❌ Error creating directory {config_dir}: {e}", 
                    fg='red'
                )
                return
            
            # Save to file
            runtime_file = os.path.join(config_dir, "runtime.json")
            
            try:
                with open(runtime_file, 'w') as f:
                    json.dump(config_data, f, indent=2)
                
                self.info_label.config(
                    text=f"✅ Configuration saved to {runtime_file}", 
                    fg='green'
                )
                
                # Close window after 2 seconds if save was successful
                self.window.after(2000, self.window.destroy)
                
            except PermissionError:
                self.info_label.config(
                    text=f"❌ Error: Permission denied. Cannot write to {runtime_file}", 
                    fg='red'
                )
            except Exception as e:
                self.info_label.config(
                    text=f"❌ Error saving configuration: {e}", 
                    fg='red'
                )
        
        def reset_to_defaults(self):
            """Reset all fields to default values"""
            for executor, config in self.default_configs.items():
                for field in config["fields"]:
                    field_name = field["name"]
                    default_value = str(field["default"])
                    if field_name in self.entries[executor]:
                        self.entries[executor][field_name].set(default_value)
            
            self.info_label.config(
                text="Reset all fields to default values", 
                fg='blue'
            )
        
        def center_window(self):
            """Center the window on screen"""
            self.window.update_idletasks()
            width = self.window.winfo_width()
            height = self.window.winfo_height()
            x = (self.window.winfo_screenwidth() // 2) - (width // 2)
            y = (self.window.winfo_screenheight() // 2) - (height // 2)
            self.window.geometry(f'{width}x{height}+{x}+{y}')
            
            
    def open_runtime_config(self):
        """Open runtime configuration window"""
        self.RuntimeConfigWindow(self.root, self.core)            

    # In the setup_executor_selection_frame method, add the Runtime Config button:
    def setup_executor_selection_frame(self, parent):
        """Right panel: Executor selection and controls"""
        frame = tk.LabelFrame(parent, text="Simulation Executors", 
                            font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Instructions
        tk.Label(frame, text="Select simulation executor:", 
                font=('Arial', 10)).pack(anchor=tk.W, pady=(0, 5))
        
        # Listbox for executors
        list_frame = tk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)
        
        self.executor_listbox = tk.Listbox(list_frame, font=('Arial', 10), 
                                        height=12, exportselection=False, 
                                        selectmode=tk.SINGLE)
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
        self.start_button = tk.Button(button_frame, text="Start Simulation", 
                                    command=self.start_simulation,
                                    bg='#E3F2FD', fg='black', font=('Arial', 11, 'bold'),
                                    height=2, width=20)
        self.start_button.pack(side=tk.LEFT, padx=5)
        
        # Stop button - red background, white text
        self.stop_button = tk.Button(button_frame, text="Stop Running Simulation", 
                                command=self.stop_running_simulation,
                                bg='#F44336', fg='white', font=('Arial', 11, 'bold'),
                                height=2, width=20)
        self.stop_button.pack(side=tk.LEFT, padx=5)
        self.stop_button.config(state=tk.DISABLED)
        
        # Stop signal controls
        stop_frame = tk.Frame(frame)
        stop_frame.pack(fill=tk.X, pady=5)
        
        tk.Label(stop_frame, text="Global Stop Control:", 
                font=('Arial', 10, 'bold')).pack(anchor=tk.W)
        
        stop_button_frame = tk.Frame(stop_frame)
        stop_button_frame.pack(fill=tk.X, pady=5)
        
        tk.Button(stop_button_frame, text="Send Stop Signal to All", 
                command=self.create_stop_signal,
                bg='#FF9800', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=2)
        
        tk.Button(stop_button_frame, text="Cancel Stop Signal", 
                command=self.cancel_stop_signal,
                bg='#9E9E9E', fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=2)
        
        # NEW: Runtime Config button (green with white text)
        tk.Button(stop_button_frame, text="Runtime Config", 
                command=self.open_runtime_config,
                bg="#F6FAF6", fg='black', font=('Arial', 9)).pack(side=tk.LEFT, padx=2)
        
        # Status
        self.status_label = tk.Label(frame, text="Status: Ready", relief=tk.SUNKEN, bd=1, 
                                font=('Arial', 10), bg='#E8F5E8')
        self.status_label.pack(fill=tk.X, pady=5)
        
    def setup_log_frame(self, parent):
        """Bottom panel: Log display"""
        frame = tk.LabelFrame(parent, text="Live Simulation Logs", 
                             font=('Arial', 12, 'bold'), padx=10, pady=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # Log file info
        info_frame = tk.Frame(frame)
        info_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.log_file_label = tk.Label(info_frame, text="No active simulation", 
                                      fg='blue', font=('Arial', 9))
        self.log_file_label.pack(side=tk.LEFT)
        
        tk.Button(info_frame, text="Clear Logs", command=self.clear_logs,
                 font=('Arial', 8)).pack(side=tk.RIGHT)
        
        # Log text area
        self.log_text = scrolledtext.ScrolledText(frame, font=('Courier', 9), height=15)
        self.log_text.pack(fill=tk.BOTH, expand=True)
        self.log_text.config(state=tk.DISABLED)
    
    # ==================== MODEL SELECTION METHODS ====================
    
    def select_all_models(self):
        """Select all models in the list"""
        self.model_listbox.select_set(0, tk.END)
        self.update_selection_info()
        
    def clear_model_selection(self):
        """Clear all model selections"""
        self.model_listbox.select_clear(0, tk.END)
        self.update_selection_info()
        
    def on_model_selection_change(self, event=None):
        """Handle listbox selection changes"""
        selected = self.model_listbox.curselection()
        self.update_selection_info()
        
        # Enable/disable CSV button based on selection
        if selected:
            self.csv_button.config(state=tk.NORMAL)
        else:
            self.csv_button.config(state=tk.DISABLED)
    
    def update_selection_info(self):
        """Update the selection info label"""
        selected = self.model_listbox.curselection()
        count = len(selected)
        if count == 0:
            self.selection_info.config(text="No models selected", fg='red')
        else:
            self.selection_info.config(text=f"{count} model(s) selected", fg='green')
    
    def update_model_list(self):
        """Update the model listbox with discovered catalog/cube pairs"""
        self.model_listbox.delete(0, tk.END)
        for pair in self.core.catalog_cube_pairs:
            self.model_listbox.insert(tk.END, pair)
    
    # ==================== CSV METHODS ====================
    
    def load_from_csv(self):
        """Open CSV configuration window"""
        selected_model_indices = self.model_listbox.curselection()
        if not selected_model_indices:
            messagebox.showwarning("Warning", "Please select at least one catalog/cube pair!")
            return
            
        selected_models = [self.core.catalog_cube_pairs[i] for i in selected_model_indices]
        
        # Open CSV configuration window
        CSVConfigWindow(self.root, self.core, selected_models, self.on_csv_config_saved)
        
    def set_csv_mode(self, file_assignments):
        """Set CSV mode with file assignments"""
        self.csv_mode = True
        self.csv_file_assignments = file_assignments
        print(f"✅ CSV mode enabled with {len(file_assignments)} catalog/cube pairs")
        
    def clear_csv_mode(self):
        """Clear CSV mode"""
        self.csv_mode = False
        self.csv_file_assignments = None
        print("✅ CSV mode cleared")
    
    def on_csv_config_saved(self, file_assignments):
        """Callback when CSV configuration is saved"""
        self.csv_file_assignments = file_assignments
        # Set CSV mode in core
        self.core.set_csv_mode(file_assignments)
        
        # Update mode indicator
        self.mode_label.config(text="Mode: CSV File (will read from CSV files)", fg='green')
        
        self.log_activity("✅ CSV configuration saved. Systems.properties updated with setIngestionFileName entries.")
        self.log_activity("⚠️  Executors will now read from CSV files instead of making live JDBC/XMLA calls.")
    # ==================== SIMULATION METHODS ====================
    
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
        
        # Determine mode
        mode = "CSV" if self.csv_file_assignments else "Live"
        
        # Update UI
        self.status_label.config(text=f"Running: {executor} ({mode} mode)", 
                               bg='#FFEBEE', fg='red')
        self.log_file_label.config(text=f"Tailing: {executor}.log - {mode} Mode")
        self.start_button.config(state=tk.DISABLED)
        self.stop_button.config(state=tk.NORMAL)
        
        # Disable model and executor selection during simulation
        self.model_listbox.config(state=tk.DISABLED)
        self.executor_listbox.config(state=tk.DISABLED)
        self.csv_button.config(state=tk.DISABLED)
        
        # Clear log display
        self.clear_logs()
        
        # Start simulation in background
        self.is_running = True
        threading.Thread(target=self.run_simulation_background, 
                        args=(executor, selected_models), daemon=True).start()
        
        # Start tailing logs
        self.start_tail_logs(executor)
        
        mode_desc = "CSV files" if self.csv_file_assignments else "live JDBC/XMLA calls"
        self.log_activity(f"Started {executor} in {mode} mode")
        self.log_activity(f"Will read from: {mode_desc}")
        self.log_activity(f"Selected models: {', '.join([m.split('::')[1].strip() for m in selected_models])}")
    
    def run_simulation_background(self, executor, selected_models):
        """Run simulation in background thread"""
        try:
            # Don't pass file_assignments - systems.properties is already written
            success = self.core.run_executor(
                executor, 
                selected_models, 
                follow_logs=False
            )
            
            if success:
                self.log_activity(f"✅ {executor} completed successfully")
            else:
                self.log_activity(f"❌ {executor} failed")
                
        except Exception as e:
            self.log_activity(f"❌ Error: {e}")
        finally:
            self.is_running = False
            self.root.after(0, self.on_simulation_complete)
    
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
            self.on_simulation_complete()
    
    def on_simulation_complete(self):
        """Update UI when simulation completes"""
        self.status_label.config(text="Status: Ready", bg='#E8F5E8', fg='black')
        self.start_button.config(state=tk.NORMAL)
        self.stop_button.config(state=tk.DISABLED)
        
        # Re-enable model and executor selection
        self.model_listbox.config(state=tk.NORMAL)
        self.executor_listbox.config(state=tk.NORMAL)
        
        # Re-enable CSV button if models are selected
        if self.model_listbox.curselection():
            self.csv_button.config(state=tk.NORMAL)
        
        # Stop tailing logs
        self.stop_tail_logs()
        
        # Don't reset CSV mode - let user run multiple times with same CSV config
        # Only reset if user explicitly clears selection or clicks "Load From CSV" again
        
        # Don't reset CSV mode - let user run multiple times with same CSV config
    
    # ==================== DISCOVERY METHODS ====================
    
    def discover_and_setup_gui(self):
        """GUI wrapper for discovery"""
        self.log_activity("Starting catalog and cube discovery...")
        if self.core.discover_and_setup():
            self.update_model_list()
            self.log_activity(f"Discovery completed: {len(self.core.catalog_cube_pairs)} models found")
        else:
            self.log_activity("Discovery failed - using empty model list")
    
    # ==================== STOP SIGNAL METHODS ====================
    
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
    
    # ==================== LOG METHODS ====================
    
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
        
        # Limit log size to prevent memory issues
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
    
    # ==================== WINDOW MANAGEMENT ====================
    
    def on_closing(self):
        """Clean up when closing the application"""
        self.stop_tail_logs()
        if self.core.current_process:
            self.core.current_process.terminate()
        self.root.destroy()