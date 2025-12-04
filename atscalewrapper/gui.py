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

class AtScaleGatlingGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("AtScale Gatling Controller")
        self.root.geometry("1200x800")
        
        self.core = AtScaleGatlingCore()
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
        
        # Selection info
        self.selection_info = tk.Label(frame, text="No models selected", 
                                      font=('Arial', 9), fg='blue')
        self.selection_info.pack(anchor=tk.W, pady=5)
        
        # Mode indicator
        self.mode_label = tk.Label(frame, text="Mode: Live (will make JDBC/XMLA calls)", 
                                  font=('Arial', 9), fg='blue')
        self.mode_label.pack(anchor=tk.W, pady=5)

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