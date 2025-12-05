"""CSV file handling for AtScale Gatling"""
import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import os
import shutil
from datetime import datetime

class CSVConfigWindow:
    """Window for configuring CSV files for selected catalog/cube pairs"""
    
    def __init__(self, parent, core, selected_pairs, config_callback):
        """
        Args:
            parent: Parent window
            core: AtScaleGatlingCore instance
            selected_pairs: List of selected catalog/cube pairs
            config_callback: Callback function when config is saved
        """
        self.parent = parent
        self.core = core
        self.selected_pairs = selected_pairs
        self.config_callback = config_callback
        
        # Data structure to store file assignments
        self.file_assignments = {}
        for pair in selected_pairs:
            self.file_assignments[pair] = {
                'jdbc_file': '',
                'xmla_file': '',
                'jdbc_has_header': tk.BooleanVar(value=True),
                'xmla_has_header': tk.BooleanVar(value=True)
            }
        
        self.setup_window()
        
    def setup_window(self):
        """Setup the CSV configuration window"""
        self.window = tk.Toplevel(self.parent)
        self.window.title("CSV File Configuration")
        self.window.geometry("1100x700")
        
        # Main container
        main_frame = tk.Frame(self.window, padx=10, pady=10)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        title_label = tk.Label(main_frame, 
                            text="Assign CSV Files for Selected Catalog/Cube Pairs",
                            font=('Arial', 14, 'bold'))
        title_label.pack(pady=(0, 15))
        
        # Instructions
        instr_label = tk.Label(main_frame,
                            text="For each selected model, assign JDBC and MDX query CSV files.\n"
                                "Files will be copied to ./working_dir/ingest/ folder.\n"
                                "setIngestionFileName will be set to just the filename (no path).",
                            font=('Arial', 10),
                            justify=tk.LEFT)
        instr_label.pack(pady=(0, 10))
        
        # BUTTONS AT THE TOP
        button_frame = tk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(0, 15))
        
        tk.Button(button_frame, text="Save CSV Configuration", 
                command=self.save_configuration,
                bg="#F6F8F6", fg='black', font=('Arial', 11, 'bold'),
                width=20).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Cancel", 
                command=self.window.destroy,
                bg='#9E9E9E', fg='black', font=('Arial', 11),
                width=20).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Clear All", 
                command=self.clear_all_assignments,
                bg='#FF9800', fg='black', font=('Arial', 11),
                width=20).pack(side=tk.LEFT, padx=5)
        
        # Create scrollable frame for catalog/cube pairs
        canvas = tk.Canvas(main_frame)
        scrollbar = tk.Scrollbar(main_frame, orient=tk.VERTICAL, command=canvas.yview)
        
        scrollable_frame = tk.Frame(canvas)
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Create configuration rows for each catalog/cube pair
        for idx, pair in enumerate(self.selected_pairs):
            self.create_pair_config_row(scrollable_frame, pair, idx)
        
        # Pack canvas and scrollbar
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Status label pinned at the bottom of the window
        self.status_label = tk.Label(self.window, text="", fg='blue')
        self.status_label.pack(side=tk.BOTTOM, fill=tk.X, pady=5)

        
    def create_pair_config_row(self, parent, pair, row_index):
        """Create configuration row for a catalog/cube pair"""
        frame = tk.Frame(parent, bd=1, relief=tk.RAISED, padx=10, pady=10)
        frame.pack(fill=tk.X, pady=5)
        
        # Catalog/Cube label
        catalog, cube = [p.strip() for p in pair.split("::")]
        pair_label = tk.Label(frame, 
                             text=f"{catalog} :: {cube}",
                             font=('Arial', 10, 'bold'),
                             width=60,
                             anchor=tk.W)
        pair_label.grid(row=0, column=0, columnspan=4, sticky=tk.W, pady=(0, 10))
        
        # JDBC CSV File
        jdbc_frame = tk.Frame(frame)
        jdbc_frame.grid(row=1, column=0, sticky=tk.W, padx=(0, 20))
        
        tk.Label(jdbc_frame, text="JDBC CSV:", font=('Arial', 9)).pack(anchor=tk.W)
        
        jdbc_file_frame = tk.Frame(jdbc_frame)
        jdbc_file_frame.pack(fill=tk.X, pady=2)
        
        jdbc_file_label = tk.Label(jdbc_file_frame, text="Not selected", 
                                  fg='red', width=40, anchor=tk.W, relief=tk.SUNKEN, padx=5)
        jdbc_file_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        tk.Button(jdbc_file_frame, text="Browse...",
                 command=lambda p=pair, l=jdbc_file_label, t="jdbc": self.browse_file(p, l, t),
                 width=10).pack(side=tk.RIGHT, padx=(5, 0))
        
        tk.Checkbutton(jdbc_frame, text="Has Header",
                      variable=self.file_assignments[pair]['jdbc_has_header'],
                      font=('Arial', 8)).pack(anchor=tk.W)
        
        # XMLA (MDX) CSV File
        xmla_frame = tk.Frame(frame)
        xmla_frame.grid(row=1, column=1, sticky=tk.W, padx=(0, 20))
        
        tk.Label(xmla_frame, text="MDX CSV:", font=('Arial', 9)).pack(anchor=tk.W)
        
        xmla_file_frame = tk.Frame(xmla_frame)
        xmla_file_frame.pack(fill=tk.X, pady=2)
        
        xmla_file_label = tk.Label(xmla_file_frame, text="Not selected", 
                                  fg='red', width=40, anchor=tk.W, relief=tk.SUNKEN, padx=5)
        xmla_file_label.pack(side=tk.LEFT, fill=tk.X, expand=True)
        
        tk.Button(xmla_file_frame, text="Browse...",
                 command=lambda p=pair, l=xmla_file_label, t="xmla": self.browse_file(p, l, t),
                 width=10).pack(side=tk.RIGHT, padx=(5, 0))
        
        tk.Checkbutton(xmla_frame, text="Has Header",
                      variable=self.file_assignments[pair]['xmla_has_header'],
                      font=('Arial', 8)).pack(anchor=tk.W)
        
        # Store references to labels
        self.file_assignments[pair]['jdbc_label'] = jdbc_file_label
        self.file_assignments[pair]['xmla_label'] = xmla_file_label
        
    def browse_file(self, pair, label_widget, file_type):
        """Browse for CSV files"""
        default_dir = os.path.join(self.core.working_dir, "ingest")
        if not os.path.exists(default_dir):
            default_dir = os.getcwd()
            
        filetypes = [
            ("CSV files", "*.csv"),
            ("All files", "*.*")
        ]
        
        filename = filedialog.askopenfilename(
            title=f"Select {file_type.upper()} CSV file",
            initialdir=default_dir,
            filetypes=filetypes
        )
        
        if filename:
            # Show just filename in label
            basename = os.path.basename(filename)
            label_widget.config(text=basename, fg='green')
            
            # Store full path temporarily
            if file_type == "jdbc":
                self.file_assignments[pair]['jdbc_file'] = filename
                self.file_assignments[pair]['jdbc_original_path'] = filename
            else:
                self.file_assignments[pair]['xmla_file'] = filename
                self.file_assignments[pair]['xmla_original_path'] = filename
                
    def validate_assignments(self):
        """Validate that all required files are assigned"""
        missing_files = []
        for pair, assignment in self.file_assignments.items():
            if not assignment['jdbc_file']:
                missing_files.append(f"{pair} - JDBC CSV")
            if not assignment['xmla_file']:
                missing_files.append(f"{pair} - MDX CSV")
        
        if missing_files:
            return False, missing_files
        return True, []
    
    def save_configuration(self):
        """Save the CSV configuration and copy files to ingest folder"""
        # Validate all files are assigned
        is_valid, missing = self.validate_assignments()
        if not is_valid:
            messagebox.showerror("Missing Files", 
                               "The following files are not assigned:\n\n" +
                               "\n".join(missing))
            return
        
        try:
            # Ensure ingest directory exists
            ingest_dir = self.core.ingest_dir
            os.makedirs(ingest_dir, exist_ok=True)
            
            # Process assignments: copy files to ingest folder and store just filenames
            processed_assignments = {}
            
            for pair, assignment in self.file_assignments.items():
                jdbc_file = assignment['jdbc_file']
                xmla_file = assignment['xmla_file']
                
                # Copy JDBC file to ingest folder
                jdbc_filename = None
                if jdbc_file:
                    jdbc_basename = os.path.basename(jdbc_file)
                    jdbc_target = os.path.join(ingest_dir, jdbc_basename)
                    
                    # Copy if source and target are different
                    if os.path.abspath(jdbc_file) != os.path.abspath(jdbc_target):
                        shutil.copy2(jdbc_file, jdbc_target)
                    
                    jdbc_filename = jdbc_basename
                
                # Copy XMLA file to ingest folder
                xmla_filename = None
                if xmla_file:
                    xmla_basename = os.path.basename(xmla_file)
                    xmla_target = os.path.join(ingest_dir, xmla_basename)
                    
                    # Copy if source and target are different
                    if os.path.abspath(xmla_file) != os.path.abspath(xmla_target):
                        shutil.copy2(xmla_file, xmla_target)
                    
                    xmla_filename = xmla_basename
                
                # Store processed assignment with just filenames
                processed_assignments[pair] = {
                    'jdbc_file': jdbc_filename,
                    'xmla_file': xmla_filename,
                    'jdbc_has_header': assignment['jdbc_has_header'].get(),
                    'xmla_has_header': assignment['xmla_has_header'].get()
                }
            
            # Write systems.properties with CSV configuration
            self.core.write_systems_properties_with_csv(
                self.selected_pairs, 
                processed_assignments
            )
            
            # Update main config with processed file assignments (just filenames)
            self.config_callback(processed_assignments)
            
            self.status_label.config(text="✅ CSV configuration saved! Files copied to ingest folder.", fg='green')
            
            # Close window after a short delay
            self.window.after(1500, self.window.destroy)
            
        except Exception as e:
            messagebox.showerror("Error", f"Failed to save configuration:\n{e}")
            self.status_label.config(text=f"❌ Error: {e}", fg='red')
    
    def clear_all_assignments(self):
        """Clear all file assignments"""
        for pair in self.selected_pairs:
            self.file_assignments[pair]['jdbc_file'] = ''
            self.file_assignments[pair]['xmla_file'] = ''
            if 'jdbc_label' in self.file_assignments[pair]:
                self.file_assignments[pair]['jdbc_label'].config(text="Not selected", fg='red')
            if 'xmla_label' in self.file_assignments[pair]:
                self.file_assignments[pair]['xmla_label'].config(text="Not selected", fg='red')
        
        self.status_label.config(text="✓ All assignments cleared", fg='orange')
    
    def log_activity(self, message):
        """Log activity message to status"""
        self.status_label.config(text=message)
        self.window.update()