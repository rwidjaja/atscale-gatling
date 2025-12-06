"""
log_manager.py
Handles log file cleanup and management.
"""

import glob
import os
import sys
from typing import List

try:
    import tkinter as tk
    from tkinter import messagebox
    GUI_AVAILABLE = True
except ImportError:
    GUI_AVAILABLE = False


class LogManager:
    def __init__(self, logs_dir: str = "working_dir/run_logs"):
        self.logs_dir = logs_dir
        
    def ensure_logs_dir(self):
        """Ensure logs directory exists."""
        os.makedirs(self.logs_dir, exist_ok=True)
    
    def get_existing_logs(self) -> List[str]:
        """Get list of existing log files."""
        return glob.glob(os.path.join(self.logs_dir, "*.log"))
    
    def clean_logs(self) -> int:
        """Clean all log files."""
        log_files = self.get_existing_logs()
        cleaned_count = 0
        
        for log_file in log_files:
            try:
                os.remove(log_file)
                cleaned_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {log_file}: {e}")
        
        return cleaned_count
    
    def check_and_clean_gui(self) -> bool:
        """Check for existing log files and ask user if they want to clean them (GUI mode)."""
        self.ensure_logs_dir()
        log_files = self.get_existing_logs()
        
        if not log_files:
            return True  # No logs to clean
        
        if not GUI_AVAILABLE:
            print(f"⚠ Found {len(log_files)} existing log files in {self.logs_dir}/")
            print("  To clean them manually, delete files from that directory.")
            return True
        
        root = tk.Tk()
        root.withdraw()  # Hide the root window
        
        # Show dialog
        response = messagebox.askyesno(
            "Clean Log Files",
            f"Found {len(log_files)} existing log files in:\n{self.logs_dir}/\n\n"
            "Do you want to clean them up before starting?",
            icon=messagebox.QUESTION
        )
        
        root.destroy()
        
        if response:
            cleaned_count = self.clean_logs()
            
            # Show confirmation
            root = tk.Tk()
            root.withdraw()
            messagebox.showinfo(
                "Log Files Cleaned",
                f"Successfully cleaned {cleaned_count} log files."
            )
            root.destroy()
        
        return True
    
    def check_and_clean_cli(self) -> bool:
        """Check for existing log files and ask user if they want to clean them (CLI mode)."""
        self.ensure_logs_dir()
        log_files = self.get_existing_logs()
        
        if not log_files:
            return True  # No logs to clean
        
        print(f"⚠ Found {len(log_files)} existing log files in {self.logs_dir}/")
        
        response = input("Do you want to clean them up before starting? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            cleaned_count = self.clean_logs()
            print(f"✅ Successfully cleaned {cleaned_count} log files.")
        
        return True