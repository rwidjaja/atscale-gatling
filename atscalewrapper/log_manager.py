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
        """Clean all log files, then create init.log."""
        log_files = self.get_existing_logs()
        cleaned_count = 0
        
        for log_file in log_files:
            try:
                os.remove(log_file)
                cleaned_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {log_file}: {e}")
        
        # Always create init.log after cleanup
        init_path = os.path.join(self.logs_dir, "init.log")
        try:
            with open(init_path, "w", encoding="utf-8") as f:
                f.write("Log initialization complete.\n")
            print(f"📝 Created {init_path}")
        except Exception as e:
            print(f"❌ Failed to create init.log: {e}")
        
        return cleaned_count
    
    def check_and_clean_gui(self) -> bool:
        """Check for existing log files and ask user if they want to clean them (GUI mode)."""
        self.ensure_logs_dir()
        log_files = self.get_existing_logs()
        
        if not log_files:
            # Still create init.log even if no logs existed
            self.clean_logs()
            return True
        
        if not GUI_AVAILABLE:
            print(f"⚠ Found {len(log_files)} existing log files in {self.logs_dir}/")
            print("  To clean them manually, delete files from that directory.")
            # Still create init.log
            self.clean_logs()
            return True
        
        root = tk.Tk()
        root.withdraw()
        
        response = messagebox.askyesno(
            "Clean Log Files",
            f"Found {len(log_files)} existing log files in:\n{self.logs_dir}/\n\n"
            "Do you want to clean them up before starting?",
            icon=messagebox.QUESTION
        )
        
        root.destroy()
        
        if response:
            cleaned_count = self.clean_logs()
            root = tk.Tk()
            root.withdraw()
            messagebox.showinfo(
                "Log Files Cleaned",
                f"Successfully cleaned {cleaned_count} log files and created init.log."
            )
            root.destroy()
        else:
            # Even if user says no, still ensure init.log exists
            self.clean_logs()
        
        return True
    
    def check_and_clean_cli(self) -> bool:
        """Check for existing log files and ask user if they want to clean them (CLI mode)."""
        self.ensure_logs_dir()
        log_files = self.get_existing_logs()
        
        if not log_files:
            # No logs, but still create init.log
            self.clean_logs()
            return True
        
        print(f"⚠ Found {len(log_files)} existing log files in {self.logs_dir}/")
        
        response = input("Do you want to clean them up before starting? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            cleaned_count = self.clean_logs()
            print(f"✅ Successfully cleaned {cleaned_count} log files and created init.log.")
        else:
            # Even if user says no, still ensure init.log exists
            self.clean_logs()
        
        return True
