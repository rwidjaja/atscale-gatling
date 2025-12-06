"""
config_manager.py
Handles configuration file management and certificate generation.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional

# GUI availability
GUI_AVAILABLE = False
try:
    import tkinter as tk
    from tkinter import messagebox, simpledialog
    GUI_AVAILABLE = True
except ImportError:
    pass


DEFAULT_CONFIG = {
    "username": "",
    "password": "",
    "host": "",
    "postgres_host": "",
    "token": "",
    "proxy": "",
    "proxyport": "",
    "proxy_username": "",
    "proxy_password": "",
    "aws.region": "",
    "aws.secrets-key": "",
    "snowflake.archive.account": "",
    "snowflake.archive.warehouse": "",
    "snowflake.archive.database": "",
    "snowflake.archive.schema": "",
    "snowflake.archive.role": "",
    "snowflake.archive.username": "",
    "snowflake.archive.password": "",
    "snowflake.archive.token": ""
}


class ConfigManager:
    def __init__(self, config_path: str = "config.json"):
        self.config_path = config_path
        self.home_dir = str(Path.home())
        
    def config_exists(self) -> bool:
        return os.path.exists(self.config_path)
    
    def load_config(self) -> Dict[str, Any]:
        if not self.config_exists():
            raise FileNotFoundError(f"Configuration file '{self.config_path}' not found")
        with open(self.config_path, 'r') as f:
            return json.load(f)
    
    def save_config(self, config_data: Dict[str, Any]):
        with open(self.config_path, 'w') as f:
            json.dump(config_data, f, indent=2)
    
    def create_config_gui(self, parent_window=None) -> bool:
        """Create configuration via GUI popup."""
        if not GUI_AVAILABLE:
            print("❌ GUI not available. Please create config.json manually.")
            return False
        
        config_data = DEFAULT_CONFIG.copy()
        
        # Create window
        if parent_window:
            config_window = tk.Toplevel(parent_window)
            config_window.transient(parent_window)
            config_window.grab_set()
        else:
            config_window = tk.Tk()
        
        config_window.title("Configuration Setup")
        
        # Make window modal
        config_window.focus_set()
        config_window.lift()
        
        # Create main frame
        main_frame = tk.Frame(config_window, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        tk.Label(main_frame, text="Configuration Setup", 
                font=("Arial", 14, "bold")).pack(pady=(0, 20))
        
        tk.Label(main_frame, text="Please fill in the configuration values:",
                font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 10))
        
        # Create scrollable area
        canvas = tk.Canvas(main_frame)
        scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Add fields
        entries = {}
        row = 0
        
        for key, value in config_data.items():
            tk.Label(scrollable_frame, text=f"{key}:", anchor="w").grid(
                row=row, column=0, sticky="w", padx=5, pady=2)
            
            if "password" in key.lower() or "token" in key.lower():
                entry = tk.Entry(scrollable_frame, width=40, show="*")
            else:
                entry = tk.Entry(scrollable_frame, width=40)
            
            entry.grid(row=row, column=1, padx=5, pady=2, sticky="w")
            entry.insert(0, str(value))
            entries[key] = entry
            row += 1
        
        # Add buttons
        button_frame = tk.Frame(scrollable_frame)
        button_frame.grid(row=row, column=0, columnspan=2, pady=20)
        
        result = {"saved": False}
        
        def on_save():
            # Get values from entries
            for key, entry in entries.items():
                config_data[key] = entry.get()
            
            try:
                self.save_config(config_data)
                result["saved"] = True
                config_window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save configuration: {e}")
        
        def on_cancel():
            config_window.destroy()
        
        # White buttons with black text
        tk.Button(button_frame, text="Save", command=on_save, 
                 bg="white", fg="black", width=15).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Cancel", command=on_cancel,
                 bg="white", fg="black", width=15).pack(side=tk.LEFT, padx=5)
        
        # Pack scrollable area
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Set window size
        config_window.geometry("700x500")
        
        # Center window
        config_window.update_idletasks()
        if parent_window:
            x = parent_window.winfo_x() + (parent_window.winfo_width() - config_window.winfo_width()) // 2
            y = parent_window.winfo_y() + (parent_window.winfo_height() - config_window.winfo_height()) // 2
        else:
            x = (config_window.winfo_screenwidth() - config_window.winfo_width()) // 2
            y = (config_window.winfo_screenheight() - config_window.winfo_height()) // 2
        
        config_window.geometry(f"+{x}+{y}")
        
        # Wait for window to close
        if parent_window:
            config_window.wait_window()
        else:
            config_window.mainloop()
        
        return result["saved"]
    
    def create_config_cli(self) -> bool:
        """Create configuration via CLI input."""
        print("\n" + "="*60)
        print("Configuration Setup")
        print("="*60)
        print("\nPlease enter configuration values (press Enter for defaults):\n")
        
        config_data = DEFAULT_CONFIG.copy()
        
        # Ask for essential fields first
        essential_fields = ['host', 'username', 'postgres_host']
        print("Essential fields:")
        for key in essential_fields:
            default = config_data.get(key, '')
            value = input(f"{key} [{default}]: ").strip()
            if value:
                config_data[key] = value
        
        # Ask for optional fields
        print("\nOptional fields (press Enter to skip):")
        other_fields = [k for k in config_data.keys() if k not in essential_fields]
        
        for key in other_fields:
            default = config_data.get(key, '')
            
            if any(sensitive in key.lower() for sensitive in ['password', 'token', 'secret']):
                import getpass
                prompt = f"{key}"
                if default:
                    masked = '*' * min(len(default), 8)
                    prompt += f" [{masked}]"
                prompt += ": "
                value = getpass.getpass(prompt)
            else:
                prompt = f"{key}"
                if default:
                    prompt += f" [{default}]"
                prompt += ": "
                value = input(prompt).strip()
            
            if value:
                config_data[key] = value
        
        try:
            self.save_config(config_data)
            print(f"\n✅ Configuration saved to {self.config_path}")
            return True
        except Exception as e:
            print(f"\n❌ Failed to save configuration: {e}")
            return False
        
# Add this method to the ConfigManager class in config_manager.py
    def edit_config_gui(self, parent_window=None) -> bool:
        """Edit existing configuration via GUI popup."""
        if not GUI_AVAILABLE:
            print("❌ GUI not available. Please edit config.json manually.")
            return False
        
        # Load existing config if it exists, otherwise use default
        if self.config_exists():
            try:
                config_data = self.load_config()
                # Ensure all default keys are present (for backward compatibility)
                for key in DEFAULT_CONFIG.keys():
                    if key not in config_data:
                        config_data[key] = DEFAULT_CONFIG[key]
            except Exception as e:
                print(f"❌ Failed to load existing config: {e}")
                config_data = DEFAULT_CONFIG.copy()
        else:
            config_data = DEFAULT_CONFIG.copy()
        
        # Create window
        if parent_window:
            config_window = tk.Toplevel(parent_window)
            config_window.transient(parent_window)
            config_window.grab_set()
        else:
            config_window = tk.Tk()
        
        config_window.title("Edit Configuration")
        
        # Make window modal
        config_window.focus_set()
        config_window.lift()
        
        # Create main frame
        main_frame = tk.Frame(config_window, padx=20, pady=20)
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # Title
        tk.Label(main_frame, text="Edit Configuration", 
                font=("Arial", 14, "bold")).pack(pady=(0, 20))
        
        tk.Label(main_frame, text="Modify configuration values as needed:",
                font=("Arial", 10)).pack(anchor=tk.W, pady=(0, 10))
        
        # Create scrollable area
        canvas = tk.Canvas(main_frame)
        scrollbar = tk.Scrollbar(main_frame, orient="vertical", command=canvas.yview)
        scrollable_frame = tk.Frame(canvas)
        
        scrollable_frame.bind(
            "<Configure>",
            lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
        )
        
        canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)
        
        # Add fields
        entries = {}
        row = 0
        
        for key, value in config_data.items():
            tk.Label(scrollable_frame, text=f"{key}:", anchor="w").grid(
                row=row, column=0, sticky="w", padx=5, pady=2)
            
            if "password" in key.lower() or "token" in key.lower():
                entry = tk.Entry(scrollable_frame, width=40, show="*")
            else:
                entry = tk.Entry(scrollable_frame, width=40)
            
            entry.grid(row=row, column=1, padx=5, pady=2, sticky="w")
            entry.insert(0, str(value))
            entries[key] = entry
            row += 1
        
        # Add buttons
        button_frame = tk.Frame(scrollable_frame)
        button_frame.grid(row=row, column=0, columnspan=2, pady=20)
        
        result = {"saved": False}
        
        def on_save():
            # Get values from entries
            for key, entry in entries.items():
                config_data[key] = entry.get()
            
            try:
                self.save_config(config_data)
                result["saved"] = True
                config_window.destroy()
            except Exception as e:
                messagebox.showerror("Error", f"Failed to save configuration: {e}")
        
        def on_cancel():
            config_window.destroy()
        
        # White buttons with black text
        tk.Button(button_frame, text="Save", command=on_save, 
                bg="white", fg="black", width=15).pack(side=tk.LEFT, padx=5)
        
        tk.Button(button_frame, text="Cancel", command=on_cancel,
                bg="white", fg="black", width=15).pack(side=tk.LEFT, padx=5)
        
        # Pack scrollable area
        canvas.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Set window size
        config_window.geometry("700x500")
        
        # Center window
        config_window.update_idletasks()
        if parent_window:
            x = parent_window.winfo_x() + (parent_window.winfo_width() - config_window.winfo_width()) // 2
            y = parent_window.winfo_y() + (parent_window.winfo_height() - config_window.winfo_height()) // 2
        else:
            x = (config_window.winfo_screenwidth() - config_window.winfo_width()) // 2
            y = (config_window.winfo_screenheight() - config_window.winfo_height()) // 2
        
        config_window.geometry(f"+{x}+{y}")
        
        # Wait for window to close
        if parent_window:
            config_window.wait_window()
        else:
            config_window.mainloop()
        
        return result["saved"]
    
    def check_and_create_certificates(self, auto_create: bool = False) -> bool:
        """Check and create certificates."""
        if not self.config_exists():
            print("❌ Configuration file not found.")
            return False
        
        try:
            config = self.load_config()
            host = config.get('host')
            postgres_host = config.get('postgres_host', host)
            
            if not host:
                print("❌ Host not specified in configuration")
                return False
            
            root_crt = os.path.join(self.home_dir, 'root.crt')
            cacerts = os.path.join(self.home_dir, 'cacerts')
            
            root_exists = os.path.exists(root_crt)
            cacerts_exists = os.path.exists(cacerts)
            
            if root_exists and cacerts_exists:
                return True
            
            if auto_create:
                success = True
                
                if not root_exists:
                    print(f"\nCreating root.crt from {postgres_host}...")
                    if self._create_root_certificate(postgres_host, root_crt):
                        print("✅ root.crt created")
                    else:
                        print("❌ Failed to create root.crt")
                        success = False
                
                if success and not cacerts_exists:
                    print(f"\nCreating cacerts...")
                    if self._create_truststore(host, root_crt, cacerts):
                        print("✅ cacerts created")
                    else:
                        print("❌ Failed to create cacerts")
                        success = False
                
                return success
            
            return False
            
        except Exception as e:
            print(f"❌ Error: {e}")
            return False
    
    def prompt_certificate_creation_gui(self, parent_window=None) -> bool:
        """Prompt for certificate creation in GUI."""
        if not GUI_AVAILABLE:
            return False
        
        root_crt = os.path.join(self.home_dir, 'root.crt')
        cacerts = os.path.join(self.home_dir, 'cacerts')
        
        if os.path.exists(root_crt) and os.path.exists(cacerts):
            return True
        
        missing = []
        if not os.path.exists(root_crt):
            missing.append("root.crt")
        if not os.path.exists(cacerts):
            missing.append("cacerts")
        
        message = f"Missing certificates:\n\n"
        for cert in missing:
            message += f"• {cert}\n"
        message += "\nCreate them now?"
        
        if parent_window:
            response = messagebox.askyesno("Missing Certificates", message, parent=parent_window)
        else:
            root = tk.Tk()
            root.withdraw()
            response = messagebox.askyesno("Missing Certificates", message)
            root.destroy()
        
        if response:
            return self.check_and_create_certificates(auto_create=True)
        
        return False
    
    def prompt_certificate_creation_cli(self) -> bool:
        """Prompt for certificate creation in CLI."""
        root_crt = os.path.join(self.home_dir, 'root.crt')
        cacerts = os.path.join(self.home_dir, 'cacerts')
        
        if os.path.exists(root_crt) and os.path.exists(cacerts):
            print("✅ Certificates found")
            return True
        
        print("\n⚠ Missing certificates:")
        if not os.path.exists(root_crt):
            print("  ❌ root.crt")
        if not os.path.exists(cacerts):
            print("  ❌ cacerts")
        
        response = input("\nCreate missing certificates? (yes/no): ").strip().lower()
        
        if response in ['yes', 'y']:
            return self.check_and_create_certificates(auto_create=True)
        
        print("⚠ Certificate creation skipped")
        return False
    
    def _create_root_certificate(self, host: str, output_path: str) -> bool:
        """Create root certificate."""
        try:
            import ssl
            import socket
            
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            with socket.create_connection((host, 10500), timeout=10) as sock:
                with context.wrap_socket(sock, server_hostname=host) as ssock:
                    cert_bin = ssock.getpeercert(binary_form=True)
                    if cert_bin:
                        cert_pem = ssl.DER_cert_to_PEM_cert(cert_bin)
                        with open(output_path, 'w') as f:
                            f.write(cert_pem)
                        return True
            return False
        except Exception:
            return False
    
    def _create_truststore(self, host: str, cert_path: str, keystore_path: str) -> bool:
        """Create Java keystore."""
        if not os.path.exists(cert_path):
            return False
        
        try:
            import shutil
            keytool = shutil.which('keytool')
            if not keytool:
                return False
            
            cmd = [
                keytool, '-importcert',
                '-storetype', 'JKS',
                '-keystore', keystore_path,
                '-storepass', 'changeit',
                '-alias', host,
                '-file', cert_path,
                '-noprompt'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0
        except Exception:
            return False