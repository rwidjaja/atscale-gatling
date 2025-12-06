#!/usr/bin/env python3
"""
Main entry point for AtScale Gatling Controller.
Clean version without debug prints.
"""

import sys
import os
import argparse

from atscalewrapper.cli import run_cli_mode, create_cli_parser


def check_dependencies() -> bool:
    """Check for required dependencies."""
    try:
        import tkinter  # noqa: F401
        import requests  # noqa: F401
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install required packages:")
        print("  pip install requests")
        return False


def ensure_config_exists(mode: str) -> bool:
    """Ensure config.json exists before proceeding."""
    try:
        from atscalewrapper.config_manager import ConfigManager
        config_manager = ConfigManager()
        
        if config_manager.config_exists():
            return True
        
        print("\n" + "="*60)
        print("Configuration Required")
        print("="*60)
        print("\nconfig.json not found.")
        
        if mode == "gui":
            print("Opening configuration window...")
            
            # Create a simple window first to ensure user sees it
            import tkinter as tk
            from tkinter import messagebox
            
            # Create a minimal window to get attention
            attention_window = tk.Tk()
            attention_window.title("Configuration Setup")
            attention_window.geometry("500x200")
            
            # Center window
            attention_window.update_idletasks()
            x = (attention_window.winfo_screenwidth() - attention_window.winfo_width()) // 2
            y = (attention_window.winfo_screenheight() - attention_window.winfo_height()) // 2
            attention_window.geometry(f"+{x}+{y}")
            
            # Add content
            tk.Label(attention_window, text="Configuration Required", 
                    font=("Arial", 14, "bold")).pack(pady=20)
            
            tk.Label(attention_window, text="Click 'Configure' to create config.json file.",
                    font=("Arial", 11)).pack(pady=10)
            
            result = {"confirmed": False}
            
            def on_configure():
                result["confirmed"] = True
                attention_window.destroy()
            
            def on_exit():
                attention_window.destroy()
            
            # White buttons with black text
            tk.Button(attention_window, text="Configure", command=on_configure,
                     bg="white", fg="black", font=("Arial", 10, "bold"),
                     width=15, height=2).pack(side=tk.LEFT, padx=20, pady=20)
            
            tk.Button(attention_window, text="Exit", command=on_exit,
                     bg="white", fg="black", font=("Arial", 10),
                     width=15, height=2).pack(side=tk.RIGHT, padx=20, pady=20)
            
            # Bring to front
            attention_window.lift()
            attention_window.attributes('-topmost', True)
            attention_window.after_idle(attention_window.attributes, '-topmost', False)
            
            attention_window.mainloop()
            
            if not result["confirmed"]:
                print("Configuration cancelled.")
                return False
            
            # Now create the actual configuration
            return config_manager.create_config_gui()
        else:
            # CLI mode
            return config_manager.create_config_cli()
            
    except Exception as e:
        print(f"❌ Configuration setup failed: {e}")
        return False


def setup_environment(args) -> bool:
    """Set up environment."""
    print("\n" + "="*60)
    print("Environment Setup")
    print("="*60)
    
    # 1. Ensure config exists FIRST
    if not ensure_config_exists(args.mode):
        return False
    
    print("✅ Configuration check passed")
    
    # 2. Create base query file
    try:
        from atscalewrapper.query_manager import QueryManager
        query_manager = QueryManager()
        query_manager.create_base_query_file()
        print("✅ Base query file created")
    except Exception:
        print("⚠ Could not create base query file")
    
    # 3. Clean logs
    try:
        from atscalewrapper.log_manager import LogManager
        log_manager = LogManager()
        if args.mode == "gui":
            log_manager.check_and_clean_gui()
        else:
            log_manager.check_and_clean_cli()
        print("✅ Logs cleaned")
    except Exception:
        print("⚠ Could not clean log files")
    
    # 4. Check certificates (just inform user)
    try:
        from atscalewrapper.config_manager import ConfigManager
        config_manager = ConfigManager()
        
        root_crt = os.path.join(config_manager.home_dir, 'root.crt')
        cacerts = os.path.join(config_manager.home_dir, 'cacerts')
        
        if os.path.exists(root_crt) and os.path.exists(cacerts):
            print("✅ Certificates found")
        else:
            print("⚠ Some certificates missing")
            print("   Will be prompted if needed during operation")
                
    except Exception:
        print("⚠ Certificate check failed")
    
    print("\n" + "="*60)
    print("Setup Complete")
    print("="*60)
    return True


def run_gui_mode() -> int:
    """Start Tkinter GUI if display is available."""
    if sys.platform not in ("darwin", "win32") and not os.getenv("DISPLAY"):
        print("❌ No GUI display detected. Run with: --mode cli")
        return 1

    try:
        import tkinter as tk
        from atscalewrapper.gui import AtScaleGatlingGUI
    except ImportError as e:
        print(f"❌ Failed to load GUI modules: {e}")
        print("Try running CLI mode instead: --mode cli")
        return 1

    # Create and run main GUI
    root = tk.Tk()
    app = AtScaleGatlingGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
    return 0


def main() -> int:
    """Application entry point."""
    print("🚀 AtScale Gatling Controller")
    
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Parse arguments
    parser = argparse.ArgumentParser(description="AtScale Gatling Controller")
    parser.add_argument(
        "--mode",
        choices=["gui", "cli"],
        default="gui",
        help="Run in GUI or CLI mode (default: GUI)",
    )
    
    args, remaining = parser.parse_known_args()
    
    # Setup environment (config MUST be created first)
    if not setup_environment(args):
        print("\n❌ Environment setup failed. Exiting.")
        return 1
    
    # Run the appropriate mode
    if args.mode == "cli":
        cli_parser = create_cli_parser()
        cli_args = cli_parser.parse_args(remaining)
        return run_cli_mode(cli_args)

    return run_gui_mode()


if __name__ == "__main__":
    sys.exit(main())