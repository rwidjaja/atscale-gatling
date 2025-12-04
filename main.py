#!/usr/bin/env python3
"""Main entry point for AtScale Gatling Controller"""
import sys
import os
import argparse
from atscalewrapper.cli import run_cli_mode, create_cli_parser
from atscalewrapper.gui import AtScaleGatlingGUI


def check_dependencies():
    """Check if required dependencies are available"""
    try:
        import tkinter
        import requests
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Please install required packages:")
        print("  pip install requests")
        return False

def run_gui_mode():
    """Run in GUI mode"""
    # Check if we have display (for SSH with X11 forwarding)
    if sys.platform != "darwin" and not os.getenv('DISPLAY'):
        print("No display available. This application requires a GUI display.")
        print("Use the CLI version or run on a system with graphical display.")
        return 1
        
    import tkinter as tk
    
    root = tk.Tk()
    app = AtScaleGatlingGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
    return 0

def main():
    """Main entry point"""
    # Check dependencies
    if not check_dependencies():
        return 1
    
    # Create main parser
    main_parser = argparse.ArgumentParser(description='AtScale Gatling Controller')
    main_parser.add_argument('--mode', choices=['gui', 'cli'], default='gui',
                           help='Run in GUI or CLI mode (default: GUI)')
    
    # Parse only the mode first
    args, remaining = main_parser.parse_known_args()
    
    if args.mode == 'cli':
        # Create CLI parser and parse remaining args
        cli_parser = create_cli_parser()
        cli_args = cli_parser.parse_args(remaining)
        return run_cli_mode(cli_args)
    else:
        return run_gui_mode()

if __name__ == "__main__":
    sys.exit(main())