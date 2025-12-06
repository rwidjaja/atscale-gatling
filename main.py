#!/usr/bin/env python3
"""
Main entry point for AtScale Gatling Controller.
Supports GUI mode and CLI mode.
"""

import sys
import os
import glob
import argparse

from atscalewrapper.cli import run_cli_mode, create_cli_parser

# GUI is imported lazily inside run_gui_mode() to allow CLI-only environments.
# from atscalewrapper.gui import AtScaleGatlingGUI


# Base SQL query used for generating working_dir/config/base_query.sql
BASE_SQL_QUERY = """SELECT
    q.service,
    q.query_language,
    q.query_text AS inbound_text,
    MAX(s.subquery_text) AS outbound_text,
    p.cube_name,
    p.project_id,
    CASE WHEN MAX(s.subquery_text) LIKE '%as_agg_%' THEN TRUE ELSE FALSE END AS used_agg,
    COUNT(*)                             AS num_times,
    AVG(r.finished - p.planning_started) AS elapsed_time_in_seconds,
    AVG(r.result_size)                   AS avg_result_size
FROM
    atscale.queries q
INNER JOIN
    atscale.query_results r ON q.query_id = r.query_id
INNER JOIN
    atscale.queries_planned p ON q.query_id = p.query_id
INNER JOIN
    atscale.subqueries s ON q.query_id = s.query_id
WHERE
    q.query_language = ?
    AND p.planning_started > CURRENT_TIMESTAMP - INTERVAL '60 day'
    AND p.cube_name = ?
    AND q.service = 'user-query'
    AND r.succeeded = TRUE
    AND LENGTH(q.query_text) > 100
    AND q.query_text NOT LIKE '/* Virtual query to get the members of a level */%'
    AND q.query_text NOT LIKE '-- statement does not return rows%'
GROUP BY
    1, 2, 3, 5, 6
ORDER BY 3;
"""


def create_base_query_file():
    """Ensure working_dir/config/base_query.sql exists."""
    config_dir = "working_dir/config"
    base_query_path = os.path.join(config_dir, "base_query.sql")

    os.makedirs(config_dir, exist_ok=True)

    if not os.path.exists(base_query_path):
        print(f"📝 Creating base_query.sql in {config_dir}")
        with open(base_query_path, "w") as f:
            f.write(BASE_SQL_QUERY)
        print("✅ base_query.sql created successfully")
    else:
        print(f"✔ base_query.sql already exists at {base_query_path}")


def check_dependencies():
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


def check_and_clean_logs_gui():
    """Check for existing log files and ask user if they want to clean them up (GUI mode)."""
    logs_dir = "working_dir/run_logs"
    
    # Create directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    # Check for .log files
    log_files = glob.glob(os.path.join(logs_dir, "*.log"))
    
    if not log_files:
        return True  # No logs to clean
    
    try:
        import tkinter as tk
        from tkinter import messagebox
    except ImportError:
        # If GUI dependencies aren't available, just print a message
        print(f"⚠ Found {len(log_files)} existing log files in {logs_dir}/")
        print("  To clean them manually, delete files from that directory.")
        return True
    
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    
    # Show dialog
    response = messagebox.askyesno(
        "Clean Log Files",
        f"Found {len(log_files)} existing log files in:\n{logs_dir}/\n\n"
        "Do you want to clean them up before starting?",
        icon=messagebox.QUESTION
    )
    
    root.destroy()
    
    if response:
        cleaned_count = 0
        for log_file in log_files:
            try:
                os.remove(log_file)
                cleaned_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {log_file}: {e}")
        
        # Show confirmation
        root = tk.Tk()
        root.withdraw()
        messagebox.showinfo(
            "Log Files Cleaned",
            f"Successfully cleaned {cleaned_count} log files."
        )
        root.destroy()
    
    return True


def check_and_clean_logs_cli():
    """Check for existing log files and ask user if they want to clean them up (CLI mode)."""
    logs_dir = "working_dir/run_logs"
    
    # Create directory if it doesn't exist
    os.makedirs(logs_dir, exist_ok=True)
    
    # Check for .log files
    log_files = glob.glob(os.path.join(logs_dir, "*.log"))
    
    if not log_files:
        return True  # No logs to clean
    
    print(f"⚠ Found {len(log_files)} existing log files in {logs_dir}/")
    
    response = input("Do you want to clean them up before starting? (yes/no): ").strip().lower()
    
    if response in ['yes', 'y']:
        cleaned_count = 0
        for log_file in log_files:
            try:
                os.remove(log_file)
                cleaned_count += 1
            except Exception as e:
                print(f"❌ Failed to delete {log_file}: {e}")
        
        print(f"✅ Successfully cleaned {cleaned_count} log files.")
    
    return True


def run_gui_mode():
    """Start Tkinter GUI if display is available."""
    # Basic display check for Linux/X11 environments
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
    
    # Check and clean logs before starting GUI
    check_and_clean_logs_gui()

    root = tk.Tk()
    app = AtScaleGatlingGUI(root)
    root.protocol("WM_DELETE_WINDOW", app.on_closing)
    root.mainloop()
    return 0


def main():
    """Application entry point."""
    if not check_dependencies():
        return 1

    create_base_query_file()

    parser = argparse.ArgumentParser(description="AtScale Gatling Controller")
    parser.add_argument(
        "--mode",
        choices=["gui", "cli"],
        default="gui",
        help="Run in GUI or CLI mode (default: GUI)",
    )

    args, remaining = parser.parse_known_args()

    if args.mode == "cli":
        # Check and clean logs for CLI mode
        check_and_clean_logs_cli()
        
        cli_parser = create_cli_parser()
        cli_args = cli_parser.parse_args(remaining)
        return run_cli_mode(cli_args)

    return run_gui_mode()


if __name__ == "__main__":
    sys.exit(main())