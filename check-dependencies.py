import os
import sys
import platform
import subprocess
import importlib

# List of Python dependencies you mentioned
DEPENDENCIES = [
    "tkinter", "threading", "subprocess", "os", "time", "queue",
    "requests", "xml.etree.ElementTree", "json", "sys", "urllib3", "InquirerPy",
    "argparse", "datetime"
]

def check_os():
    system = platform.system()
    machine = platform.machine()
    if system == "Darwin":
        if "arm" in machine.lower():
            return "macOS (Apple Silicon)"
        else:
            return "macOS (Intel)"
    elif system == "Linux":
        # crude check for Ubuntu
        try:
            with open("/etc/os-release") as f:
                data = f.read().lower()
                if "ubuntu" in data:
                    return "Ubuntu Linux"
        except FileNotFoundError:
            pass
        return "Linux (non-Ubuntu)"
    else:
        return f"Other ({system})"

def check_python():
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {platform.python_version()}")

def check_dependencies():
    missing = []
    for dep in DEPENDENCIES:
        try:
            importlib.import_module(dep.split('.')[0])
        except ImportError:
            missing.append(dep)
    return missing

def check_docker():
    try:
        subprocess.run(["docker", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        return False

def main():
    print("=== System Dependency Check ===")
    print("OS:", check_os())
    check_python()

    print("\nChecking Python dependencies...")
    missing = check_dependencies()
    if missing:
        print("Missing dependencies:", ", ".join(missing))
        choice = input("Do you want to install them into your venv? (y/n): ").strip().lower()
        if choice == "y":
            for dep in missing:
                # Only install external packages (skip stdlib)
                if dep in ["requests", "urllib3"]:
                    print(f"Installing {dep}...")
                    subprocess.run([sys.executable, "-m", "pip", "install", dep])
    else:
        print("All dependencies are satisfied.")

    print("\nChecking Docker...")
    if check_docker():
        print("Docker is installed and running.")
    else:
        print("Docker is not installed or not running.")

if __name__ == "__main__":
    main()
