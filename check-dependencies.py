#!/usr/bin/env python3
import os
import sys
import platform
import subprocess
import importlib

# External dependencies only (stdlib modules are always present)
EXTERNAL_DEPENDENCIES = ["requests", "urllib3", "InquirerPy"]

def check_os():
    system = platform.system()
    machine = platform.machine()
    if system == "Darwin":
        if "arm" in machine.lower():
            return "macOS (Apple Silicon)"
        else:
            return "macOS (Intel)"
    elif system == "Linux":
        try:
            with open("/etc/os-release") as f:
                data = f.read().lower()
                if "ubuntu" in data:
                    return "Ubuntu Linux"
        except FileNotFoundError:
            pass
        return "Linux (non-Ubuntu)"
    elif system == "Windows":
        return "Windows"
    else:
        return f"Other ({system})"

def check_python():
    print(f"Python executable: {sys.executable}")
    print(f"Python version: {platform.python_version()}")

def check_dependencies():
    missing = []
    for dep in EXTERNAL_DEPENDENCIES:
        try:
            importlib.import_module(dep)
        except ImportError:
            missing.append(dep)
    return missing

def ensure_venv():
    # If not inside a venv, create one
    if sys.prefix == sys.base_prefix:
        print("⚠️ Not inside a virtual environment. Creating one...")
        subprocess.run([sys.executable, "-m", "venv", "venv"])
        print("✅ Virtual environment created at ./venv")
        if platform.system() == "Windows":
            print("Activate it with: venv\\Scripts\\activate")
        else:
            print("Activate it with: source venv/bin/activate")

def install_dependencies(missing):
    for dep in missing:
        print(f"Installing {dep}...")
        subprocess.run([sys.executable, "-m", "pip", "install", dep])

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
            ensure_venv()
            install_dependencies(missing)
    else:
        print("All external dependencies are satisfied.")

    print("\nChecking Docker...")
    if check_docker():
        print("Docker is installed and running.")
    else:
        print("Docker is not installed or not running.")

if __name__ == "__main__":
    main()
