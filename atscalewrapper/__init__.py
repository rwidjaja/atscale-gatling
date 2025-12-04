"""atscalewrapper package - lightweight wrapper over project modules

This package provides a small compatibility layer so tools and the
top-level `main.py` can import `atscalewrapper.cli`, `atscalewrapper.gui`,
etc., while the actual implementation remains in the existing modules
(`cli`, `gui`, `core`).
"""

"""AtScale Gatling Wrapper Package"""

# Expose submodules
from . import cli
from . import gui
from . import core
from . import config

# Expose key classes/functions at top level
from .core import AtScaleGatlingCore
from .gui import AtScaleGatlingGUI
from .cli import run_cli_mode, create_cli_parser
from .config import ConfigManager, Constants
from .csv_handler import CSVConfigWindow

__all__ = [
    "cli",
    "gui",
    "core",
    "config",
    "AtScaleGatlingCore",
    "AtScaleGatlingGUI",
    "run_cli_mode",
    "create_cli_parser",
    "ConfigManager",
    "Constants",
    "CSVConfigWindow",
]


__version__ = "1.0.0"
__author__ = "Rudy Widjaja"
