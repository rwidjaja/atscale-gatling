"""Simple config helper for atscalewrapper."""
import json
import os


def load_config(path=None):
    """Load JSON config from `path` or from package-local `config.json`.

    Returns a dict (may raise FileNotFoundError).
    """
    if path is None:
        pkg_cfg = os.path.join(os.path.dirname(__file__), "config.json")
        if os.path.exists(pkg_cfg):
            path = pkg_cfg
        elif os.path.exists("config.json"):
            path = "config.json"
        else:
            raise FileNotFoundError("config.json not found")

    with open(path, "r") as f:
        return json.load(f)

__all__ = ["load_config"]
"""Configuration and constants"""
import os
import json

class ConfigManager:
    """Manages configuration loading and validation"""
    
    DEFAULT_CONFIG = {
        "host": "",
        "username": "",
        "password": "",
        "token": "",
        "postgres_host": "",
        "proxy": "",
        "proxyport": "",
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
    
    @staticmethod
    def load_config(config_path="config.json"):
        """Load configuration from JSON file"""
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
            
        with open(config_path) as f:
            config = json.load(f)
            
        # Validate required fields
        required_fields = ["host", "username", "password", "token", "postgres_host"]
        for field in required_fields:
            if field not in config or not config[field]:
                raise ValueError(f"Required field '{field}' is missing or empty in config.json")
                
        return config
    
    @staticmethod
    def create_default_config():
        """Create a default config.json template"""
        template = {
            "host": "your-atscale-host.com",
            "username": "your-username",
            "password": "your-password",
            "token": "your-xmla-token",
            "postgres_host": "your-postgres-host",
            "proxy": "optional-proxy-host",
            "proxyport": "optional-proxy-port",
            "aws.region": "optional-aws-region",
            "aws.secrets-key": "optional-aws-secret",
            "snowflake.archive.account": "optional-snowflake-account",
            "snowflake.archive.warehouse": "optional-snowflake-warehouse",
            "snowflake.archive.database": "optional-snowflake-database",
            "snowflake.archive.schema": "optional-snowflake-schema",
            "snowflake.archive.role": "optional-snowflake-role",
            "snowflake.archive.username": "optional-snowflake-username",
            "snowflake.archive.password": "optional-snowflake-password",
            "snowflake.archive.token": "optional-snowflake-token"
        }
        
        return template

class Constants:
    """Application constants"""
    DOCKER_IMAGE = "rwidjaja/atscale-gatling:latest"
    
    EXECUTORS = [
        "InstallerVerQueryExtractExecutor",
        "CustomQueryExtractExecutor", 
        "QueryExtractExecutor",
        "OpenStepConcurrentSimulationExecutor",
        "ClosedStepConcurrentSimulationExecutor",
        "OpenStepSequentialSimulationExecutor",
        "ClosedStepSequentialSimulationExecutor",
        "ArchiveJdbcToSnowflake",
        "ArchiveXmlaToSnowflake"
    ]
    
    # Directory structure
    WORKING_DIR = "working_dir"
    CONTROL_DIR = "working_dir/control"
    RUN_LOGS_DIR = "working_dir/run_logs"
    CONFIG_DIR = "working_dir/config"
    INGEST_DIR = "working_dir/ingest"
    
    # XMLA Queries
    CATALOG_QUERY = """<?xml version="1.0" encoding="utf-8"?>
    <soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/"
                   xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                   xmlns:xsd="http://www.w3.org/2001/XMLSchema">
      <soap:Body>
        <Execute xmlns="urn:schemas-microsoft-com:xml-analysis">
          <Command>
            <Statement>
                  SELECT [CATALOG_NAME] from $system.DBSCHEMA_CATALOGS
            </Statement>
          </Command>
          <Properties>
            <PropertyList>
              <Catalog>Default</Catalog>
              <Cube>Default</Cube>
            </PropertyList>
          </Properties>
        </Execute>
      </soap:Body>
    </soap:Envelope>"""