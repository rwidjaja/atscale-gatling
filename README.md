# atscale-gatling

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://hub.docker.com/r/rwidjaja/atscale-gatling)

A unified automation framework for running Gatling-based simulations, custom query extraction, data ingestion, and archiving workloads against AtScale or other analytical engines. Supports both GUI and CLI modes, includes multiple workload models (open/closed, sequential/concurrent), and provides optional Docker and Docker-Compose deployment.

Key features:

- Multiple execution modes: GUI, CLI, and auto-detect
- Built-in executors for extract, simulation, and archive workflows
- Open and closed workload models (arrival-rate and concurrent-user)
- Docker-friendly with helper scripts and optional truststore support

📦 Features

- **Multiple Execution Modes:**
  - GUI mode: `--mode gui`
  - CLI mode: `--mode cli`
  - Auto-detect (default): run `python main.py`

- **Executors Included:**
  - `CustomQueryExtractExecutor`
  - `InstallerVerQueryExtractExecutor`
  - `OpenStepConcurrentSimulationExecutor`
  - `ClosedStepConcurrentSimulationExecutor`
  - `OpenStepSequentialSimulationExecutor`
  - `ClosedStepSequentialSimulationExecutor`
  - `ArchiveJdbcToSnowflake`
  - `ArchiveXmlaToSnowflake`

- **Workload Models:**
  - Open (arrival-rate based) simulations
  - Closed (concurrent-user based) simulations
  - Sequential or concurrent looping
  - Fully configurable per run via `config.json` and `systems.properties`

First-run auto setup (runtime layout created under `working_dir/`):

```
working_dir/
  config/systems.properties
  run_logs/
  app_logs/
  queries/
  control/
```

✔ Docker Support

- Local run via Python
- Run completely inside containers
- Helper scripts:
  - `build-docker.sh`
  - `publish-docker.sh`
  - `run-executor.sh`
  - `run-interactive.sh`

Project layout (relevant):

```
atscale-gatling/
├── atscalewrapper/             # Main wrapper package / entrypoint
│   ├── __init__.py
│   ├── core.py
│   ├── gui.py
│   ├── cli.py
│   ├── main.py
│   ├── config.py
│   ├── config.json
│   └── cacerts/
├── ingest/                     # Ingestion / archive helpers
├── lib/                        # Utilities, constants, helpers
├── scripts/                    # Snowflake + other helper scripts
├── config.json.example         # Example configuration
├── example_systems.properties  # Example systems.properties
├── check-dependencies.py       # Diagnose missing requirements
├── dockerfile
├── docker-compose.yml
├── build-docker.sh
├── run-executor.sh
├── run-interactive.sh
├── publish-docker.sh
└── README.md
```

🧰 Prerequisites

Local run:

- `Python 3.8+`
- `Java` (required for running Gatling simulations)
- Optional: Snowflake CLI / JDBC drivers (for archive jobs)

Containerized run:

- `Docker`
- (Optional) `docker-compose`

Additional runtime files often required:

- `root.crt` for Postgres
- `cacerts` directory (if using a truststore) and password `changeit` by default in examples

🚀 Running the Application

Local – Auto mode:

```
python main.py
```

Force GUI mode:

```
python main.py --mode gui
```

Force CLI mode:

```
python main.py --mode cli
```

Example CLI run:

```
python main.py \
  --mode cli \
  --executor CustomQueryExtractExecutor \
  --models "Catalog1,Catalog2"
```

Run from Docker (simple):

```
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  rwidjaja/atscale-gatling:latest \
  CustomQueryExtractExecutor
```

With truststore and PostgreSQL root certificate (example):

```
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  -v $(pwd)/cacerts:/app/cacerts \
  -v $(pwd)/root.crt:/root/.postgresql/root.crt \
  -e JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/app/cacerts -Djavax.net.ssl.trustStorePassword=changeit" \
  -e PGSSLROOTCERT=/root/.postgresql/root.crt \
  rwidjaja/atscale-gatling:latest \
  OpenStepConcurrentSimulationExecutor
```

📄 Configuration

`systems.properties`

Defines:

- JDBC / HTTP / XMLA endpoints
- Credentials
- Snowflake settings (if used)

Example located at: `example_systems.properties`

`config.json`

Configures:

- Executors
- Simulation settings
- Query folders and output formats

See: `config.json.example`

🧪 Executors Overview

| Executor | Description |
|---|---|
| `CustomQueryExtractExecutor` | Run your own SQL files located in `working_dir/queries/` |
| `InstallerVerQueryExtractExecutor` | Query installer/version metadata |
| `OpenStepConcurrentSimulationExecutor` | Open workload, concurrent step-based |
| `ClosedStepConcurrentSimulationExecutor` | Closed workload, concurrent step-based |
| `OpenStepSequentialSimulationExecutor` | Sequential open workload |
| `ClosedStepSequentialSimulationExecutor` | Sequential closed workload |
| `ArchiveJdbcToSnowflake` | Push JDBC resultsets into Snowflake |
| `ArchiveXmlaToSnowflake` | Push XMLA resultsets into Snowflake |

🧩 Stopping a Running Simulation

Create this file to request a graceful shutdown:

```
working_dir/control/stop_simulation
```

Executors watch for the file and will shut down gracefully when detected.

Checking Your Environment

```
python3 check-dependencies.py
```

This verifies:

- Python modules
- Java installation
- Gatling dependencies
- Folder structure and permissions

📌 Notes

- `working_dir/` is auto-generated at runtime — safe to delete/reset.
- Do not commit `config.json` or `systems.properties` (they contain credentials).
- GUI mode is available via the internal wrapper (`atscalewrapper.gui`).
- The wrapper (`atscalewrapper/`) exposes the public execution entrypoints and the unified `main.py`.
- Make helper scripts executable when needed:

```
chmod +x build-docker.sh run-executor.sh run-interactive.sh
```

If you want, I can also:

- Run `python3 check-dependencies.py` locally and report findings
- Create a short CONTRIBUTING or QUICKSTART section for your CI/docker usage

---

© Project maintained by the repository owner
