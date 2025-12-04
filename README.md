# atscale-gatling (wrapped)

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://hub.docker.com/r/rwidjaja/atscale-gatling)

This repository ships the AtScale Gatling tooling. The project now exposes a small wrapper package
`atscalewrapper/` that provides a single, consistent entrypoint: `python main.py`.

## Quick Summary

- Run the program: `python main.py`
- GUI mode: `python main.py --mode gui`
- CLI mode: `python main.py --mode cli`

## Project layout (relevant parts)

atscalewrapper/
├── __init__.py       # Package exports
├── core.py           # Core functionality (factory/helpers)
├── gui.py            # GUI adapter/export
├── cli.py            # CLI adapter/export
├── config.py         # Configuration helper
├── main.py           # Top-level entrypoint (use `python main.py`)
├── config.json       # Optional packaged configuration (edit before run)
└── cacerts           # Optional truststore

Other runtime directories (created by the app):

```
working_dir/
├── config/systems.properties
├── run_logs/
├── app_logs/
└── queries/
```

## ⚡ Quick Start

### Prerequisites

- Docker
- Python 3.7+
- `curl` or `git` (optional)

- Optional: run the included dependency checker to validate your environment:

```bash
python3 check-dependencies.py
```

### Running the app

Use the packaged main entrypoint at the repository root.

- Auto-detect (default) mode:

```bash
python main.py
```

- Force GUI mode:

```bash
python main.py --mode gui
```

- Force CLI mode:

```bash
python main.py --mode cli
```

Example CLI invocation:

```bash
python main.py --mode cli --executor CustomQueryExtractExecutor --models "Catalog1,Catalog2"
```

Note: use `python` or `python3` depending on your environment.
---

## 3️⃣ Build or run Docker image

### Optional: Build locally
```bash
./mvnw clean package -DskipTests
docker build -t rwidjaja/atscale-gatling:latest .
```

### Run with truststore example
```bash
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  -v $(pwd)/cacerts:/app/cacerts \
  -e JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/app/cacerts -Djavax.net.ssl.trustStorePassword=changeit" \
  rwidjaja/atscale-gatling:latest CustomQueryExtractExecutor
```

---

## 🐳 Docker Compose (optional)
Create service definitions and run executors via:

```bash
docker-compose run <service>
```

---

## 🧰 Available Executors
| Executor | Description |
|----------|-------------|
| InstallerVerQueryExtractExecutor | Targeted installer version queries |
| CustomQueryExtractExecutor | Custom SQL query execution |
| OpenStepConcurrentSimulationExecutor | Open concurrency workload |
| ClosedStepConcurrentSimulationExecutor | Closed concurrency workload |
| OpenStepSequentialSimulationExecutor | Sequential open workload |
| ClosedStepSequentialSimulationExecutor | Sequential closed workload |
| ArchiveJdbcToSnowflake | Archive over JDBC |
| ArchiveXmlaToSnowflake | Archive over XMLA |

---

## Simulation Modes: Open vs Closed

Open and Closed simulations represent two common workload models used by the executors above. Briefly:

- **Open (arrival-rate based):** users arrive according to a rate (users per second). Use when modelling an external traffic flow.
- **Closed (concurrent-user based):** a fixed pool of concurrent users is maintained; users loop through tasks. Use when modelling a fixed number of clients consuming the system.

Executor Class | Injection Step Type | Meaning
---|---|---
`OpenStepConcurrentSimulationExecutor` | `AtOnceUsersOpenInjectionStep`, `RampUsersPerSecOpenInjectionStep` | Defines user arrivals per second (traffic flow).
`ClosedStepConcurrentSimulationExecutor` | `ConstantConcurrentUsersClosedInjectionStep`, `IncrementConcurrentUsersClosedInjectionStep` | Defines number of concurrent users (fixed or stepped) for closed workload modelling.


## 📁 Runtime Layout
```
atscale-gatling/
├── atscale-gatling.py
├── config.json
└── working_dir/
    ├── config/systems.properties
    ├── run_logs/
    ├── app_logs/
    └── queries/
```

---

## 📝 Notes & Recommendations
- **Do not commit credentials or generated configs**
  (`config.json` and `systems.properties` must be ignored in Git)
 - **Terminate simulation:** create a file at `working_dir/control/stop_simulation`.
   Executors watch for this file; creating it signals a graceful shutdown of the running simulation.
 - **GUI version available:** `atscale-gatling.py` is the text/interactive generator; the GUI variant is
   `atscale-gatling-gui.py` (windowed interface). Run the GUI with `python3 atscale-gatling-gui.py`.
- Make helper scripts executable:
  ```bash
  chmod +x build-docker.sh run-executor.sh run-interactive.sh
  ```
- Example automation artifacts (optional): `build-docker.sh`, `run-executor.sh`, `docker-compose.yml`

**Dependency Check Script**
- **`check-dependencies.py`**: A small utility included with this distribution to help verify that your development/runtime environment meets the minimum requirements. Run it manually with:

```bash
python3 check-dependencies.py
```

It prints a short report of any missing or incompatible tools and libraries so you can address issues before attempting to run executors or build the Docker image.
