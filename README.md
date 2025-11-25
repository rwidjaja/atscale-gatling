# atscale-gatling

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](./LICENSE)
[![Docker Image](https://img.shields.io/badge/docker-ready-brightgreen.svg)](https://hub.docker.com/r/rwidjaja/atscale-gatling)

## 🚩 Branch Notice
This README covers the **Docker distribution branch**.

For alternative workflows:
- **OneExecutor branch** — Unified JAR + Python selector  
  https://github.com/rwidjaja/atscale-gatling/tree/OneExecutor
- **main branch** — Source & Maven builds  
  https://github.com/rwidjaja/atscale-gatling/

---

## Table of Contents
- [Quick Start](#-quick-start)
- [Prerequisites](#-prerequisites)
- [Dependency Check Script](#dependency-check-script)
- [Download Scripts](#1️⃣-download-scripts)
- [Generate runtime configuration](#2️⃣-generate-runtime-configuration-interactive)
- [Running the scripts](#-running-the-scripts-cli--gui-examples)
- [Build or run Docker image](#3️⃣-build-or-run-docker-image)
- [Available Executors](#-available-executors)
- [Simulation Modes: Open vs Closed](#simulation-modes-open-vs-closed)
- [Runtime Layout](#-runtime-layout)
- [Notes & Recommendations](#-notes--recommendations)
- [Contributing](#contributing)

---

## ⚡ Quick Start

### **Prerequisites**
- Docker
- Python **3.7+** (used by the interactive config generator)
- `curl` or `git`
- Optional: run the included `check-dependencies.py` to validate your local environment before running the scripts. Example:

```bash
python3 check-dependencies.py
```

---

## 1️⃣ Download Scripts

Use `curl` to fetch the interactive script, configuration template, and the dependency check utility:

```bash
curl -O https://raw.githubusercontent.com/rwidjaja/atscale-gatling/DockerBranch/atscale-gatling/atscale-gatling.py
curl -O https://raw.githubusercontent.com/rwidjaja/atscale-gatling/DockerBranch/atscale-gatling/config.json
curl -O https://raw.githubusercontent.com/rwidjaja/atscale-gatling/DockerBranch/atscale-gatling/check-dependencies.py

```

Edit `config.json` and provide the required AtScale / Snowflake credentials.

---

## 2️⃣ Generate runtime configuration (interactive)
Generates `systems.properties` and working directory structure:

```bash
python3 atscale-gatling.py
```

---

## 🧭 Running the scripts (CLI / GUI examples)

Use the hyphenated script filenames `atscale-gatling.py` and `atscale-gatling-gui.py`.

- Auto-detect mode (CLI over SSH):

```bash
python3 atscale-gatling-gui.py
```

- Force GUI mode:

```bash
python3 atscale-gatling.py --mode gui
```

- Force CLI mode:

```bash
python3 atscale-gatling-gui.py --mode cli
```

CLI usage examples that already work:

```bash
python atscale-gatling-gui.py --mode cli --executor QueryExtractExecutor --all-models --follow
python atscale-gatling-gui.py --mode cli --executor CustomQueryExtractExecutor --models "Catalog1,Catalog2"
python atscale-gatling-gui.py --mode cli  # interactive mode
```

Note: `python` or `python3` may be used depending on your environment.
---

## 3️⃣ Build or run Docker image

### Optional: Build locally
```bash
./mvnw clean package -DskipTests
docker build -t rwidjaja/atscale-gatling:latest .
```

### Run an executor
```bash
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  rwidjaja/atscale-gatling:latest QueryExtractExecutor
```

### Run with truststore example
```bash
docker run --rm \
  -v $(pwd)/working_dir:/app/working_dir \
  -v $(pwd)/cacerts:/app/cacerts \
  -e JAVA_TOOL_OPTIONS="-Djavax.net.ssl.trustStore=/app/cacerts -Djavax.net.ssl.trustStorePassword=changeit" \
  rwidjaja/atscale-gatling:latest QueryExtractExecutor
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
| QueryExtractExecutor | Standard workload query extraction |
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
