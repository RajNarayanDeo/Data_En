<div align="center">

# ✈️ Flights Operations Medallion Data Pipeline

### Apache Airflow · Snowflake · Docker · Python · Pandas

<br/>

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.3-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)](https://airflow.apache.org/)
[![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=Snowflake&logoColor=white)](https://snowflake.com/)
[![Python](https://img.shields.io/badge/Python-3.10+-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://python.org/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://docker.com/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?style=for-the-badge&logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![License](https://img.shields.io/badge/License-MIT-green?style=for-the-badge)](LICENSE)

<br/>

> *A production-grade, fully containerized Medallion Architecture data pipeline that ingests real-time global flight data, transforms it through Bronze → Silver → Gold layers, and loads aggregated KPIs into Snowflake every 30 minutes.*

</div>

---

## 📋 Table of Contents

1. [Project Overview](#-project-overview)
2. [Medallion Architecture](#-medallion-architecture)
3. [Tech Stack](#%EF%B8%8F-tech-stack)
4. [Project Structure](#-project-structure)
5. [DAG Workflow](#-dag-workflow)
6. [Code Walkthrough — Every Line Explained](#-code-walkthrough--every-line-explained)
   - [DAG Definition — flight-pipeline.py](#1-dag-definition--flight-pipelinepy)
   - [Bronze Layer — bronze_ingest.py](#2-bronze-layer--bronze_ingestpy)
   - [Silver Layer — silver_transform.py](#3-silver-layer--silver_transformpy)
   - [Gold Layer — gold_aggregate.py](#4-gold-layer--gold_aggregatepy)
   - [Snowflake Load — load_gold_to_snowflake.py](#5-snowflake-load--load_gold_to_snowflakepy)
7. [Docker Setup](#-docker-setup)
8. [Getting Started](#-getting-started)
9. [Snowflake Configuration](#%EF%B8%8F-snowflake-configuration)
10. [Database Schema](#-database-schema)
11. [Example Output](#-example-output-in-snowflake)
12. [Common Issues & Fixes](#-common-issues--fixes)
13. [Future Enhancements](#-future-enhancements)
14. [Key Highlights](#-key-highlights)
15. [Author](#-author)

---

## 🚀 Project Overview

This project implements a **modern data pipeline** using the **Medallion Architecture** (Bronze → Silver → Gold) to process real-time flight data from the [OpenSky Network API](https://opensky-network.org/). The pipeline is orchestrated using **Apache Airflow**, containerized with **Docker**, and loads aggregated insights into **Snowflake** for analytics.

### Why Medallion Architecture?

The Medallion pattern progressively improves data quality at each layer — raw data is never deleted, transformations are traceable, and analytics-ready aggregations are cleanly separated from ingestion logic.

```
Raw API Data → Bronze (as-is) → Silver (clean) → Gold (aggregated) → Snowflake (analytics)
```

### System Design Goals

| Goal | How It's Achieved |
|------|-------------------|
| **Modular** | Each layer is an independent Python script and Airflow task |
| **Scalable** | Runs every 30 min; can be parallelised across airports/routes |
| **Production-ready** | Retries, scheduling, XCom-based task communication, MERGE upserts |
| **Traceable** | Raw JSON is always preserved in `/bronze`; no destructive operations |
| **Idempotent** | Snowflake `MERGE` statement prevents duplicate records on reruns |

---

## 🏗️ Medallion Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                    MEDALLION DATA PIPELINE                            │
│                                                                        │
│   ┌─────────────┐                                                     │
│   │ OpenSky API │  ← Real-time global flight state vectors            │
│   └──────┬──────┘                                                     │
│          │  HTTP GET                                                   │
│          ▼                                                             │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  🥉 BRONZE  /data/bronze/flights_YYYYMMDDHHMMSS.json        │    │
│   │  Raw JSON — no transformation, no filtering, never modified  │    │
│   └──────────────────────────┬──────────────────────────────────┘    │
│                               │                                        │
│                               ▼                                        │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  🥈 SILVER  /data/silver/flights_silver_YYYYMMDD.csv        │    │
│   │  Cleaned DataFrame: renamed cols, filtered fields, typed     │    │
│   └──────────────────────────┬──────────────────────────────────┘    │
│                               │                                        │
│                               ▼                                        │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  🥇 GOLD    /data/gold/flights_gold_YYYYMMDD.csv            │    │
│   │  Aggregated KPIs: by country, avg velocity, ground count     │    │
│   └──────────────────────────┬──────────────────────────────────┘    │
│                               │                                        │
│                               ▼                                        │
│   ┌─────────────────────────────────────────────────────────────┐    │
│   │  ❄️  SNOWFLAKE  →  FLIGHT_KPIS table (MERGE upsert)         │    │
│   │  Analytics-ready, deduplicated, time-windowed KPIs           │    │
│   └─────────────────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────────────────┘
```

### Layer Descriptions

| Layer | Location | Format | Purpose |
|-------|----------|--------|---------|
| 🥉 **Bronze** | `/data/bronze/` | JSON | Raw API ingestion — stored as-is, never modified |
| 🥈 **Silver** | `/data/silver/` | CSV | Cleaned, renamed, and filtered structured data |
| 🥇 **Gold** | `/data/gold/` | CSV | Aggregated KPIs grouped by country |
| ❄️ **Snowflake** | `FLIGHT_KPIS` table | SQL Table | Final analytics destination with MERGE upserts |

---

## ⚙️ Tech Stack

| Category | Technology | Version | Role |
|----------|-----------|---------|------|
| **Orchestration** | Apache Airflow | 2.9.3 | DAG scheduling, task management, retries |
| **Containerization** | Docker + Docker Compose | Latest | Reproducible environment |
| **Data Processing** | Python + Pandas | 3.10+ | Transformation, aggregation, file I/O |
| **Data Source** | OpenSky Network API | REST | Real-time global flight state vectors |
| **Data Warehouse** | Snowflake | Cloud | Analytics storage with MERGE upserts |
| **Metadata Store** | PostgreSQL | 13 | Airflow's internal metadata database |
| **File Storage** | Local Filesystem | — | Intermediate layer files at `/opt/airflow/data` |

---

## 📁 Project Structure

```
Flight_Operations_AIRFLOW/
│
├── dags/
│   └── flight-pipeline.py            # Master Airflow DAG: task definitions & wiring
│
├── scripts/
│   ├── bronze_ingest.py              # Layer 1: Fetch raw API data → save as JSON
│   ├── silver_transform.py           # Layer 2: Clean & structure → save as CSV
│   ├── gold_aggregate.py             # Layer 3: Compute KPIs → save aggregated CSV
│   └── load_gold_to_snowflake.py     # Layer 4: MERGE gold CSV into Snowflake
│
├── data/
│   ├── bronze/                       # Raw JSON files (timestamped, never overwritten)
│   ├── silver/                       # Daily processed CSV files
│   └── gold/                         # Daily aggregated KPI CSV files
│
├── logs/                             # Airflow task execution logs
├── plugins/                          # Optional custom Airflow plugins
│
├── docker-compose.yaml               # Container definitions
├── .env                              # Environment variables (not committed to Git)
├── .env.example                      # Template for .env
└── README.md                         # This file
```

> **Convention:** Bronze files use `YYYYMMDDHHMMSS` (one per run), Silver/Gold use `YYYYMMDD` (daily).

---

## 🔄 DAG Workflow

**DAG ID:** `flights_ops_medallion_pipe`
**Schedule:** Every 30 minutes — `*/30 * * * *`

### Task Dependency Graph

```
[ bronze_ingest ]
       │  pushes → XCom: bronze_path
       ▼
[ silver_transform ]
       │  pushes → XCom: silver_path
       ▼
[ gold_aggregate ]
       │  pushes → XCom: gold_path
       ▼
[ load_gold_to_snowflake ]
```

Each task passes its output file path to the next via **Airflow XCom**, making every task decoupled and independently testable.

### Task Summary

| # | Task ID | Script | Input | Output |
|---|---------|--------|-------|--------|
| 1 | `bronze_ingest` | `bronze_ingest.py` | OpenSky REST API | Raw JSON file |
| 2 | `silver_transform` | `silver_transform.py` | Bronze path (XCom) | Cleaned CSV |
| 3 | `gold_aggregate` | `gold_aggregate.py` | Silver path (XCom) | Aggregated CSV |
| 4 | `load_gold_to_snowflake` | `load_gold_to_snowflake.py` | Gold path (XCom) | Snowflake `FLIGHT_KPIS` |

---

## 🔬 Code Walkthrough — Every Line Explained

### 1. DAG Definition — `flight-pipeline.py`

This is the **master orchestrator file**. It configures the schedule, task defaults, and wires all four scripts together into a directed pipeline.

```python
# ═══════════════════════════════════════════════════════════════════
# FILE: dags/flight-pipeline.py
# PURPOSE: Defines the Airflow DAG that runs every 30 minutes.
#          Links the four Medallion tasks in strict sequence.
# ═══════════════════════════════════════════════════════════════════

import sys
# sys: Python's built-in system module.
# Gives access to the Python interpreter's runtime state and configuration.

sys.path.insert(0, '/opt/airflow')
# sys.path: the list of directories Python searches when you write `import x`.
# insert(0, ...): adds /opt/airflow at position 0 (highest priority) of that list.
# Why: Inside Docker, `scripts/` lives at /opt/airflow/scripts.
# Without this line, `from scripts.bronze_ingest import ...` would fail with
# ModuleNotFoundError because Python wouldn't know where to look.

from datetime import datetime, timedelta
# datetime: a class representing a specific point in time (year, month, day, etc.).
# timedelta: a class representing a duration — used for retry_delay and date math.

from airflow import DAG
# DAG (Directed Acyclic Graph): the core Airflow container class.
# "Directed" = tasks flow in one direction (no loops).
# "Acyclic" = a task cannot depend on itself (no cycles).
# All tasks, their schedules, and their relationships are defined inside a DAG.

from airflow.operators.python import PythonOperator
# PythonOperator: an Airflow operator that executes any Python callable as a task.
# It is the most flexible operator — perfect for calling our custom scripts.


# ───────────────────────────────────────────────────────────────────
# DEFAULT ARGUMENTS
# A dictionary of settings inherited by every task in the DAG.
# Individual tasks can override these, but they serve as pipeline-wide defaults.
# ───────────────────────────────────────────────────────────────────
default_args = {
    'owner': 'airflow',
    # Ownership label visible in the Airflow UI.
    # Used for team-based access control in multi-user Airflow environments.

    'depends_on_past': False,
    # False = each pipeline run is completely independent.
    # True would block a run if the SAME task in the previous run had failed —
    # risky for a 30-minute schedule where failures would cascade forward.

    'email_on_failure': False,
    # Set True + add 'email': ['you@company.com'] for production alerting.
    # False is fine for development.

    'email_on_retry': False,
    # Prevents inbox flooding when transient errors cause retries.

    'retries': 1,
    # If a task fails, Airflow will retry it once automatically.
    # Total attempts = 1 (original) + 1 (retry) = 2 tries per task.

    'retry_delay': timedelta(minutes=5),
    # Waits 5 minutes between the failed attempt and the retry.
    # Useful when failures are caused by temporary API rate limiting or network blips.
}


# ───────────────────────────────────────────────────────────────────
# DAG OBJECT
# The `with DAG(...) as dag:` context manager creates the DAG.
# All PythonOperators defined inside belong to this DAG.
# ───────────────────────────────────────────────────────────────────
with DAG(
    'flights_ops_medallion_pipe',
    # Unique identifier for this DAG — must be globally unique in your Airflow instance.
    # Shown in the DAG list, logs, and URLs in the Airflow web UI.

    default_args=default_args,
    # Applies the dictionary above to all tasks inside this DAG.

    description='Medallion pipeline: Bronze → Silver → Gold → Snowflake',
    # Human-readable description shown in the Airflow UI DAG detail page.

    schedule_interval='*/30 * * * *',
    # CRON expression — triggers the DAG on every 30-minute mark.
    # Format: minute  hour  day-of-month  month  day-of-week
    # */30 = "every 30 minutes" (0, 30 past each hour).
    # Equivalent Airflow presets: '@hourly', '@daily', '@weekly'.

    start_date=datetime(2024, 1, 1),
    # The earliest date the scheduler considers for scheduling runs.
    # With catchup=False below, historical runs between this date and now
    # are NOT triggered — only future runs are scheduled.

    catchup=False,
    # False = don't backfill all missed 30-minute windows since start_date.
    # True would trigger potentially thousands of runs — dangerous on first enable.

    tags=['flights', 'medallion', 'snowflake'],
    # Optional labels for filtering DAGs in the Airflow UI search bar.

) as dag:

    # Import task functions from scripts/
    from scripts.bronze_ingest import ingest_bronze
    from scripts.silver_transform import transform_silver
    from scripts.gold_aggregate import aggregate_gold
    from scripts.load_gold_to_snowflake import load_to_snowflake
    # Imports are placed INSIDE the `with` block so that the sys.path change
    # above is guaranteed to have taken effect before Python resolves these paths.

    # ── Task 1: Bronze ────────────────────────────────────────────
    bronze_task = PythonOperator(
        task_id='bronze_ingest',
        # Unique task identifier within this DAG.
        # Used in logs, XCom references, and the Airflow Graph View.

        python_callable=ingest_bronze,
        # The function to call when this task executes.
        # Airflow calls: ingest_bronze(**context)

        provide_context=True,
        # Passes Airflow's runtime context as **kwargs into the function.
        # Gives access to: kwargs['ti'] (Task Instance), kwargs['execution_date'], etc.
    )

    # ── Task 2: Silver ────────────────────────────────────────────
    silver_task = PythonOperator(
        task_id='silver_transform',
        python_callable=transform_silver,
        provide_context=True,
    )

    # ── Task 3: Gold ──────────────────────────────────────────────
    gold_task = PythonOperator(
        task_id='gold_aggregate',
        python_callable=aggregate_gold,
        provide_context=True,
    )

    # ── Task 4: Snowflake Load ────────────────────────────────────
    snowflake_task = PythonOperator(
        task_id='load_gold_to_snowflake',
        python_callable=load_to_snowflake,
        provide_context=True,
    )

    # ── Set Dependency Chain ──────────────────────────────────────
    bronze_task >> silver_task >> gold_task >> snowflake_task
    # >> is Airflow's bitshift "set downstream" operator.
    # Reads: "run bronze, THEN silver, THEN gold, THEN snowflake."
    # If bronze_task fails: silver, gold, snowflake are marked UPSTREAM_FAILED.
    # Airflow guarantees strict sequential ordering with this syntax.
```

---

### 2. Bronze Layer — `bronze_ingest.py`

The Bronze layer is the **raw ingestion layer**. It fetches live flight state data from OpenSky and saves it unchanged to disk — no filtering, no transformation. This preserves the source of truth for debugging and reprocessing.

```python
# ═══════════════════════════════════════════════════════════════════
# FILE: scripts/bronze_ingest.py
# PURPOSE: Calls the OpenSky API for the current 30-min window,
#          saves the raw JSON as-is, and shares the path via XCom.
# ═══════════════════════════════════════════════════════════════════

import requests
# requests: the standard Python HTTP library for making API calls.
# Handles connection pooling, headers, query params, and response parsing.

import json
# json: Python's built-in JSON library.
# json.dump(obj, file) → writes Python object to JSON file.
# json.load(file) → reads JSON file back into Python object.

import os
# os: built-in module for interacting with the operating system.
# Used here to create directories and build file paths.

import logging
# logging: Python's built-in structured log system.
# Preferred over print() — logs include timestamps and severity levels,
# and they appear in Airflow's Task Log viewer in the UI.

from datetime import datetime, timedelta

logger = logging.getLogger(__name__)
# Creates a named logger for this module ('scripts.bronze_ingest').
# __name__ resolves to the module path at runtime.
# The name appears in Airflow logs, making it easy to trace which file logged what.


def ingest_bronze(**kwargs):
    """
    Fetches real-time flight state vectors from the OpenSky Network API.
    Saves the raw response as a timestamped JSON file in /data/bronze/.
    Pushes the saved file path to XCom for the next task.

    OpenSky 'state vectors': one record per aircraft tracked by the global
    ADS-B receiver network. Each vector contains position, speed, and status.

    **kwargs: Airflow injects the task execution context here automatically.
    """

    # ── Step 1: Define the 30-minute API time window ─────────────
    execution_date = kwargs['execution_date']
    # execution_date: the logical run time of this DAG instance (a datetime).
    # For a pipeline scheduled at 06:30, execution_date = 2024-01-15 06:30:00.
    # Using execution_date (not datetime.now()) makes reruns fetch the same window —
    # this property is called "idempotency" and is critical for reliable pipelines.

    end_time = int(execution_date.timestamp())
    # .timestamp(): converts Python datetime → Unix epoch float (seconds since 1970-01-01).
    # int(): removes the decimal portion.
    # Result: 1705289400  ← this is what the OpenSky API expects as a time parameter.

    begin_time = int((execution_date - timedelta(minutes=30)).timestamp())
    # Subtracts 30 minutes to get the start of the window.
    # timedelta(minutes=30): creates a duration object representing 30 minutes.
    # Subtracting it from execution_date shifts the time back by 30 minutes.
    # This ensures we capture all flights active in [execution_date-30min, execution_date].

    logger.info(f"Fetching OpenSky data: window {begin_time} → {end_time}")

    # ── Step 2: Call the OpenSky REST API ────────────────────────
    url = "https://opensky-network.org/api/states/all"
    # /api/states/all: returns all currently tracked aircraft worldwide.
    # Free tier: no authentication needed, ~400 requests/day rate limit.
    # With credentials, rate limits are much higher.

    params = {'time': end_time}
    # Query parameters appended to the URL: ?time=1705289400
    # OpenSky returns the aircraft state snapshot at this Unix timestamp.

    response = requests.get(url, params=params, timeout=60)
    # requests.get(): sends HTTP GET request.
    # params=: URL-encodes the dict as query string parameters automatically.
    # timeout=60: raises Timeout exception if the server doesn't respond in 60 seconds.
    #             Always set timeouts — without them, a hanging API call can stall
    #             the Airflow task indefinitely, blocking the entire scheduler.

    response.raise_for_status()
    # Checks the HTTP status code.
    # 200 OK → passes silently.
    # 4xx/5xx (e.g., 429 Too Many Requests, 500 Server Error) → raises HTTPError.
    # The raised exception propagates up to Airflow, which marks the task FAILED
    # and triggers the retry defined in default_args (retries=1, delay=5min).

    data = response.json()
    # .json(): parses the JSON response body into a Python dict.
    # OpenSky response shape:
    # {
    #   "time": 1705289400,
    #   "states": [
    #     ["icao24", "callsign", "origin_country", ...],  ← one list per aircraft
    #     ...
    #   ]
    # }

    # ── Step 3: Ensure output directory exists ───────────────────
    bronze_dir = '/opt/airflow/data/bronze'
    # The directory path inside the Docker container where Bronze files are stored.
    # This maps to ./data/bronze/ on your host via the Docker volume mount.

    os.makedirs(bronze_dir, exist_ok=True)
    # os.makedirs(): creates the directory and all missing parent directories.
    # exist_ok=True: suppresses FileExistsError if the directory already exists.
    # Without this, every run after the first would crash on this line.

    # ── Step 4: Generate a timestamped file path ─────────────────
    timestamp = execution_date.strftime('%Y%m%d%H%M%S')
    # strftime (string format time): converts datetime to a formatted string.
    # '%Y' = 4-digit year   '%m' = 2-digit month  '%d' = 2-digit day
    # '%H' = hour (24h)     '%M' = minutes        '%S' = seconds
    # Result: '20240115063000' — compact, sortable, collision-free filename suffix.

    bronze_path = f'{bronze_dir}/flights_{timestamp}.json'
    # f-string: Python 3.6+ inline string interpolation.
    # Result: '/opt/airflow/data/bronze/flights_20240115063000.json'
    # Each run gets a unique file — raw data is never overwritten.

    # ── Step 5: Write raw JSON to disk ───────────────────────────
    with open(bronze_path, 'w') as f:
        json.dump(data, f)
    # open(path, 'w'): opens/creates file for writing in text mode.
    # json.dump(data, f): serialises Python object `data` directly into the file.
    #                     More memory-efficient than json.dumps() + f.write().
    # The `with` block is a context manager — it closes the file automatically
    # when the block exits, even if an exception occurs inside it.

    state_count = len(data.get('states') or [])
    logger.info(f"Bronze saved: {bronze_path} | {state_count} flight states")
    # data.get('states') or []: handles the case where 'states' is None (empty API response).
    # The `or []` ensures len() receives a list, not None (which would raise TypeError).

    # ── Step 6: Share file path with downstream tasks via XCom ───
    kwargs['ti'].xcom_push(key='bronze_path', value=bronze_path)
    # ti = Task Instance: the Airflow runtime object for this specific task execution.
    # xcom_push(key, value): stores a named value in Airflow's XCom store (backed by Postgres).
    # key='bronze_path': the label — downstream tasks use this key to retrieve the value.
    # value=bronze_path: the string file path being shared.
    # The Silver task will call ti.xcom_pull(task_ids='bronze_ingest', key='bronze_path').

    return bronze_path
    # Returning a value also stores it in XCom automatically under key='return_value'.
    # The explicit xcom_push above with a named key is more readable and less error-prone.
```

---

### 3. Silver Layer — `silver_transform.py`

The Silver layer **cleans and structures** the raw Bronze data. It selects only the four fields needed for analytics, renames them, handles null values, and outputs a well-typed CSV.

```python
# ═══════════════════════════════════════════════════════════════════
# FILE: scripts/silver_transform.py
# PURPOSE: Reads Bronze JSON → builds a Pandas DataFrame → selects
#          and cleans only the required fields → saves a Silver CSV.
# ═══════════════════════════════════════════════════════════════════

import pandas as pd
# Pandas: the core Python library for tabular data manipulation.
# The DataFrame (2D table with named columns) is this pipeline's primary structure.

import json, os, logging
from datetime import datetime

logger = logging.getLogger(__name__)


# OpenSky state vectors are ordered lists — not dicts with named keys.
# This mapping translates list positions into human-readable column names.
OPENSKY_COLUMNS = [
    'icao24',           # [0]  ICAO 24-bit hex transponder address (unique per aircraft)
    'callsign',         # [1]  Airline callsign / flight number (e.g., 'AI101   ')
    'origin_country',   # [2]  Country of aircraft registration
    'time_position',    # [3]  Unix timestamp of last position update
    'last_contact',     # [4]  Unix timestamp of last ADS-B signal received
    'longitude',        # [5]  WGS-84 longitude (decimal degrees)
    'latitude',         # [6]  WGS-84 latitude (decimal degrees)
    'baro_altitude',    # [7]  Barometric altitude (metres)
    'on_ground',        # [8]  Boolean: True if the aircraft is on the ground
    'velocity',         # [9]  Ground speed (metres per second)
    'true_track',       # [10] Heading clockwise from North (decimal degrees)
    'vertical_rate',    # [11] Climb/descent rate (m/s; positive = climbing)
    'sensors',          # [12] Receiver IDs that contributed data
    'geo_altitude',     # [13] GPS-based altitude (metres)
    'squawk',           # [14] Transponder squawk code
    'spi',              # [15] Special purpose indicator flag
    'position_source',  # [16] 0=ADS-B, 1=ASTERIX, 2=MLAT
]
# Storing this as a module-level constant makes it easy to update if
# OpenSky ever adds or reorders fields in their API response.


def transform_silver(**kwargs):
    """
    Transforms the Bronze raw JSON into a cleaned Silver CSV.
    Selects: icao24, origin_country, velocity, on_ground.
    These four fields are all that's needed for Gold-layer KPI aggregation.
    """

    ti = kwargs['ti']
    # ti = Task Instance: the Airflow object for this specific task execution.
    # Provides access to XCom (inter-task communication) and task metadata.

    # ── Step 1: Pull Bronze file path from XCom ──────────────────
    bronze_path = ti.xcom_pull(
        task_ids='bronze_ingest',
        # task_ids: which task's XCom to read. Must match the upstream task_id exactly.

        key='bronze_path'
        # key: the label used in the bronze_ingest xcom_push call. Must match exactly.
    )

    if not bronze_path:
        raise ValueError("XCom key 'bronze_path' not found. Did bronze_ingest complete?")
    # Defensive check: xcom_pull returns None if the key doesn't exist.
    # Raising a clear ValueError here gives a meaningful error message in Airflow logs
    # instead of a confusing AttributeError or FileNotFoundError further down.

    # ── Step 2: Load the raw JSON file ───────────────────────────
    with open(bronze_path, 'r') as f:
        raw_data = json.load(f)
    # open(path, 'r'): opens the file in text read mode.
    # json.load(f): parses the file's JSON content into a Python dict.

    states = raw_data.get('states', [])
    # raw_data is: {"time": 1705289400, "states": [[...], [...], ...]}
    # .get('states', []): safely retrieves 'states', returning [] if the key is absent.
    # Prevents TypeError when OpenSky returns an empty or malformed response.

    if not states:
        logger.warning("No flight states found. Silver transform skipped.")
        return
    # Empty data guard. Returns early (task = SUCCESS, no CSV written).
    # Downstream tasks will need to handle a missing/empty Silver path gracefully.

    # ── Step 3: Build a Pandas DataFrame ─────────────────────────
    df = pd.DataFrame(states, columns=OPENSKY_COLUMNS)
    # pd.DataFrame(data, columns=[...]): creates a 2D table.
    # states = [[...], [...], ...] → each inner list becomes one row.
    # columns=OPENSKY_COLUMNS: assigns the positional name to each column.
    # Without columns=, column names would be 0, 1, 2... (useless integers).

    logger.info(f"DataFrame built: {len(df)} rows × {len(df.columns)} columns")

    # ── Step 4: Select only the fields needed downstream ─────────
    df = df[['icao24', 'origin_country', 'velocity', 'on_ground']]
    # df[[...]]: subset the DataFrame to only the listed column names.
    # Discards the 13 other columns (latitude, longitude, altitude, etc.)
    # that are not used in the Gold aggregation.
    # Reducing columns at this stage shrinks file size and speeds up downstream work.

    # ── Step 5: Drop rows with missing country ───────────────────
    df = df.dropna(subset=['origin_country'])
    # dropna(subset=[...]): removes rows where any listed column value is NaN/None.
    # origin_country is our GROUP BY key in Gold. Rows without it would create
    # a meaningless 'unknown' group that pollutes country-level analytics.

    # ── Step 6: Cast velocity to numeric ─────────────────────────
    df['velocity'] = pd.to_numeric(df['velocity'], errors='coerce')
    # pd.to_numeric(): converts a Series to numeric type (float or int).
    # errors='coerce': invalid values (e.g., None, 'N/A') become NaN instead of raising.
    # This is safer than df['velocity'].astype(float), which crashes on bad values.
    # NaN velocities are automatically excluded from mean() in the Gold layer.

    # ── Step 7: Cast on_ground to boolean ────────────────────────
    df['on_ground'] = df['on_ground'].astype(bool)
    # .astype(bool): ensures the column is typed as Python boolean (True/False).
    # OpenSky returns on_ground as True/False, but this guarantees clean typing
    # (e.g., prevents 1/0 integers from confusing the Gold summation logic).

    # ── Step 8: Add ingestion date metadata ──────────────────────
    df['ingestion_date'] = datetime.utcnow().strftime('%Y-%m-%d')
    # Adds a column recording when this silver file was created.
    # datetime.utcnow(): current UTC time — consistent regardless of server timezone.
    # '%Y-%m-%d': ISO 8601 date format ('2024-01-15').
    # Useful for incremental loading and partition-based analytics later.

    # ── Step 9: Save the Silver CSV ──────────────────────────────
    silver_dir = '/opt/airflow/data/silver'
    os.makedirs(silver_dir, exist_ok=True)

    date_str = kwargs['execution_date'].strftime('%Y%m%d')
    silver_path = f'{silver_dir}/flights_silver_{date_str}.csv'

    df.to_csv(silver_path, index=False)
    # df.to_csv(path, index=False): writes DataFrame to CSV.
    # index=False: omits the auto-generated DataFrame row numbers (0, 1, 2...).
    # Without index=False, an unnamed 'Unnamed: 0' column appears in the output.

    logger.info(f"Silver saved: {silver_path} ({len(df)} rows)")

    ti.xcom_push(key='silver_path', value=silver_path)
    return silver_path
```

---

### 4. Gold Layer — `gold_aggregate.py`

The Gold layer performs **business-level aggregation**. It groups the cleaned Silver data by `origin_country` and computes three KPIs that become the final Snowflake records.

```python
# ═══════════════════════════════════════════════════════════════════
# FILE: scripts/gold_aggregate.py
# PURPOSE: Reads Silver CSV → groups by origin_country → computes
#          total_flights, avg_velocity, on_ground KPIs → saves Gold CSV.
# ═══════════════════════════════════════════════════════════════════

import pandas as pd
import os, logging
from datetime import datetime

logger = logging.getLogger(__name__)


def aggregate_gold(**kwargs):
    """
    Aggregates Silver flight data into country-level KPI metrics.

    Output columns:
      origin_country  — country name (group-by key)
      total_flights   — count of tracked aircraft from that country
      avg_velocity    — mean ground speed in m/s
      on_ground       — count of aircraft currently landed
      window_start    — DAG execution timestamp (used in Snowflake MERGE key)
    """

    ti = kwargs['ti']

    # ── Step 1: Pull Silver path from XCom ───────────────────────
    silver_path = ti.xcom_pull(task_ids='silver_transform', key='silver_path')

    if not silver_path or not os.path.exists(silver_path):
        raise FileNotFoundError(f"Silver file missing: {silver_path}")
    # os.path.exists(): checks that the file actually exists on disk.
    # Double validation: XCom may have a path, but the file may not have been written
    # due to an empty API response guard in the Silver task. Raise a clear error here.

    # ── Step 2: Load Silver CSV ───────────────────────────────────
    df = pd.read_csv(silver_path)
    # pd.read_csv(): reads a CSV file into a Pandas DataFrame.
    # Automatically infers column names from the header row and data types from values.

    logger.info(f"Aggregating {len(df)} silver records")

    # ── Step 3: GroupBy aggregation — the core KPI computation ───
    gold_df = df.groupby('origin_country').agg(
        total_flights=('icao24', 'count'),
        # Named aggregation syntax: result_col=(source_col, aggregation_function)
        # ('icao24', 'count'): counts non-null values in the icao24 column per group.
        # Since icao24 is the aircraft identifier, this = number of flights per country.

        avg_velocity=('velocity', 'mean'),
        # ('velocity', 'mean'): calculates the arithmetic average of velocity per group.
        # NaN values in velocity are automatically excluded from the mean calculation.
        # Result: average ground speed (m/s) for all flights from each country.

        on_ground=('on_ground', 'sum'),
        # ('on_ground', 'sum'): sums the boolean on_ground column per group.
        # Python/NumPy treat True=1 and False=0 in arithmetic, so sum() = count of True.
        # Result: number of aircraft from each country currently on the ground.

    ).reset_index()
    # .groupby('origin_country'): creates sub-groups, one per unique country name.
    # .agg(...): applies the named aggregation functions to each group simultaneously.
    # .reset_index(): after groupby, 'origin_country' becomes the DataFrame index.
    #                 reset_index() promotes it back to a regular column.
    #                 This gives a flat table structure suitable for CSV and SQL.

    # ── Step 4: Round and type-cast KPI values ───────────────────
    gold_df['avg_velocity'] = gold_df['avg_velocity'].round(2)
    # .round(2): rounds float values to 2 decimal places.
    # 230.4567891 → 230.46. Cleaner for display in Snowflake dashboards.

    gold_df['on_ground'] = gold_df['on_ground'].astype(int)
    # .astype(int): converts float (40.0) to integer (40).
    # sum() of booleans produces a float in Pandas; int is more appropriate
    # for a count metric and matches the INTEGER column type in Snowflake.

    # ── Step 5: Add execution window timestamp ───────────────────
    gold_df['window_start'] = kwargs['execution_date'].strftime('%Y-%m-%d %H:%M:%S')
    # Adds a column that records WHICH 30-minute window this Gold data covers.
    # '%Y-%m-%d %H:%M:%S': ISO 8601 datetime — '2024-01-15 06:30:00'
    # This is the KEY field in the Snowflake MERGE statement:
    #   ON (target.WINDOW_START = source.WINDOW_START AND target.ORIGIN_COUNTRY = ...)
    # It enables idempotent upserts: same window rerun → update rows, not duplicates.

    # ── Step 6: Sort by activity for readability ─────────────────
    gold_df = gold_df.sort_values('total_flights', ascending=False)
    # Sorts countries by flight count, most active first.
    # Makes manual inspection of the CSV and Snowflake output intuitive.

    # ── Step 7: Save Gold CSV ────────────────────────────────────
    gold_dir = '/opt/airflow/data/gold'
    os.makedirs(gold_dir, exist_ok=True)
    date_str = kwargs['execution_date'].strftime('%Y%m%d')
    gold_path = f'{gold_dir}/flights_gold_{date_str}.csv'
    gold_df.to_csv(gold_path, index=False)

    logger.info(f"Gold saved: {gold_path} | {len(gold_df)} countries aggregated")

    ti.xcom_push(key='gold_path', value=gold_path)
    return gold_path
```

---

### 5. Snowflake Load — `load_gold_to_snowflake.py`

The final task **upserts Gold KPIs into Snowflake** using a SQL `MERGE` statement. Re-running the same DAG window will update existing records rather than inserting duplicates — ensuring the pipeline is fully **idempotent**.

```python
# ═══════════════════════════════════════════════════════════════════
# FILE: scripts/load_gold_to_snowflake.py
# PURPOSE: Reads Gold CSV → connects to Snowflake via Airflow Hook →
#          uses MERGE SQL to upsert each row into FLIGHT_KPIS table.
# ═══════════════════════════════════════════════════════════════════

import pandas as pd
import logging
from airflow.hooks.base import BaseHook
# BaseHook: Airflow's base class for reading external service connections.
# It retrieves credentials from Airflow's Connection Manager (Admin → Connections)
# so that passwords are NEVER stored in source code or environment variables.

import snowflake.connector
# The official Snowflake Python connector library.
# pip install snowflake-connector-python
# Handles session management, SQL execution, and result fetching.

logger = logging.getLogger(__name__)


def load_to_snowflake(**kwargs):
    """
    Reads the Gold CSV and MERGE-upserts each country KPI row into
    Snowflake's FLIGHT_KPIS table.

    MERGE logic:
      - If a row with the same (window_start, origin_country) exists → UPDATE
      - If no matching row exists → INSERT
    This prevents duplicate rows when the same pipeline window is rerun.
    """

    ti = kwargs['ti']

    # ── Step 1: Pull Gold file path from XCom ────────────────────
    gold_path = ti.xcom_pull(task_ids='gold_aggregate', key='gold_path')
    logger.info(f"Loading Gold file: {gold_path}")

    # ── Step 2: Load Gold CSV into Pandas ────────────────────────
    df = pd.read_csv(gold_path)

    if df.empty:
        logger.warning("Gold DataFrame is empty. Skipping Snowflake load.")
        return
    # Cost guard: avoids opening a Snowflake warehouse session for zero rows.
    # Snowflake warehouses bill per second — no-op runs should be skipped.

    # ── Step 3: Read Snowflake credentials from Airflow ───────────
    conn = BaseHook.get_connection('flight_snowflake')
    # get_connection('flight_snowflake'): looks up the connection by its ID
    # in Airflow's Connection store (configured in Admin → Connections UI).
    # Returns a Connection object with:
    #   conn.login      → Snowflake username
    #   conn.password   → Snowflake password (stored encrypted in Airflow's DB)
    #   conn.host       → account identifier (e.g., 'abc12345.ap-southeast-1')
    #   conn.schema     → Snowflake schema name
    #   conn.extra_dejson → parsed JSON from the "Extra" field

    extra = conn.extra_dejson
    # extra_dejson: parses the "Extra" field from the Airflow Connection.
    # The Extra field (JSON format) stores Snowflake-specific parameters
    # not covered by standard connection fields:
    # {"warehouse": "COMPUTE_WH", "database": "FLIGHT_DB", "role": "SYSADMIN"}

    # ── Step 4: Open a Snowflake connection ───────────────────────
    sf_conn = snowflake.connector.connect(
        user=conn.login,
        # Snowflake username from the Airflow Connection

        password=conn.password,
        # Snowflake password (decrypted by Airflow, never visible in plain text)

        account=conn.host,
        # Snowflake account identifier — the subdomain of your Snowflake URL.
        # URL: https://abc12345.ap-southeast-1.snowflakecomputing.com
        # account: 'abc12345.ap-southeast-1'

        warehouse=extra.get('warehouse', 'COMPUTE_WH'),
        # The virtual compute cluster to execute SQL on.
        # .get('warehouse', 'COMPUTE_WH'): uses 'COMPUTE_WH' as default fallback.

        database=extra.get('database'),
        # The Snowflake database containing the FLIGHT_KPIS table.

        schema=conn.schema,
        # The schema within the database (e.g., PUBLIC or RAW).

        role=extra.get('role'),
        # Optional Snowflake RBAC role. None if not configured.
    )

    cursor = sf_conn.cursor()
    # cursor(): creates a database cursor — the object used to send SQL to Snowflake
    # and retrieve results. One cursor per logical unit of work is best practice.

    # ── Step 5: Ensure target table exists ───────────────────────
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS FLIGHT_KPIS (
            WINDOW_START     TIMESTAMP,
            ORIGIN_COUNTRY   VARCHAR(100),
            TOTAL_FLIGHTS    INTEGER,
            AVG_VELOCITY     FLOAT,
            ON_GROUND        INTEGER,
            PRIMARY KEY (WINDOW_START, ORIGIN_COUNTRY)
        )
    """)
    # CREATE TABLE IF NOT EXISTS: only creates the table if it doesn't already exist.
    # Safe to run on every DAG run — has no effect if the table is already there.
    # PRIMARY KEY (WINDOW_START, ORIGIN_COUNTRY): composite key.
    # The MERGE statement uses this composite key to match existing rows.
    # It ensures one row per (time window + country) combination — no duplicates.

    # ── Step 6: MERGE upsert — one row at a time ─────────────────
    for _, row in df.iterrows():
        # df.iterrows(): yields (index, Series) tuples for each DataFrame row.
        # _: the row index (discarded — Python convention for unused variables).
        # row: a Pandas Series; access values as row['column_name'].
        # For Gold data (~200 countries max), iterrows() is fast enough.

        merge_sql = f"""
            MERGE INTO FLIGHT_KPIS AS target

            USING (
                SELECT
                    '{row['window_start']}'::TIMESTAMP   AS WINDOW_START,
                    -- ::TIMESTAMP: Snowflake cast operator (like SQL CAST(x AS TIMESTAMP)).
                    -- Converts the Python string '2024-01-15 06:30:00' → Snowflake TIMESTAMP.

                    '{row['origin_country']}'             AS ORIGIN_COUNTRY,
                    {row['total_flights']}                AS TOTAL_FLIGHTS,
                    {row['avg_velocity']}                 AS AVG_VELOCITY,
                    {row['on_ground']}                    AS ON_GROUND
            ) AS source
            -- USING (...) AS source: defines the "incoming row" as a virtual single-row table.
            -- This is the new data we want to merge into the target.

            ON (
                target.WINDOW_START   = source.WINDOW_START
                AND target.ORIGIN_COUNTRY = source.ORIGIN_COUNTRY
            )
            -- ON (...): the join condition that determines whether a match exists.
            -- Matching = a row for the same time window AND same country already exists.
            -- No match = this is a new (window, country) combination.

            WHEN MATCHED THEN
                UPDATE SET
                    TOTAL_FLIGHTS = source.TOTAL_FLIGHTS,
                    AVG_VELOCITY  = source.AVG_VELOCITY,
                    ON_GROUND     = source.ON_GROUND
            -- WHEN MATCHED: a row with the same composite key already exists in the table.
            -- UPDATE: overwrites its values with the new data from this pipeline run.
            -- This handles reruns — reprocessing the same window updates, not duplicates.

            WHEN NOT MATCHED THEN
                INSERT (WINDOW_START, ORIGIN_COUNTRY, TOTAL_FLIGHTS, AVG_VELOCITY, ON_GROUND)
                VALUES (
                    source.WINDOW_START, source.ORIGIN_COUNTRY,
                    source.TOTAL_FLIGHTS, source.AVG_VELOCITY, source.ON_GROUND
                )
            -- WHEN NOT MATCHED: no existing row for this (window, country).
            -- INSERT: adds a brand new row with all five column values.
        """
        cursor.execute(merge_sql)
        # cursor.execute(sql): sends the MERGE statement to Snowflake for execution.
        # Snowflake's query optimizer handles each MERGE as a single atomic statement.

    # ── Step 7: Commit the transaction ───────────────────────────
    sf_conn.commit()
    # commit(): finalises all MERGE statements as a single atomic database transaction.
    # Without commit(), Snowflake rolls back ALL changes when the connection closes.
    # With commit(), either ALL rows are persisted or NONE (no partial writes).

    logger.info(f"Snowflake MERGE complete: {len(df)} country rows upserted into FLIGHT_KPIS")

    # ── Step 8: Release resources ─────────────────────────────────
    cursor.close()
    # Releases the cursor's server-side resources.

    sf_conn.close()
    # Terminates the Snowflake session.
    # CRITICAL for cost control: Snowflake virtual warehouses bill per second.
    # Not closing the connection means the warehouse keeps running and consuming credits.
```

---

## 🐳 Docker Setup

The entire stack runs from a single `docker-compose.yaml` with four services:

```yaml
services:

  postgres:
    image: postgres:13
    # Airflow's metadata store: stores DAG runs, task states, XCom values, Connections.

  airflow-init:
    image: apache/airflow:2.9.3
    depends_on: [postgres]
    command: >
      bash -c "airflow db init &&
               airflow users create --username ${AIRFLOW_ADMIN_USER} ..."
    # One-time setup: creates Airflow DB tables and admin user. Exits when done.

  airflow-webserver:
    image: apache/airflow:2.9.3
    ports: ["8080:8080"]
    volumes:
      - ./dags:/opt/airflow/dags       # Auto-discovered DAG files
      - ./scripts:/opt/airflow/scripts # Importable from DAG via sys.path
      - ./data:/opt/airflow/data       # Bronze/Silver/Gold persistence
      - ./logs:/opt/airflow/logs       # Task log persistence
    command: webserver
    # Hosts the Airflow UI at http://localhost:8080

  airflow-scheduler:
    image: apache/airflow:2.9.3
    volumes: [same as webserver]
    command: scheduler
    # Reads DAG files, triggers runs on schedule, manages retries, queues tasks.
```

### Service Responsibilities

| Service | Role | Port |
|---------|------|------|
| `postgres` | Airflow metadata (runs/tasks/XComs/connections) | `5432` |
| `airflow-init` | One-time DB setup + admin user creation | — |
| `airflow-webserver` | DAG UI, logs, connection manager | `8080` |
| `airflow-scheduler` | Schedule enforcement + task execution | — |

---

## 🚀 Getting Started

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) installed and running
- [Git](https://git-scm.com/) installed
- A [Snowflake account](https://signup.snowflake.com/) (free trial works perfectly)

### Step 1 — Clone the Repository

```bash
git clone https://github.com/Devmahey/Flight_Operations_AIRFLOW.git
cd Flight_Operations_AIRFLOW
```

### Step 2 — Create the `.env` File

```bash
cp .env.example .env
```

Edit `.env` with your values:

```dotenv
# Airflow metadata database
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow_db

# Airflow web UI admin account
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_PASSWORD=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com
```

> ⚠️ Never commit `.env` — it is listed in `.gitignore`.

### Step 3 — Initialize and Start Services

```bash
# One-time DB initialization (creates Airflow tables + admin user)
docker-compose up airflow-init

# Start all services in background
docker-compose up -d

# Confirm all containers are healthy
docker-compose ps
```

### Step 4 — Open the Airflow UI

**http://localhost:8080** — Login: `admin` / `admin`

### Step 5 — Add Snowflake Connection

**Admin → Connections → + New Connection:**

| Field | Value |
|-------|-------|
| Conn ID | `flight_snowflake` |
| Conn Type | `Snowflake` |
| Host | `<account>.snowflakecomputing.com` |
| Login | Your Snowflake user |
| Password | Your Snowflake password |
| Schema | `PUBLIC` (or your schema) |
| Extra | `{"warehouse": "COMPUTE_WH", "database": "FLIGHT_DB"}` |

### Step 6 — Enable and Run the DAG

1. Find `flights_ops_medallion_pipe` in the DAG list
2. Toggle the slider to **On**
3. Click **▶ Trigger DAG** for an immediate manual run
4. Watch tasks turn green in the Graph View: `bronze → silver → gold → snowflake`

---

## ❄️ Snowflake Configuration

### Target Table DDL

```sql
-- Run once in your Snowflake worksheet
-- (the pipeline creates this automatically, but explicit creation is good practice)

CREATE TABLE IF NOT EXISTS FLIGHT_KPIS (
    WINDOW_START     TIMESTAMP        NOT NULL,   -- 30-min execution window
    ORIGIN_COUNTRY   VARCHAR(100)     NOT NULL,   -- Aircraft registration country
    TOTAL_FLIGHTS    INTEGER,                     -- Count of tracked aircraft
    AVG_VELOCITY     FLOAT,                       -- Mean ground speed (m/s)
    ON_GROUND        INTEGER,                     -- Count of grounded aircraft
    PRIMARY KEY (WINDOW_START, ORIGIN_COUNTRY)    -- Composite key for MERGE
);
```

### Minimum Required Grants

```sql
GRANT USAGE ON WAREHOUSE <warehouse>  TO ROLE <role>;
GRANT USAGE ON DATABASE  <database>   TO ROLE <role>;
GRANT ALL   ON SCHEMA    <schema>     TO ROLE <role>;
GRANT ALL   ON TABLE     FLIGHT_KPIS  TO ROLE <role>;
```

---

## 📊 Database Schema

```
FLIGHT_KPIS
├── WINDOW_START      TIMESTAMP    ← 30-min window start (composite PK part 1)
├── ORIGIN_COUNTRY    VARCHAR(100) ← Registration country (composite PK part 2)
├── TOTAL_FLIGHTS     INTEGER      ← Aircraft tracked in this window
├── AVG_VELOCITY      FLOAT        ← Mean ground speed (m/s)
└── ON_GROUND         INTEGER      ← Count currently landed
```

---

## 📈 Example Output in Snowflake

| WINDOW_START | ORIGIN_COUNTRY | TOTAL_FLIGHTS | AVG_VELOCITY | ON_GROUND |
|---|---|---|---|---|
| 2026-04-01 06:00 | United States | 4520 | 245.3 | 890 |
| 2026-04-01 06:00 | Germany | 1230 | 238.7 | 210 |
| 2026-04-01 06:00 | India | 120 | 230.5 | 40 |
| 2026-04-01 06:00 | China | 980 | 251.2 | 155 |

---

## ⚠️ Common Issues & Fixes

### ❌ `ModuleNotFoundError: No module named 'scripts'`

**Cause:** Python can't locate the `scripts/` folder inside Docker.

**Fix:** Ensure `sys.path.insert(0, '/opt/airflow')` is at the top of your DAG file. Verify this volume is in `docker-compose.yaml`:

```yaml
volumes:
  - ./scripts:/opt/airflow/scripts
```

Then: `docker-compose down && docker-compose up -d`

---

### ❌ Snowflake Connection Failure

**Checklist:**
- Account format must be `abc12345.region` — no `https://` and no `.snowflakecomputing.com`
- Disable MFA for the pipeline user, or switch to key-pair authentication
- Test: Airflow UI → Admin → Connections → Edit → **Test Connection**

---

### ❌ `xcom_pull` Returns `None`

**Cause:** The `task_ids` or `key` argument doesn't match what was pushed.

**Fix:**
- Confirm `task_ids='bronze_ingest'` matches the actual `task_id` in the PythonOperator
- In Airflow UI: click the upstream task → **XCom** tab to see stored values

---

### ❌ Empty Bronze / No Flight States

**Cause:** OpenSky free tier rate limits; or the API returned `null` states for the requested time.

**Behaviour:** The Silver transform includes a guard (`if not states: return`) — the task exits cleanly with status SUCCESS and a warning log. No CSV is written.

---

## 📈 Future Enhancements

| Enhancement | Description |
|-------------|-------------|
| ✅ **Data Validation** | Add Great Expectations for schema and value-level checks between layers |
| ☁️ **Cloud Storage** | Replace local `/data/` with AWS S3 or GCS for scalable, persistent storage |
| 📊 **Dashboard** | Connect Snowflake to Streamlit or Power BI for live flight visualizations |
| 🔔 **Alerting** | Add Slack/Email notifications on pipeline failures or anomaly thresholds |
| 🔄 **CI/CD** | GitHub Actions workflow to test DAG imports and lint scripts on every PR |
| 📦 **dbt Integration** | Replace Python Gold aggregation with dbt models for version-controlled SQL |
| 🌐 **Route Analytics** | Retain lat/lon in Silver for airport-pair and route-level analysis |
| 🏆 **SLA Monitoring** | Define Airflow SLAs to alert when the pipeline exceeds expected runtime |

---

## 🎯 Key Highlights

```
✔  End-to-end Medallion ETL: Bronze → Silver → Gold → Snowflake
✔  Real-time flight ingestion via OpenSky Network API
✔  Idempotent MERGE-based Snowflake upserts — no duplicate rows
✔  XCom-based decoupled task communication
✔  Fully containerized — one docker-compose up -d to run
✔  Credentials managed via Airflow Connections — zero hardcoded secrets
✔  Automatic retries on transient failures (retries=1, 5-min delay)
✔  Detailed structured logging at every pipeline stage
```

---

## 👨‍💻 Author

Developed as a hands-on **Data Engineering Project** demonstrating:

- ⚙️ Apache Airflow orchestration and Medallion DAG design
- 🏗️ Bronze / Silver / Gold layered architecture
- ❄️ Snowflake cloud data warehouse with `MERGE` upsert patterns
- 🐳 Production-level containerization with Docker Compose
- 🐍 Python-based ETL using Pandas and Snowflake Connector

> **GitHub:** [@Devmahey](https://github.com/Devmahey)

---

## 🏁 Conclusion

This project showcases how to build a **robust, production-grade data pipeline** using industry-standard tools and best practices. By separating concerns across Bronze, Silver, and Gold layers, and using Apache Airflow for orchestration and Snowflake for analytics storage, it serves as a strong foundation for scaling into enterprise-level data engineering systems.

---

<div align="center">
Made with ❤️ · Powered by <b>Apache Airflow</b> & <b>Snowflake</b>
</div>
