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
