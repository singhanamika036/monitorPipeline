"""
dag_simulator.py

Simulates a set of production-style Airflow tasks, each of which has a
chance of failing with a realistic error signature (schema drift, timeout,
OOM, etc). This stands in for real Airflow task execution so the monitoring
layer can be demoed without a full Airflow install.
"""

import random
import time
from dataclasses import dataclass


FAILURE_SIGNATURES = {
    "schema_drift": (
        "Traceback (most recent call last):\n"
        "  File \"transform.py\", line 42, in run\n"
        "    df['customer_region'] = df['region_code'].map(region_lookup)\n"
        "KeyError: 'region_code'\n"
        "ERROR - Column not found in source dataframe. Schema mismatch detected."
    ),
    "timeout": (
        "INFO - Task started, waiting on upstream sensor 'raw_events_ready'\n"
        "INFO - 3600s elapsed, still waiting...\n"
        "ERROR - Task timed out. Deadline exceeded after 3600s."
    ),
    "resource_exhaustion": (
        "INFO - Loading 45GB partition into memory for join operation\n"
        "ERROR - MemoryError: Unable to allocate array. Out of memory on worker node.\n"
        "ERROR - Worker killed by OOM killer."
    ),
    "connection_error": (
        "INFO - Connecting to snowflake warehouse ANALYTICS_WH\n"
        "ERROR - snowflake.connector.errors.OperationalError: Could not connect to Snowflake backend.\n"
        "ERROR - Connection refused after 3 retries."
    ),
    "data_quality": (
        "INFO - Running validation suite on orders_staging\n"
        "ERROR - Validation failed: 1,204 rows contain unexpected null in 'order_total'\n"
        "ERROR - Duplicate key constraint violated on order_id."
    ),
    "upstream_dependency": (
        "INFO - Waiting on upstream task 'extract_raw_orders'\n"
        "ERROR - Sensor timeout: upstream task did not complete within SLA window.\n"
        "ERROR - Dependency not met, aborting downstream execution."
    ),
}

TASKS = [
    {"task_id": "extract_raw_orders", "dag": "orders_pipeline", "owner": "data-eng"},
    {"task_id": "transform_customer_dim", "dag": "orders_pipeline", "owner": "data-eng"},
    {"task_id": "load_snowflake_fact_orders", "dag": "orders_pipeline", "owner": "data-eng"},
    {"task_id": "validate_orders_quality", "dag": "orders_pipeline", "owner": "data-eng"},
    {"task_id": "aggregate_daily_revenue", "dag": "revenue_pipeline", "owner": "analytics-eng"},
]


@dataclass
class TaskResult:
    task_id: str
    dag: str
    success: bool
    log: str = ""
    failure_category: str = ""


def run_task(task: dict, failure_rate: float = 0.35) -> TaskResult:
    """Simulate running a single task. Randomly fails ~failure_rate of the time."""
    time.sleep(0.01)  # simulate execution
    if random.random() < failure_rate:
        category = random.choice(list(FAILURE_SIGNATURES.keys()))
        log = FAILURE_SIGNATURES[category]
        return TaskResult(task["task_id"], task["dag"], success=False, log=log, failure_category=category)
    return TaskResult(task["task_id"], task["dag"], success=True)


def run_pipeline(tasks=None, failure_rate: float = 0.35):
    """Run all tasks in the (simulated) pipeline and return results."""
    tasks = tasks or TASKS
    return [run_task(t, failure_rate) for t in tasks]
