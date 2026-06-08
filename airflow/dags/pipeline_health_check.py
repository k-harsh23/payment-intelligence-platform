from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2


DB_CONFIG = {
    "host": "host.docker.internal",
    "database": "payment_db",
    "user": "admin",
    "password": "admin123",
    "port": 5432
}


def check_pipeline_health():

    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    tables = [
        "payment_metrics_history",
        "risk_metrics_history",
        "city_metrics_history"
    ]

    threshold_minutes = 10

    current_time = datetime.now()

    for table in tables:

        cur.execute(
            f"""
            SELECT MAX(snapshot_time)
            FROM {table}
            """
        )

        latest_snapshot = cur.fetchone()[0]

        print(f"\nTable: {table}")
        print(f"Latest Snapshot: {latest_snapshot}")

        if latest_snapshot is None:
            raise Exception(
                f"{table} has no records"
            )

        age_minutes = (
            current_time - latest_snapshot
        ).total_seconds() / 60

        print(
            f"Data Age: {age_minutes:.2f} minutes"
        )

        if age_minutes > threshold_minutes:
            raise Exception(
                f"{table} is stale "
                f"({age_minutes:.2f} mins old)"
            )

    cur.close()
    conn.close()

    print("\nPipeline Health Check Passed")


with DAG(
    dag_id="pipeline_health_check",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1)
    },
    tags=["monitoring"]
) as dag:

    health_check_task = PythonOperator(
        task_id="check_pipeline_health",
        python_callable=check_pipeline_health
    )