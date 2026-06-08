from datetime import datetime

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


def generate_metrics_report():

    conn = psycopg2.connect(**DB_CONFIG)

    cur = conn.cursor()

    tables = [
        "payment_metrics_history",
        "risk_metrics_history",
        "city_metrics_history"
    ]

    print("\n" + "=" * 50)
    print("PAYMENT PLATFORM METRICS REPORT")
    print("=" * 50)

    for table in tables:

        cur.execute(
            f"""
            SELECT
                COUNT(*),
                MAX(snapshot_time)
            FROM {table}
            """
        )

        row_count, latest_snapshot = cur.fetchone()

        print(f"\nTable: {table}")
        print(f"Rows: {row_count}")
        print(f"Latest Snapshot: {latest_snapshot}")

    print("\nReport Generated Successfully")

    cur.close()
    conn.close()


with DAG(
    dag_id="platform_metrics_report",
    start_date=datetime(2025, 1, 1),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["monitoring"]
) as dag:

    report_task = PythonOperator(
        task_id="generate_metrics_report",
        python_callable=generate_metrics_report
    )