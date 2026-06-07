from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import time

# --------------------------------------------------
# Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("GoldToPostgresSync")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# PostgreSQL Config
# --------------------------------------------------

JDBC_URL = "jdbc:postgresql://payment-postgres:5432/payment_db"

PROPERTIES = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

SYNC_INTERVAL_SECONDS = 60

# --------------------------------------------------
# Generic Sync Function
# --------------------------------------------------

def sync_table(
    parquet_path,
    postgres_table,
    timestamp_column="snapshot_time"
):

    print(f"\nSyncing: {postgres_table}")

    try:

        # ------------------------------------------
        # Read PostgreSQL Table
        # ------------------------------------------

        postgres_df = spark.read.jdbc(
            url=JDBC_URL,
            table=postgres_table,
            properties=PROPERTIES
        )

        max_timestamp = (
            postgres_df
            .agg({timestamp_column: "max"})
            .collect()[0][0]
        )

        print(
            f"Latest PostgreSQL Snapshot: {max_timestamp}"
        )

        # ------------------------------------------
        # Read Gold History
        # ------------------------------------------

        parquet_df = spark.read.parquet(
            parquet_path
        )

        # ------------------------------------------
        # Filter New Records
        # ------------------------------------------

        new_records_df = parquet_df.filter(
            col(timestamp_column) > max_timestamp
        )

        new_count = new_records_df.count()

        print(
            f"New Records Found: {new_count}"
        )

        if new_count == 0:
            print(
                f"No New Records For {postgres_table}"
            )
            return

        # ------------------------------------------
        # Write To PostgreSQL
        # ------------------------------------------

        (
            new_records_df
            .write
            .jdbc(
                url=JDBC_URL,
                table=postgres_table,
                mode="append",
                properties=PROPERTIES
            )
        )

        print(
            f"Inserted {new_count} Rows Into {postgres_table}"
        )

    except Exception as e:

        print(
            f"Error Syncing {postgres_table}"
        )

        print(str(e))

# --------------------------------------------------
# Main Loop
# --------------------------------------------------

print(
    "Gold To PostgreSQL Sync Started..."
)

while True:

    try:

        sync_table(
            "/app/data/gold/payment_metrics_history",
            "payment_metrics_history"
        )

        sync_table(
            "/app/data/gold/risk_metrics_history",
            "risk_metrics_history"
        )

        sync_table(
            "/app/data/gold/city_metrics_history",
            "city_metrics_history"
        )

        print(
            f"\nSleeping For {SYNC_INTERVAL_SECONDS} Seconds..."
        )

        time.sleep(
            SYNC_INTERVAL_SECONDS
        )

    except Exception as e:

        print(
            f"Main Loop Error: {e}"
        )

        time.sleep(
            SYNC_INTERVAL_SECONDS
        )