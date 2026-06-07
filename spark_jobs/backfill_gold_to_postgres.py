"""
One-time historical backfill job.

Used to load existing Gold Parquet history
into PostgreSQL serving tables.
"""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("GoldToPostgresBackfill")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

jdbc_url = (
    "jdbc:postgresql://payment-postgres:5432/payment_db"
)

connection_properties = {
    "user": "admin",
    "password": "admin123",
    "driver": "org.postgresql.Driver"
}

# --------------------------------------------------
# Payment Metrics
# --------------------------------------------------

payment_df = spark.read.parquet(
    "/app/data/gold/payment_metrics_history"
)

payment_df.write \
    .mode("append") \
    .jdbc(
        jdbc_url,
        "payment_metrics_history",
        properties=connection_properties
    )

print("Payment Metrics Loaded")

# --------------------------------------------------
# Risk Metrics
# --------------------------------------------------

risk_df = spark.read.parquet(
    "/app/data/gold/risk_metrics_history"
)

risk_df.write \
    .mode("append") \
    .jdbc(
        jdbc_url,
        "risk_metrics_history",
        properties=connection_properties
    )

print("Risk Metrics Loaded")

# --------------------------------------------------
# City Metrics
# --------------------------------------------------

city_df = spark.read.parquet(
    "/app/data/gold/city_metrics_history"
)

city_df.write \
    .mode("append") \
    .jdbc(
        jdbc_url,
        "city_metrics_history",
        properties=connection_properties
    )

print("City Metrics Loaded")

print("Backfill Complete")