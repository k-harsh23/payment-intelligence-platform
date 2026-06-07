from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg,
    current_timestamp
)

spark = (
    SparkSession.builder
    .appName("GoldPaymentMetrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Infer Silver Schema
# --------------------------------------------------

silver_schema = (
    spark.read
    .parquet("/app/data/silver")
    .schema
)

# --------------------------------------------------
# Read Silver Stream
# --------------------------------------------------

silver_df = (
    spark.readStream
    .schema(silver_schema)
    .option("maxFilesPerTrigger", 1)
    .parquet("/app/data/silver")
)

# --------------------------------------------------
# Process Each Batch
# --------------------------------------------------

def process_batch(batch_df, batch_id):

    print(f"Processing Batch: {batch_id}")

    # Read COMPLETE Silver Layer
    silver_full_df = spark.read.parquet(
        "/app/data/silver"
    )

    print(
        "Total Silver Records:",
        silver_full_df.count()
    )

    payment_metrics_df = (
        silver_full_df
        .groupBy("payment_type")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        .withColumn(
            "snapshot_time",
            current_timestamp()
        )
    )

    print(
        "Aggregated Records:",
        payment_metrics_df.count()
    )

    payment_metrics_df.show(
        truncate=False
    )

    (
        payment_metrics_df
        .coalesce(1)
        .write
        .mode("append")
        .parquet(
            "/app/data/gold/payment_metrics_history"
        )
    )

    print(
        f"Historical Snapshot Written: {batch_id}"
    )

# --------------------------------------------------
# Start Stream
# --------------------------------------------------

query = (
    silver_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/app/checkpoints/gold/payment_metrics_history"
    )
    .trigger(processingTime="1 minute")
    .start()
)

print(
    "Gold Payment Metrics History Streaming Started..."
)

query.awaitTermination()