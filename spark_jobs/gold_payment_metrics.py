from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg
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

    print("Incoming Records:", batch_df.count())

    payment_metrics_df = (
        batch_df
        .groupBy("payment_type")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    )

    print("Aggregated Records:", payment_metrics_df.count())

    payment_metrics_df.show(truncate=False)

    (
        payment_metrics_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet("/app/data/gold/payment_metrics")
    )

    print(f"Processed Batch: {batch_id}")

# --------------------------------------------------
# Start Stream
# --------------------------------------------------

query = (
    silver_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/app/checkpoints/gold/payment_metrics"
    )
    .start()
)

print("Gold Payment Metrics Streaming Started...")

query.awaitTermination()