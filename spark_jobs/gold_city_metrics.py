from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg
)

spark = (
    SparkSession.builder
    .appName("GoldCityMetrics")
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

    # Read complete Silver layer for full aggregation
    silver_full_df = spark.read.parquet("/app/data/silver")

    city_metrics_df = (
        silver_full_df
        .groupBy("city")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    )

    print("City Metrics Count:", city_metrics_df.count())

    city_metrics_df.show(10, truncate=False)

    (
        city_metrics_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet("/app/data/gold/city_metrics")
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
        "/app/checkpoints/gold/city_metrics"
    )
    .start()
)

print("Gold City Metrics Streaming Started...")

query.awaitTermination()