from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    count,
    sum,
    avg
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    BooleanType
)

spark = (
    SparkSession.builder
    .appName("GoldCityMetrics")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Silver schema
silver_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("transaction_timestamp", StringType(), True),
    StructField("transaction_time", TimestampType(), True),
    StructField("is_high_value", BooleanType(), True),
    StructField("is_night_transaction", BooleanType(), True),
    StructField("risk_level", StringType(), True)
])

# Read Silver Stream
silver_df = (
    spark.readStream
    .schema(silver_schema)
    .parquet("/app/silver")
)


def process_batch(batch_df, batch_id):

    print(f"Processing Batch: {batch_id}")

    # Read complete Silver layer
    silver_full_df = spark.read.parquet("/app/silver")

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

    (
        city_metrics_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .parquet("/app/gold/city_metrics")
    )

    print(f"Processed batch: {batch_id}")


query = (
    silver_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/app/gold_checkpoint/city_metrics"
    )
    .start()
)

print("Gold City Metrics Streaming Started...")

query.awaitTermination()