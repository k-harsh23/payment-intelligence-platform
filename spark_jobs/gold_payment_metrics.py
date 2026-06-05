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
    .appName("GoldPaymentMetrics")
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

# Process each micro-batch
def process_batch(batch_df, batch_id):

    payment_metrics_df = (
        batch_df
        .groupBy("payment_type")
        .agg(
            count("*").alias("transaction_count"),
            sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
    )

    (
        payment_metrics_df
        .write
        .mode("overwrite")
        .parquet("/app/gold/payment_metrics")
    )

    print(f"Processed batch: {batch_id}")

# Streaming Query
query = (
    silver_df.writeStream
    .foreachBatch(process_batch)
    .option(
        "checkpointLocation",
        "/app/gold_checkpoint/payment_metrics"
    )
    .start()
)

print("Gold Payment Metrics Streaming Started...")

query.awaitTermination()