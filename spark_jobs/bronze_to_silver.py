from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
    hour
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)

spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# Transaction Schema
# --------------------------------------------------

transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("transaction_timestamp", StringType(), True)
])

# --------------------------------------------------
# Bronze Schema
# --------------------------------------------------

bronze_schema = (
    spark.read
    .parquet("/app/data/bronze")
    .schema
)

# --------------------------------------------------
# Read Bronze Stream
# --------------------------------------------------

bronze_df = (
    spark.readStream
    .schema(bronze_schema)
    .format("parquet")
    .option("maxFilesPerTrigger", 1)
    .load("/app/data/bronze")
)

# --------------------------------------------------
# Parse JSON
# --------------------------------------------------

parsed_df = (
    bronze_df
    .withColumn(
        "parsed_json",
        from_json(
            col("json_data"),
            transaction_schema
        )
    )
)

# --------------------------------------------------
# Build Silver Layer
# --------------------------------------------------

silver_df = (
    parsed_df.select(
        col("parsed_json.transaction_id").alias("transaction_id"),
        col("parsed_json.user_id").alias("user_id"),
        col("parsed_json.merchant_id").alias("merchant_id"),
        col("parsed_json.amount").alias("amount"),
        col("parsed_json.payment_type").alias("payment_type"),
        col("parsed_json.city").alias("city"),
        to_timestamp(
            col("parsed_json.transaction_timestamp")
        ).alias("transaction_timestamp")
    )
    .filter(col("transaction_id").isNotNull())
    .filter(col("amount").isNotNull())
)

# --------------------------------------------------
# Feature Engineering
# --------------------------------------------------

silver_df = (
    silver_df

    # High Value Transaction
    .withColumn(
        "is_high_value",
        when(col("amount") > 3000, True)
        .otherwise(False)
    )

    # Night Transaction
    .withColumn(
        "is_night_transaction",
        when(
            hour(col("transaction_timestamp")).between(0, 6),
            True
        ).otherwise(False)
    )

    # Risk Level
    .withColumn(
        "risk_level",
        when(col("amount") > 4000, "HIGH")
        .when(col("amount") > 2000, "MEDIUM")
        .otherwise("LOW")
    )
)

# --------------------------------------------------
# Write Silver Layer
# --------------------------------------------------

query = (
    silver_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/app/data/silver")
    .option(
        "checkpointLocation",
        "/app/checkpoints/silver"
    )
    .start()
)

print("Silver Streaming Started")

query.awaitTermination()