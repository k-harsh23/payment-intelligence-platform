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

# Bronze schema
bronze_schema = StructType([
    StructField("json_data", StringType(), True)
])

# Transaction schema
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("merchant_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("payment_type", StringType(), True),
    StructField("city", StringType(), True),
    StructField("transaction_timestamp", StringType(), True)
])

# Read Bronze Layer
bronze_df = (
    spark.readStream
    .schema(bronze_schema)
    .parquet("/app/bronze")
)

# Parse JSON and enrich data
silver_df = (
    bronze_df
    .select(
        from_json(
            col("json_data"),
            transaction_schema
        ).alias("data")
    )
    .select("data.*")

    .withColumn(
        "transaction_time",
        to_timestamp(col("transaction_timestamp"))
    )

    .withColumn(
        "is_high_value",
        when(col("amount") > 4000, True)
        .otherwise(False)
    )

    .withColumn(
        "is_night_transaction",
        when(
            hour(col("transaction_time")).between(0, 4),
            True
        ).otherwise(False)
    )

    .withColumn(
        "risk_level",
        when(col("amount") > 4500, "HIGH")
        .when(col("amount") > 4000, "MEDIUM")
        .otherwise("LOW")
    )

    .filter(col("transaction_id").isNotNull())
    .filter(col("amount") > 0)
)

# Write to Silver Layer
query = (
    silver_df.writeStream
    .format("parquet")
    .option("path", "/app/silver")
    .option("checkpointLocation", "/app/silver_checkpoint")
    .outputMode("append")
    .start()
)

print("Silver Streaming Started...")

query.awaitTermination()