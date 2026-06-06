from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("KafkaToBronze")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "payment-kafka:29092")
    .option("subscribe", "transactions_raw")
    .option("startingOffsets", "latest")
    .load()
)

bronze_df = kafka_df.select(
    col("value").cast("string").alias("json_data")
)

query = (
    bronze_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", "/app/data/bronze")
    .option("checkpointLocation", "/app/checkpoints/bronze")
    .start()
)

print("Bronze Streaming Started")

query.awaitTermination()