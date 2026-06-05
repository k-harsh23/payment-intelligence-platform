from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaToBronze")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "payment-kafka:29092")
    .option("subscribe", "transactions_raw")
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

raw_df = kafka_df.selectExpr(
    "CAST(value AS STRING) as json_data"
)

query = (
    raw_df.writeStream
    .format("parquet")
    .option("path", "/app/bronze")
    .option("checkpointLocation", "/app/bronze_checkpoint")
    .outputMode("append")
    .start()
)

print("Bronze Streaming Started...")

query.awaitTermination()