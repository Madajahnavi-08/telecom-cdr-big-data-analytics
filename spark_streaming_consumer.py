"""Spark Structured Streaming Kafka consumer; writes parsed CDR JSON to disk."""
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "telecom-cdr")
OUTPUT = os.getenv("CDR_OUTPUT_PATH", "output/cdr_stream")
CHECKPOINT = os.getenv("CDR_CHECKPOINT_PATH", "output/checkpoint")

schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("source", StringType(), True),
    StructField("caller_id", StringType(), True),
    StructField("receiver_id", StringType(), True),
    StructField("duration_sec", DoubleType(), True),
    StructField("data_mb", DoubleType(), True),
    StructField("cell_id", StringType(), True),
    StructField("call_status", StringType(), True),
])

spark = (SparkSession.builder
         .appName("TelecomCDRStructuredStreaming")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "earliest")
       .load())

parsed = (raw.select(from_json(col("value").cast("string"), schema).alias("cdr"))
          .select("cdr.*")
          .filter(col("timestamp").isNotNull()))

query = (parsed.writeStream
         .format("json")
         .option("path", OUTPUT)
         .option("checkpointLocation", CHECKPOINT)
         .outputMode("append")
         .start())

print(f"Streaming CDR events from Kafka topic '{TOPIC}' to '{OUTPUT}'")
query.awaitTermination()
