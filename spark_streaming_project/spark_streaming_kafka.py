from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
 
# Define the schema for the incoming data using StructField
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("price", FloatType(), True)
])
 
# Initialize Spark session with Kafka
spark = SparkSession.builder \
    .appName("BitcoinPriceStreamingWithConfluentKafka") \
    .getOrCreate()
 
# Kafka stream source
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bitcoin-prices") \
    .option("startingOffsets", "latest") \
    .load()
 
# Parse the JSON data from Kafka
df = df.selectExpr("CAST(value AS STRING)") \
       .select(from_json(col("value"), schema).alias("data")) \
       .select("data.*")
 
# Output the stream to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()
 
query.awaitTermination()