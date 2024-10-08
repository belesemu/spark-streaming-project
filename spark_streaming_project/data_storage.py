from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType
 
# Define schema for the incoming data using StructField
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("price", FloatType(), True)
])
 
# Initialize Spark session with Hive support
spark = SparkSession.builder \
    .appName("BitcoinPriceStreamingWithHive") \
    .enableHiveSupport() \
    .config("spark.sql.warehouse.dir", "/usr/local/var/lib/hive") \
    .getOrCreate()
 
# Set up Kafka stream source
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
 
# Save to Hive
def save_to_hive(df, table_name="bitcoin_prices"):
    df.write.mode("append").saveAsTable(table_name)
    print(f"Data saved to Hive table: {table_name}")
 
# Write the stream to Hive in batch
query = df.writeStream \
    .foreachBatch(lambda batch_df, batch_id: save_to_hive(batch_df)) \
    .outputMode("append") \
    .start()
 
query.awaitTermination()