import requests
import time
from confluent_kafka import Producer
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, FloatType
 
# Kafka Producer configuration
producer_conf = {
    'bootstrap.servers': 'localhost:9092'
}
producer = Producer(producer_conf)
 
# Delivery callback for Kafka producer
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
 
# Function to fetch Bitcoin prices
def fetch_bitcoin_price():
    try:
        response = requests.get('https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd')
        if response.status_code == 200:
            data = response.json()
            return float(data['bitcoin']['usd'])
        else:
            return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None
 
# Stream Bitcoin prices to Kafka
def stream_bitcoin_data_to_kafka():
    while True:
        price = fetch_bitcoin_price()
        if price is not None:
            data = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price
            }
            producer.produce('bitcoin-prices', key="bitcoin", value=str(data), callback=delivery_report)
            producer.poll(0)
            print(f"Sent data to Kafka: {data}")
        time.sleep(5)
 
# Kafka consumer using Spark Streaming
def consume_bitcoin_data_from_kafka():
    schema = StructType([
        ("timestamp", StringType()),
        ("price", FloatType())
    ])
 
    spark = SparkSession.builder \
        .appName("BitcoinPriceStreamingWithKafka") \
        .getOrCreate()
 
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "bitcoin-prices") \
        .option("startingOffsets", "latest") \
        .load()
 
    df = df.selectExpr("CAST(value AS STRING)") \
           .select(from_json(col("value"), schema).alias("data")) \
           .select("data.*")
 
    query = df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
 
    query.awaitTermination()
 
if __name__ == "__main__":
    stream_bitcoin_data_to_kafka()
    consume_bitcoin_data_from_kafka()