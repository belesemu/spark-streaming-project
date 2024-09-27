import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# Initialize the Spark session
spark = SparkSession.builder \
    .appName("BitcoinPriceStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
# Function to fetch real-time Bitcoin prices
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

# Simulate the stream by fetching Bitcoin prices every 5 seconds
def simulate_bitcoin_stream():
    schema = StructType([
        StructField("timestamp", StringType(), True),
        StructField("price", FloatType(), True)
    ])

    # Create an empty DataFrame to hold the streaming data
    data = []

    for _ in range(10):  # Stream for 10 intervals
        price = fetch_bitcoin_price()
        if price is not None:
            current_time = time.strftime("%Y-%m-%d %H:%M:%S")
            data.append((current_time, price))
            print(f"Fetched Bitcoin price: {price} USD at {current_time}")
        
        time.sleep(5)  # Wait 5 seconds before fetching the next price

    # Create a DataFrame from the fetched data
    df = spark.createDataFrame(data, schema)
    return df
# Process the streamed data
def process_streamed_data(df):
    df.show()

    # Calculate moving average over the last 3 intervals
    moving_avg = df.withColumn("moving_avg", F.avg("price").over(Window.rowsBetween(-2, 0)))
    moving_avg.show()

# Main execution
if __name__ == "__main__":
    bitcoin_df = simulate_bitcoin_stream()
    process_streamed_data(bitcoin_df)

    # Stop the Spark session
    spark.stop()
