from kafka import KafkaProducer
import requests
import time
import json

# Initialize the Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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

# Stream Bitcoin prices to Kafka topic
def stream_bitcoin_data():
    while True:
        price = fetch_bitcoin_price()
        if price is not None:
            data = {
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                "price": price
            }
            producer.send('bitcoin-prices', data)
            print(f"Sent data: {data}")
        time.sleep(5)  # Sleep for 5 seconds before fetching again

if __name__ == "__main__":
    stream_bitcoin_data()
