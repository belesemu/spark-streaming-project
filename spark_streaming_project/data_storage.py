from pyspark.sql import SparkSession
from pyspark.sql import DataFrame

def save_to_hive(df: DataFrame, table_name: str):
    """
    Save the processed data into Hive for long-term storage.
    
    Parameters:
    df (DataFrame): The DataFrame to store
    table_name (str): The name of the Hive table to store the data
    """
    # Initialize Spark session with Hive support
    spark = SparkSession.builder \
        .appName("BitcoinDataStorage") \
        .enableHiveSupport() \
        .getOrCreate()

    # Create the Hive table if it doesn't exist
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table_name} (timestamp STRING, price FLOAT)")

    # Write data to the Hive table
    df.write.mode("append").saveAsTable(table_name)

    print(f"Data successfully saved to Hive table {table_name}")

    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    # Assuming that bitcoin_df is the processed DataFrame we want to save
    from bitcoin_stream import simulate_bitcoin_stream

    bitcoin_df = simulate_bitcoin_stream()
    save_to_hive(bitcoin_df, "bitcoin_price_data")