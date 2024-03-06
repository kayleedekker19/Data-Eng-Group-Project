from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("ParquetDataLoading") \
    .getOrCreate()

# Path to your Parquet file
parquet_file_path = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/historic_weather_collected_data/ACK_2023-1.parquet"

# Read the Parquet file
df = spark.read.parquet(parquet_file_path)

# Show the DataFrame to verify it's loaded correctly
df.show()
