# Here we are testing the spark configuration

# Load library
from pyspark.sql import SparkSession
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def main():
    spark = SparkSession.builder \
        .appName("SimpleGCSReadTest") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", GOOGLE_APPLICATION_CREDENTIALS) \
        .config("spark.jars", "/Users/kayleedekker/Library/Mobile Documents/com~apple~CloudDocs/Documents/aa - UCL/Business Analytics/gcs-connector-hadoop3-2.2.20.jar,/Users/kayleedekker/Library/Mobile Documents/com~apple~CloudDocs/Documents/aa - UCL/Business Analytics/google-api-client-1.22.0.jar") \
        .getOrCreate()

    # Replace the file path with your specific Parquet file path in GCS
    parquet_file_path = "gs://data_eng_group_bucket/weather/historic_data/ACK_2023-1.parquet"

    # Read the Parquet file
    df = spark.read.parquet(parquet_file_path)

    # Show the DataFrame content
    df.show()

if __name__ == "__main__":
    main()
