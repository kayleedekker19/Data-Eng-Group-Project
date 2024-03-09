from pyspark.sql import SparkSession
from pyspark.sql.functions import desc

def main():
    spark = SparkSession.builder \
        .appName("LoadAndDisplayParquetData") \
        .getOrCreate()

    # Define the path where the Parquet file is saved
    save_path = "/data_sources/combined_weather_data.parquet"

    # Load the DataFrame from the Parquet file
    df = spark.read.parquet(save_path)

    # Show the first 20 rows
    print("First 20 rows:")
    df.show(20)

    # For the last 20 rows, sort the DataFrame in a descending order based on a column (e.g., Date) and show the top 20
    # This assumes there's a 'Date' column or another column you can sort by to determine the "last" rows
    # If there's no such column, adjust accordingly
    print("Last 20 rows:")
    df.orderBy(desc("Date")).show(20)

if __name__ == "__main__":
    main()
