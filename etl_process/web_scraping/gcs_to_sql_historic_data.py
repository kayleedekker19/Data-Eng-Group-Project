# Extract data from the bucket and processed it into Google Cloud SQL

# Load libraries
from pyspark.sql.window import Window
from pyspark.sql.functions import lit, to_date, input_file_name
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, to_date
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def process_files_from_gcs(spark, gcs_bucket_path):
    """
    Process all Parquet files in the specified GCS bucket path. For each file, it extracts airport_code
    and year_month from the file name, then adds these as columns in the DataFrame.

    Parameters:
    - spark: SparkSession instance.
    - gcs_bucket_path: The path to the GCS bucket containing Parquet files.

    Returns:
    - final_df: A combined Spark DataFrame with data from all processed files.
    """
    # Read all Parquet files from the specified path into a single DataFrame
    df = spark.read.option("basePath", gcs_bucket_path).parquet(f"{gcs_bucket_path}/*.parquet")

    # Add the path of each file as a column to extract airport_code and year_month
    df = df.withColumn("file_path", input_file_name())

    # Define a UDF to extract airport_code and year_month from the file path
    def extract_info_from_path(path):
        pattern = r".*/([A-Z]+)_([0-9]{4}-[0-9]+).parquet"
        match = re.search(pattern, path)
        if match:
            return match.groups()
        else:
            return (None, None)

    extract_info_udf = spark.udf.register("extract_info_from_path", extract_info_from_path)

    # Apply the UDF to create new columns for airport_code and year_month
    df = df.withColumn("info", extract_info_udf("file_path"))
    df = df.withColumn("airport_code", df["info"].getItem(0))
    df = df.withColumn("year_month", df["info"].getItem(1))

    # Now, adjust the Date column based on the extracted year_month
    df = df.withColumn("Date", to_date(df["Date"], "yyyy-M-d"))

    # Drop temporary columns
    df = df.drop("file_path", "info")

    return df


def main():
    spark = SparkSession.builder \
        .appName("DataProcessingAndLoad") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/Users/kayleedekker/Library/Mobile Documents/com~apple~CloudDocs/Documents/aa - UCL/Business Analytics/data-engineering-group-b1735678d2bc.json") \
        .config("spark.jars", GOOGLE_APPLICATION_CREDENTIALS) \
        .getOrCreate()

    # Construct the data_path using BUCKET_NAME from environment variable
    bucket_name = os.getenv('BUCKET_NAME')  # Returns None if BUCKET_NAME is not set
    if not bucket_name:
        raise ValueError("BUCKET_NAME environment variable not set.")
    gcs_bucket_path = f"gs://{bucket_name}/weather/historic_data"

    # Process files from GCS
    processed_df = process_files_from_gcs(spark, gcs_bucket_path)

    # Apply transformations as per your requirements
    transformed_df = processed_df.withColumnRenamed("airport_code", "airport_code") \
        .withColumnRenamed("Date", "date_recorded") \
        .withColumnRenamed("Temperature (°F), max", "temperature_max") \
        .withColumnRenamed("Temperature (°F), avg", "temperature_avg") \
        .withColumnRenamed("Temperature (°F), min", "temperature_min") \
        .withColumnRenamed("Dew Point (°F), max", "dew_point_max") \
        .withColumnRenamed("Dew Point (°F), avg", "dew_point_avg") \
        .withColumnRenamed("Dew Point (°F), min", "dew_point_min") \
        .withColumnRenamed("Humidity (%), max", "humidity_max") \
        .withColumnRenamed("Humidity (%), avg", "humidity_avg") \
        .withColumnRenamed("Humidity (%), min", "humidity_min") \
        .withColumnRenamed("Wind Speed (mph), max", "wind_speed_max") \
        .withColumnRenamed("Wind Speed (mph), avg", "wind_speed_avg") \
        .withColumnRenamed("Wind Speed (mph), min", "wind_speed_min") \
        .withColumnRenamed("Pressure (in), max", "pressure_max") \
        .withColumnRenamed("Pressure (in), avg", "pressure_avg") \
        .withColumnRenamed("Pressure (in), min", "pressure_min") \
        .withColumnRenamed("Precipitation (in)", "precipitation")

    window_spec = Window.orderBy("airport_code", "date_recorded")

    # Extra step to ensure consistency before loading into the schema
    historic_df = transformed_df.select(
        "airport_code",
        "date_recorded",
        "temperature_max",
        "temperature_avg",
        "temperature_min",
        "dew_point_max",
        "dew_point_avg",
        "dew_point_min",
        "humidity_max",
        "humidity_avg",
        "humidity_min",
        "wind_speed_max",
        "wind_speed_avg",
        "wind_speed_min",
        "pressure_max",
        "pressure_avg",
        "pressure_min",
        "precipitation"
    )

    # Show DataFrame statistics and display the first 20 rows
    print(f"DataFrame shape: {transformed_df.count()} rows, {len(transformed_df.columns)} columns")
    transformed_df.show(n=20)

    # Define JDBC URL and connection properties for the database
    jdbc_url = os.getenv('DATABASE_URL')
    connection_properties = {
        "user": os.getenv('DATABASE_USER'),
        "password": os.getenv('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to the Google Cloud SQL database
    transformed_df.write.jdbc(url=jdbc_url, table="your_table_name", mode="overwrite", properties=connection_properties)

    print("Data successfully loaded into the database.")

    spark.stop()


if __name__ == "__main__":
    main()
