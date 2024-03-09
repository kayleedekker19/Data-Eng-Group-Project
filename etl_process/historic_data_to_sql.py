# The scrips uses data avaliable via a path to add to the database

# Load libraries
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, concat, col, to_date, col, explode, when, to_timestamp
from pyspark.sql.types import FloatType, IntegerType, DateType
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def process_parquet_file(spark, file_path):
    """
    Process a single Parquet file to add airport_code and adjust the Date column.
    Parameters:
    - spark: SparkSession instance.
    - file_path: The path to the Parquet file to process.
    Returns:
    - A Spark DataFrame with the processed data.
    """
    file_name = os.path.basename(file_path)
    file_name_parts = file_name.split("_")  # ['ACK', '2023-1.parquet']
    airport_code = file_name_parts[0]  # Extracted airport code
    year_month = file_name_parts[1].split(".")[0]  # Extracted year and month

    df = spark.read.parquet(file_path)
    df = df.withColumn("airport_code", lit(airport_code))
    df = df.withColumn("Date", to_date(concat(lit(year_month + "-"), col("Date")), "yyyy-M-d"))
    return df


def process_all_files(spark, directory):
    """
    Process all Parquet files in the specified directory.
    Parameters:
    - directory: The directory containing Parquet files to process.
    Returns:
    - A Spark DataFrame containing the appended data from all files.
    """
    spark = SparkSession.builder.appName("ParquetDataAggregation").getOrCreate()
    dataframes = []

    for file in os.listdir(directory):
        if file.endswith(".parquet"):
            file_path = os.path.join(directory, file)
            df = process_parquet_file(spark, file_path)
            dataframes.append(df)

    # Combine all DataFrames into one
    if dataframes:
        final_df = dataframes[0]
        for df in dataframes[1:]:
            final_df = final_df.union(df)
        return final_df
    else:
        return spark.createDataFrame([], schema=None)  # Return empty DataFrame if no files were processed


def main():
    # # Initialize SparkSession
    # spark_jars_path = os.getenv('SPARK_JARS_PATH')
    # spark = SparkSession.builder \
    #     .appName("PostgreSQL Historic Data Connection") \
    #     .config("spark.jars", spark_jars_path) \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .appName("Airport Data ETL") \
        .config("spark.jars", "/app/libs/postgresql-42.7.2.jar") \
        .getOrCreate()

    # Get the directory containing historic data Parquet files
    directory = os.getenv('HISTORIC_DATA_PATH')
    if not directory:
        raise ValueError("HISTORIC_DATA_PATH environment variable not set.")

    # Process all Parquet files and return a combined DataFrame
    historic_df = process_all_files(spark, directory)

    # Apply transformations to historic_df
    historic_df = historic_df.withColumnRenamed("airport_code", "airport_code") \
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

    # We need to make sure it is all in the correct datatypes
    historic_df = historic_df \
        .withColumn("date_recorded", col("date_recorded").cast(DateType())) \
        .withColumn("temperature_max", col("temperature_max").cast(FloatType())) \
        .withColumn("temperature_avg", col("temperature_avg").cast(FloatType())) \
        .withColumn("temperature_min", col("temperature_min").cast(FloatType())) \
        .withColumn("dew_point_max", col("dew_point_max").cast(FloatType())) \
        .withColumn("dew_point_avg", col("dew_point_avg").cast(FloatType())) \
        .withColumn("dew_point_min", col("dew_point_min").cast(FloatType())) \
        .withColumn("humidity_max", col("humidity_max").cast(IntegerType())) \
        .withColumn("humidity_avg", col("humidity_avg").cast(IntegerType())) \
        .withColumn("humidity_min", col("humidity_min").cast(IntegerType())) \
        .withColumn("wind_speed_max", col("wind_speed_max").cast(FloatType())) \
        .withColumn("wind_speed_avg", col("wind_speed_avg").cast(FloatType())) \
        .withColumn("wind_speed_min", col("wind_speed_min").cast(FloatType())) \
        .withColumn("pressure_max", col("pressure_max").cast(FloatType())) \
        .withColumn("pressure_avg", col("pressure_avg").cast(FloatType())) \
        .withColumn("pressure_min", col("pressure_min").cast(FloatType())) \
        .withColumn("precipitation", col("precipitation").cast(FloatType()))

    window_spec = Window.orderBy("airport_code", "date_recorded")

    historic_df = historic_df.select(
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

    # Display the first 20 rows
    # historic_df.show()

    # Print DataFrame shapes
    # print("Shape of historic_df: ", historic_df.count(), len(historic_df.columns))

    # Define JDBC URL and connection properties for PostgreSQL
    # Access environment variables
    jdbc_url = os.getenv('DATABASE_URL')
    connection_properties = {
        "user": os.getenv('DATABASE_USER'),
        "password": os.getenv('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    # # Write DataFrames to PostgreSQL
    historic_df.write.jdbc(url=jdbc_url, table="historic_weather", mode="overwrite", properties=connection_properties)

    print("Successfully uploaded all the data to the database")

    spark.stop()

if __name__ == "__main__":
    main()


