from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, desc

def main():
    spark = SparkSession.builder \
        .appName("LoadAndDisplayParquetData") \
        .getOrCreate()

    # Define the path where the Parquet file is saved
    save_path = "/data_sources/combined_weather_data.parquet"

    # Load the DataFrame from the Parquet file
    historic_df = spark.read.parquet(save_path)

    # Rename columns as specified
    historic_df = historic_df.withColumnRenamed("airport_code", "airport_code") \
           .withColumnRenamed("Year", "date_recorded") \
           .withColumnRenamed("Month", "date_recorded") \
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

    # Add a new column record_id with consecutive numbers starting from 1
    window_spec = Window.orderBy("airport_code", "date_recorded") # You can order by any column here
    historic_df = historic_df.withColumn("record_id", row_number().over(window_spec))

    # Reorder the columns as specified
    historic_df = historic_df.select(
        "record_id",
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

    # Show the first 20 rows
    print("First 20 rows:")
    historic_df.show()

    # For the last 20 rows, sort the DataFrame in a descending order based on a column (e.g., Date) and show the top 20
    # This assumes there's a 'Date' column or another column you can sort by to determine the "last" rows
    # If there's no such column, adjust accordingly
    print("Last 20 rows:")
    historic_df.orderBy(desc("record_id")).show(20)


if __name__ == "__main__":
    main()
