from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when
import json
from pyspark.sql.window import Window
import psycopg2

# Define functions for flattening forecast data and handling PostgreSQL constraints
def flatten_forecast(forecasts_df):
    # Function to flatten forecast data and create a structured DataFrame
    exploded_df = forecasts_df.withColumn("forecast", explode("list")).drop("list")

    exploded_df = exploded_df.withColumn("main", exploded_df.forecast.main.dropFields("temp_kf"))
    exploded_df = exploded_df.withColumn("weather", exploded_df.forecast.weather.getItem(0).dropFields("id", "icon"))
    exploded_df = exploded_df.withColumn("clouds", exploded_df.forecast.clouds) \
        .withColumn("wind", exploded_df.forecast.wind) \
        .withColumn("visibility", exploded_df.forecast.visibility) \
        .withColumn("pop", exploded_df.forecast.pop) \
        .withColumn("pod", exploded_df.forecast.sys.pod) \
        .withColumn("dt_txt", exploded_df.forecast.dt_txt)

    exploded_df = exploded_df.withColumn("3h", when(exploded_df["forecast.rain"].isNull(), 0)
                                         .otherwise(when(exploded_df["forecast.rain"]["3h"].isNull(), 0)
                                                    .otherwise(exploded_df["forecast.rain"]["3h"])))

    final_df = exploded_df.select(
        col("city.name").alias("airport_code"),
        col("city.country").alias("country"),
        col("city.timezone").alias("timezone"),
        col("city.sunrise").alias("sunrise"),
        col("city.sunset").alias("sunset"),
        col("latitude"),
        col("longitude"),
        col("main.temp").alias("temperature"),
        col("main.feels_like").alias("feels_like"),
        col("main.temp_min").alias("temperature_min"),
        col("main.temp_max").alias("temperature_max"),
        col("main.pressure").alias("pressure"),
        col("main.sea_level").alias("sea_level"),
        col("main.grnd_level").alias("ground_level"),
        col("main.humidity").alias("humidity"),
        col("weather.main").alias("weather_main"),
        col("weather.description").alias("weather_description"),
        col("clouds.all").alias("clouds_all"),
        col("wind.speed").alias("wind_speed"),
        col("wind.deg").alias("wind_deg"),
        col("wind.gust").alias("wind_gust"),
        col("visibility"),
        col("pop"),
        col("3h").alias("rain_3hr"),
        col("pod"),
        col("dt_txt").alias("datetime_recorded"))

    return final_df

# def drop_foreign_key_constraints(connection):
#     try:
#         cursor = connection.cursor()
#         cursor.execute("""
#             ALTER TABLE historic_weather DROP CONSTRAINT IF EXISTS historic_weather_airport_code_fkey;
#             ALTER TABLE weather_forecasts DROP CONSTRAINT IF EXISTS weather_forecasts_airport_code_fkey;
#         """)
#         connection.commit()
#         cursor.close()
#         print("Foreign key constraints dropped successfully.")
#     except Exception as e:
#         print("Error dropping foreign key constraints:", e)
#
# def recreate_foreign_key_constraints(connection):
#     try:
#         cursor = connection.cursor()
#         cursor.execute("""
#             ALTER TABLE historic_weather ADD CONSTRAINT historic_weather_airport_code_fkey FOREIGN KEY (airport_code) REFERENCES airports(airport_code);
#             ALTER TABLE weather_forecasts ADD CONSTRAINT weather_forecasts_airport_code_fkey FOREIGN KEY (airport_code) REFERENCES airports(airport_code);
#         """)
#         connection.commit()
#         cursor.close()
#         print("Foreign key constraints recreated successfully.")
#     except Exception as e:
#         print("Error recreating foreign key constraints:", e)

def main():

    # Assuming your JSON and Parquet files have already been correctly parsed into DataFrame formats in your question

    # Process your airports, forecasts, and historic data as per your existing logic
    # This includes all the steps for data loading, transformation, and flattening as you've described

    # Initialize SparkSession
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection") \
        .config("spark.jars", "/Users/kayleedekker/Downloads/postgresql-42.7.2.jar") \
        .getOrCreate()

    ### DEFINING PATH FILES
    airport_path = "/data_sources/manual_data_collected/airports_data.json"
    forecast_path = '/data_sources/manual_data_collected/weather_forecasts.json'
    historic_path = "/data_sources/combined_weather_data.parquet"

    # Apply transformations to airports_df
    with open(airport_path, 'r') as input_file:
        data = json.load(input_file)
        results = data['results']  # This extracts the array associated with the "results" key

    # Convert the extracted array back into JSON format and parallelize it using SparkContext to create a DataFrame
    airports_df = spark.createDataFrame(results)

    # Replace "\N" with NULL in city_name_geo_name_id and apply any other transformations as needed
    airports_df = airports_df.select(
        "airport_code",
        "airport_name",
        "city_name",
        "country_name",
        "country_code",
        "latitude",
        "longitude",
        "world_area_code",
        "city_name_geo_name_id",
        "country_name_geo_name_id"
    )

    # Load and preprocess forecast data
    # Apply transformations to forecasts_df
    df = spark.read.option("multiLine", "true").json(forecast_path)

    df = df.drop("cod", "message", "cnt")
    df = df.withColumn("city", df.city.dropFields("id", "coord", "population"))
    df = df.withColumn("latitude", df.city.coordinates.lat).withColumn("longitude", df.city.coordinates.lon)

    flattened_df = flatten_forecast(df)

    window_spec = Window.orderBy("airport_code", "datetime_recorded")

    forecasts_df = flattened_df.select(
        "airport_code", "datetime_recorded", "temperature", "feels_like",
        "temperature_min", "temperature_max", "pressure", "sea_level", "ground_level",
        "humidity", "weather_main", "weather_description", "clouds_all", "wind_speed",
        "wind_deg", "wind_gust", "visibility", "pop", "rain_3hr", "pod", "country",
        "timezone", "latitude", "longitude", "sunrise", "sunset"
    )

    # Load and preprocess historic weather data
    # Apply transformations to historic_df
    historic_df = spark.read.parquet(historic_path)

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

    # Display the first 20 rows of all DataFrames and their shapes
    airports_df.show()
    forecasts_df.show()
    historic_df.show()

    # Print DataFrame shapes
    print("Shape of airports_df: ", airports_df.count(), len(airports_df.columns))
    print("Shape of forecasts_df: ", forecasts_df.count(), len(forecasts_df.columns))
    print("Shape of historic_df: ", historic_df.count(), len(historic_df.columns))

    # Define JDBC URL and connection properties
    jdbc_url = "jdbc:postgresql://34.89.0.15:5432/Weather_database"
    connection_properties = {
        "user": "postgres",
        "password": "dataeng",
        "driver": "org.postgresql.Driver"
    }

    # Establish PostgreSQL connection using psycopg2 for schema manipulation
    connection = psycopg2.connect(
        user="postgres",
        password="dataeng",
        host="34.89.0.15",
        port="5432",
        database="Weather_database"
    )

    # # Drop foreign key constraints to enable table overwriting
    # drop_foreign_key_constraints(connection)

    # Execute your Spark DataFrame writes here
    # Replace the placeholders with your DataFrames and ensure they are correctly prepared before this step
    # airports_df.write.jdbc(url=jdbc_url, table="airports", mode="append", properties=connection_properties)
    # forecasts_df.write.jdbc(url=jdbc_url, table="weather_forecasts", mode="append", properties=connection_properties)
    # historic_df.write.jdbc(url=jdbc_url, table="historic_weather", mode="append", properties=connection_properties)

    # # Recreate foreign key constraints after data has been loaded
    # recreate_foreign_key_constraints(connection)

    # Close the database connection
    connection.close()

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
