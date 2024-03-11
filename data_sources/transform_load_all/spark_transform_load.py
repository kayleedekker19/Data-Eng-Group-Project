# The scrips loads all the data from the three sources in the Google Cloud PostgreSQL database

# Load the libraries
import json
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, to_timestamp
from pyspark.sql.types import FloatType, IntegerType, DateType
from pyspark.sql.window import Window
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Define the function that is needed to flatten the json output of the forecast weather api call
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


def main():
    # Initialize SparkSession
    # Create a SparkSession
    spark_jars_path = os.getenv('SPARK_JARS_PATH')
    spark = SparkSession.builder \
        .appName("PostgreSQL Connection") \
        .config("spark.jars", spark_jars_path) \
        .getOrCreate()


    ### DEFINING PATH FILES
    airport_path = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/manual_data_collected/airports_data.json"
    forecast_path = '/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/manual_data_collected/weather_forecasts.json'
    historic_path = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/combined_weather_data.parquet"


    ### 1. AIRPORT DATA
    # Apply transformations to airports_df
    with open(airport_path, 'r') as input_file:
        data = json.load(input_file)
        results = data['results']  # This extracts the array associated with the "results" key

    # Convert the extracted array back into JSON format and parallelize it using SparkContext to create a DataFrame
    airports_df = spark.createDataFrame(results)

    # Change the data type of 'city_name_geo_name_id' from String to Integer
    airports_df = airports_df.withColumn("city_name_geo_name_id", col("city_name_geo_name_id").cast("integer"))

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

    ### 2. FORECAST DATA
    # Load and preprocess forecast data
    # Apply transformations to forecasts_df
    df = spark.read.option("multiLine", "true").json(forecast_path)

    # Data transformation - drop columns
    df = df.drop("cod", "message", "cnt")
    df = df.withColumn("city", df.city.dropFields("id", "coord", "population"))
    df = df.withColumn("latitude", df.city.coordinates.lat).withColumn("longitude", df.city.coordinates.lon)

    # Apply the flatten_forecast funciton
    flattened_df = flatten_forecast(df)

    # Convert "datetime_recorded" from string to timestamp
    flattened_df = flattened_df.withColumn("datetime_recorded", to_timestamp("datetime_recorded", "yyyy-MM-dd HH:mm:ss"))

    window_spec = Window.orderBy("airport_code", "datetime_recorded")

    forecasts_df = flattened_df.select(
        "airport_code", "datetime_recorded", "temperature", "feels_like",
        "temperature_min", "temperature_max", "pressure", "sea_level", "ground_level",
        "humidity", "weather_main", "weather_description", "clouds_all", "wind_speed",
        "wind_deg", "wind_gust", "visibility", "pop", "rain_3hr", "pod", "country",
        "timezone", "latitude", "longitude", "sunrise", "sunset"
    )

    ### 3. HISTORIC DATA
    # Load and preprocess historic weather data
    # Apply transformations to historic_df
    historic_df = spark.read.parquet(historic_path)

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

    # Display the first 20 rows of all DataFrames and their shapes
    # to check all transformations are correct
    airports_df.show()
    forecasts_df.show()
    historic_df.show()

    # Print DataFrame shapes
    print("Shape of airports_df: ", airports_df.count(), len(airports_df.columns))
    print("Shape of forecasts_df: ", forecasts_df.count(), len(forecasts_df.columns))
    print("Shape of historic_df: ", historic_df.count(), len(historic_df.columns))

    print("Data types of historic_df: ", historic_df.dtypes)

    # Define JDBC URL and connection properties for PostgreSQL
    # Access environment variables
    jdbc_url = os.getenv('DATABASE_URL')
    connection_properties = {
        "user": os.getenv('DATABASE_USER'),
        "password": os.getenv('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    # # Populate DataFrames to PostgreSQL
    airports_df.write.jdbc(url=jdbc_url, table="airports", mode="overwrite", properties=connection_properties)
    forecasts_df.write.jdbc(url=jdbc_url, table="weather_forecasts", mode="overwrite", properties=connection_properties)
    historic_df.write.jdbc(url=jdbc_url, table="historic_weather", mode="append", properties=connection_properties)

    print("Successfully uploaded all the data to the database")

if __name__ == "__main__":
    main()