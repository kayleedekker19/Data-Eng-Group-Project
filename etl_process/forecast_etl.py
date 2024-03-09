# In this script we want to perform the whole ETL process for our second data collection method
# Extract API weather forecast data
# Transform them in Apache Spark
# Upload the data into our SQL Google Cloud Postgres database

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when
from pyspark.sql.window import Window
from pyspark.sql.functions import to_timestamp
import requests
import random
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# This script draws on functions in other scripts so lets import that
from airport_data import airport_etl

def extract_airport_code_and_coordinates(airports_data):
    extracted_data = []
    for item in airports_data:
        airport_code = item['airport_code']
        lon = item['coordinates']['lon']
        lat = item['coordinates']['lat']
        extracted_data.append((airport_code, lat, lon))
    return extracted_data

def fetch_weather_forecast(api_key, locations):
    all_forecasts = []
    for airport_code, lat, lon in locations:
        url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=imperial"
        response = requests.get(url)
        if response.status_code == 200:
            forecast_data = response.json()
            forecast_data['city']['name'] = airport_code  # Use airport_code to override the city name
            forecast_data['city']['coordinates'] = {'lat': lat, 'lon': lon}  # Add coordinates
            all_forecasts.append(forecast_data)
        else:
            print(f"Failed to fetch the weather data for {airport_code} at coordinates: {lat}, {lon}")

    return all_forecasts
    pass

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
    pass


def main():
    # Fetch and process airport data
    # Ensure the URLs are corrected for direct API access
    base_url = os.getenv('API_BASE_URL')
    urls = [
        f"{base_url}/airports-code/records?limit=50&refine=country_name%3A%22United%20States%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Canada%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Argentina%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Brazil%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Mexico%22",
        f"{base_url}/airports-code/records?limit=10&refine=country_name%3A%22Peru%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Colombia%22"
    ]

    airports_data = []
    for url in urls:
        airports_data.extend(airport_etl.fetch_and_process_data(url))

    # # Initialize SparkSession
    # spark_jars_path = os.getenv('SPARK_JARS_PATH')
    # spark = SparkSession.builder \
    #     .appName("Weather Data ETL") \
    #     .config("spark.jars", spark_jars_path) \
    #     .getOrCreate()

    spark = SparkSession.builder \
        .appName("Weather Data ETL") \
        .config("spark.jars", "/app/libs/postgresql-42.7.2.jar") \
        .getOrCreate()

    # Extract codes and coordinates from the fetched airport data
    locations = extract_airport_code_and_coordinates(airports_data)

    # Let's decide how many locations we want to sample (for example, 10)
    sample_size = 10
    sampled_locations = random.sample(locations, min(sample_size, len(locations)))

    # Fetch weather forecast data
    api_key = os.getenv('FORECAST_API_KEY')
    forecasts = fetch_weather_forecast(api_key, sampled_locations)

    # Convert forecasts list into RDD then into DataFrame
    forecasts_rdd = spark.sparkContext.parallelize(forecasts)  # Convert list to RDD
    df = spark.read.json(forecasts_rdd)

    # df = spark.read.option("multiLine", "true").json(forecasts)

    df = df.drop("cod", "message", "cnt")
    df = df.withColumn("city", df.city.dropFields("id", "coord", "population"))
    df = df.withColumn("latitude", df.city.coordinates.lat).withColumn("longitude", df.city.coordinates.lon)

    # Apply the flatten_forecast function
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

    # Define JDBC URL and connection properties
    # Access environment variables
    jdbc_url = os.getenv('DATABASE_URL')
    connection_properties = {
        "user": os.getenv('DATABASE_USER'),
        "password": os.getenv('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    # Load to PostgreSQL
    forecasts_df.write.jdbc(url=jdbc_url, table="weather_forecasts", mode="overwrite", properties=connection_properties)

    print("Data successfully written to database.")

    spark.stop()

if __name__ == "__main__":
    main()
