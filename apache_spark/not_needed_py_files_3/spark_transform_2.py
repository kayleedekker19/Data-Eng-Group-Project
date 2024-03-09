from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, when, desc, lit
import json
from pyspark.sql.window import Window

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("WeatherDataIntegration") \
        .getOrCreate()

    ### DEFINING PATH FILES
    airport_path = "/data_sources/manual_data_collected/airports_data.json"
    forecast_path = '/data_sources/manual_data_collected/weather_forecasts.json'
    historic_path = "/data_sources/combined_weather_data.parquet"

    #### AIRPORTS DATAFRAME
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

    ##### FORECAST DATAFRAME
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

    ##### HISTORIC DATAFRAME
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

    # Show the first 20 rows of all 3 datasets
    print("First 20 rows of airports_df:")
    airports_df.show()

    print("First 20 rows of forecasts_df:")
    forecasts_df.show()

    print("First 20 rows of historic_df:")
    historic_df.show()

    # Print the number of rows and columns (shape) of the airports_df DataFrame
    print("Shape of airports_df: (Rows, Columns) = ({}, {})".format(airports_df.count(), len(airports_df.columns)))

    # Print the number of rows and columns (shape) of the forecasts_df DataFrame
    print("Shape of forecasts_df: (Rows, Columns) = ({}, {})".format(forecasts_df.count(), len(forecasts_df.columns)))

    # Print the number of rows and columns (shape) of the historic_df DataFrame
    print("Shape of historic_df: (Rows, Columns) = ({}, {})".format(historic_df.count(), len(historic_df.columns)))

def flatten_forecast(forecasts_df):
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

if __name__ == "__main__":
    main()

