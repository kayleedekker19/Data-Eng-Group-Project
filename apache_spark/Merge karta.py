from pyspark.sql import SparkSession

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("WeatherDataIntegration") \
    .getOrCreate()

forecasts_df = spark.read.json("/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/weather_forecasts.json")
airports_df = spark.read.json("/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/airports_data.json")
historical_df = spark.read.parquet("/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/historic_weather_collected_data/ACK_2023-1.parquet")

# import json
#
# # Path to your original pretty-printed JSON file
# input_file_path = '/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/airports_data.json'
#
# # Path where the line-delimited JSON file will be saved
# output_file_path = '/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/airports_format.json'
#
# # Read the original JSON file
# with open(input_file_path, 'r') as input_file:
#     data = json.load(input_file)
#     results = data['results']  # Extracts the array associated with the "results" key
#
# # Write each JSON object in the array to a new file, one per line
# with open(output_file_path, 'w') as output_file:
#     for entry in results:
#         json.dump(entry, output_file)  # Converts the Python dictionary (each entry) to a JSON string
#         output_file.write('\n')  # Writes a newline character to separate JSON objects
#
# airports_correct_df = spark.read.json("/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/airports_format.json")
# airports_correct_df.show()


from pyspark.sql.functions import col, explode
forecasts_df = df.drop("cod", "message", "cnt")

# Save the cleaned DataFrame back to a JSON file, if needed
forecasts_df.write.json("path/to/your/cleaned_json_file.json")


# Stop the Spark session
#spark.stop()

