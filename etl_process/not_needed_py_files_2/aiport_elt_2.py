# In this script we want to perform the whole ETL process for our first data collection method
# Extract airport data
# Transform them in Apache Spark
# Upload the data into our SQL Google Cloud Postgres database

import requests
import json
from pyspark.sql import SparkSession
import psycopg2
from pyspark.sql.functions import col

# Function to fetch and process airport data
def fetch_and_process_data(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to retrieve data with status code {response.status_code}")
        return []

    data = response.json()
    processed_data = []  # This will hold the processed records

    # Iterate directly over the items in the 'results' key
    for item in data.get('results', []):
        # Check if 'column_1' exists in the item and rename it to 'airport_code'
        if 'column_1' in item:
            item['airport_code'] = item.pop('column_1')
        processed_data.append(item)

    return processed_data



# Main function to orchestrate the ETL process
def main():
    spark = SparkSession.builder \
        .appName("Airport Data ETL") \
        .config("spark.jars", "/Users/kayleedekker/Downloads/postgresql-42.7.2.jar") \
        .getOrCreate()

    base_url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets"
    urls = [
        f"{base_url}/airports-code/records?limit=50&refine=country_name%3A%22United%20States%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Canada%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Argentina%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Brazil%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Mexico%22",
        f"{base_url}/airports-code/records?limit=10&refine=country_name%3A%22Peru%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Colombia%22"
    ]

    all_data = []
    for url in urls:
        all_data.extend(fetch_and_process_data(url))

    if not all_data:
        print("No data fetched.")
        return

    # Convert list of dicts to Spark DataFrame
    airports_df = spark.createDataFrame(all_data)

    # Change the data type of 'city_name_geo_name_id' from String to Integer
    airports_df = airports_df.withColumn("city_name_geo_name_id", col("city_name_geo_name_id").cast("integer"))

    # Define transformation logic if needed
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


    # Define JDBC URL and connection properties
    jdbc_url = "jdbc:postgresql://34.89.0.15:5432/Weather_database"
    connection_properties = {
        "user": "postgres",
        "password": "dataeng",
        "driver": "org.postgresql.Driver"
    }

    # Write DataFrame to Google Cloud SQL Postgres
    airports_df.write.jdbc(url=jdbc_url, table="airports", mode="append", properties=connection_properties)

    print("Data successfully written to database.")

    # Don't forget to close the Spark session
    spark.stop()


if __name__ == "__main__":
    main()

