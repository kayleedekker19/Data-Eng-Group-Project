from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, WebDriverException
import pandas as pd
import os
from pyspark.sql import SparkSession, functions as F, Window
import psycopg2
import json
import random
from io import StringIO
from airport_data import airport_etl
from pyspark.sql.functions import lit, to_date, concat, col
from pyspark.sql.types import *

# Function definitions for init_webdriver, accept_cookies, fetch_table_html remain unchanged
# def init_webdriver():
#     """Initializes and returns a Chrome WebDriver."""
#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
#     driver.maximize_window()
#     return driver

def init_webdriver():
    """Initializes and returns a Chrome WebDriver with headless mode."""
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Enable headless mode
    chrome_options.add_argument("--disable-gpu")  # Optional argument for headless mode
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    return driver


def accept_cookies(driver):
    """Navigates cookie consent iframe and accepts cookies, with increased wait time and debugging."""
    try:
        WebDriverWait(driver, 40).until(
            EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//*[@id="sp_message_iframe_977869"]')))
        WebDriverWait(driver, 40).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(),'Accept all')]"))).click()
        driver.switch_to.default_content()
    except (NoSuchElementException, TimeoutException) as e:
        print("Error accepting cookies or locating iframe:", e)
        # Debugging: You might want to take a screenshot or dump the page source here.
        # driver.save_screenshot("debug_screenshot.png")
        # with open("debug_page.html", "w") as f:
        #     f.write(driver.page_source)
        driver.quit()
        raise


def fetch_table_html(driver, xpath, wait_time=40):
    """Fetches and returns the HTML of a table specified by the xpath, with increased wait time for reliability."""
    try:
        table_element = WebDriverWait(driver, wait_time).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        return table_element.get_attribute("outerHTML")
    except TimeoutException as e:
        print(f"Error fetching table HTML for xpath {xpath}: ", e)
        driver.quit()
        raise

def scrape_weather_data(spark, location, start_year, end_year):
    """Scrapes weather data for a given location and date range, directly returning a Spark DataFrame."""
    driver = init_webdriver()
    scraped_data = []

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):  # Loop through all months
            try:
                date = f'{year}-{month}'
                url = f'https://www.wunderground.com/history/monthly/{location}/date/{date}'
                driver.get(url)
                accept_cookies(driver)

                for i in range(3, 10):  # Adjust based on the number of tables you're interested in
                    try:
                        table_html = fetch_table_html(driver, f"(//table)[{i}]")
                        if table_html:
                            df = pd.read_html(StringIO(table_html))[0]
                            if not df.empty:
                                temp_spark_df = spark.createDataFrame(df)
                                # Rename columns as specified
                                temp_spark_df = temp_spark_df.toDF(
                                    'Date', 'Temperature (°F), max', 'Temperature (°F), avg', 'Temperature (°F), min',
                                    'Dew Point (°F), max', 'Dew Point (°F), avg', 'Dew Point (°F), min',
                                    'Humidity (%), max', 'Humidity (%), avg', 'Humidity (%), min',
                                    'Wind Speed (mph), max', 'Wind Speed (mph), avg', 'Wind Speed (mph), min',
                                    'Pressure (in), max', 'Pressure (in), avg', 'Pressure (in), min',
                                    'Precipitation (in)'
                                ).withColumn("airport_code", lit(location)) \
                                .withColumn("Year_Month", lit(f"{year}-{month:02d}")) \
                                .withColumn("Date", to_date(concat(col("Year_Month"), lit("-"), col("Date")), "yyyy-MM-d"))

                                # Drop the temporary Year_Month column
                                temp_spark_df = temp_spark_df.drop("Year_Month")
                                scraped_data.append(temp_spark_df)
                    except Exception as e:
                        print(f"Error processing table for {location} in {date}: {e}")
                        continue  # Move onto the next table or month
            except Exception as e:
                print(f"An error occurred while scraping {location} in {year}-{month}: {e}")
                continue  # Move onto the next month or year
            finally:
                driver.quit()

    # Combine all Spark DataFrames in the scraped_data list
    if scraped_data:
        final_df = scraped_data[0]
        for df in scraped_data[1:]:
            final_df = final_df.unionByName(df)
        return final_df
    else:
        print(f"No data scraped for {location} in the specified date range.")
        # Optionally, return None or handle this case in the calling function
        return None

def check_airport_code(url):
    """Checks if the webpage for the given URL loads successfully or returns an error."""
    driver = init_webdriver()
    try:
        driver.get(url)
        # Check if "Error 404: Page Not Found" is present in the page
        if "Error 404: Page Not Found" in driver.page_source:
            return False
    except WebDriverException as e:
        print(f"WebDriverException encountered for URL {url}: {e}")
        return False
    finally:
        driver.quit()
    return True

def process_airport_codes(airport_codes):
    working_codes = []
    non_working_codes = []

    for code in airport_codes:
        url = f'https://www.wunderground.com/history/monthly/{code}/date/2023-1'
        if check_airport_code(url):
            working_codes.append(code)
        else:
            non_working_codes.append(code)

    # print(f"Number of working codes: {len(working_codes)}")
    # print(f"Number of non-working codes: {len(non_working_codes)}")
    return working_codes


from pyspark.sql import SparkSession
import random
import psycopg2


def main():
    # Initialize SparkSession
    spark_jars_path = os.getenv('SPARK_JARS_PATH')
    spark = SparkSession.builder \
        .appName("Historic Weather Data ETL") \
        .config("spark.jars", spark_jars_path) \
        .getOrCreate()

    # # Fetch and process airport data
    # # For simplicity we are going to upload the list of working codes
    # # This reduces the number of requests made to the wesbite
    # base_url = os.getenv('API_BASE_URL')
    # urls = [
    #     f"{base_url}/airports-code/records?limit=50&refine=country_name%3A%22United%20States%22",
    #     f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Canada%22",
    #     f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Argentina%22",
    #     f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Brazil%22",
    #     f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Mexico%22",
    #     f"{base_url}/airports-code/records?limit=10&refine=country_name%3A%22Peru%22",
    #     f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Colombia%22"
    # ]
    #
    # # Assuming airport_etl.fetch_and_process_data(url) is a function that you have defined
    # # that fetches and processes the data from the given URL and returns a list of dictionaries
    # airports_data = []
    # for url in urls:
    #     airports_data.extend(airport_etl.fetch_and_process_data(url))
    #
    # # Extract only the "airport_codes" from airports_data
    # airport_codes = [airport['airport_code'] for airport in airports_data]
    #
    # # Run the "process_airport_codes" to get working codes
    # working_codes = process_airport_codes(airport_codes)
    # print(working_codes)

    # Manually list the working codes:
    working_codes = [
      'OMA', 'JHM', 'IML', 'KMO', 'ACK', 'SZL', 'EMP', 'FRI', 'TCS', 'PAO', 'POE', 'OTM', 'WNA', 'SPS', 'KKB',
      'ATL', 'DAL', 'ABI', 'MEI', 'MRF', 'DRE', 'FDY', 'LCH', 'LRJ', 'GED', 'BKD', 'FSI', 'DAG', 'JOT', 'MDJ',
      'SPZ', 'SLK', 'BIH', 'WAL', 'ELP', 'AIK', 'OEO', 'SCM', 'BQV', 'JCT', 'YUB', 'YQB', 'YKX', 'YSU', 'YSE',
      'YRL', 'YRB', 'ILF', 'YPW', 'ZGI', 'YFR', 'YXR', 'YYL', 'YYD', 'AFA', 'RHD', 'JNI', 'EQS', 'PUD', 'ROS',
      'JSM', 'USH', 'JUJ', 'LGS', 'COC', 'NEC', 'LCM', 'UAQ', 'SDE', 'VDC', 'TBT', 'CAC', 'MEA', 'MNX', 'ERM',
      'PAV', 'AQA', 'PMG', 'RIA', 'TSL', 'TGZ', 'QRO', 'JAL', 'GYM', 'CVM', 'LOV', 'LAP', 'BJX', 'PPE', 'SLP',
      'VER', 'MZT', 'ACN', 'PCL', 'JAU', 'AYP', 'RIM', 'BLP', 'ILQ', 'TCQ', 'TRU', 'CHM', 'JUL', 'VGZ',
      'GPI', 'LPD', 'PUU', 'PSO', 'PVA', 'PCR', 'BAQ', 'APO'
      ] # 'SRL',

    # Take a random sample of 5 working codes
    n = 1 # Set the number of random samples of codes
    sampled_codes = random.sample(working_codes, min(len(working_codes), n))
    print(sampled_codes)

    # Define the schema based on the expected structure of your scraped data
    schema = StructType([
        StructField("airport_code", StringType(), True),
        StructField("Date", DateType(), True),
        StructField("Temperature (°F), max", FloatType(), True),
        StructField("Temperature (°F), avg", FloatType(), True),
        StructField("Temperature (°F), min", FloatType(), True),
        StructField("Dew Point (°F), max", FloatType(), True),
        StructField("Dew Point (°F), avg", FloatType(), True),
        StructField("Dew Point (°F), min", FloatType(), True),
        StructField("Humidity (%), max", IntegerType(), True),
        StructField("Humidity (%), avg", IntegerType(), True),
        StructField("Humidity (%), min", IntegerType(), True),
        StructField("Wind Speed (mph), max", FloatType(), True),
        StructField("Wind Speed (mph), avg", FloatType(), True),
        StructField("Wind Speed (mph), min", FloatType(), True),
        StructField("Pressure (in), max", FloatType(), True),
        StructField("Pressure (in), avg", FloatType(), True),
        StructField("Pressure (in), min", FloatType(), True),
        StructField("Precipitation (in)", FloatType(), True),
    ])

    # Initialize an empty DataFrame with the defined schema
    all_scraped_df = spark.createDataFrame([], schema=schema)

    # Your existing logic for scraping and accumulating weather data
    for code in sampled_codes:
        scraped_df = scrape_weather_data(spark, code, 2023, 2023)
        # Ensure scraped_df is not None before union
        if scraped_df is not None:
            all_scraped_df = all_scraped_df.unionByName(scraped_df, allowMissingColumns=True)

    # Apply transformations to the combined scraped data
    historic_df = all_scraped_df.withColumnRenamed("airport_code", "airport_code") \
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

    # Define JDBC URL and connection properties
    jdbc_url = os.getenv('DATABASE_URL')
    connection_properties = {
        "user": os.getenv('DATABASE_USER'),
        "password": os.getenv('DATABASE_PASSWORD'),
        "driver": "org.postgresql.Driver"
    }

    # Write the DataFrame to PostgreSQL
    historic_df.write.jdbc(url=jdbc_url, table="historic_weather", mode="append", properties=connection_properties)

    print("Data successfully written to database.")

    # Stop the Spark session
    spark.stop()


if __name__ == "__main__":
    main()

