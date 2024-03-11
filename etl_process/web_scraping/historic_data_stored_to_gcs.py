from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import WebDriverException
import pandas as pd
from io import StringIO  # Import StringIO for handling HTML strings
import json
import random
from google.cloud import storage
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()
GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

def init_webdriver():
    """Initializes and returns a Chrome WebDriver."""
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    return driver

# For larger amounts of data/scraping, headless mode can be used to run the webdriver without  visible browser window.
# Allows for faster execution and reduced resource consumption
# def init_webdriver():
#     """Initializes and returns a Chrome WebDriver with headless mode."""
#     chrome_options = Options()
#     chrome_options.add_argument("--headless")  # Enable headless mode
#     chrome_options.add_argument("--disable-gpu")  # Optional argument for headless mode
#     driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
#     return driver

def accept_cookies(driver):
    """Navigates to cookie consent iframe and accepts, with a wait time to allow for the webdriver to load."""
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
    """Fetches and returns the HTML of a table specified by the xpath, again increased wait time for reliability."""
    try:
        table_element = WebDriverWait(driver, wait_time).until(EC.visibility_of_element_located((By.XPATH, xpath)))
        return table_element.get_attribute("outerHTML")
    except TimeoutException as e:
        print(f"Error fetching table HTML for xpath {xpath}: ", e)
        driver.quit()
        raise


def scrape_weather_data(location, start_year, end_year):
    """Scrapes weather data for a given location and date range, then saves to Google Cloud Storage."""
    BUCKET_NAME = os.getenv('BUCKET_NAME')  # Make sure to set this environment variable
    if not BUCKET_NAME:
        raise ValueError("BUCKET_NAME environment variable not set.")
    base_path = "weather/historic_data"  # Adjust as needed
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):  # Loop through all months 1 to 12
            try:  # Catch any exceptions
                date = f'{year}-{month}'
                url = f'https://www.wunderground.com/history/monthly/{location}/date/{date}'
                driver = init_webdriver()
                driver.get(url)
                accept_cookies(driver)

                result_dfs = []
                for i in range(3, 10):  # On the scraped webpage, tables 3 to 9 were of interest
                    try:
                        table_html = fetch_table_html(driver, f'(//table)[{i}]')
                        df = pd.read_html(StringIO(table_html))[0]
                        result_dfs.append(df)
                    except Exception as e:
                        print(f"Error processing table {i}: ", e)
                        continue

                if not result_dfs:  # Check if the list is empty
                    print(f"No data found for {location} in {date}. Skipping.")
                    continue

                result = pd.concat(result_dfs, axis=1)
                result = result.iloc[1:]  # Remove first row, old sub-column names, but will be renamed next
                # Define new column names to fit the scraped data
                new_column_names = [
                    'Date', 'Temperature (°F), max', 'Temperature (°F), avg', 'Temperature (°F), min',
                    'Dew Point (°F), max', 'Dew Point (°F), avg', 'Dew Point (°F), min',
                    'Humidity (%), max', 'Humidity (%), avg', 'Humidity (%), min',
                    'Wind Speed (mph), max', 'Wind Speed (mph), avg', 'Wind Speed (mph), min',
                    'Pressure (in), max', 'Pressure (in), avg', 'Pressure (in), min',
                    'Precipitation (in)'
                ]
                # Check if the number of new columns names matches the dataframe's columns
                if len(result.columns) != len(new_column_names):
                    print(
                        f"Column length mismatch: Dataframe has {len(result.columns)}, but trying to set {len(new_column_names)}"
                    )
                    continue

                result.columns = new_column_names
                # Define the full path within the bucket for the file
                file_path = f"{base_path}/{location}_{date}.parquet"
                # Save DataFrame to a parquet file buffer
                buffer = StringIO()
                result.to_parquet(buffer, index=False)
                # Upload the buffer content to GCS
                blob = bucket.blob(file_path)
                blob.upload_from_string(buffer.getvalue(), content_type='application/octet-stream')
                print(f"Data saved for {location} for {date} in Google Cloud Storage")

            except Exception as e:
                print(f"An error occurred for {location} in {year}-{month}: {e}")
                continue
            finally:
                driver.quit()


def read_airport_codes(json_filename):
    """Reads airport codes from a given JSON file."""
    with open(json_filename, 'r') as file:
        data = json.load(file)
    return [item['airport_code'] for item in data.get('results', [])]


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
    """Processes all airport codes from JSON file and checks whether they work"""
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

def main():
    # For full data analysis - Read airport codes from JSON
    # airport_codes = read_airport_codes("../manual_data_collected/airports_data.json")
    # working_codes = process_airport_codes(airport_codes)
    # To minimize current gathered data, working codes provided already
    working_codes = [
        'OMA', 'JHM', 'IML', 'KMO', 'ACK', 'SZL', 'EMP', 'FRI', 'TCS', 'PAO', 'POE', 'OTM', 'WNA', 'SPS', 'KKB',
        'ATL', 'DAL', 'ABI', 'MEI', 'MRF', 'DRE', 'FDY', 'LCH', 'LRJ', 'GED', 'BKD', 'FSI', 'DAG', 'JOT', 'MDJ',
        'SPZ', 'SLK', 'BIH', 'WAL', 'ELP', 'AIK', 'OEO', 'SCM', 'BQV', 'JCT', 'YUB', 'YQB', 'YKX', 'YSU', 'YSE',
        'YRL', 'YRB', 'ILF', 'YPW', 'ZGI', 'YFR', 'YXR', 'YYL', 'YYD', 'AFA', 'RHD', 'JNI', 'EQS', 'PUD', 'ROS',
        'JSM', 'USH', 'JUJ', 'LGS', 'COC', 'NEC', 'LCM', 'UAQ', 'SDE', 'VDC', 'TBT', 'CAC', 'MEA', 'MNX', 'ERM',
        'PAV', 'AQA', 'PMG', 'RIA', 'TSL', 'TGZ', 'QRO', 'JAL', 'GYM', 'CVM', 'LOV', 'LAP', 'BJX', 'PPE', 'SLP',
        'VER', 'MZT', 'ACN', 'SRL', 'PCL', 'JAU', 'AYP', 'RIM', 'BLP', 'ILQ', 'TCQ', 'TRU', 'CHM', 'JUL', 'VGZ',
        'GPI', 'LPD', 'PUU', 'PSO', 'PVA', 'PCR', 'BAQ', 'APO'
    ]

    # Define the range of years you're interested in
    start_year = 2023
    end_year = 2023

    # Randomly select airport codes - to minimize gathered data
    selected_codes = random.sample(working_codes, 1)
    print(f"The selected codes are: {selected_codes}")

    for airport_code in selected_codes:
        scrape_weather_data(airport_code, start_year, end_year)

if __name__ == "__main__":
    main()
