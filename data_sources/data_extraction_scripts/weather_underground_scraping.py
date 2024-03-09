from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.common.exceptions import WebDriverException
import pandas as pd
import os
from io import StringIO  # Import StringIO for handling HTML strings
import json
import random


def init_webdriver():
    """Initializes and returns a Chrome WebDriver."""
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
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

def scrape_weather_data(location, start_year, end_year):
    """Scrapes weather data for a given location and date range, then saves to Parquet."""
    output_dir = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/manual_data_collected/historic_data"
    os.makedirs(output_dir, exist_ok=True)  # Ensure the directory exists

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):  # Loop through all months
            try:  # Add a try block to catch any exceptions
                date = f'{year}-{month}'
                url = f'https://www.wunderground.com/history/monthly/{location}/date/{date}'
                driver = init_webdriver()
                driver.get(url)
                accept_cookies(driver)

                result_dfs = []
                for i in range(3, 10):  # Adjust based on the number of tables you're interested in
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
                result = result.iloc[1:]  # Adjust based on the actual header row position

                new_column_names = [  # Define or adjust column names as needed
                    'Date', 'Temperature (°F), max', 'Temperature (°F), avg', 'Temperature (°F), min',
                    'Dew Point (°F), max', 'Dew Point (°F), avg', 'Dew Point (°F), min',
                    'Humidity (%), max', 'Humidity (%), avg', 'Humidity (%), min',
                    'Wind Speed (mph), max', 'Wind Speed (mph), avg', 'Wind Speed (mph), min',
                    'Pressure (in), max', 'Pressure (in), avg', 'Pressure (in), min',
                    'Precipitation (in)'
                ]

                # Check if the number of new columns matches the dataframe's columns
                if len(result.columns) != len(new_column_names):
                    print(
                        f"Column length mismatch: Dataframe has {len(result.columns)}, but trying to set {len(new_column_names)}")
                    continue  # Skip saving this result due to mismatch

                result.columns = new_column_names
                filename = os.path.join(output_dir, f"{location}_{date}.parquet")
                result.to_parquet(filename, index=False)
                print(f"Data saved for {location} for {date}")

            except Exception as e:  # Catch any error that occurs within the loop
                print(f"An error occurred for {location} in {year}-{month}: {e}")
                continue  # Continue to the next iteration
            finally:
                driver.quit()  # Ensure the driver is quit even if there's an error



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
    # Read airport codes from JSON
    # airport_codes = read_airport_codes("../manual_data_collected/airports_data.json")
    # working_codes = process_airport_codes(airport_codes)

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

    # Randomly select 5 airport codes
    selected_codes = random.sample(working_codes, 1)
    print(selected_codes)

    for airport_code in selected_codes:
        scrape_weather_data(airport_code, start_year, end_year)

    # # Define the range of years you're interested in
    # start_year = 2023
    # end_year = 2023
    #
    # # Loop through every xth airport code
    # x = 22  # Change depending on how much data - to minimise data we are only gathering data for every 5th airport code
    # for i in range(0, len(working_codes), x):
    #     airport_code = working_codes[i]
    #     scrape_weather_data(airport_code, start_year, end_year)


if __name__ == "__main__":
    main()
