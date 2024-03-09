# In this script, we find out what airport codes work with Underground Weather

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import WebDriverException
import json

def init_webdriver():
    """Initializes and returns a Chrome WebDriver."""
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.maximize_window()
    return driver

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

    print(f"Number of working codes: {len(working_codes)}")
    print(f"Number of non-working codes: {len(non_working_codes)}")
    return working_codes, non_working_codes

def read_airport_codes(json_filename):
    """Reads airport codes from a given JSON file."""
    with open(json_filename, 'r') as file:
        data = json.load(file)
    return [item['airport_code'] for item in data.get('results', [])]

def main():
    json_filename = "/Users/kayleedekker/PycharmProjects/DataEngineeringProject/data_sources/manual_data_collected/airports_data.json"
    airport_codes = read_airport_codes(json_filename)
    working_codes, non_working_codes = process_airport_codes(airport_codes)

    # Optional: Print or process the lists of working and non-working airport codes as needed
    print(f"Working codes: {working_codes}")
    print(f"Non-working codes: {non_working_codes}")

if __name__ == "__main__":
    main()

# Working codes = [
#   'OMA', 'JHM', 'IML', 'KMO', 'ACK', 'SZL', 'EMP', 'FRI', 'TCS', 'PAO', 'POE', 'OTM', 'WNA', 'SPS', 'KKB',
#   'ATL', 'DAL', 'ABI', 'MEI', 'MRF', 'DRE', 'FDY', 'LCH', 'LRJ', 'GED', 'BKD', 'FSI', 'DAG', 'JOT', 'MDJ',
#   'SPZ', 'SLK', 'BIH', 'WAL', 'ELP', 'AIK', 'OEO', 'SCM', 'BQV', 'JCT', 'YUB', 'YQB', 'YKX', 'YSU', 'YSE',
#   'YRL', 'YRB', 'ILF', 'YPW', 'ZGI', 'YFR', 'YXR', 'YYL', 'YYD', 'AFA', 'RHD', 'JNI', 'EQS', 'PUD', 'ROS',
#   'JSM', 'USH', 'JUJ', 'LGS', 'COC', 'NEC', 'LCM', 'UAQ', 'SDE', 'VDC', 'TBT', 'CAC', 'MEA', 'MNX', 'ERM',
#   'PAV', 'AQA', 'PMG', 'RIA', 'TSL', 'TGZ', 'QRO', 'JAL', 'GYM', 'CVM', 'LOV', 'LAP', 'BJX', 'PPE', 'SLP',
#   'VER', 'MZT', 'ACN', 'SRL', 'PCL', 'JAU', 'AYP', 'RIM', 'BLP', 'ILQ', 'TCQ', 'TRU', 'CHM', 'JUL', 'VGZ',
#   'GPI', 'LPD', 'PUU', 'PSO', 'PVA', 'PCR', 'BAQ', 'APO'
#   ]

