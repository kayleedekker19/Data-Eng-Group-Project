import requests
from bs4 import BeautifulSoup
import pandas as pd

# Replace with your chosen location and date
LOCATION = 'KVGT'  # Example: airport code for London City Airport (EGLC) # or Las Vegas Airport (KVGT)
DATE = '2021-8'  # Format: YYYY-M

def scrape_weather_data(location, date):
    url = f'https://www.wunderground.com/history/monthly/{location}/date/{date}'
    response = requests.get(url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.content, 'html.parser')

        # The following selector will depend on the website's structure, which might change over time
        # This is a generic selector that may need adjustments
        table = soup.find('table', class_='days ng-star-inserted')
        if not table:
            print(f"No data found for {location} on {date}")
            return pd.DataFrame()

        headers = [header.text.strip() for header in table.findAll('th')]
        rows = table.findAll('tr')

        data = []
        for row in rows[1:]:  # Skip header row
            columns = row.findAll('td')
            if columns:
                data.append({headers[i]: columns[i].text.strip() for i in range(len(columns))})

        return pd.DataFrame(data)
    else:
        print(f"Failed to retrieve data: {response.status_code}")
        return pd.DataFrame()


def save_data_to_parquet(df, location, date):
    filename = f'weather_data_{location}_{date}.parquet'
    df.to_parquet(filename, index=False)
    print(f"Data saved to {filename}")

# def save_data_to_parquet(df, location, date):
#     filename = f'weather_data_{location}.parquet'
#     if df.empty:
#         print(f"No data to save for {location} on {date}")
#         return
#
#     # Check if the file exists
#     try:
#         existing_df = pd.read_parquet(filename)
#         combined_df = pd.concat([existing_df, df], ignore_index=True)
#     except FileNotFoundError:
#         combined_df = df
#
#     combined_df.to_parquet(filename, index=False)
#     print(f"Data saved to {filename}")


def main():
    df = scrape_weather_data(LOCATION, DATE)
    if not df.empty:
        save_data_to_parquet(df, LOCATION, DATE)


if __name__ == '__main__':
    main()

