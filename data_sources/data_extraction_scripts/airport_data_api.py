import requests
import json

"""
This script fetches airport data from the Datasoft API for selected countries in North and South America, 
processes the data by renaming the 'column_1' to 'airport_code,'and then saves the modified data to a JSON file. 
The processed data includes critical information about airport locations and their corresponding codes. 

Usage:
- Ensure you have the required libraries installed: `requests` and `json`.
- Run the script to fetch, process, and save airport data to 'airports_data_2.json'.
"""

def fetch_and_process_data(url):
    # Make the API request
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code != 200:
        print("Failed to retrieve data", response.status_code)
        return

    # Parse the response JSON
    data = response.json()

    # Process each item to rename 'column_1' to 'airport_code' and maintain order
    processed_data = []
    for item in data.get('results', []):
        # Create a new dictionary where 'airport_code' is inserted where 'column_1' used to be
        new_item = {}
        for key, value in item.items():
            if key == 'column_1':
                new_item['airport_code'] = value  # Insert with new key name
            else:
                new_item[key] = value  # Copy other items as is

        processed_data.append(new_item)

    # Replace the original list with the processed list
    data['results'] = processed_data

    return data

def save_data_to_json(data, filename):
    # Save the modified data to a JSON file
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)
    print(f"Data successfully saved to {filename}")

def main():
    # Define the base URL for API requests
    base_url = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets"

    # Specify URLs for different countries, limiting the number of records
    urls = [
        f"{base_url}/airports-code/records?limit=50&refine=country_name%3A%22United%20States%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Canada%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Argentina%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Brazil%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Mexico%22",
        f"{base_url}/airports-code/records?limit=10&refine=country_name%3A%22Peru%22",
        f"{base_url}/airports-code/records?limit=20&refine=country_name%3A%22Colombia%22"
    ]
    all_data = {"results": []}  # Initialize a dictionary to hold all data

    # Iterate through the specified URLs and fetch/process data
    for url in urls:
        processed_data = fetch_and_process_data(url)
        if processed_data is not None:
            all_data["results"].extend(processed_data["results"])  # Append the results from each URL to the all_data list

    # Check if there is data and save it to a JSON file
    if all_data["results"]:
        save_data_to_json(all_data, "../not_needed_py_files/airports_data_2.json")

if __name__ == "__main__":
    main()
