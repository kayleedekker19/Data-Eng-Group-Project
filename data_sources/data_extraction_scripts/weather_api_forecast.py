# # In this script we are getting our "FUTURE" forecast data
# # The steps are:
#     # 1. Read and extract airport codes and coordinates from airports_data.json.
#     # 2. Use these coordinates to call the OpenWeatherMap API and fetch the weather forecast data.
#     # 3. Save the forecast data for all cities into a single JSON file, including airport codes, coordinates, and detailed forecast information.

# Load libraries
import requests
import json
import random
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

def extract_airport_code_and_coordinates(json_filename):
    """
    Extracts airport codes and coordinates from the provided JSON structure.

    Parameters:
    - json_filename (str): The filename of the JSON file containing airport data.

    Returns:
    List[Tuple[str, float, float]]: A list of tuples containing airport code, latitude, and longitude.
    """
    extracted_data = []
    try:
        with open(json_filename, 'r') as file:
            data = json.load(file)['results']
        for item in data:
            airport_code = item['airport_code']  # Use airport_code instead of city_name
            lon = item['coordinates']['lon']
            lat = item['coordinates']['lat']
            extracted_data.append((airport_code, lat, lon))
    except FileNotFoundError:
        print(f"File {json_filename} not found.")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from file {json_filename}.")
    return extracted_data

def fetch_weather_forecast(api_key, locations):
    """
    Fetches weather forecasts for specified locations from the OpenWeatherMap API.

    Parameters:
    - api_key (str): The API key for accessing the OpenWeatherMap API.
    - locations (List[Tuple[str, float, float]]): A list of tuples containing airport code, latitude, and longitude.

    Returns:
    List[Dict]: A list of dictionaries containing forecast data for each specified location.
    """
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

def save_forecasts_to_json(forecasts, output_filename):
    """
    Saves the fetched weather forecasts to a JSON file.

    Parameters:
    - forecasts (List[Dict]): A list of dictionaries containing forecast data.
    - output_filename (str): The filename for the output JSON file.
    """
    with open(output_filename, 'w') as file:
        json.dump(forecasts, file, indent=4)

def main():
    """
    Main function orchestrating the execution of the script.
    """
    json_filename = '../manual_data_collected/airports_data.json'
    output_filename = '../manual_data_collected/weather_forecasts.json'
    api_key = os.getenv('FORECAST_API_KEY')

    # Extract airport codes and coordinates from the JSON file
    locations = extract_airport_code_and_coordinates(json_filename)

    # Decide how many locations to sample and fetch weather forecasts
    sample_size = 10
    sampled_locations = random.sample(locations, min(sample_size, len(locations)))

    # Save the fetched forecasts to a JSON file
    forecasts = fetch_weather_forecast(api_key, sampled_locations)
    save_forecasts_to_json(forecasts, output_filename)
    print(f"Saved all forecasts to {output_filename}")

if __name__ == "__main__":
    main()
