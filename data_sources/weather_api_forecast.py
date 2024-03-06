# # In this script we are getting our "FUTURE" data
# # The steps are:
#     # 1. Read and extract city names and coordinates from airports_data.json.
#     # 2. Use these coordinates to call the OpenWeatherMap API and fetch the weather forecast data.
#     # 3. Save the forecast data for all cities into a single JSON file, including city names, coordinates, and detailed forecast information.

# import requests
# import json
# import random
#
# def extract_city_and_coordinates(json_filename):
#     """
#     Extracts city names and coordinates from the provided JSON structure.
#     """
#     extracted_data = []
#     try:
#         with open(json_filename, 'r') as file:
#             data = json.load(file)['results']
#         for item in data:
#             city_name = item['city_name']
#             lon = item['coordinates']['lon']
#             lat = item['coordinates']['lat']
#             extracted_data.append((city_name, lat, lon))
#     except FileNotFoundError:
#         print(f"File {json_filename} not found.")
#     except json.JSONDecodeError:
#         print(f"Error decoding JSON from file {json_filename}.")
#     return extracted_data
#
# def fetch_weather_forecast(api_key, locations):
#     all_forecasts = []
#     for city, lat, lon in locations:
#         url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={api_key}&units=imperial" # set to Fahrenheit
#         response = requests.get(url)
#         if response.status_code == 200:
#             forecast_data = response.json()
#             forecast_data['city']['name'] = city  # Override the city name with our city name
#             forecast_data['city']['coordinates'] = {'lat': lat, 'lon': lon}  # Add coordinates
#             all_forecasts.append(forecast_data)
#         else:
#             print(f"Failed to fetch the weather data for {city} at coordinates: {lat}, {lon}")
#     return all_forecasts
#
# def save_forecasts_to_json(forecasts, output_filename):
#     with open(output_filename, 'w') as file:
#         json.dump(forecasts, file, indent=4)
#
# def main():
#     json_filename = 'airports_data.json'
#     output_filename = 'weather_forecasts.json'
#     api_key = '906fcb9f2543d14922e383611e0c3862'
#
#     locations = extract_city_and_coordinates(json_filename)
#
#     # Let's decide how many locations we want to sample (for example, 10)
#     sample_size = 10
#     sampled_locations = random.sample(locations, min(sample_size, len(locations)))
#
#     forecasts = fetch_weather_forecast(api_key, sampled_locations)
#     save_forecasts_to_json(forecasts, output_filename)
#     print(f"Saved all forecasts to {output_filename}")
#
# if __name__ == "__main__":
#     main()

import requests
import json
import random

def extract_airport_code_and_coordinates(json_filename):
    """
    Extracts airport codes and coordinates from the provided JSON structure.
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
    with open(output_filename, 'w') as file:
        json.dump(forecasts, file, indent=4)

def main():
    json_filename = 'airports_data.json'
    output_filename = 'weather_forecasts.json'
    api_key = '906fcb9f2543d14922e383611e0c3862'

    locations = extract_airport_code_and_coordinates(json_filename)

    # Let's decide how many locations we want to sample (for example, 10)
    sample_size = 10
    sampled_locations = random.sample(locations, min(sample_size, len(locations)))

    forecasts = fetch_weather_forecast(api_key, sampled_locations)
    save_forecasts_to_json(forecasts, output_filename)
    print(f"Saved all forecasts to {output_filename}")

if __name__ == "__main__":
    main()
