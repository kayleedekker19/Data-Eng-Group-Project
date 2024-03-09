import requests
import json

# This is where we input our parameters
# We'd want to map the longitude and latitude values to our other datasets
API_KEY = 'aed2c774a1add7281d8f579bcadc8c7f'
LATITUDE = '35'  # Example latitude
LONGITUDE = '139'  # Example longitude
UNITS = 'metric'  # We're using 'metric' for Celsius and meters/sec

def fetch_weather_forecast(latitude, longitude, api_key, units='metric'):
    base_url = "https://pro.openweathermap.org/data/2.5/forecast/climate"
    # https://pro.openweathermap.org/data/2.5/forecast/climate?lat={lat}&lon={lon}&appid={API key}
    params = {
        'lat': latitude,
        'lon': longitude,
        'appid': api_key,
        'units': units,
        'mode': 'json'  # Ensures the response is in JSON format
    }
    response = requests.get(base_url, params=params)
    return response.json()

def save_forecast_to_json(forecast_data, file_name='weather_forecast.json'):
    with open(file_name, 'w') as f:
        json.dump(forecast_data, f, indent=4)

def main():
    forecast_data = fetch_weather_forecast(LATITUDE, LONGITUDE, API_KEY, UNITS)
    save_forecast_to_json(forecast_data)
    print(f"Weather forecast saved to weather_forecast.json")

if __name__ == "__main__":
    main()

