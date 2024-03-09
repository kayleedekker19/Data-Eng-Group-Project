import pandas as pd
import numpy as np
import os
from datetime import datetime


# Function to generate synthetic weather data
def generate_synthetic_weather_data(month, year):
    # Generate dates for the given month and year
    num_days = pd.Period(f'{year}-{month}').days_in_month
    dates = pd.date_range(start=f"{year}-{month:02d}-01", periods=num_days, freq='D')

    # Generate synthetic data
    data = {
        'Time': dates.strftime('%B %d'),
        'Temperature (°F) Max': np.random.uniform(80, 100, size=len(dates)),
        'Temperature (°F) Avg': np.random.uniform(70, 90, size=len(dates)),
        'Temperature (°F) Min': np.random.uniform(60, 80, size=len(dates)),
        'Dew Point (°F) Max': np.random.uniform(50, 70, size=len(dates)),
        'Dew Point (°F) Avg': np.random.uniform(40, 60, size=len(dates)),
        'Dew Point (°F) Min': np.random.uniform(30, 50, size=len(dates)),
        'Humidity (%) Max': np.random.uniform(40, 100, size=len(dates)),
        'Humidity (%) Avg': np.random.uniform(30, 90, size=len(dates)),
        'Humidity (%) Min': np.random.uniform(20, 80, size=len(dates)),
        'Wind Speed (mph) Max': np.random.uniform(0, 20, size=len(dates)),
        'Wind Speed (mph) Avg': np.random.uniform(0, 15, size=len(dates)),
        'Wind Speed (mph) Min': np.random.uniform(0, 10, size=len(dates)),
        'Pressure (in) Max': np.random.uniform(27, 30, size=len(dates)),
        'Pressure (in) Avg': np.random.uniform(26, 29, size=len(dates)),
        'Pressure (in) Min': np.random.uniform(25, 28, size=len(dates)),
        'Precipitation (in) Total': np.random.uniform(0, 1, size=len(dates)).round(2)
    }

    # Convert to DataFrame
    df = pd.DataFrame(data)
    return df


# Function to save DataFrame to Parquet
def save_df_to_parquet(df, filename):
    df.to_parquet(filename, index=False)
    print(f"Data saved to {filename}")


# Generating data for 5 different months and years
for year in range(2019, 2024):
    for month in range(1, 6):  # Let's say we want to generate data for the first 5 months
        df = generate_synthetic_weather_data(month, year)
        filename = f'weather_data_{year}_{month:02d}.parquet'
        save_df_to_parquet(df, filename)
