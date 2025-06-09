# data_collector.py

import os
import requests
from dotenv import load_dotenv

# --- SETUP ---
# We still load dotenv for the IQAir key
load_dotenv()

# We only need the IQAir key from the environment now
IQVA_API_KEY = os.getenv("IQVA_API_KEY")

# Define the European cities we want to track
CITIES = {
    "Munich": {"lat": 48.1351, "lon": 11.5820},
    "Berlin": {"lat": 52.5200, "lon": 13.4050},
    "Paris": {"lat": 48.8566, "lon": 2.3522},
    "Vienna": {"lat": 48.2082, "lon": 16.3738}
}

# --- FUNCTIONS ---

def get_weather_for_city(city_name, lat, lon):
    """Fetches current weather data for a specified city from OpenWeatherMap."""

    # We will keep the hardcoded key to ensure consistency
    owm_key = "01eb72a396030664e172eb129b5daa91" 

    base_url = "https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": owm_key,
        "units": "metric"
    }

    #  Add a header to make our script look like a browser ---
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"Fetching weather for {city_name} with browser header...")
    try:
        # We add the 'headers=headers' argument to the request call
        response = requests.get(base_url, params=params, headers=headers)
        response.raise_for_status()
        print(f"-> Weather for {city_name}: Success")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"--> ERROR fetching weather for {city_name}: {e}")
        return None

def get_air_quality_for_city(city_name, lat, lon, api_key):
    """Fetches air quality data for the nearest city from IQAir."""
    base_url = "http://api.airvisual.com/v2/nearest_city"
    params = {
        "lat": lat,
        "lon": lon,
        "key": api_key
    }

    print(f"Fetching air quality for {city_name}...")
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()

        if data.get("status") == "success":
            print(f"-> Air Quality for {city_name}: Success")
            return data.get("data")
        else:
            error_message = data.get("data", {}).get("message", "Unknown API error")
            print(f"--> API ERROR for {city_name}: {error_message}")
            return None

    except requests.exceptions.RequestException as e:
        print(f"--> HTTP ERROR fetching air quality for {city_name}: {e}")
        return None

# --- MAIN EXECUTION ---

if __name__ == "__main__":
    print("--- Starting Data Collection ---")

    all_weather_data = {}
    all_aq_data = {}

    for city, details in CITIES.items():
        # Notice we no longer pass a key to get_weather_for_city
        weather = get_weather_for_city(city, details["lat"], details["lon"])
        if weather:
            all_weather_data[city] = weather

        aq = get_air_quality_for_city(city, details["lat"], details["lon"], IQVA_API_KEY)
        if aq:
            all_aq_data[city] = aq

    print("\n--- Data Collection Finished ---")

    # We will print the collected data to verify it's working
    print("\n--- Collected Weather Data (RAW) ---")
    print(all_weather_data)

    print("\n--- Collected Air Quality Data (RAW) ---")
    print(all_aq_data)