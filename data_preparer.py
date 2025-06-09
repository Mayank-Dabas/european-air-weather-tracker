# data_preparer.py

import pandas as pd
from datetime import datetime

def prepare_weather_data(all_weather_data):
    """
    Transforms the raw weather data dictionary into a clean Pandas DataFrame.
    """
    all_weather_records = []
    
    # Loop through the raw data dictionary (key=city_name, value=json_data)
    for city_name, data in all_weather_data.items():
        try:
            # Extract only the fields we need from the nested JSON
            record = {
                "city": city_name,
                "observation_time": datetime.fromtimestamp(data['dt']), # Convert timestamp to datetime
                "temperature_celsius": data['main']['temp'],
                "feels_like_celsius": data['main']['feels_like'],
                "humidity_percent": data['main']['humidity'],
                "weather_description": data['weather'][0]['description'],
                "wind_speed_mps": data['wind']['speed'],
                "clouds_percent": data['clouds']['all']
            }
            all_weather_records.append(record)
        except KeyError as e:
            print(f"KeyError while processing weather data for {city_name}: {e}")

    # Create a Pandas DataFrame from our list of records
    df = pd.DataFrame(all_weather_records)
    return df

def prepare_air_quality_data(all_aq_data):
    """
    Transforms the raw air quality data dictionary into a clean Pandas DataFrame.
    """
    all_aq_records = []

    # Loop through the raw data dictionary
    for city_name, data in all_aq_data.items():
        try:
            # The data is nested under 'current' and 'pollution' keys
            pollution_data = data['current']['pollution']
            
            # Extract the fields we need
            record = {
                "city": city_name,
                # The timestamp is a string, so we need to parse it into a datetime object
                "observation_time": datetime.strptime(pollution_data['ts'], '%Y-%m-%dT%H:%M:%S.%fZ'),
                "aqi_us": pollution_data['aqius'],
                "main_pollutant_us": pollution_data['mainus']
            }
            all_aq_records.append(record)
        except (KeyError, TypeError) as e:
            # TypeError can happen if data is None
            print(f"Error while processing air quality data for {city_name}: {e}")
            
    # Create a Pandas DataFrame
    df = pd.DataFrame(all_aq_records)
    return df

# You can add a test block to see this script in action
if __name__ == "__main__":
    # This part allows us to test this file independently
    print("--- Testing data_preparer.py ---")
    
    # We will import the data collection functions to get live data for testing
    from data_collector import CITIES, get_weather_for_city, get_air_quality_for_city, IQVA_API_KEY
    
    # --- Collect fresh data for the test ---
    test_weather_data = {}
    test_aq_data = {}
    for city, details in CITIES.items():
        weather = get_weather_for_city(city, details["lat"], details["lon"])
        if weather: test_weather_data[city] = weather
        
        aq = get_air_quality_for_city(city, details["lat"], details["lon"], IQVA_API_KEY)
        if aq: test_aq_data[city] = aq

    # --- Prepare the collected data ---
    df_weather = prepare_weather_data(test_weather_data)
    df_air_quality = prepare_air_quality_data(test_aq_data)

    # --- Print the results ---
    print("\n--- Clean Weather DataFrame ---")
    print(df_weather.head()) # .head() prints the first 5 rows
    print("\n")
    df_weather.info() # .info() gives a summary of the DataFrame

    print("\n--- Clean Air Quality DataFrame ---")
    print(df_air_quality.head())
    print("\n")
    df_air_quality.info()