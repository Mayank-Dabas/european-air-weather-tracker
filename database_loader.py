# database_loader.py

import os
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

def get_db_connection():
    """Establishes a connection to the MySQL database."""
    load_dotenv()
    try:
        connection = mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            database=os.getenv("DB_NAME")
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        return None

def load_df_to_db(df, table_name, connection):
    """
    Loads data from a Pandas DataFrame into a specified database table.
    Uses INSERT IGNORE to avoid duplicating rows based on the UNIQUE KEY.
    """
    if df is None or df.empty:
        print(f"No data to load into '{table_name}'. DataFrame is empty.")
        return

    cursor = connection.cursor()
    
    # Create the column list and the placeholder string (e.g., "%s, %s, %s")
    cols = ", ".join([f"`{col}`" for col in df.columns])
    val_placeholders = ", ".join(["%s"] * len(df.columns))
    
    # Construct the SQL query with INSERT IGNORE
    # INSERT IGNORE will skip inserting rows that violate a primary or unique key constraint
    sql = f"INSERT IGNORE INTO {table_name} ({cols}) VALUES ({val_placeholders})"
    
    # Convert DataFrame to a list of tuples, which is what the DB driver expects
    data_tuples = [tuple(row) for row in df.to_numpy()]

    try:
        cursor.executemany(sql, data_tuples)
        connection.commit()
        print(f"{cursor.rowcount} records were inserted into the '{table_name}' table.")
    except Error as e:
        print(f"Error loading data into '{table_name}': {e}")
        connection.rollback()
    finally:
        cursor.close()


if __name__ == "__main__":
    print("--- Testing database_loader.py ---")
    
    # 1. Import all necessary functions
    from data_collector import CITIES, get_weather_for_city, get_air_quality_for_city, IQVA_API_KEY
    from data_preparer import prepare_weather_data, prepare_air_quality_data
    
    # 2. Collect fresh data
    print("\nStep 1: Collecting fresh data...")
    test_weather_data = {}
    test_aq_data = {}
    for city, details in CITIES.items():
        weather = get_weather_for_city(city, details["lat"], details["lon"])
        if weather: test_weather_data[city] = weather
        
        aq = get_air_quality_for_city(city, details["lat"], details["lon"], IQVA_API_KEY)
        if aq: test_aq_data[city] = aq
    
    # 3. Prepare the data into DataFrames
    print("\nStep 2: Preparing data into clean DataFrames...")
    df_weather = prepare_weather_data(test_weather_data)
    df_air_quality = prepare_air_quality_data(test_aq_data)
    
    print("Weather DataFrame ready to load:")
    print(df_weather.head())
    print("\nAir Quality DataFrame ready to load:")
    print(df_air_quality.head())

    # 4. Load the data into the database
    print("\nStep 3: Loading DataFrames into MySQL Database...")
    db_connection = get_db_connection()
    
    if db_connection:
        load_df_to_db(df_weather, "weather_observations", db_connection)
        load_df_to_db(df_air_quality, "air_quality_observations", db_connection)
        db_connection.close()
        print("\nDatabase connection closed.")
    else:
        print("\nCould not establish database connection. Data was not loaded.")