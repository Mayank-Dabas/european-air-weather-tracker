# main_pipeline.py

import logging
from datetime import datetime

# Import the functions from our other scripts
from data_collector import CITIES, get_weather_for_city, get_air_quality_for_city, IQVA_API_KEY
from data_preparer import prepare_weather_data, prepare_air_quality_data
from database_loader import get_db_connection, load_df_to_db

# Set up basic logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"), # Log to a file
        logging.StreamHandler() # Also print to the console
    ]
)

def run_etl_pipeline():
    """
    Runs the complete ETL pipeline from data collection to database loading.
    """
    logging.info("--- Starting ETL Pipeline ---")

    try:
        # --- 1. EXTRACT ---
        logging.info("Step 1: Collecting fresh data...")
        all_weather_data = {}
        all_aq_data = {}
        for city, details in CITIES.items():
            weather = get_weather_for_city(city, details["lat"], details["lon"])
            if weather: all_weather_data[city] = weather

            aq = get_air_quality_for_city(city, details["lat"], details["lon"], IQVA_API_KEY)
            if aq: all_aq_data[city] = aq

        if not all_weather_data and not all_aq_data:
            logging.warning("No data collected. Halting pipeline.")
            return

        # --- 2. TRANSFORM ---
        logging.info("Step 2: Preparing data into clean DataFrames...")
        df_weather = prepare_weather_data(all_weather_data)
        df_air_quality = prepare_air_quality_data(all_aq_data)
        logging.info("Data preparation complete.")

        # --- 3. LOAD ---
        logging.info("Step 3: Loading DataFrames into MySQL Database...")
        db_connection = get_db_connection()

        if db_connection:
            load_df_to_db(df_weather, "weather_observations", db_connection)
            load_df_to_db(df_air_quality, "air_quality_observations", db_connection)
            db_connection.close()
            logging.info("Database connection closed.")
        else:
            logging.error("Could not establish database connection. Data was not loaded.")

    except Exception as e:
        logging.error(f"An unexpected error occurred during the ETL process: {e}", exc_info=True)

    logging.info("--- ETL Pipeline Finished ---")


if __name__ == "__main__":
    run_etl_pipeline()