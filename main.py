import os
import requests
import json
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery

# --- Configuration ---
# Replace with your actual project ID, bucket name, dataset ID, and table ID
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "weather-data-pipeline-project") # e.g., weather-data-pipeline-project-12345
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "weather-data-pipeline-project-weather-data") # e.g., your-project-id-weather-data
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "weather_data")
BIGQUERY_TABLE_ID = os.environ.get("BIGQUERY_TABLE_ID", "current_weather")
OPENWEATHER_API_KEY = os.environ.get("OPENWEATHER_API_KEY").strip("") # Will be set via env var or Secrets Manager

# List of cities to fetch weather for (you can expand this)
CITIES = [
    {"name": "Bhubaneswar", "lat": 20.27, "lon": 85.84, "country": "IN"},
    {"name": "Delhi", "lat": 28.70, "lon": 77.10, "country": "IN"},
    {"name": "Mumbai", "lat": 19.07, "lon": 72.87, "country": "IN"}
]
def fetch_weather_data(api_key, city_data):
    """Fetches current weather data for a given city."""
    lat = city_data["lat"]
    lon = city_data["lon"]
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    response.raise_for_status() # Raise an exception for HTTP errors
    return response.json()

def transform_weather_data(raw_data, city_name, country_code):
    """Transforms raw weather data into a structured format."""
    if not raw_data:
        return None

    main_data = raw_data.get('main', {})
    wind_data = raw_data.get('wind', {})
    clouds_data = raw_data.get('clouds', {})
    weather_info = raw_data.get('weather', [{}])[0]

    return {
        "city_name": city_name,
        "country_code": country_code,
        "timestamp": datetime.utcfromtimestamp(raw_data.get('dt')).isoformat(),
        "temperature_c": main_data.get('temp'),
        "feels_like_c": main_data.get('feels_like'),
        "humidity": main_data.get('humidity'),
        "pressure": main_data.get('pressure'),
        "wind_speed_mps": wind_data.get('speed'),
        "weather_main": weather_info.get('main'),
        "weather_description": weather_info.get('description'),
        "cloudiness_percent": clouds_data.get('all'),
        "data_ingestion_time": datetime.utcnow().isoformat() # When this record was processed
    }

def upload_to_gcs(bucket_name, file_name, data):
    """Uploads a string to a GCS bucket."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type='application/json')
    print(f"Uploaded {file_name} to gs://{bucket_name}/{file_name}")

def load_to_bigquery(dataset_id, table_id, data_records):
    """Loads data records into a BigQuery table."""
    if not data_records:
        print("No data records to load into BigQuery.")
        return

    bigquery_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    errors = bigquery_client.insert_rows_json(table_ref, data_records)

    if errors:
        print(f"Encountered errors while inserting rows: {errors}")
    else:
        print(f"Successfully loaded {len(data_records)} rows into {dataset_id}.{table_id}")

def main(event=None, context=None): # Added event, context for Cloud Function compatibility
    """Main pipeline function to fetch, transform, and load weather data."""
    if not OPENWEATHER_API_KEY:
        print(f"DEBUG: API Key being used: '{OPENWEATHER_API_KEY}'")
        raise ValueError("OPENWEATHER_API_KEY environment variable is not set.")
    if not GCP_PROJECT_ID or not GCS_BUCKET_NAME or not BIGQUERY_DATASET_ID or not BIGQUERY_TABLE_ID:
        raise ValueError("GCP environment variables are not set. Check GCP_PROJECT_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID.")

    all_transformed_data = []

    for city in CITIES:
        try:
            print(f"Fetching data for {city['name']}...")
            raw_data = fetch_weather_data(OPENWEATHER_API_KEY, city)
            transformed_record = transform_weather_data(raw_data, city['name'], city['country'])
            if transformed_record:
                all_transformed_data.append(transformed_record)
            else:
                print(f"Failed to transform data for {city['name']}.")
        except requests.exceptions.RequestException as e:
            print(f"Error fetching data for {city['name']}: {e}")
        except Exception as e:
            print(f"An unexpected error occurred for {city['name']}: {e}")

    if all_transformed_data:
        # 1. Upload raw JSON to GCS (as a single file for this example)
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        gcs_file_name = f"raw_weather_data/{timestamp_str}_weather.json"
        upload_to_gcs(GCS_BUCKET_NAME, gcs_file_name, json.dumps(all_transformed_data, indent=2))

        # 2. Load transformed data to BigQuery
        load_to_bigquery(BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID, all_transformed_data)
    else:
        print("No data was collected to process.")

if __name__ == "__main__":

    # For local testing, set environment variables
    os.environ["GCP_PROJECT_ID"] = "weather-data-pipeline-project"
    os.environ["GCS_BUCKET_NAME"] = "weather-data-pipeline-project-weather-data"
    os.environ["BIGQUERY_DATASET_ID"] = "weather_data"
    os.environ["BIGQUERY_TABLE_ID"] = "current_weather"
    os.environ["OPENWEATHER_API_KEY"] = "02762022e018d8785890124cf197d16c" # For local testing ONLY!

    print("Running locally. Ensure environment variables are set.")
    main()