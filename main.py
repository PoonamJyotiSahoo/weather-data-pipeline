import os
import requests
import json
from datetime import datetime
import pandas as pd
from google.cloud import storage
from google.cloud import bigquery
from google.cloud import secretmanager # New import

# --- Configuration ---
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME", "your-gcs-bucket-name")
BIGQUERY_DATASET_ID = os.environ.get("BIGQUERY_DATASET_ID", "weather_data")
BIGQUERY_TABLE_ID = os.environ.get("BIGQUERY_TABLE_ID", "current_weather")
# OPENWEATHER_API_KEY will now be fetched from Secret Manager

# List of cities to fetch weather for
CITIES = [
    {"name": "Bhubaneswar", "lat": 20.27, "lon": 85.84, "country": "IN"},
    {"name": "Delhi", "lat": 28.70, "lon": 77.10, "country": "IN"},
    {"name": "Mumbai", "lat": 19.07, "lon": 72.87, "country": "IN"}
]

def get_secret_value(secret_name):
    """Fetches a secret value from Secret Manager."""
    client = secretmanager.SecretManagerServiceClient()
    # Build the resource name of the secret version.
    name = f"projects/{GCP_PROJECT_ID}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def fetch_weather_data(api_key, city_data):
    # ... (same as before) ...
    lat = city_data["lat"]
    lon = city_data["lon"]
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def transform_weather_data(raw_data, city_name, country_code):
    # ... (same as before) ...
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
        "data_ingestion_time": datetime.utcnow().isoformat()
    }

def upload_to_gcs(bucket_name, file_name, data):
    # ... (same as before) ...
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(data, content_type='application/json')
    print(f"Uploaded {file_name} to gs://{bucket_name}/{file_name}")

def load_to_bigquery(dataset_id, table_id, data_records):
    # ... (same as before) ...
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

def main(event=None, context=None):
    """Main pipeline function to fetch, transform, and load weather data."""
    # Fetch API key securely
    api_key = get_secret_value("OPENWEATHER_API_KEY")

    if not api_key:
        raise ValueError("OPENWEATHER_API_KEY could not be fetched from Secret Manager.")
    if not GCP_PROJECT_ID or not GCS_BUCKET_NAME or not BIGQUERY_DATASET_ID or not BIGQUERY_TABLE_ID:
        raise ValueError("GCP environment variables are not set. Check GCP_PROJECT_ID, GCS_BUCKET_NAME, BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID.")

    all_transformed_data = []

    for city in CITIES:
        try:
            print(f"Fetching data for {city['name']}...")
            raw_data = fetch_weather_data(api_key, city) # Use fetched API key
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
        timestamp_str = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        gcs_file_name = f"raw_weather_data/{timestamp_str}_weather.json"
        upload_to_gcs(GCS_BUCKET_NAME, gcs_file_name, json.dumps(all_transformed_data, indent=2))
        load_to_bigquery(BIGQUERY_DATASET_ID, BIGQUERY_TABLE_ID, all_transformed_data)
    else:
        print("No data was collected to process.")

if __name__ == "__main__":
    # For local testing, ensure you manually set the API key or implement local Secret Manager mocking
    # For simplicity here, if you're testing locally, revert to os.environ.get("OPENWEATHER_API_KEY")
    # and set it via `export` for testing, but remove before pushing to Cloud Function.
    # For this example, we assume Secret Manager access only in Cloud Function context.
    # If testing locally with Secret Manager, you'd need GCP credentials setup locally too.
    print("Running locally. For Cloud Function deployment, Secret Manager will be used.")
    # If you really want to test locally with Secret Manager, uncomment and ensure
    # your gcloud authentication is set up for your local machine:
    # os.environ["GCP_PROJECT_ID"] = "your-gcp-project-id"
    # os.environ["GCS_BUCKET_NAME"] = "your-gcs-bucket-name"
    # os.environ["BIGQUERY_DATASET_ID"] = "weather_data"
    # os.environ["BIGQUERY_TABLE_ID"] = "current_weather"
    # main()