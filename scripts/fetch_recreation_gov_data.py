import requests
import json
import os
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
RECREATION_GOV_API_KEY = os.environ['RECREATION_GOV_API_KEY']
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
BASE_URL = "https://ridb.recreation.gov/api/v1"

# Voyageurs National Park facility ID
# Only national park that doesn't have campground data through NPS API
VOYAGEURS_FACILITY_ID = '249981'

# Set up BigQuery client
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
credentials = service_account.Credentials.from_service_account_info(credentials_json)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_campsites(facility_id):
    """Fetch all campsites for a facility with pagination"""
    print(f"\n=== Fetching campsites for facility {facility_id} ===")
    
    all_campsites = []
    offset = 0
    limit = 50
    
    headers = {"apikey": RECREATION_GOV_API_KEY}
    
    while True:
        url = f"{BASE_URL}/facilities/{facility_id}/campsites"
        params = {'limit': limit, 'offset': offset}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching campsites: {e}")
            break
        
        campsites = data.get('RECDATA', [])
        
        if not campsites:
            break
        
        all_campsites.extend(campsites)
        print(f"Fetched {len(all_campsites)} campsites so far...")
        
        if len(campsites) < limit:
            break
        
        offset += limit
    
    print(f"Total campsites: {len(all_campsites)}")
    return all_campsites

def load_to_bigquery(data, table_name):
    """Load JSON data to BigQuery table with native types preserved"""
    if not data:
        print(f"No data to load for {table_name}")
        return
    
    print(f"\nLoading {len(data)} items to {table_name}")
    
    # Add metadata column
    load_timestamp = datetime.utcnow().isoformat()
    processed_data = []
    
    for i, record in enumerate(data):
        if not isinstance(record, dict):
            print(f"Warning: Skipping non-dict record at index {i} in {table_name}: {type(record)}")
            continue
        
        record['_loaded_at'] = load_timestamp
        record['_facility_id'] = VOYAGEURS_FACILITY_ID
        processed_data.append(record)
    
    if not processed_data:
        print(f"No valid records to load for {table_name}")
        return
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    job = client.load_table_from_json(processed_data, table_id, job_config=job_config)
    job.result()
    
    print(f"Loaded {len(processed_data)} rows to {table_id}")

def main():
    """Main execution function"""
    print("Starting Recreation.gov data fetch...")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}")
    
    # Fetch Voyageurs campsites
    campsites = fetch_campsites(VOYAGEURS_FACILITY_ID)
    
    # Load to BigQuery
    load_to_bigquery(campsites, 'recreation_gov__src_campsites')
    
    print("\n=== Data fetch complete ===")

if __name__ == "__main__":
    main()