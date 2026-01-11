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

# Set up BigQuery client
credentials_file = os.environ['GOOGLE_CREDENTIALS_JSON']
credentials = service_account.Credentials.from_service_account_file(credentials_file)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def fetch_all_facilities():
    """Fetch all facility IDs that have campsites"""
    print("\n=== Fetching all facilities ===")
    
    all_facilities = []
    offset = 0
    limit = 50
    
    headers = {"apikey": RECREATION_GOV_API_KEY}
    
    while True:
        url = f"{BASE_URL}/facilities"
        params = {'limit': limit, 'offset': offset}
        
        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"Error fetching facilities: {e}")
            break
        
        facilities = data.get('RECDATA', [])
        
        if not facilities:
            break
        
        all_facilities.extend(facilities)
        print(f"Fetched {len(all_facilities)} facilities so far...")
        
        if len(facilities) < limit:
            break
        
        offset += limit
    
    print(f"Total facilities: {len(all_facilities)}")
    return all_facilities

def fetch_campsites_for_facility(facility_id):
    """Fetch all campsites for a specific facility"""
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
            # Some facilities don't have campsites - that's ok
            break
        
        campsites = data.get('RECDATA', [])
        
        if not campsites:
            break
        
        all_campsites.extend(campsites)
        
        if len(campsites) < limit:
            break
        
        offset += limit
    
    return all_campsites

def fetch_all_campsites():
    """Fetch all campsites from all facilities"""
    print("\n=== Fetching all campsites from all facilities ===")
    
    # First, get all facilities
    facilities = fetch_all_facilities()
    
    all_campsites = []
    facilities_with_campsites = 0
    
    # Then, fetch campsites for each facility
    for i, facility in enumerate(facilities, 1):
        facility_id = facility.get('FacilityID')
        facility_name = facility.get('FacilityName', 'Unknown')
        
        print(f"\n[{i}/{len(facilities)}] Fetching campsites for: {facility_name} (ID: {facility_id})")
        
        campsites = fetch_campsites_for_facility(facility_id)
        
        if campsites:
            # Add facility metadata to each campsite
            for campsite in campsites:
                campsite['_facility_id'] = facility_id
                campsite['_facility_name'] = facility_name
            
            all_campsites.extend(campsites)
            facilities_with_campsites += 1
            print(f"  ✓ Found {len(campsites)} campsites | Total so far: {len(all_campsites)}")
        else:
            print(f"  - No campsites")
    
    print(f"\n=== Summary ===")
    print(f"Total facilities: {len(facilities)}")
    print(f"Facilities with campsites: {facilities_with_campsites}")
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
    
    # Fetch all campsites from all facilities
    campsites = fetch_all_campsites()
    
    # Load to BigQuery
    load_to_bigquery(campsites, 'recreation_gov__src_campsites')
    
    print("\n=== Data fetch complete ===")

if __name__ == "__main__":
    main()