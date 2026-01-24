import requests
import json
import os
import time
from datetime import datetime, date
from google.cloud import bigquery
from google.oauth2 import service_account

# Configuration
NPS_KEY = os.environ['NPS_KEY']
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
BASE_URL = "https://developer.nps.gov/api/v1"

# Expected minimum counts (80% threshold for safety)
EXPECTED_COUNTS = {
    'nps__src_parks': 474,
    'nps__src_amenities': 127,
    'nps__src_amenities_parks': 127,
    'nps__src_tours': 706,
    'nps__src_things_to_do': 3579,
    'nps__src_events': 10,  # Lower threshold since events fluctuate
    'nps__src_places': 5378,
    'nps__src_alerts': 100,  # Alerts fluctuate
    'nps__src_campgrounds': 664,
    'nps__src_park_boundaries': 62,
}

# Set up BigQuery client
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
credentials = service_account.Credentials.from_service_account_info(credentials_json)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

def get_national_park_codes():
    """Fetch national park codes from BigQuery"""
    query = os.environ['GET_NATIONAL_PARKS_QUERY']
    result = client.query(query).result()
    park_codes = [row.park_code for row in result]
    print(f"Loaded {len(park_codes)} national park codes from BigQuery")
    return park_codes

# Define endpoints - table names follow pattern: nps_src_{key}
ENDPOINTS = {
    'parks': '/parks',
    'amenities': '/amenities',
    'amenities_parks': '/amenities/parksplaces',
    'tours': '/tours',
    'thingstodo': '/thingstodo',
    'events': '/events',
    'places': '/places',
    'alerts': '/alerts',
    'campgrounds': '/campgrounds',
}

# Endpoints that require individual park code calls
PARK_SPECIFIC_ENDPOINTS = {
    'park_boundaries': '/mapdata/parkboundaries/{parkCode}'
}

# Special case mappings for table names that don't follow the pattern
TABLE_NAME_OVERRIDES = {
    'thingstodo': 'nps__src_things_to_do',
}

def get_table_name(endpoint_key):
    """Get BigQuery table name for an endpoint"""
    if endpoint_key in TABLE_NAME_OVERRIDES:
        return TABLE_NAME_OVERRIDES[endpoint_key]
    return f'nps__src_{endpoint_key}'

def fetch_with_retry(url, headers, max_retries=3, timeout=60):
    """Fetch URL with retry logic and timeout"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            
            # Handle rate limiting separately
            if response.status_code == 429:
                wait = 60
                print(f"  Rate limited (429). Waiting {wait}s...")
                time.sleep(wait)
                continue
            
            response.raise_for_status()
            return response.json()
        except requests.exceptions.Timeout:
            print(f"  Timeout on attempt {attempt + 1}/{max_retries}")
        except requests.exceptions.RequestException as e:
            print(f"  Request error on attempt {attempt + 1}/{max_retries}: {e}")
        except json.JSONDecodeError as e:
            print(f"  JSON decode error on attempt {attempt + 1}/{max_retries}: {e}")
        
        if attempt < max_retries - 1:
            wait = 10 * (attempt + 1)
            print(f"  Retrying in {wait}s...")
            time.sleep(wait)
    
    return None

def fetch_endpoint_data(endpoint_name, endpoint_path, park_codes):
    """Fetch all data from an NPS API endpoint with pagination"""
    print(f"\n=== Fetching {endpoint_name} ===")
    
    all_data = []
    seen_ids = set()
    start = 0
    limit = 50
    
    today = date.today().isoformat()
    park_codes_param = ','.join(park_codes)
    headers = {"X-Api-Key": NPS_KEY}
    
    while True:
        if endpoint_name == 'events':
            url = f"{BASE_URL}{endpoint_path}?parkCode={park_codes_param}&dateEnd={today}&start={start}&limit={limit}"
        elif endpoint_name == 'places':
            url = f"{BASE_URL}{endpoint_path}?parkCode={park_codes_param}&start={start}&limit={limit}"
        else:
            url = f"{BASE_URL}{endpoint_path}?start={start}&limit={limit}"
        
        data = fetch_with_retry(url, headers)
        
        if data is None:
            print(f"Failed to fetch {endpoint_name} after retries, stopping pagination")
            break
        
        items = data.get('data', [])
        
        if not items:
            break
        
        if endpoint_name == 'events':
            new_count = 0
            for item in items:
                event_id = item.get('id')
                if event_id not in seen_ids:
                    seen_ids.add(event_id)
                    all_data.append(item)
                    new_count += 1
            
            print(f"Fetched {len(items)} events, {new_count} new | Total unique: {len(all_data)}")
            
            if new_count == 0:
                break
        else:
            all_data.extend(items)
            print(f"Fetched {len(all_data)} {endpoint_name} so far...")
        
        start += limit
        time.sleep(0.5)
    
    print(f"Total {endpoint_name}: {len(all_data)}")
    return all_data

def fetch_park_specific_data(endpoint_name, endpoint_path_template, park_codes):
    """Fetch data for endpoints that require individual park code calls"""
    print(f"\n=== Fetching {endpoint_name} for each park ===")
    
    all_data = []
    headers = {"X-Api-Key": NPS_KEY}
    
    for park_code in park_codes:
        endpoint_path = endpoint_path_template.format(parkCode=park_code)
        url = f"{BASE_URL}{endpoint_path}"
        
        data = fetch_with_retry(url, headers)
        
        if data is None:
            print(f"Failed to fetch {endpoint_name} for {park_code} after retries, skipping")
            continue
            
        items = data.get('features', data.get('data', []))
        
        if items:
            for item in items:
                item['_park_code'] = park_code
            all_data.extend(items)
            print(f"Fetched {len(items)} boundaries for {park_code}")
        else:
            print(f"No boundaries for {park_code}")
        
        time.sleep(1)
    
    print(f"Total {endpoint_name}: {len(all_data)}")
    return all_data

def load_to_bigquery(data, table_name):
    """Load JSON data to BigQuery table with safety check"""
    if not data:
        print(f"No data to load for {table_name}, keeping existing data")
        return
    
    # Safety check - don't overwrite if we got way less than expected
    expected = EXPECTED_COUNTS.get(table_name)
    if expected and len(data) < expected * 0.8:
        print(f"WARNING: Only got {len(data)} {table_name}, expected ~{expected}. Skipping write to preserve existing data.")
        return
    
    print(f"Loading {len(data)} items to {table_name}")
    
    load_timestamp = datetime.utcnow().isoformat()
    processed_data = []
    
    for i, record in enumerate(data):
        if table_name == 'nps__src_amenities_parks' and isinstance(record, list):
            if len(record) == 1 and isinstance(record[0], dict):
                record = record[0]
            else:
                print(f"Warning: Skipping list record at index {i} in {table_name} with {len(record)} elements")
                continue
        
        if not isinstance(record, dict):
            print(f"Warning: Skipping non-dict record at index {i} in {table_name}: {type(record)}")
            continue
        
        if table_name == 'nps__src_park_boundaries' and 'geometry' in record:
            record['geometry_json'] = json.dumps(record['geometry'])
            del record['geometry']
        
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
    print("Starting NPS data fetch...")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}")
    
    park_codes = get_national_park_codes()
    
    for endpoint_name, endpoint_path in ENDPOINTS.items():
        data = fetch_endpoint_data(endpoint_name, endpoint_path, park_codes)
        table_name = get_table_name(endpoint_name)
        load_to_bigquery(data, table_name)
    
    for endpoint_name, endpoint_path_template in PARK_SPECIFIC_ENDPOINTS.items():
        data = fetch_park_specific_data(endpoint_name, endpoint_path_template, park_codes)
        table_name = get_table_name(endpoint_name)
        load_to_bigquery(data, table_name)
    
    print("\n=== Data fetch complete ===")

if __name__ == "__main__":
    main()