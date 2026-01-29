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

# Expected minimum counts (80% threshold for safety) - DAILY/ACTIVE endpoints
EXPECTED_COUNTS = {
    'nps__src_events': 10,   # Lower threshold since events fluctuate
    'nps__src_alerts': 100,  # Alerts fluctuate based on conditions
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

def fetch_with_retry(url, headers, max_retries=3, timeout=60):
    """Fetch URL with retry logic and timeout"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            
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

def fetch_events(park_codes):
    """Fetch events with deduplication"""
    print(f"\n=== Fetching events ===")
    
    all_data = []
    seen_ids = set()
    start = 0
    limit = 50
    
    today = date.today().isoformat()
    park_codes_param = ','.join(park_codes)
    headers = {"X-Api-Key": NPS_KEY}
    
    while True:
        url = f"{BASE_URL}/events?parkCode={park_codes_param}&dateEnd={today}&start={start}&limit={limit}"
        
        data = fetch_with_retry(url, headers)
        
        if data is None:
            print(f"Failed to fetch events after retries, stopping pagination")
            break
        
        items = data.get('data', [])
        
        if not items:
            break
        
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
        
        start += limit
        time.sleep(0.5)
    
    print(f"Total events: {len(all_data)}")
    return all_data

def fetch_alerts():
    """Fetch all alerts"""
    print(f"\n=== Fetching alerts ===")
    
    all_data = []
    start = 0
    limit = 50
    
    headers = {"X-Api-Key": NPS_KEY}
    
    while True:
        url = f"{BASE_URL}/alerts?start={start}&limit={limit}"
        
        data = fetch_with_retry(url, headers)
        
        if data is None:
            print(f"Failed to fetch alerts after retries, stopping pagination")
            break
        
        items = data.get('data', [])
        
        if not items:
            break
        
        all_data.extend(items)
        print(f"Fetched {len(all_data)} alerts so far...")
        
        start += limit
        time.sleep(0.5)
    
    print(f"Total alerts: {len(all_data)}")
    return all_data

def load_to_bigquery(data, table_name):
    """Load JSON data to BigQuery table with safety check"""
    if not data:
        print(f"No data to load for {table_name}, keeping existing data")
        return
    
    expected = EXPECTED_COUNTS.get(table_name)
    if expected and len(data) < expected * 0.8:
        print(f"WARNING: Only got {len(data)} {table_name}, expected ~{expected}. Skipping write to preserve existing data.")
        return
    
    print(f"Loading {len(data)} items to {table_name}")
    
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
    """Main execution function - DAILY active data"""
    print("Starting NPS DAILY data fetch (active endpoints)...")
    print(f"Target: {PROJECT_ID}.{DATASET_ID}")
    
    park_codes = get_national_park_codes()
    
    # Fetch and load events
    events_data = fetch_events(park_codes)
    load_to_bigquery(events_data, 'nps__src_events')
    
    # Fetch and load alerts
    alerts_data = fetch_alerts()
    load_to_bigquery(alerts_data, 'nps__src_alerts')
    
    print("\n=== DAILY data fetch complete ===")

if __name__ == "__main__":
    main()