import anthropic
from google.cloud import bigquery
from datetime import datetime
import pandas as pd
import os
import time
from google.oauth2 import service_account
import json

# Initialize Anthropic client
client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

# Set up BigQuery client
credentials_json = json.loads(os.environ['GOOGLE_CREDENTIALS_JSON'])
credentials = service_account.Credentials.from_service_account_info(credentials_json)
bq = bigquery.Client(credentials=credentials, project=os.environ['PROJECT_ID'])

# Configuration
PROJECT_ID = os.environ['PROJECT_ID']
DATASET_ID = os.environ['DATASET_ID']
TABLE_ID = "nps__src_hike_length_categories"

def ensure_table_exists():
    """Create the length categories table if it doesn't exist."""
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    schema = [
        bigquery.SchemaField("hike_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("length_category", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("categorized_at", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("category_source", "STRING", mode="REQUIRED"),
    ]
    
    table = bigquery.Table(full_table_id, schema=schema)
    
    try:
        bq.get_table(full_table_id)
        print(f"✓ Table {full_table_id} already exists")
    except Exception:
        table = bq.create_table(table)
        print(f"✓ Created table {full_table_id}")

def categorize_length_with_retry(hike, max_retries=3, base_delay=5):
    """Categorize hike length with exponential backoff retry logic."""
    
    context_parts = []
    
    if pd.notna(hike.get('hike_distance')):
        context_parts.append(f"Distance: {hike['hike_distance']}")
    if pd.notna(hike.get('hike_duration')):
        context_parts.append(f"Duration: {hike['hike_duration']}")
    if pd.notna(hike.get('hike_description')):
        context_parts.append(f"Description: {hike['hike_description']}")
    
    full_context = "\n".join(context_parts) if context_parts else "No details available"
    
    prompt = f"""Categorize this hike's length as Short, Medium, or Long.

Guidelines:
- Short: Under 2 miles, or under 1 hour
- Medium: 2-5 miles, or 1-3 hours
- Long: Over 5 miles, or over 3 hours

Hike ID: {hike['hike_id']}

{full_context}

Use distance first if available, then duration, then infer from description.
If truly unable to determine, default to Medium.

Respond with ONLY one word: Short, Medium, or Long."""
    
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929"),
                max_tokens=10,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            category = response.content[0].text.strip()
            
            if category not in ['Short', 'Medium', 'Long']:
                print(f"⚠️  Unexpected category '{category}' for {hike['hike_id']}, defaulting to Medium")
                category = 'Medium'
            
            return category
            
        except anthropic.APIError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)
                print(f"  ⏳ API overloaded, waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                raise
    
    raise Exception(f"Failed after {max_retries} retries")

# Ensure table exists before querying
ensure_table_exists()

# Get uncategorized hikes
query = f"""
SELECT 
    hike_id,
    hike_description,
    hike_duration,
    hike_distance
FROM `{PROJECT_ID}.{DATASET_ID}.nps__mart_national_park_hikes`
WHERE hike_id NOT IN (
    SELECT hike_id 
    FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
)
"""

print("Fetching uncategorized hikes from BigQuery...")
uncategorized = bq.query(query).to_dataframe()
print(f"Found {len(uncategorized)} uncategorized hikes to process\n")

if len(uncategorized) == 0:
    print("No new hikes to categorize. Exiting.")
    exit(0)

categories = []
failed_hikes = []

for idx, hike in uncategorized.iterrows():
    try:
        category = categorize_length_with_retry(hike)
        
        categories.append({
            'hike_id': hike['hike_id'],
            'length_category': category,
            'categorized_at': datetime.utcnow(),
            'category_source': 'claude_api'
        })
        
        print(f"✓ [{idx+1}/{len(uncategorized)}] {hike['hike_id'][:50]:50} -> {category}")
        
        time.sleep(2)
        
    except Exception as e:
        print(f"✗ Error categorizing {hike['hike_id']}: {e}")
        failed_hikes.append({
            'hike_id': hike['hike_id'],
            'error': str(e)
        })
        continue

# Write categories back to BigQuery
if categories:
    print(f"\nWriting {len(categories)} categories to BigQuery...")
    categories_df = pd.DataFrame(categories)
    
    full_table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    
    job = bq.load_table_from_dataframe(
        categories_df, 
        full_table_id, 
        job_config=job_config
    )
    job.result()
    
    print(f"✓ Successfully wrote {len(categories)} categories to BigQuery")
    print(f"\nSummary:")
    print(categories_df['length_category'].value_counts())
else:
    print("No categories to write.")

if failed_hikes:
    print(f"\n⚠️  {len(failed_hikes)} hikes failed to categorize:")
    for failed in failed_hikes:
        print(f"  - {failed['hike_id']}: {failed['error']}")