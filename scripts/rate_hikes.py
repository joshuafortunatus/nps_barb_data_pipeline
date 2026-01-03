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

def rate_hike_with_retry(hike, max_retries=3, base_delay=5):
    """Rate a hike with exponential backoff retry logic."""
    description_parts = []
    if pd.notna(hike['short_description']):
        description_parts.append(f"Short: {hike['short_description']}")
    if pd.notna(hike['long_description']):
        description_parts.append(f"Long: {hike['long_description']}")
    
    full_description = "\n\n".join(description_parts) if description_parts else "No description available"
    
    prompt = f"""Rate this hike as Easy, Moderate, or Strenuous based on the description.

Title: {hike['hike_title']}

{full_description}

URL: {hike['activity_url']}

Respond with ONLY one word: Easy, Moderate, or Strenuous."""
    
    for attempt in range(max_retries):
        try:
            response = client.messages.create(
                model=os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929"),
                max_tokens=100,
                messages=[{
                    "role": "user",
                    "content": prompt
                }]
            )
            
            rating = response.content[0].text.strip()
            
            # Validate rating
            if rating not in ['Easy', 'Moderate', 'Strenuous']:
                print(f"⚠️  Unexpected rating '{rating}' for {hike['hike_title']}, defaulting to Moderate")
                rating = 'Moderate'
            
            return rating
            
        except anthropic.APIError as e:
            if e.status_code == 529 and attempt < max_retries - 1:
                wait_time = base_delay * (2 ** attempt)  # 5, 10, 20 seconds
                print(f"  ⏳ API overloaded, waiting {wait_time}s before retry (attempt {attempt + 1}/{max_retries})...")
                time.sleep(wait_time)
            else:
                raise
    
    raise Exception(f"Failed after {max_retries} retries")

# Get unrated hikes
query = f"""
SELECT 
    hike_id,
    hike_title,
    short_description,
    long_description,
    activity_url
FROM `{PROJECT_ID}.{DATASET_ID}.nps__int_activity_hikes`
WHERE hike_id NOT IN (
    SELECT activity_id 
    FROM `{PROJECT_ID}.{DATASET_ID}.nps__mart_activity_difficulty_ratings`
)
"""

print("Fetching unrated hikes from BigQuery...")
unrated = bq.query(query).to_dataframe()
print(f"Found {len(unrated)} unrated hikes to process\n")

if len(unrated) == 0:
    print("No new hikes to rate. Exiting.")
    exit(0)

ratings = []
failed_hikes = []

for idx, hike in unrated.iterrows():
    try:
        rating = rate_hike_with_retry(hike)
        
        ratings.append({
            'activity_id': hike['hike_id'],
            'difficulty_rating': rating,
            'rated_at': datetime.utcnow(),
            'rating_source': 'claude_api'
        })
        
        print(f"✓ [{idx+1}/{len(unrated)}] {hike['hike_title'][:50]:50} -> {rating}")
        
        # Delay between requests to avoid overloading
        time.sleep(2)
        
    except Exception as e:
        print(f"✗ Error rating {hike['hike_title']}: {e}")
        failed_hikes.append({
            'hike_id': hike['hike_id'],
            'hike_title': hike['hike_title'],
            'error': str(e)
        })
        continue

# Write ratings back to BigQuery
if ratings:
    print(f"\nWriting {len(ratings)} ratings to BigQuery...")
    ratings_df = pd.DataFrame(ratings)
    
    table_id = f"{PROJECT_ID}.{DATASET_ID}.nps__mart_activity_difficulty_ratings"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )
    
    job = bq.load_table_from_dataframe(
        ratings_df, 
        table_id, 
        job_config=job_config
    )
    job.result()  # Wait for completion
    
    print(f"✓ Successfully wrote {len(ratings)} ratings to BigQuery")
    print(f"\nSummary:")
    print(ratings_df['difficulty_rating'].value_counts())
else:
    print("No ratings to write.")

if failed_hikes:
    print(f"\n⚠️  {len(failed_hikes)} hikes failed to rate:")
    for failed in failed_hikes:
        print(f"  - {failed['hike_title']}: {failed['error']}")