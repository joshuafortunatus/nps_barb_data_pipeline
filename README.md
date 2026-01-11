# NPS & Recreation.gov Data Pipeline For BARB

Automated data collection from the National Parks Service API and Recreation.gov API with AI-powered hike difficulty ratings.

## What it does

* 🌲 Fetches parks, activities, events, amenities, and campgrounds from NPS API
* 🏕️ Fetches campsite data with coordinates from Recreation.gov API (Voyageurs National Park)
* 🤖 Rates hiking difficulty using Claude AI
* ☁️ Loads everything to BigQuery
* ⏰ Runs automatically every day at 8am UTC via GitHub Actions

## Architecture
```
NPS API → fetch_nps_data.py → BigQuery (nps__src_* tables)
Recreation.gov API → fetch_recreation_gov_data.py → BigQuery (recreation_gov__src_campsites)
BigQuery → rate_hikes.py (Claude AI) → BigQuery (nps__src_activity_difficulty_ratings)
```

## Setup

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Required Environment Variables

Create a `.env` file:
```bash
# NPS API
NPS_KEY=your-nps-api-key
GET_NATIONAL_PARKS_QUERY=SELECT park_code FROM your_table

# Recreation.gov API
RECREATION_GOV_API_KEY=your-recreation-gov-api-key

# Anthropic Claude API
ANTHROPIC_API_KEY=your-anthropic-api-key
CLAUDE_MODEL=claude-sonnet-4-20250514

# Google Cloud / BigQuery
PROJECT_ID=your-gcp-project-id
DATASET_ID=your-bigquery-dataset-id
GOOGLE_CREDENTIALS_JSON=/path/to/service-account-key.json
```

### Required GitHub Secrets (for Actions)

Add these secrets to your GitHub repository:

* `NPS_KEY` - Your NPS API key ([get one here](https://www.nps.gov/subjects/developer/get-started.htm))
* `RECREATION_GOV_API_KEY` - Your Recreation.gov API key ([get one here](https://ridb.recreation.gov/))
* `ANTHROPIC_API_KEY` - Your Claude API key ([get one here](https://console.anthropic.com/))
* `PROJECT_ID` - GCP project ID
* `DATASET_ID` - BigQuery dataset name
* `GOOGLE_CREDENTIALS_JSON` - GCP service account JSON (full JSON string)
* `GET_NATIONAL_PARKS_QUERY` - SQL query to fetch national park codes

## Local Testing

### Fetch NPS Data
```bash
python scripts/fetch_nps_data.py
```

### Fetch Recreation.gov Campsite Data
```bash
python scripts/fetch_recreation_gov_data.py
```

### Rate Hikes with AI
```bash
python scripts/rate_hikes.py
```

## Output Tables

### NPS Data (from fetch_nps_data.py)
* `nps__src_parks` - National park information
* `nps__src_things_to_do` - Activities and attractions
* `nps__src_events` - Park events and programs
* `nps__src_amenities` - Park amenities
* `nps__src_amenities_parks` - Amenity-park relationships
* `nps__src_tours` - Guided tours
* `nps__src_campgrounds` - Campground information
* `nps__src_alerts` - Park alerts and notices
* `nps__src_places` - Points of interest
* `nps__src_park_boundaries` - Geographic boundaries (GeoJSON)

### Recreation.gov Data (from fetch_recreation_gov_data.py)
* `recreation_gov__src_campsites` - Voyageurs NP campsites with lat/long coordinates

### AI-Generated Data (from rate_hikes.py)
* `nps__src_activity_difficulty_ratings` ⭐ - AI-powered hiking difficulty ratings

## Pipeline Schedule

The GitHub Actions workflow runs daily at 8am UTC:

1. **Fetch NPS Data** - ~2-3 minutes
2. **Fetch Recreation.gov Data** - ~5 seconds (159 Voyageurs campsites)
3. **Rate Hikes with AI** - ~5-10 minutes (only rates new/unrated hikes)

## Data Transformations

After raw data is loaded to BigQuery, dbt models transform it for analysis and visualization:

* Joins campsite coordinates with park data
* Enriches activities with difficulty ratings
* Creates analytics-ready views for the BARB website

## About BARB

BARB is a comprehensive National Parks data visualization project that reimagines the UX for exploring National Park Service data. It features interactive maps, activity filters, event calendars, and AI-powered trail difficulty ratings.

## Tech Stack

* **Data Sources**: NPS API, Recreation.gov RIDB API
* **AI**: Anthropic Claude (Sonnet 4)
* **Cloud**: Google Cloud Platform, BigQuery
* **Orchestration**: GitHub Actions
* **Languages**: Python 3.11+
* **Key Libraries**: `requests`, `google-cloud-bigquery`, `anthropic`

## License

MIT