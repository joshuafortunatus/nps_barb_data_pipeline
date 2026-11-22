"""Rate hike difficulty and attributes using TypeSafe's Jev decision model.

One Jev request per hike returns a typed difficulty label (constrained to the
criteria keys, so it cannot drift) plus the model's confidence in it.

Usage:
    python scripts/rate_hikes.py                      # rate unrated hikes only
    python scripts/rate_hikes.py --backfill           # re-rate every hike
    python scripts/rate_hikes.py --backfill --limit 10 --dry-run
"""
import argparse
import json
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from google.cloud import bigquery
from google.oauth2 import service_account

JEV_URL = os.environ.get("JEV_URL", "https://api.typesafe.ai/v1/systemone")
JEV_MODEL = os.environ.get("JEV_MODEL", "jev-latest")
JEV_API_KEY = os.environ.get("TYPESAFE_API_KEY")

PROJECT_ID = os.environ["PROJECT_ID"]
DATASET_ID = os.environ["DATASET_ID"]
RATINGS_TABLE = f"{PROJECT_ID}.{DATASET_ID}.nps__mart_activity_difficulty_ratings"
HIKES_TABLE = f"{PROJECT_ID}.{DATASET_ID}.nps__int_activity_hikes"

QUESTIONS = {
    "difficulty": {
        "type": "choice",
        "instructions": "How physically demanding is this hike overall?",
        "criteria": {
            "Easy": "Short, flat or gently rolling, paved or well-maintained tread; "
                    "suitable for most visitors including children.",
            "Moderate": "Some sustained elevation gain, uneven footing, or several "
                        "miles; needs average fitness.",
            "Strenuous": "Steep or long climbs, rough or exposed terrain, scrambling, "
                         "or all-day effort; needs good fitness.",
        },
    },
}

NEW_COLUMNS = [
    ("rating_confidence", "FLOAT64"),
]


def bq_client():
    raw = os.environ["GOOGLE_CREDENTIALS_JSON"].strip()
    if os.path.exists(raw):
        creds = service_account.Credentials.from_service_account_file(raw)
    else:
        creds = service_account.Credentials.from_service_account_info(json.loads(raw))
    return bigquery.Client(credentials=creds, project=PROJECT_ID)


def ensure_schema(bq):
    """Add the Jev output columns if they aren't there yet. Idempotent."""
    existing = {f.name for f in bq.get_table(RATINGS_TABLE).schema}
    missing = [(n, t) for n, t in NEW_COLUMNS if n not in existing]
    if not missing:
        return
    clauses = ", ".join(f"ADD COLUMN {name} {dtype}" for name, dtype in missing)
    print(f"Adding columns to ratings table: {[n for n, _ in missing]}")
    bq.query(f"ALTER TABLE `{RATINGS_TABLE}` {clauses}").result()


def fetch_hikes(bq, backfill, limit):
    # NOT EXISTS rather than NOT IN: a single NULL activity_id would make
    # NOT IN return zero rows and silently stop rating new hikes forever.
    where = "" if backfill else (
        f"WHERE NOT EXISTS (SELECT 1 FROM `{RATINGS_TABLE}` r "
        f"WHERE r.activity_id = h.hike_id)"
    )
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    # QUALIFY guards the MERGE: a duplicate hike_id upstream would fail it with
    # "UPDATE/MERGE must match at most one source row".
    query = f"""
    SELECT h.hike_id, h.hike_title, h.short_description, h.long_description,
           h.activity_url
    FROM `{HIKES_TABLE}` AS h
    {where}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY h.hike_id ORDER BY h.hike_title) = 1
    ORDER BY FARM_FINGERPRINT(h.hike_id)
    {limit_clause}
    """
    return bq.query(query).to_dataframe()


def rate_hike_with_retry(hike, max_retries=3, base_delay=5):
    """Rate one hike with exponential backoff. Returns the Jev answers dict."""
    description_parts = []
    if pd.notna(hike["short_description"]):
        description_parts.append(f"Short: {hike['short_description']}")
    if pd.notna(hike["long_description"]):
        description_parts.append(f"Long: {hike['long_description']}")

    payload = {
        "model": JEV_MODEL,
        "state": {
            "title": hike["hike_title"],
            "description": "\n\n".join(description_parts) or "No description available",
            "url": hike["activity_url"],
        },
        "questions": QUESTIONS,
    }

    for attempt in range(max_retries):
        resp = requests.post(
            JEV_URL,
            headers={"Authorization": f"Bearer {JEV_API_KEY}",
                     "Content-Type": "application/json"},
            json=payload,
            timeout=60,
        )
        if resp.status_code in (429, 500, 502, 503, 529) and attempt < max_retries - 1:
            wait_time = base_delay * (2 ** attempt)
            print(f"  API busy ({resp.status_code}), waiting {wait_time}s "
                  f"before retry (attempt {attempt + 1}/{max_retries})...")
            time.sleep(wait_time)
            continue
        resp.raise_for_status()
        return resp.json()["answers"]

    raise RuntimeError(f"Failed after {max_retries} retries")


def merge_ratings(bq, ratings_df):
    """MERGE on activity_id so a backfill updates rather than duplicating.

    Duplicate activity_ids would fan out the LEFT JOIN in
    nps__mart_national_park_hikes and break its primary key.
    """
    staging = f"{PROJECT_ID}.{DATASET_ID}._stg_difficulty_ratings"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    bq.load_table_from_dataframe(ratings_df, staging, job_config=job_config).result()

    bq.query(f"""
    MERGE `{RATINGS_TABLE}` AS target
    USING `{staging}` AS source
    ON target.activity_id = source.activity_id
    WHEN MATCHED THEN UPDATE SET
        difficulty_rating = source.difficulty_rating,
        rating_confidence = source.rating_confidence,
        rated_at          = source.rated_at,
        rating_source     = source.rating_source
    WHEN NOT MATCHED THEN INSERT (
        activity_id, difficulty_rating, rating_confidence, rated_at,
        rating_source
    ) VALUES (
        source.activity_id, source.difficulty_rating, source.rating_confidence,
        source.rated_at, source.rating_source
    )
    """).result()

    bq.delete_table(staging, not_found_ok=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backfill", action="store_true",
                        help="Re-rate every hike, not just unrated ones.")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true",
                        help="Call Jev but do not write to BigQuery.")
    args = parser.parse_args()

    if not JEV_API_KEY:
        raise SystemExit("TYPESAFE_API_KEY is not set.")

    bq = bq_client()
    if not args.dry_run:
        ensure_schema(bq)

    scope = "all hikes (backfill)" if args.backfill else "unrated hikes"
    print(f"Fetching {scope} from BigQuery...")
    hikes = fetch_hikes(bq, args.backfill, args.limit)
    print(f"Found {len(hikes)} hikes to rate\n")

    if len(hikes) == 0:
        print("Nothing to rate. Exiting.")
        return

    ratings, failed = [], []
    for i, (_, hike) in enumerate(hikes.iterrows(), start=1):
        try:
            answers = rate_hike_with_retry(hike)
        except Exception as exc:
            print(f"X Error rating {hike['hike_title']}: {exc}")
            failed.append({"hike_id": hike["hike_id"],
                           "hike_title": hike["hike_title"], "error": str(exc)})
            continue

        difficulty = answers["difficulty"]
        ratings.append({
            "activity_id": hike["hike_id"],
            "difficulty_rating": difficulty["choice"],
            "rating_confidence": difficulty.get("confidence"),
            "rated_at": datetime.now(timezone.utc),
            "rating_source": "jev",
        })

        conf = difficulty.get("confidence")
        conf_str = f"{conf:.2f}" if conf is not None else "n/a"
        print(f"* [{i}/{len(hikes)}] {hike['hike_title'][:46]:46} -> "
              f"{difficulty['choice']:9} (conf {conf_str})")

    if not ratings:
        print("\nNo ratings produced.")
        return

    ratings_df = pd.DataFrame(ratings)

    if args.dry_run:
        print(f"\n--dry-run: would write {len(ratings_df)} ratings. Skipping BigQuery.")
    else:
        print(f"\nMerging {len(ratings_df)} ratings into BigQuery...")
        merge_ratings(bq, ratings_df)
        print(f"Successfully merged {len(ratings_df)} ratings")

    print("\nSummary:")
    print(ratings_df["difficulty_rating"].value_counts().to_string())

    if failed:
        print(f"\n{len(failed)} hikes failed to rate:")
        for f in failed:
            print(f"  - {f['hike_title']}: {f['error']}")


if __name__ == "__main__":
    main()
