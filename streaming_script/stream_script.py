import time
import random
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
import os
from dotenv import load_dotenv

dotenv_path = "/path/to/env_file/.env"

load_dotenv(dotenv_path)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
STREAM_TABLE = os.getenv("STREAM_TABLE")

client = bigquery.Client(project=PROJECT_ID)
table_ref = client.dataset(DATASET).table(STREAM_TABLE)


channels = ["Google / CPC", "Facebook / Social", "Email / Newsletter", "Direct / None"]


def generate_event_id(user_id):
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S%f")
    return f"{user_id}_{timestamp}"

def stream_events(num_events=15):
    rows_to_insert = []
    for _ in range(num_events):
        user_id = f"user_{random.randint(1, 50)}"
        first_channel = random.choice(channels)
        last_channel = random.choice(channels)
        conversion_at = datetime.now(timezone.utc)
        purchase_value = round(random.uniform(10, 500), 2)

        row = {
            "conversion_id": generate_event_id(user_id),
            "user_pseudo_id": user_id,
            "conversion_at": conversion_at.isoformat(),
            "purchase_value": purchase_value,
            "first_click_channel": first_channel,
            "last_click_channel": last_channel
        }

        rows_to_insert.append(row)
    
    errors = client.insert_rows_json(
        table_ref,
        rows_to_insert,
        row_ids=[row['conversion_id'] for row in rows_to_insert]
    )

    if not errors:
        print(f"Successfully inserted a batch of {len(rows_to_insert)} events.")
    else:
        print("Encountered errors while inserting rows:", errors)


if __name__ == "__main__":
    stream_events(random.randint(5, 20))