import time, random, uuid
from datetime import datetime, timedelta, timezone
from google.cloud import bigquery
from google.api_core import retry as retries
import os
from dotenv import load_dotenv

dotenv_path = "/home/murali/dbt-env/marketing-attribution-pipeline/.env"

load_dotenv(dotenv_path)

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
STREAM_TABLE = os.getenv("STREAM_TABLE")

client = bigquery.Client(project=PROJECT_ID)
table_ref = client.dataset(DATASET).table(STREAM_TABLE)


channels = ["Google / CPC", "Facebook / Social", "Email / Newsletter", "Direct / None"]


def new_conversion_id():
    return str(uuid.uuid4())

@retries.Retry(predicate=retries.if_exception_type(Exception), deadline=30)
def insert_batch(rows):
    return client.insert_rows_json(table_ref, rows, row_ids=[row['conversion_id'] for row in rows])


def stream_events(num_events=15,sleep_between=0.15):
    for _ in range(num_events):
        user_id = f"user_{random.randint(1, 50)}"
        first_channel = random.choice(channels)
        last_channel = random.choice(channels)
        conversion_at = datetime.now(timezone.utc)
        purchase_value = round(random.uniform(10, 500), 2)

        row = {
            "conversion_id": new_conversion_id(),
            "user_pseudo_id": user_id,
            "conversion_at": conversion_at.isoformat(),
            "purchase_value": purchase_value,
            "first_click_channel": first_channel,
            "last_click_channel": last_channel
        }

        errors = insert_batch([row])
        time.sleep(sleep_between)
    
        if not errors:
            print(f"Successfully inserted {row["conversion_id"]} events.")
        else:
            print("Encountered errors while inserting rows:", errors)


if __name__ == "__main__":
    stream_events(random.randint(5, 20),5)