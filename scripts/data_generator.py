import json
import random
import uuid
from datetime import datetime
from google.cloud import storage


def generate_event():
    return {
        "user_id": str(random.randint(1, 1000)),
        "event": random.choice(["page_view", "add_to_cart", "purchase", "checkout", "login"]),
        "page_url": random.choice([
            "/home", "/product/123", "/category/electronics", "/cart", "/checkout"]),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "latency_ms": random.randint(10, 2000)
    }


def upload_events_to_gcs(bucket_name: str, prefix: str, events: list):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    filename = f"{prefix}/{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex}.json"
    blob = bucket.blob(filename)

    # Convert events to NDJSON
    ndjson = "\n".join(json.dumps(evt) for evt in events)
    blob.upload_from_string(ndjson, content_type="application/json")
    print(f"Uploaded {len(events)} events to gs://{bucket_name}/{filename}")


def main():
    import argparse
    parser = argparse.ArgumentParser(
        description="Generate synthetic clickstream events and upload to GCS."
    )
    parser.add_argument(
        "--bucket", required=True,
        help="GCS bucket name where events will be stored"
    )
    parser.add_argument(
        "--prefix", default="clickstream",
        help="GCS object prefix (folder) for storing event files"
    )
    parser.add_argument(
        "--count", type=int, default=1000,
        help="Number of events to generate in each batch"
    )
    args = parser.parse_args()

    events = [generate_event() for _ in range(args.count)]

    upload_events_to_gcs(args.bucket, args.prefix, events)

if __name__ == "__main__":
    main()
