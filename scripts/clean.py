import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import boto3
import pandas as pd
from io import StringIO
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION
)


def connect_to_s3():
    """Connect to S3"""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print("Connected to S3.")
    return s3


def pull_latest_raw(s3):
    """Pull the most recent raw JSON file from S3"""
    # List all files in the raw/ folder
    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="raw/")
    files = response.get("Contents", [])

    if not files:
        print("No raw files found in S3.")
        return None

    # Sort by last modified and grab the latest
    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    print(f"Pulling latest file: {latest['Key']}")

    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=latest["Key"])
    data = json.loads(obj["Body"].read())
    return data


def clean(data):
    """Loop through the object and match values into columns"""
    pulled_at = data.get("pulled_at")
    rows = []

    # Loop through each coin object
    for coin, values in data.items():
        # Skip the timestamp key — it's not a coin
        if coin == "pulled_at":
            continue

        # Match values into columns one by one
        rows.append({
            "coin":        coin,
            "price_usd":  values.get("usd"),
            "volume_24h": values.get("usd_24h_vol"),
            "change_24h": values.get("usd_24h_change"),
            "pulled_at":  pulled_at
        })

    df = pd.DataFrame(rows)
    print(f"Cleaned {len(df)} rows:")
    print(df)
    return df


def push_to_s3(s3, df):
    """Convert to CSV and push cleaned file to S3"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    # Use same timestamp as the data for the filename
    timestamp = df["pulled_at"].iloc[0].replace(" ", "_").replace(":", "-")
    key = f"cleaned/prices_{timestamp}.csv"

    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=key,
        Body=csv_buffer.getvalue(),
        ContentType="text/csv"
    )
    print(f"Cleaned CSV pushed to S3: s3://{AWS_BUCKET_NAME}/{key}")


def run():
    s3 = connect_to_s3()
    data = pull_latest_raw(s3)
    if data:
        df = clean(data)
        push_to_s3(s3, df)
        print("Clean complete.")


if __name__ == "__main__":
    run()