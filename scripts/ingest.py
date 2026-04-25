import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
import boto3
from datetime import datetime
from config.config import (
    COINGECKO_API_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION
)

# Coins we're tracking
COINS = "bitcoin,ethereum,solana"
CURRENCY = "usd"

def fetch_prices():
    """Step 1-4: Connect to CoinGecko and pull current prices"""
    url = (
        f"https://api.coingecko.com/api/v3/simple/price"
        f"?ids={COINS}"
        f"&vs_currencies={CURRENCY}"
        f"&include_24hr_change=true"
        f"&include_24hr_vol=true"
        f"&x_cg_demo_api_key={COINGECKO_API_KEY}"
    )

    print("Connecting to CoinGecko API...")
    response = requests.get(url)

    # Confirm connection
    if response.status_code != 200:
        print(f"Connection failed: {response.status_code}")
        return None

    print("Connected. Pulling prices...")
    data = response.json()

    # Step 5: Add timestamp to the data
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    data["pulled_at"] = timestamp

    print(f"Prices pulled at {timestamp}")
    return data


def save_to_s3(data):
    """Step 6-8: Connect to S3 and dump the JSON file"""
    print("Connecting to AWS S3...")
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )
    print("Connected to S3.")

    # Create unique filename using timestamp
    timestamp = datetime.utcnow().strftime("%Y-%m-%d_%H-%M-%S")
    filename = f"raw/prices_{timestamp}.json"

    # Dump JSON to S3
    s3.put_object(
        Bucket=AWS_BUCKET_NAME,
        Key=filename,
        Body=json.dumps(data),
        ContentType="application/json"
    )

    print(f"Raw data saved to S3: s3://{AWS_BUCKET_NAME}/{filename}")
    return filename


def run():
    data = fetch_prices()
    if data:
        save_to_s3(data)
        print("Ingest complete.")
    else:
        print("No data retrieved. Check API credentials.")


if __name__ == "__main__":
    run()
