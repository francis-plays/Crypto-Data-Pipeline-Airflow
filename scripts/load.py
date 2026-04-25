import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import pandas as pd
import snowflake.connector
from io import StringIO
from config.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_BUCKET_NAME,
    AWS_REGION,
    SNOWFLAKE_ACCOUNT,
    SNOWFLAKE_USER,
    SNOWFLAKE_PASSWORD,
    SNOWFLAKE_WAREHOUSE,
    SNOWFLAKE_DATABASE,
    SNOWFLAKE_SCHEMA
)


def pull_csv_from_s3():
    """Connect to S3 and pull the latest cleaned CSV"""
    s3 = boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Get the latest cleaned file
    response = s3.list_objects_v2(Bucket=AWS_BUCKET_NAME, Prefix="cleaned/")
    files = response.get("Contents", [])

    if not files:
        print("No cleaned files found in S3.")
        return None

    latest = sorted(files, key=lambda x: x["LastModified"], reverse=True)[0]
    print(f"Pulling latest cleaned file: {latest['Key']}")

    obj = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=latest["Key"])
    df = pd.read_csv(StringIO(obj["Body"].read().decode("utf-8")))
    print(f"Loaded {len(df)} rows from S3")
    return df


def connect_to_snowflake():
    """Connect to Snowflake"""
    print("Connecting to Snowflake...")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE
    )
    print("Connected to Snowflake.")
    return conn


def load_to_snowflake(conn, df):
    """Match CSV columns to Snowflake table and append rows"""
    cursor = conn.cursor()
    inserted = 0

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO prices (
                coin, price_usd, volume_24h, change_24h, pulled_at
            ) VALUES (%s, %s, %s, %s, %s)
        """, (
            str(row["coin"]),
            float(row["price_usd"]),
            float(row["volume_24h"]),
            float(row["change_24h"]),
            str(row["pulled_at"])
        ))
        inserted += 1

    conn.commit()
    print(f"Inserted {inserted} rows into Snowflake prices table.")


def run():
    df = pull_csv_from_s3()
    if df is not None:
        conn = connect_to_snowflake()
        load_to_snowflake(conn, df)
        conn.close()
        print("Load complete. Connection closed.")


if __name__ == "__main__":
    run()