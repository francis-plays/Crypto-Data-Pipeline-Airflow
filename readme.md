Here's a concise README for Project 5:

```markdown
# Crypto Price Pipeline with Apache Airflow

an update on my Automated data pipeline that extracts cryptocurrency prices from CoinGecko, transforms the data, and loads it into Snowflake for analysis. Orchestrated with Apache Airflow to run every hour without manual intervention.

## Architecture

```
CoinGecko API → ingest.py → S3 (raw JSON)
                    ↓
               clean.py → S3 (cleaned CSV)
                    ↓
               load.py → Snowflake
```

**Orchestration:** Apache Airflow DAG with 3 tasks running sequentially  
**Schedule:** Hourly execution (configurable via cron syntax)  
**Retry Logic:** 3 attempts per task with 5-minute delays

## Technologies Used

- **Python 3.11** — Pipeline logic
- **Apache Airflow** — Workflow orchestration and scheduling
- **AWS S3** — Raw and cleaned data storage
- **Snowflake** — Data warehouse
- **CoinGecko API** — Cryptocurrency price data source
- **Pandas** — Data transformation
- **boto3** — AWS S3 integration

## Pipeline Tasks

1. **ingest_prices** — Fetches BTC, ETH, SOL prices from CoinGecko and saves raw JSON to S3
2. **clean_and_score** — Pulls latest raw data from S3, cleans it, converts to CSV, saves to S3
3. **load_to_snowflake** — Loads cleaned CSV from S3 into Snowflake `CRYPTO_MARKET.RAW.PRICES` table

## How to Run Locally

### Prerequisites
- Python 3.11+
- Apache Airflow installed
- AWS S3 bucket configured
- Snowflake account with database/schema/warehouse set up
- CoinGecko API key

### Setup
1. Clone the repository
2. Install dependencies: `pip install apache-airflow boto3 pandas snowflake-connector-python requests`
3. Create `.env` file with credentials (see `.env.example`)
4. Copy DAG file to your airflow root folder: `cp dags/crypto_pipeline_dag.py ~/airflow/dags/`
5. Start Airflow: `airflow standalone`
6. Access UI at `http://localhost:8080`
7. Enable the `crypto_price_pipeline` DAG

### Manual Trigger
In the Airflow UI, click the DAG name, then click the play button (▶) to run immediately.

## Project Structure

```
Crypto-Data-Pipeline-Airflow/
├── config/
│   └── config.py          # Environment variable loader
├── dags/
│   └── crypto_pipeline_dag.py  # Airflow DAG definition
├── scripts/
│   ├── ingest.py          # API extraction logic
│   ├── clean.py           # Data transformation logic
│   └── load.py            # Snowflake loading logic
├── sql/
│   └── analysis.sql       # Sample analysis queries
├── .env                   # Credentials (not committed)
├── .gitignore
└── README.md
```

## Key Learnings

- Orchestrating multi-step data pipelines with Airflow
- Implementing task dependencies and retry logic
- Automating ELT workflows with cron scheduling
- Integrating cloud storage (S3) with data warehouses (Snowflake)

## Future Enhancements

- Add data quality checks with Great Expectations
- Implement Slack/email alerts on pipeline failures
- Extend to more cryptocurrencies and additional metrics
- Deploy to AWS EC2 for 24/7 execution

---

**Author:** Francis Ukpan  
**GitHub:** github.com/francis-plays  
**Date:** April 2026
```

---

Copy that into a file called `README.md` in your project root, then you're done. Want to push to GitHub now, or call it a day here?