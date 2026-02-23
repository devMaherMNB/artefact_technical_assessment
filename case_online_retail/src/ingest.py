import pandas as pd
import os
import uuid
import pathlib
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from common.logger import get_logger

load_dotenv()
logger = get_logger("Online Retail")

DATA_PATH = os.getenv('DATA_PATH', 'case_online_retail/archive/online_retail.csv')
DATABASE_URL = os.getenv('DATABASE_URL')

SCHEMA_FILE = pathlib.Path(__file__).resolve().parents[1] / "sql" / "schema.sql"

def run_ingest():
    logger.info("Starting ingestion...")

    # Run schema.sql first — creates all schemas and tables if they don't exist.
    # Using IF NOT EXISTS throughout so this is safe and idempotent on every run.
    engine = create_engine(DATABASE_URL)
    with engine.begin() as conn:
        conn.execute(text(SCHEMA_FILE.read_text()))
    logger.info("Schema initialised")

    df = pd.read_csv(DATA_PATH, encoding='ISO-8859-1') # the encoding is required for special characters
    logger.info(f"Loaded {len(df)} rows from {DATA_PATH}")

    # Add pipeline metadata for observability: allows tracing which run loaded which rows
    df['load_timestamp'] = datetime.now()
    df['batch_id'] = str(uuid.uuid4())

    # Standardize column names
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]

    # Explicit rename to match table schema exactly
    df = df.rename(columns={
        'invoiceno':   'invoice_no',
        'stockcode':   'stock_code',
        'invoicedate': 'invoice_date',
        'unitprice':   'unit_price',
        'customerid':  'customer_id'
    })


    # TRUNCATE before load ensures idempotency: it's faster than DELETE and resets no identity.
    # Every run starts with a clean slate in the staging area.
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging_online_retail.raw_transactions"))
        # Using if_exists='append' after TRUNCATE is safe and avoids dropping/recreating the table schema.
        df.to_sql(
            'raw_transactions',
            conn,
            schema='staging_online_retail',
            if_exists='append',
            index=False
        )

    logger.info(f"Ingested {len(df)} rows into staging_online_retail.raw_transactions")
    return len(df)

if __name__ == '__main__':
    run_ingest()
