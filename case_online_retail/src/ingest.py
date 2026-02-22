import pandas as pd
import os
import uuid
from datetime import datetime
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

from common.logger import get_logger

load_dotenv()
logger = get_logger("Online Retail")  # for logging

DATA_PATH = os.getenv('DATA_PATH', 'archive/online_retail.csv')
DATABASE_URL = os.getenv('DATABASE_URL')

def run_ingest():
    logger.info("Starting ingestion...")

    df = pd.read_csv(DATA_PATH, encoding='ISO-8859-1') # the encoding is required for special characters
    logger.info(f"Loaded {len(df)} rows from {DATA_PATH}")

    # Add pipeline metadata
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


    engine = create_engine(DATABASE_URL)
    
    # Ensure schema and table exist before doing anything
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging_online_retail"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staging_online_retail.raw_transactions (
                invoice_no      VARCHAR(20),
                stock_code      VARCHAR(20),
                description     VARCHAR(255),
                quantity        INTEGER,
                invoice_date    VARCHAR(50),
                unit_price      NUMERIC(10,2),
                customer_id     VARCHAR(20),
                country         VARCHAR(100),
                load_timestamp  TIMESTAMP DEFAULT NOW(),
                batch_id        UUID DEFAULT gen_random_uuid()
            )
        """))
    
    # Now safe to truncate and load (idempotent)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging_online_retail.raw_transactions"))
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
