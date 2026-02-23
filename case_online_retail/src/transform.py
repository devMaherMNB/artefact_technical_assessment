import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os
from common.logger import get_logger

load_dotenv()
logger = get_logger("Online Retail")
DATABASE_URL = os.getenv("DATABASE_URL")

# if_exists='replace' on Silver: Idempotent. Silver is a transient cleaned layer, 
# acceptable to drop and recreate on each run as it's not queried by analysts directly.
def write_to_silver(df_products, df_customers, df_facts, engine):
    logger.info("Writing cleaned DataFrames to Silver Layer...")
    
    # Write to products
    df_products.to_sql(
        'products', engine, schema='silver_online_retail', 
        if_exists='replace', index=False
    )
    
    # Write to customers
    df_customers.to_sql(
        'customers', engine, schema='silver_online_retail', 
        if_exists='replace', index=False
    )
    
    # Write to transactions (facts with natural keys)
    df_facts.to_sql(
        'transactions', engine, schema='silver_online_retail', 
        if_exists='replace', index=False
    )
    logger.info("Silver Layer load completed successfully.")

def run_transform():
    logger.info("Starting transformation process")
    # create_engine inside function: If DATABASE_URL is None, module-level fails at import; 
    # inside function fails at call time — much easier to debug.
    engine = create_engine(DATABASE_URL)
    df = pd.read_sql("SELECT * FROM staging_online_retail.raw_transactions", engine)
    
    # capture initial_count
    initial_count = len(df)
    
    # drop_duplicates() on all columns: Subset subset=['invoice_no','stock_code'] would remove 
    # ~10k rows; same product can legitimately appear twice on same invoice (e.g., separate order lines).
    df = df.drop_duplicates()
    duplicates_count = initial_count - len(df)
    logger.info(f"Dropped {duplicates_count} duplicate rows")
    
    # dropna(subset=['invoice_no', 'stock_code', 'unit_price']) — log how many dropped
    count_before_dropna = len(df)
    df = df.dropna(subset=['invoice_no', 'stock_code', 'unit_price'])
    dropna_count = count_before_dropna - len(df)
    logger.info(f"Dropped {dropna_count} rows with null mandatory fields (invoice_no, stock_code, unit_price)")
    
    # Fill customer_id nulls with 'UNKNOWN' instead of dropping: 135k rows represent real revenue. 
    # Dropping loses business data; sentinel preserves FK integrity in the DW.
    customer_id_nulls = df['customer_id'].isnull().sum()
    df['customer_id'] = df['customer_id'].fillna('UNKNOWN')
    logger.info(f"Filled {customer_id_nulls} null customer_id values with 'UNKNOWN'")
    
    # fill description nulls AND empty strings — log count
    desc_to_fill = df['description'].isnull() | (df['description'].str.strip() == '')
    desc_fill_count = desc_to_fill.sum()
    df.loc[desc_to_fill, 'description'] = 'NO DESCRIPTION'
    logger.info(f"Filled {desc_fill_count} null or empty description values with 'NO DESCRIPTION'")
    
    # strip whitespace from string columns
    df = df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)
    logger.info("Stripped whitespace from all string columns")

    # a) Cast invoice_date
    df['invoice_date'] = pd.to_datetime(df['invoice_date'])
    logger.info("Casted invoice_date to datetime")

    # b) Derive date_id (YYYYMMDD integer)
    df['date_id'] = df['invoice_date'].dt.strftime('%Y%m%d').astype(int)
    logger.info(f"Derived date_id in YYYYMMDD format")

    # Filter unit_price <= 0 but keep negative quantity: 
    # Bad price = data error; negative quantity = valid return/cancellation event.
    bad_price_count = (df['unit_price'] <= 0).sum()
    df = df[df['unit_price'] > 0]
    logger.info(f"Dropped {bad_price_count} rows with unit_price <= 0")

    # d) Compute total_value
    df['total_value'] = df['quantity'] * df['unit_price']
    logger.info("Computed total_value (quantity * unit_price)")

    # building dimension tables
    df_products = df[['stock_code', 'description']].drop_duplicates(subset=['stock_code'])
    df_customers = df[['customer_id', 'country']].drop_duplicates(subset=['customer_id']).rename(columns={'customer_id': 'raw_customer_id'})
    # renamed customer_id to raw_customer_id in df_facts
    df_facts = df[['invoice_no', 'stock_code', 'customer_id', 'date_id', 'quantity', 'unit_price', 'total_value']].rename(columns={'customer_id': 'raw_customer_id'})

    # Write cleaned DataFrames to Silver layer
    write_to_silver(df_products, df_customers, df_facts, engine)

    return df_products, df_customers, df_facts
    
if __name__ == '__main__':
    run_transform()
    logger.info("Transform completed successfully")