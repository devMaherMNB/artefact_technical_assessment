import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from common.logger import get_logger
from case_online_retail.src.transform import run_transform

load_dotenv()
logger = get_logger("Online Retail")
DATABASE_URL = os.getenv("DATABASE_URL")

class RetailLoader:
    def __init__(self, engine):
        self.engine = engine

    def load_dim_date(self, df_facts):
        logger.info("Loading dim_date...")
        unique_date_ids = df_facts['date_id'].unique()
        dates = pd.to_datetime(unique_date_ids.astype(str), format='%Y%m%d')
        
        dim_date_df = pd.DataFrame({
            'date_id': unique_date_ids,
            'full_date': dates.date,
            'year': dates.year,
            'quarter': dates.quarter,
            'month': dates.month,
            'month_name': dates.strftime('%B'),
            'week': dates.isocalendar().week,
            'day_of_week': dates.dayofweek + 1,
            'day_name': dates.strftime('%A'),
            'is_weekend': dates.dayofweek >= 5
        })

        with self.engine.connect() as conn:
            for _, row in dim_date_df.iterrows(): # too slow
                insert_stmt = text("""
                    INSERT INTO dw_online_retail.dim_date (
                        date_id, full_date, year, quarter, month, month_name, week, day_of_week, day_name, is_weekend
                    ) VALUES (
                        :date_id, :full_date, :year, :quarter, :month, :month_name, :week, :day_of_week, :day_name, :is_weekend
                    ) ON CONFLICT (date_id) DO NOTHING;
                """)
                conn.execute(insert_stmt, row.to_dict())
            conn.commit()
        logger.info(f"Loaded {len(dim_date_df)} dates into dim_date")

    def load_dim_products(self, df_products):
        logger.info("Loading dim_products...")
        with self.engine.connect() as conn:
            for _, row in df_products.iterrows(): # too slow
                insert_stmt = text("""
                    INSERT INTO dw_online_retail.dim_products (stock_code, description)
                    VALUES (:stock_code, :description)
                    ON CONFLICT (stock_code) DO UPDATE SET description = EXCLUDED.description;
                """)
                conn.execute(insert_stmt, row.to_dict())
            conn.commit()
        logger.info(f"Loaded {len(df_products)} products into dim_products")

    def load_dim_customers(self, df_customers):
        logger.info("Loading dim_customers...")
        with self.engine.connect() as conn:
            for _, row in df_customers.iterrows(): # too slow
                insert_stmt = text("""
                    INSERT INTO dw_online_retail.dim_customers (raw_customer_id, country)
                    VALUES (:raw_customer_id, :country)
                    ON CONFLICT (raw_customer_id) DO UPDATE SET country = EXCLUDED.country;
                """)
                conn.execute(insert_stmt, row.to_dict())
            conn.commit()
        logger.info(f"Loaded {len(df_customers)} customers into dim_customers")

    def resolve_surrogate_keys(self, df_facts):
        logger.info("Resolving surrogate keys for fact table...")
        
        # Get mapping from dim_products
        dim_products = pd.read_sql("SELECT product_id, stock_code FROM dw_online_retail.dim_products", self.engine)
        # Get mapping from dim_customers
        dim_customers = pd.read_sql("SELECT customer_id, raw_customer_id FROM dw_online_retail.dim_customers", self.engine)
        
        # Merge with df_facts
        df_enriched = df_facts.merge(dim_products, on='stock_code', how='left')
        df_enriched = df_enriched.merge(dim_customers, on='raw_customer_id', how='left')
        
        # Select final columns for fact_sales
        df_final = df_enriched[['invoice_no', 'customer_id', 'product_id', 'date_id', 'quantity', 'unit_price', 'total_value']]
        
        logger.info(f"Surrogate keys resolved. Enriched dataframe has {len(df_final)} rows")
        return df_final

    def load_fact_sales(self, df_facts_enriched):
        logger.info("Loading fact_sales...")
        df_facts_enriched.to_sql(
            'fact_sales', 
            self.engine, 
            schema='dw_online_retail', 
            if_exists='append', 
            index=False,
            method='multi',
            chunksize=1000
        )
        logger.info(f"Loaded {len(df_facts_enriched)} facts into fact_sales")

    def run_load(self, df_products, df_customers, df_facts):
        self.load_dim_date(df_facts)
        self.load_dim_products(df_products)
        self.load_dim_customers(df_customers)
        df_facts_enriched = self.resolve_surrogate_keys(df_facts)
        self.load_fact_sales(df_facts_enriched)

def run_load(df_products, df_customers, df_facts):
    engine = create_engine(DATABASE_URL)
    loader = RetailLoader(engine)
    loader.run_load(df_products, df_customers, df_facts)

if __name__ == '__main__':
    df_products, df_customers, df_facts = run_transform()
    run_load(df_products, df_customers, df_facts)