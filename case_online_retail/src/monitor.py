from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from common.logger import get_logger

load_dotenv()
logger = get_logger("Online Retail Monitor")
DATABASE_URL = os.getenv("DATABASE_URL")

def run_monitor():
    engine = create_engine(DATABASE_URL)
    logger.info("Starting Data Quality Monitor...")

    with engine.connect() as conn:
        fact_count = conn.execute(text("SELECT COUNT(*) FROM dw_online_retail.fact_sales")).scalar()
        logger.info(f"DQ Check: Total rows in fact_sales = {fact_count}")
        
        if fact_count == 0:
            logger.error("ANOMALY: fact_sales is empty. Pipeline failure suspected.")

        null_value_count = conn.execute(text("SELECT COUNT(*) FROM dw_online_retail.fact_sales WHERE total_value IS NULL")).scalar()
        if null_value_count > 0:
            logger.warning(f"WARNING: Found {null_value_count} rows with NULL total_value. Calculation error likely.")
        else:
            logger.info("DQ Check: No NULL total_values found.")

        negative_count = conn.execute(text("SELECT COUNT(*) FROM dw_online_retail.fact_sales WHERE total_value < 0")).scalar()
        logger.info(f"DQ Check: Total rows with negative total_value (returns) = {negative_count}")

        date_range = conn.execute(text("SELECT MIN(date_id), MAX(date_id) FROM dw_online_retail.fact_sales")).fetchone()
        min_date, max_date = date_range
        logger.info(f"DQ Check: Date range found: {min_date} to {max_date}")
        
        if min_date and min_date < 20100000:
            logger.warning(f"WARNING: Unexpectedly old date detected ({min_date}).")
        if max_date and max_date > 20261231: 
            logger.warning(f"WARNING: Future date detected ({max_date}). Check casting logic.")

        staging_count = conn.execute(text("SELECT COUNT(*) FROM staging_online_retail.raw_transactions")).scalar()
        gap = staging_count - fact_count
        logger.info(f"DQ Check: Staging count: {staging_count}, Warehouse count: {fact_count}, Gap: {gap}")
        
        if gap > 0:
            logger.info(f"Information: {gap} rows filtered during transformation (expected due to DQ rules).")
        elif gap < 0:
            logger.error(f"ANOMALY: Warehouse has more rows than staging ({gap}). Integrity issue.")

if __name__ == "__main__":
    run_monitor()