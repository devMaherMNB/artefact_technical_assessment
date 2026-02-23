from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from common.logger import get_logger

load_dotenv()
logger = get_logger("Online Retail Monitor")
DATABASE_URL = os.getenv("DATABASE_URL")

# Known baseline from the UCI dataset.
# Bronze → Silver expects ~7k row loss (dedup + bad price filter).
# Silver → Gold expects ~0 loss — load.py only resolves surrogate keys, no filtering.
EXPECTED_MIN_ROWS = 500000
MAX_SILVER_TO_GOLD_LOSS = 1000

def run_monitor():
    engine = create_engine(DATABASE_URL)
    logger.info("Starting Data Quality Monitor...")

    with engine.connect() as conn:

        # --- Bronze → Silver → Gold row count pipeline audit ---
        # Validates the full pipeline in one shot by tracking row counts across all three layers.
        # Any unexpected drop between layers indicates a failure in that stage.
        bronze_count = conn.execute(text(
            "SELECT COUNT(*) FROM staging_online_retail.raw_transactions"
        )).scalar()

        silver_count = conn.execute(text(
            "SELECT COUNT(*) FROM silver_online_retail.transactions"
        )).scalar()

        gold_count = conn.execute(text(
            "SELECT COUNT(*) FROM dw_online_retail.fact_sales"
        )).scalar()

        bronze_to_silver_loss = bronze_count - silver_count
        silver_to_gold_loss = silver_count - gold_count

        logger.info(f"Pipeline audit — Bronze: {bronze_count} | Silver: {silver_count} | Gold: {gold_count}")
        logger.info(f"Bronze → Silver loss = {bronze_to_silver_loss} rows (dedup + bad price filter, expected ~7k)")
        logger.info(f"Silver → Gold loss   = {silver_to_gold_loss} rows (expected ~0)")

        if bronze_count < EXPECTED_MIN_ROWS:
            logger.error(f"ANOMALY: Bronze has {bronze_count} rows — below {EXPECTED_MIN_ROWS}. Incomplete ingest suspected.")

        if gold_count == 0:
            logger.error("ANOMALY: fact_sales is empty. Pipeline failure suspected.")

        if bronze_to_silver_loss < 0:
            logger.error(f"ANOMALY: Silver has MORE rows than Bronze. Data integrity issue.")

        if silver_to_gold_loss > MAX_SILVER_TO_GOLD_LOSS:
            logger.warning(f"WARNING: {silver_to_gold_loss} rows lost between Silver and Gold. Surrogate key resolution may have failed.")
        elif silver_to_gold_loss < 0:
            logger.error(f"ANOMALY: Gold has MORE rows than Silver. Duplicate load suspected.")
        else:
            logger.info("Pipeline audit passed — row counts within expected bounds.")

if __name__ == "__main__":
    run_monitor()
