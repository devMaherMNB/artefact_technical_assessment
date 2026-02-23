from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from case_online_retail.src.ingest import run_ingest
from case_online_retail.src.transform import run_transform
from case_online_retail.src.load import run_load
from case_online_retail.src.monitor import run_monitor
import os

def run_snapshot():
    """
    Daily versioning step: snapshots the current staging table into
    raw_transactions_archive with a snapshot_date key.
    One snapshot per day — idempotent via the snapshot_date column.
    Runs after ingest so the archive always reflects the latest raw load.
    """
    from dotenv import load_dotenv
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))
    with engine.begin() as conn:
        logger.info("Creating raw_transactions_archive table if it doesn't exist")
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staging_online_retail.raw_transactions_archive (
                LIKE staging_online_retail.raw_transactions INCLUDING ALL,
                snapshot_id     UUID NOT NULL DEFAULT gen_random_uuid(),
                snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE
            )
        """))

        logger.info("Deleting today's snapshot if it exists to ensure idempotency on reruns")
        # Delete today's snapshot if it exists to ensure idempotency on reruns
        conn.execute(text("DELETE FROM staging_online_retail.raw_transactions_archive WHERE snapshot_date = CURRENT_DATE"))
        conn.execute(text("""
            INSERT INTO staging_online_retail.raw_transactions_archive
            SELECT *, gen_random_uuid(), CURRENT_DATE
            FROM staging_online_retail.raw_transactions
        """))

with DAG(   
    dag_id='retail_etl_dag',
    schedule_interval= '@daily',
    start_date=datetime(2026, 2, 22),
    catchup=False,
    tags=['retail_etl']
) as dag:
    
    ingest = PythonOperator(
        task_id='ingest_raw_data',
        python_callable=run_ingest
    )

    # Versioning: snapshot staging data after ingest for audit trail and historical replay
    snapshot = PythonOperator(
        task_id='snapshot_staging',
        python_callable=run_snapshot
    )

    transform = PythonOperator(
        task_id='transform_to_silver',
        python_callable=run_transform
    )

    load = PythonOperator(
        task_id='load_to_gold_dw',
        python_callable=run_load
    )

    monitor = PythonOperator(
        task_id='monitor_data_quality',
        python_callable=run_monitor
    )

    ingest >> snapshot >> transform >> load >> monitor