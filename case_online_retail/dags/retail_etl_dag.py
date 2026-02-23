from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from sqlalchemy import create_engine, text
from case_online_retail.src.ingest import run_ingest
from case_online_retail.src.transform import run_transform
from case_online_retail.src.load import run_load
from case_online_retail.src.monitor import run_monitor
import os
import logging

def on_failure_alert(context):
    """
    Called automatically by Airflow when any task in this DAG fails.
    Logs a structured alert with task and DAG context.
    In production, replace logger with a Slack webhook or smtplib email call.
    """
    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date")
    log_url = context.get("task_instance").log_url
    logging.error(
        f"PIPELINE FAILURE | DAG: {dag_id} | Task: {task_id} | "
        f"Date: {execution_date} | Logs: {log_url}"
    )

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
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS staging_online_retail.raw_transactions_archive (
                LIKE staging_online_retail.raw_transactions INCLUDING ALL,
                snapshot_id     UUID NOT NULL DEFAULT gen_random_uuid(),
                snapshot_date   DATE NOT NULL DEFAULT CURRENT_DATE
            )
        """))

        
        # Delete today's snapshot if it exists to ensure idempotency on reruns
        conn.execute(text("DELETE FROM staging_online_retail.raw_transactions_archive WHERE snapshot_date = CURRENT_DATE"))
        conn.execute(text("""
            INSERT INTO staging_online_retail.raw_transactions_archive
            SELECT *, gen_random_uuid(), CURRENT_DATE
            FROM staging_online_retail.raw_transactions
        """))

with DAG(
    dag_id='retail_etl_dag',
    schedule_interval='@daily',
    start_date=datetime(2026, 2, 22),
    catchup=False,
    tags=['retail_etl'],
    # on_failure_callback fires on any task failure — structured alert with task context.
    # Production: replace logging.error with Slack webhook or smtplib email.
    on_failure_callback=on_failure_alert,
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