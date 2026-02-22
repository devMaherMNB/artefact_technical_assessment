from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from case_online_retail.src.ingest import run_ingest
from case_online_retail.src.transform import run_transform
from case_online_retail.src.load import run_load
from case_online_retail.src.monitor import run_monitor

def transform_and_load():
    df_products, df_customers, df_facts = run_transform()
    run_load(df_products, df_customers, df_facts)

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
    transform_and_load = PythonOperator(
        task_id='clean_and_transform',
        python_callable=transform_and_load
    )
    monitor = PythonOperator(
        task_id='monitor_data',
        python_callable=run_monitor
    )
    ingest >> transform_and_load >> monitor 