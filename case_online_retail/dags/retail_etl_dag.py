from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from case_online_retail.src.ingest import run_ingest
from case_online_retail.src.transform import run_transform
from case_online_retail.src.load import run_load
from case_online_retail.src.monitor import run_monitor

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

    ingest >> transform >> load >> monitor