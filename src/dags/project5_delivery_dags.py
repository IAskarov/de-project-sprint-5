"""Child DAGs run daily through the existing order DAG's explicit triggers."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from project5.tasks import migrate, load_api, load_dimensions

DEFAULTS = {'owner':'Ильнур Аскаров','retries':2,'retry_delay':timedelta(minutes=2)}

with DAG('project5_delivery_stg',start_date=datetime(2022,1,1),schedule_interval=None,
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['project5']) as project5_delivery_stg:
    init = PythonOperator(task_id='migrate',python_callable=migrate)
    raw = PythonOperator(task_id='load_api',python_callable=load_api)
    init >> raw

with DAG('project5_delivery_dimensions',start_date=datetime(2022,1,1),schedule_interval=None,
         catchup=False,max_active_runs=1,default_args=DEFAULTS,tags=['project5']) as project5_delivery_dimensions:
    dims = PythonOperator(task_id='load_dimensions',python_callable=load_dimensions)
