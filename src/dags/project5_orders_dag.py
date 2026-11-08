"""Daily source/order facts DAG, extended with delivery facts and courier CDM."""
from datetime import datetime,timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from project5.orders import init_orders,load_mongo,load_bonus,load_orders_dds
from project5.integration import extend_orders_dag

with DAG('project5_orders',start_date=datetime(2022,1,1),schedule_interval='0 2 * * *',
    catchup=False,max_active_runs=1,is_paused_upon_creation=True,
    default_args={'owner':'Ильнур Аскаров','retries':2,'retry_delay':timedelta(minutes=2)},
    tags=['project5']) as project5_orders:
    init=PythonOperator(task_id='init_order_schema',python_callable=init_orders)
    mongo=PythonOperator(task_id='load_mongo_sources',python_callable=load_mongo)
    bonus=PythonOperator(task_id='load_bonus_source',python_callable=load_bonus)
    facts=PythonOperator(task_id='load_order_facts',python_callable=load_orders_dds)
    init >> [mongo,bonus] >> facts
extend_orders_dag(project5_orders)
# The delivery migration requires the base tables, including on the first run.
project5_orders.get_task('init_order_schema') >> project5_orders.get_task('project5_trigger_stg')
