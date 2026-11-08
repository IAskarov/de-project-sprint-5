"""Extend the existing order/fact DAG instead of racing another fact DAG."""
from datetime import timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from project5.tasks import load_facts, build_ledger


def extend_orders_dag(dag):
    if dag.has_task('project5_build_ledger'):
        return
    original_leaves = list(dag.leaves)
    if not original_leaves:
        raise ValueError('Order DAG must contain the existing source and fact tasks')
    with dag:
        raw_trigger = TriggerDagRunOperator(
            task_id='project5_trigger_stg',trigger_dag_id='project5_delivery_stg',
            execution_date='{{ ts }}',reset_dag_run=True,wait_for_completion=False,
            conf={'window_end':"{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}",
                  'from':"{{ dag_run.conf.get('from', '') }}",'to':"{{ dag_run.conf.get('to', '') }}"})
        raw_wait = ExternalTaskSensor(task_id='project5_wait_stg',
            external_dag_id='project5_delivery_stg',external_task_id=None,
            allowed_states=['success'],failed_states=['failed'],mode='reschedule',
            poke_interval=30,timeout=7200,check_existence=True)
        dims_trigger = TriggerDagRunOperator(
            task_id='project5_trigger_dimensions',trigger_dag_id='project5_delivery_dimensions',
            execution_date='{{ ts }}',reset_dag_run=True,wait_for_completion=False)
        dims_wait = ExternalTaskSensor(task_id='project5_wait_dimensions',
            external_dag_id='project5_delivery_dimensions',external_task_id=None,
            allowed_states=['success'],failed_states=['failed'],mode='reschedule',
            poke_interval=30,timeout=7200,check_existence=True)
        facts = PythonOperator(task_id='project5_load_facts',python_callable=load_facts,
                               retries=2,retry_delay=timedelta(minutes=2))
        ledger = PythonOperator(task_id='project5_build_ledger',python_callable=build_ledger,
                                retries=2,retry_delay=timedelta(minutes=2))
        raw_trigger >> raw_wait >> dims_trigger >> dims_wait >> facts >> ledger
        for task in original_leaves:
            task >> facts
    # Serialise parent runs so two runs cannot reset the same child while it is being read.
    dag.max_active_runs = 1
