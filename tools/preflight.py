#!/usr/bin/env python3
"""Read-only check inside the trainer; does not print credentials."""
from airflow.models import DagBag,Variable
from airflow.configuration import conf
from airflow.providers.postgres.hooks.postgres import PostgresHook

conn_id=Variable.get('project5_dwh_conn_id',default_var='PG_WAREHOUSE_CONNECTION')
hook=PostgresHook(postgres_conn_id=conn_id)
required={
 'stg.ordersystem_orders':{'object_id','object_value'},
 'dds.dm_orders':{'id','order_key','timestamp_id'},
 'dds.dm_timestamps':{'id','ts','year','month'},
}
for table,columns in required.items():
    schema,name=table.split('.')
    actual={r[0] for r in hook.get_records('''SELECT column_name FROM information_schema.columns
             WHERE table_schema=%s AND table_name=%s''',parameters=(schema,name))}
    missing=columns-actual
    if missing:
        raise RuntimeError('%s: missing columns %s. Deploy the sprint order pipeline first.' % (table,sorted(missing)))
    print(table,'OK')
for table in ('stg.ordersystem_orders','dds.dm_orders'):
    print(table,'rows:',hook.get_first('SELECT count(*) FROM '+table)[0])
bag=DagBag(dag_folder=conf.get('core','dags_folder'),include_examples=False)
if bag.import_errors:
    raise RuntimeError(bag.import_errors)
print('DAGs:',', '.join(sorted(bag.dags)))
print('API nickname:',Variable.get('project5_nickname',default_var='ilnur_askarov'))
print('API cohort:',Variable.get('project5_cohort',default_var='49'))
print('API key configured:',bool(Variable.get('project5_api_key',default_var='')))
