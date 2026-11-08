"""Airflow task bodies. Connections and Variables are read only at execution."""
import json
import logging
from datetime import datetime
from pathlib import Path

from project5.api import DeliveryAPI
from project5.domain import order_total, timestamp, window

SQL = Path(__file__).parent / 'sql'
log = logging.getLogger(__name__)


def connection():
    from airflow.models import Variable
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    return PostgresHook(postgres_conn_id=Variable.get(
        'project5_dwh_conn_id', default_var='PG_WAREHOUSE_CONNECTION')).get_conn()


def execute_file(name, params=None):
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute((SQL / name).read_text(), params)
    finally:
        conn.close()


def migrate():
    execute_file('001_schema.sql')


def load_api(**context):
    from airflow.models import Variable
    from psycopg2.extras import Json, execute_values
    conf = context['dag_run'].conf or {}
    interval_end = context['data_interval_end'].strftime('%Y-%m-%d %H:%M:%S')
    left, right = window(conf.get('window_end') or interval_end,
                         conf.get('from'), conf.get('to'))
    api = DeliveryAPI(Variable.get('project5_api_url', default_var=
        'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'), {
        'X-Nickname': Variable.get('project5_nickname', default_var='ilnur_askarov'),
        'X-Cohort': Variable.get('project5_cohort', default_var='49'),
        'X-API-KEY': Variable.get('project5_api_key')})
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # Serialize overlapping refreshes; rollback keeps prior snapshot on API failure.
                cur.execute('SELECT pg_advisory_xact_lock(549001)')
                for entity in ('couriers', 'deliveries'):
                    total = 0
                    if entity == 'deliveries':
                        cur.execute('DELETE FROM stg.project5_deliveries WHERE delivery_ts >= %s AND delivery_ts < %s',
                                    (left, right))
                    for page in api.pages(entity, left, right):
                        if entity == 'couriers':
                            execute_values(cur, '''INSERT INTO stg.project5_couriers(object_id,object_value)
                              VALUES %s ON CONFLICT(object_id) DO UPDATE SET
                              object_value=EXCLUDED.object_value,loaded_at=now()''',
                              [(r['_id'],Json(r)) for r in page])
                        else:
                            execute_values(cur, '''INSERT INTO stg.project5_deliveries(object_id,object_value,delivery_ts)
                              VALUES %s ON CONFLICT(object_id) DO UPDATE SET
                              object_value=EXCLUDED.object_value,delivery_ts=EXCLUDED.delivery_ts,loaded_at=now()''',
                              [(r['delivery_id'],Json(r),timestamp(r['delivery_ts'])) for r in page])
                        total += len(page)
                    cur.execute('''INSERT INTO stg.project5_loads(entity,window_from,window_to,rows_count)
                                   VALUES(%s,%s,%s,%s)''',
                                (entity,left if entity == 'deliveries' else None,
                                 right if entity == 'deliveries' else None,total))
                    log.info('Loaded %s: %s objects; window %s -- %s',entity,total,left,right)
    finally:
        conn.close()


def load_dimensions():
    execute_file('002_dimensions.sql')


def load_facts():
    from psycopg2.extras import execute_values
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(549002)')
                # Remove facts removed by a corrected source snapshot.
                cur.execute('''UPDATE dds.dm_orders o SET project5_courier_id=NULL
                  WHERE o.project5_courier_id IS NOT NULL AND NOT EXISTS (
                  SELECT 1 FROM stg.project5_deliveries s WHERE s.object_value->>'order_id'=o.order_key)''')
                cur.execute('''DELETE FROM dds.project5_deliveries d WHERE NOT EXISTS
                  (SELECT 1 FROM stg.project5_deliveries s WHERE s.object_id=d.delivery_key)''')
                cur.execute((SQL / '003_facts.sql').read_text())
                # No watermark: updated old orders and late links are retried too.
                cur.execute('''SELECT o.id,s.object_value::jsonb FROM dds.dm_orders o
                  JOIN stg.ordersystem_orders s ON s.object_id=o.order_key
                  WHERE EXISTS(SELECT 1 FROM dds.project5_deliveries d WHERE d.order_id=o.id)''')
                rows = cur.fetchall()
                values = [(oid,order_total(json.loads(raw) if isinstance(raw,str) else raw))
                          for oid,raw in rows]
                if values:
                    execute_values(cur, '''INSERT INTO dds.project5_order_amounts(order_id,total_sum)
                      VALUES %s ON CONFLICT(order_id) DO UPDATE SET total_sum=EXCLUDED.total_sum''', values)
                log.info('Reconciled Mongo order amounts: %s', len(values))
    finally:
        conn.close()


def covers(intervals, left, right):
    """True only if committed API windows cover the complete requested period."""
    edge = left
    for start,stop in sorted(intervals):
        if start > edge:
            break
        edge = max(edge,stop)
        if edge >= right:
            return True
    return False


def build_ledger(**context):
    end = context['data_interval_end'].replace(tzinfo=None)
    cutoff = datetime(end.year,end.month,1)
    previous = datetime(cutoff.year-1,12,1) if cutoff.month == 1 else datetime(cutoff.year,cutoff.month-1,1)
    conn = connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute('''SELECT window_from,window_to FROM stg.project5_loads
                               WHERE entity='deliveries' ORDER BY window_from''')
                intervals = cur.fetchall()
                if not covers(intervals,previous,cutoff):
                    raise ValueError('Incomplete previous-month API coverage. Run initial load with from=%s and to at least %s' %
                                     (previous,cutoff))
                # Check every retained delivery of a closed month. Missing order links must not silently reduce pay.
                cur.execute('''SELECT count(*) FROM stg.project5_deliveries s
                  LEFT JOIN dds.dm_orders o ON o.order_key=s.object_value->>'order_id'
                  LEFT JOIN dds.dm_timestamps t ON t.id=o.timestamp_id
                  LEFT JOIN dds.project5_deliveries d ON d.delivery_key=s.object_id
                  LEFT JOIN dds.project5_order_amounts a ON a.order_id=o.id
                  WHERE coalesce(t.ts,(s.object_value->>'order_ts')::timestamp) < %s
                  AND (o.id IS NULL OR t.id IS NULL OR d.id IS NULL OR a.order_id IS NULL)''',(cutoff,))
                if cur.fetchone()[0]:
                    raise ValueError('Unresolved delivery/order/amount links: finish order pipeline and rerun')
                # Avoid publishing partial historical months from a seven-day window crossing month start.
                cur.execute('''SELECT DISTINCT date_trunc('month',t.ts)::timestamp
                  FROM dds.project5_deliveries d JOIN dds.dm_orders o ON o.id=d.order_id
                  JOIN dds.dm_timestamps t ON t.id=o.timestamp_id WHERE t.ts < %s''',(cutoff,))
                eligible = []
                for (start,) in cur.fetchall():
                    stop = datetime(start.year+1,1,1) if start.month == 12 else datetime(start.year,start.month+1,1)
                    if covers(intervals,start,stop):
                        eligible.append(start)
                # Include an empty previous month so corrected deletions remove stale ledger rows.
                if previous not in eligible:
                    eligible.append(previous)
                cur.execute((SQL / '004_ledger.sql').read_text(),
                            {'report_before':cutoff,'eligible_months':eligible})
                log.info('Monthly ledger refreshed; closed-month cutoff: %s',cutoff)
    finally:
        conn.close()
