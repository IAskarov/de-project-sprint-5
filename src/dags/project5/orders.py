"""MongoDB/bonus baseline, matching the observed training source schemas."""
import json
from datetime import datetime,timedelta
from project5.tasks import connection,execute_file
from project5.domain import order_total


def init_orders():
    execute_file('000_orders_schema.sql')


def _encode(value):
    if isinstance(value,datetime):
        return value.isoformat()
    return str(value)  # BSON ObjectId/Decimal128 -> JSON strings


def load_mongo():
    from airflow.models import Variable
    from pymongo import MongoClient
    from psycopg2.extras import Json,execute_values
    client=MongoClient(host=Variable.get('MONGO_DB_HOST'),
        username=Variable.get('MONGO_DB_USER'),password=Variable.get('MONGO_DB_PASSWORD'),
        authSource=Variable.get('MONGO_DB_DATABASE_NAME'),
        replicaSet=Variable.get('MONGO_DB_REPLICA_SET'),tls=True,
        tlsCAFile=Variable.get('MONGO_DB_CERTIFICATE_PATH'),
        serverSelectionTimeoutMS=20000,connectTimeoutMS=15000,socketTimeoutMS=60000)
    conn=connection()
    try:
        db=client[Variable.get('MONGO_DB_DATABASE_NAME')]
        # Each source checkpoint and all its batches commit together.
        for collection in ('users','restaurants','orders'):
            with conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT pg_advisory_xact_lock(549010)')
                    key='mongo_'+collection
                    cur.execute('SELECT value FROM stg.project5_state WHERE source=%s',(key,))
                    saved=cur.fetchone()
                    previous=datetime.fromisoformat(saved[0]['update_ts']) if saved else None
                    # Revisit updates at equal timestamps and the previous seven days.
                    query={'update_ts':{'$gte':previous-timedelta(days=7)}} if previous else {}
                    docs=db[collection].find(query).sort([('update_ts',1),('_id',1)]).batch_size(500)
                    latest=previous
                    batch=[]
                    statement='''INSERT INTO stg.ordersystem_%s(object_id,object_value,update_ts)
                        VALUES %%s ON CONFLICT(object_id) DO UPDATE SET
                        object_value=EXCLUDED.object_value,update_ts=EXCLUDED.update_ts''' % collection
                    try:
                        for doc in docs:
                            if collection=='orders':
                                order_total(doc)
                                if doc.get('order_items') or doc.get('final_status') == 'CLOSED':
                                    order_total({'order_items':doc.get('order_items')})
                            updated=doc['update_ts']
                            if not isinstance(updated,datetime):
                                raise ValueError('Mongo update_ts must be a datetime')
                            raw=json.loads(json.dumps(doc,default=_encode))
                            batch.append((str(doc['_id']),Json(raw),updated))
                            latest=max(latest,updated) if latest else updated
                            if len(batch)==500:
                                execute_values(cur,statement,batch)
                                batch=[]
                        if batch:
                            execute_values(cur,statement,batch)
                    finally:
                        docs.close()
                    if latest:
                        cur.execute('''INSERT INTO stg.project5_state VALUES(%s,%s)
                            ON CONFLICT(source) DO UPDATE SET value=EXCLUDED.value''',
                            (key,Json({'update_ts':latest.isoformat()})))
    finally:
        conn.close()
        client.close()


def load_bonus():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2.extras import Json,execute_values
    origin=PostgresHook(postgres_conn_id='PG_ORIGIN_BONUS_SYSTEM_CONNECTION').get_conn()
    dest=connection()
    try:
        with dest:
            with dest.cursor() as cur:
                cur.execute('SELECT pg_advisory_xact_lock(549011)')
                with origin.cursor() as source:
                    for table in ('ranks','users'):
                        source.execute('SELECT row_to_json(t) FROM public.'+table+' t')
                        rows=source.fetchall()
                        if rows:
                            execute_values(cur,'INSERT INTO stg.project5_bonus_'+table+'''(id,object_value)
                                VALUES %s ON CONFLICT(id) DO UPDATE SET object_value=EXCLUDED.object_value''',
                                [(r[0]['id'],Json(r[0])) for r in rows])
                    cur.execute("SELECT value FROM stg.project5_state WHERE source='bonus_events'")
                    saved=cur.fetchone()
                    # ID is monotonic in outbox; overlap also refreshes recent corrections.
                    last=int(saved[0]['id']) if saved else -1
                    source.execute('''SELECT id,event_ts,event_type,event_value FROM public.outbox
                        WHERE id >= %s ORDER BY id''',(max(-1,last-10000),))
                    latest=last
                    while True:
                        rows=source.fetchmany(1000)
                        if not rows: break
                        values=[]
                        for oid,ts,kind,raw in rows:
                            raw=json.loads(raw) if isinstance(raw,str) else raw
                            values.append((oid,ts,kind,Json(raw)))
                            latest=max(latest,oid)
                        execute_values(cur,'''INSERT INTO stg.project5_bonus_events
                            (id,event_ts,event_type,event_value) VALUES %s ON CONFLICT(id) DO UPDATE SET
                            event_ts=EXCLUDED.event_ts,event_type=EXCLUDED.event_type,event_value=EXCLUDED.event_value''',values)
                    cur.execute('''INSERT INTO stg.project5_state VALUES('bonus_events',%s)
                        ON CONFLICT(source) DO UPDATE SET value=EXCLUDED.value''',(Json({'id':latest}),))
    finally:
        origin.close()
        dest.close()


def load_orders_dds():
    execute_file('005_orders_dds.sql')
