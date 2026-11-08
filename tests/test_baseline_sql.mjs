// Isolated test of the fresh warehouse, with observed Mongo/bonus field shapes.
import {createRequire} from 'node:module';
import {readFileSync} from 'node:fs';
import {resolve} from 'node:path';
import assert from 'node:assert/strict';
const require=createRequire(import.meta.url);
const {PGlite}=require(process.env.PROJECT5_PGLITE || '@electric-sql/pglite');
const db=new PGlite();
const root=resolve(process.argv[2] || '.');
const sql=n=>readFileSync(resolve(root,'src/dags/project5/sql',n),'utf8');
await db.exec(sql('000_orders_schema.sql'));
await db.exec(sql('001_schema.sql'));
await db.exec(sql('000_orders_schema.sql'));
const rows=[
 ['users','u1',{name:'User',login:'login'}],
 ['restaurants','r1',{name:'Restaurant',menu:[]}],
 ['orders','o1',{user:{id:'u1'},restaurant:{id:'r1'},date:'2026-10-31T23:59:00',final_status:'CLOSED',cost:1000,
  order_items:[{id:'p1',name:'Item',price:600,quantity:1},{id:'p2',name:'Other',price:400,quantity:1}]}],
 ['orders','o2',{user:{id:'u1'},restaurant:{id:'r1'},date:'2026-10-30T12:00:00',final_status:'CLOSED',cost:10000,
  order_items:[{id:'p1',name:'Item',price:5000,quantity:2}]}],
 ['orders','o3',{user:{id:'u1'},restaurant:{id:'r1'},date:'2026-10-30T12:30:00',final_status:'CANCELLED',cost:100,
  order_items:[{id:'p1',name:'Item',price:100,quantity:1}]}]
];
for(const [table,id,doc] of rows){
 await db.query(`INSERT INTO stg.ordersystem_${table}(object_id,object_value,update_ts) VALUES($1,$2::jsonb,'2026-11-08')`,[id,JSON.stringify(doc)]);
}
await db.exec(`INSERT INTO stg.project5_bonus_ranks VALUES(1,'{"name":"Rank","bonus_percent":5,"min_payment_threshold":0}');
 INSERT INTO stg.project5_bonus_users VALUES(1,'{"order_user_id":"u1"}');
 INSERT INTO stg.project5_bonus_events VALUES(1,'2026-11-08','bonus_transaction','{"order_id":"o1"}');`);
await db.exec(sql('005_orders_dds.sql'));
await db.exec(sql('005_orders_dds.sql'));
let check=(await db.query('SELECT count(*)::int n,sum(total_sum) total FROM dds.fct_product_sales')).rows[0];
assert.equal(check.n,3); assert.equal(Number(check.total),11000);
assert.equal((await db.query('SELECT count(*)::int n FROM dds.dm_orders')).rows[0].n,3);
assert.equal((await db.query('SELECT count(*)::int n FROM dds.project5_bonus_events')).rows[0].n,1);
await db.exec(`INSERT INTO stg.project5_couriers(object_id,object_value) VALUES('c1','{"_id":"c1","name":"Courier"}');
 INSERT INTO stg.project5_deliveries(object_id,object_value,delivery_ts) VALUES
 ('d1','{"delivery_id":"d1","order_id":"o1","courier_id":"c1","rate":4,"tip_sum":100,"sum":1}','2026-11-01 00:30'),
 ('d2','{"delivery_id":"d2","order_id":"o2","courier_id":"c1","rate":5,"tip_sum":20,"sum":1}','2026-10-30 13:00');`);
await db.exec(sql('002_dimensions.sql')); await db.exec(sql('003_facts.sql'));
await db.exec(`INSERT INTO dds.project5_order_amounts
 SELECT o.id,(s.object_value->>'cost')::numeric FROM stg.ordersystem_orders s JOIN dds.dm_orders o ON o.order_key=s.object_id;`);
const ledger=sql('004_ledger.sql').replaceAll('%(report_before)s',"'2026-11-01'::timestamp")
 .replaceAll('%(eligible_months)s',"ARRAY['2026-10-01'::timestamp]");
await db.exec('BEGIN;'+ledger+'COMMIT;');
check=(await db.query('SELECT * FROM cdm.dm_courier_ledger')).rows[0];
assert.equal(Number(check.orders_count),2);assert.equal(Number(check.orders_total_sum),11000);
assert.equal(Number(check.courier_reward_sum),1089);
// A repeat of the order pipeline preserves the new courier reference.
await db.exec(sql('005_orders_dds.sql'));
assert.equal((await db.query('SELECT count(*)::int n FROM dds.dm_orders WHERE project5_courier_id IS NOT NULL')).rows[0].n,2);
console.log('Fresh warehouse: Mongo shapes, dimensions, line facts, bonus data, rerun, delivery links, CDM OK');
await db.close();
