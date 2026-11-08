// Optional isolated PostgreSQL-engine tests: npm install --no-save @electric-sql/pglite
// Then: node tests/test_sql.mjs .
import { createRequire } from 'node:module';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import assert from 'node:assert/strict';
const require = createRequire(import.meta.url);
const { PGlite } = require(process.env.PROJECT5_PGLITE || '@electric-sql/pglite');
const root=resolve(process.argv[2] || '.');
const db = new PGlite();
const sql=(name)=>readFileSync(resolve(root,'src/dags/project5/sql',name),'utf8');
await db.exec(`CREATE SCHEMA stg; CREATE SCHEMA dds;
 CREATE TABLE stg.ordersystem_orders(object_id text PRIMARY KEY,object_value jsonb);
 CREATE TABLE dds.dm_timestamps(id integer PRIMARY KEY,ts timestamp,year integer,month integer);
 CREATE TABLE dds.dm_orders(id integer PRIMARY KEY,order_key text UNIQUE,timestamp_id integer REFERENCES dds.dm_timestamps);
 INSERT INTO dds.dm_timestamps VALUES
 (1,'2026-10-31 23:59',2026,10),(2,'2026-10-30',2026,10),(3,'2026-10-01',2026,10),
 (4,'2026-11-01',2026,11),(5,'2026-12-31 23:59',2026,12),(6,'2027-01-01',2027,1);
 INSERT INTO dds.dm_orders SELECT n,'o'||n,n FROM generate_series(1,6) n;`);
await db.exec(sql('001_schema.sql'));
await db.exec(sql('001_schema.sql')); // migration is repeatable
await db.exec(`INSERT INTO stg.project5_couriers VALUES
 ('c1','{"_id":"c1","name":"First"}',now()),('c2','{"_id":"c2","name":"Second"}',now());`);
await db.exec(sql('002_dimensions.sql'));
const facts=[
 ['d1','o1','c1','2026-11-01 00:30',4,100,1000],
 ['d2','o2','c1','2026-10-30 12:00',5,20,10000],
 ['d3','o3','c2','2026-10-01 12:00',5,0,100],
 ['d4','o4','c1','2026-11-01 12:00',1,999,999999],
 ['d5','o5','c1','2027-01-01 00:30',5,10,2000],
 ['d6','o6','c1','2027-01-01 12:00',1,999,999999]];
for(const [id,order,courier,ts,rate,tip,amount] of facts){
 await db.query(`INSERT INTO stg.project5_deliveries(object_id,object_value,delivery_ts) VALUES($1,$2::jsonb,$3)`,
  [id,JSON.stringify({delivery_id:id,order_id:order,courier_id:courier,rate,tip_sum:tip,sum:888888}),ts]);
 await db.query('INSERT INTO dds.project5_order_amounts VALUES($1,$2)',[Number(order.slice(1)),amount]);
}
await db.exec(sql('003_facts.sql'));
await db.exec(sql('003_facts.sql'));
assert.equal((await db.query('SELECT count(*)::int n FROM dds.project5_deliveries')).rows[0].n,6);
const ledger=sql('004_ledger.sql').replaceAll('%(report_before)s',"'2027-01-01'::timestamp")
 .replaceAll('%(eligible_months)s',"ARRAY['2026-10-01'::timestamp,'2026-12-01'::timestamp]");
const run=()=>db.exec('BEGIN;'+ledger+'COMMIT;');
await run();
let rows=(await db.query('SELECT * FROM cdm.dm_courier_ledger ORDER BY settlement_month,courier_id')).rows;
assert.equal(rows.length,3);
assert.equal(Number(rows[0].orders_count),2);
assert.equal(Number(rows[0].orders_total_sum),11000);
assert.equal(Number(rows[0].rate_avg),4.5);
assert.equal(Number(rows[0].courier_order_sum),975);
assert.equal(Number(rows[0].courier_tips_sum),120);
assert.equal(Number(rows[0].courier_reward_sum),1089);
assert.equal(Number(rows[0].order_processing_fee),2750);
assert.equal(Number(rows[1].courier_order_sum),200);
assert.equal(Number(rows[2].courier_reward_sum),209.5);
const before=JSON.stringify(rows);
await run();
assert.equal(JSON.stringify((await db.query('SELECT * FROM cdm.dm_courier_ledger ORDER BY settlement_month,courier_id')).rows),before);
// A source correction changes the month average, hence all payouts for this courier/month.
await db.exec(`UPDATE stg.project5_deliveries SET object_value=jsonb_set(object_value,'{rate}','2') WHERE object_id='d2';`);
await db.exec(sql('003_facts.sql'));
await run();
rows=(await db.query("SELECT * FROM cdm.dm_courier_ledger WHERE courier_id='c1' AND settlement_month=10")).rows;
assert.equal(Number(rows[0].rate_avg),3);
assert.equal(Number(rows[0].courier_order_sum),600);
assert.equal(Number(rows[0].courier_reward_sum),714);
// Removing the last delivery removes the stale monthly ledger row.
await db.exec("DELETE FROM dds.project5_deliveries WHERE delivery_key='d3';");
await run();
assert.equal((await db.query("SELECT count(*)::int n FROM cdm.dm_courier_ledger WHERE courier_id='c2'")).rows[0].n,0);
// A distinct delivery key cannot silently double one order.
await db.exec(`INSERT INTO stg.project5_deliveries(object_id,object_value,delivery_ts)
 SELECT 'duplicate',object_value,delivery_ts FROM stg.project5_deliveries WHERE object_id='d1';`);
let rejected=false;
try { await db.exec(sql('003_facts.sql')); } catch(e) { rejected=true; }
assert.equal(rejected,true);
console.log('PostgreSQL engine: migrations, facts, money, month boundary, year rollover, repeat, correction, deletion, duplicate rejection OK');
await db.close();
