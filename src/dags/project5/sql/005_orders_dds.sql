SELECT pg_advisory_xact_lock(549000);
INSERT INTO dds.dm_users(user_id,user_name,user_login)
SELECT object_id,object_value::jsonb->>'name',coalesce(object_value::jsonb->>'login','') FROM stg.ordersystem_users
ON CONFLICT(user_id) DO UPDATE SET user_name=EXCLUDED.user_name,user_login=EXCLUDED.user_login;
INSERT INTO dds.dm_restaurants(restaurant_id,restaurant_name)
SELECT object_id,object_value::jsonb->>'name' FROM stg.ordersystem_restaurants
ON CONFLICT(restaurant_id) DO UPDATE SET restaurant_name=EXCLUDED.restaurant_name;
INSERT INTO dds.dm_timestamps(ts,year,month,day,time,date)
SELECT DISTINCT (object_value::jsonb->>'date')::timestamp,
 extract(year FROM (object_value::jsonb->>'date')::timestamp)::integer,
 extract(month FROM (object_value::jsonb->>'date')::timestamp)::integer,
 extract(day FROM (object_value::jsonb->>'date')::timestamp)::integer,
 (object_value::jsonb->>'date')::timestamp::time,(object_value::jsonb->>'date')::timestamp::date
FROM stg.ordersystem_orders ON CONFLICT(ts) DO NOTHING;
-- Include historical order items even if the restaurant removed them from its menu.
INSERT INTO dds.dm_products(product_id,restaurant_id,product_name,product_price)
SELECT DISTINCT ON (item->>'id',r.id) item->>'id',r.id,item->>'name',(item->>'price')::numeric
FROM stg.ordersystem_orders s
JOIN dds.dm_restaurants r ON r.restaurant_id=s.object_value::jsonb->'restaurant'->>'id'
CROSS JOIN LATERAL jsonb_array_elements(s.object_value::jsonb->'order_items') item
ORDER BY item->>'id',r.id,s.update_ts DESC,s.object_id DESC
ON CONFLICT(product_id,restaurant_id) DO UPDATE SET
 product_name=EXCLUDED.product_name,product_price=EXCLUDED.product_price;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM stg.ordersystem_orders s
 LEFT JOIN dds.dm_users u ON u.user_id=s.object_value::jsonb->'user'->>'id'
 LEFT JOIN dds.dm_restaurants r ON r.restaurant_id=s.object_value::jsonb->'restaurant'->>'id'
 WHERE u.id IS NULL OR r.id IS NULL) THEN
 RAISE EXCEPTION 'Mongo orders reference missing users/restaurants; rerun source sync';
 END IF;
END $$;
INSERT INTO dds.dm_orders(order_key,user_id,restaurant_id,timestamp_id,order_status)
SELECT s.object_id,u.id,r.id,t.id,s.object_value::jsonb->>'final_status'
FROM stg.ordersystem_orders s
JOIN dds.dm_users u ON u.user_id=s.object_value::jsonb->'user'->>'id'
JOIN dds.dm_restaurants r ON r.restaurant_id=s.object_value::jsonb->'restaurant'->>'id'
JOIN dds.dm_timestamps t ON t.ts=(s.object_value::jsonb->>'date')::timestamp
ON CONFLICT(order_key) DO UPDATE SET user_id=EXCLUDED.user_id,restaurant_id=EXCLUDED.restaurant_id,
 timestamp_id=EXCLUDED.timestamp_id,order_status=EXCLUDED.order_status;
-- Replace line snapshots atomically, including deleted lines and nonclosed orders.
DELETE FROM dds.fct_product_sales;
INSERT INTO dds.fct_product_sales(order_id,line_no,product_id,count,price,total_sum)
SELECT o.id,x.line_no,p.id,(x.item->>'quantity')::bigint,(x.item->>'price')::numeric,
 (x.item->>'quantity')::numeric*(x.item->>'price')::numeric
FROM stg.ordersystem_orders s JOIN dds.dm_orders o ON o.order_key=s.object_id
CROSS JOIN LATERAL jsonb_array_elements(s.object_value::jsonb->'order_items') WITH ORDINALITY x(item,line_no)
JOIN dds.dm_products p ON p.product_id=x.item->>'id' AND p.restaurant_id=o.restaurant_id
WHERE o.order_status='CLOSED';
INSERT INTO dds.project5_bonus_ranks
SELECT id,object_value->>'name',(object_value->>'bonus_percent')::numeric,
 (object_value->>'min_payment_threshold')::numeric FROM stg.project5_bonus_ranks
ON CONFLICT(id) DO UPDATE SET name=EXCLUDED.name,bonus_percent=EXCLUDED.bonus_percent,
 min_payment_threshold=EXCLUDED.min_payment_threshold;
INSERT INTO dds.project5_bonus_users
SELECT id,object_value->>'order_user_id' FROM stg.project5_bonus_users
ON CONFLICT(id) DO UPDATE SET order_user_id=EXCLUDED.order_user_id;
INSERT INTO dds.project5_bonus_events SELECT * FROM stg.project5_bonus_events
ON CONFLICT(id) DO UPDATE SET event_ts=EXCLUDED.event_ts,event_type=EXCLUDED.event_type,event_value=EXCLUDED.event_value;
