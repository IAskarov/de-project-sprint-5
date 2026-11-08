-- Reject ambiguous mappings rather than multiplying payments.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM stg.project5_deliveries
   GROUP BY object_value->>'order_id' HAVING count(*) > 1) THEN
  RAISE EXCEPTION 'Multiple delivery IDs for one order: reconcile source before payment';
 END IF;
END $$;
INSERT INTO dds.project5_deliveries(delivery_key,order_id,courier_id,delivery_ts,rate,tip_sum)
SELECT s.object_id,o.id,c.id,s.delivery_ts,
       (s.object_value->>'rate')::integer,(s.object_value->>'tip_sum')::numeric
FROM stg.project5_deliveries s
JOIN dds.dm_orders o ON o.order_key=s.object_value->>'order_id'
JOIN dds.project5_couriers c ON c.courier_key=s.object_value->>'courier_id'
ON CONFLICT(delivery_key) DO UPDATE SET
 order_id=EXCLUDED.order_id,courier_id=EXCLUDED.courier_id,
 delivery_ts=EXCLUDED.delivery_ts,rate=EXCLUDED.rate,tip_sum=EXCLUDED.tip_sum;

UPDATE dds.dm_orders o SET project5_courier_id=d.courier_id
FROM dds.project5_deliveries d WHERE o.id=d.order_id
AND o.project5_courier_id IS DISTINCT FROM d.courier_id;
