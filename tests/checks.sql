-- Run after a successful order DAG. Each anomaly query must return zero.
SELECT count(*) AS duplicate_months FROM (
 SELECT courier_id,settlement_year,settlement_month FROM cdm.dm_courier_ledger
 GROUP BY 1,2,3 HAVING count(*)>1
) x;
SELECT count(*) AS duplicate_orders FROM (
 SELECT order_id FROM dds.project5_deliveries GROUP BY order_id HAVING count(*)>1
) x;
SELECT count(*) AS incorrect_reward FROM cdm.dm_courier_ledger
WHERE courier_reward_sum <> round(courier_order_sum+courier_tips_sum*0.95,2)
   OR order_processing_fee <> round(orders_total_sum*0.25,2);
SELECT count(*) AS missing_amounts FROM dds.project5_deliveries d
LEFT JOIN dds.project5_order_amounts a USING(order_id) WHERE a.order_id IS NULL;
-- Previous-month result (business timestamps and source dates use the same timezone).
SELECT * FROM cdm.dm_courier_ledger
WHERE settlement_year=extract(year FROM current_date-interval '1 month')
AND settlement_month=extract(month FROM current_date-interval '1 month')
ORDER BY courier_id;
