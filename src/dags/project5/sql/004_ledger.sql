-- All retained closed months are recomputed; late deliveries can change the
-- monthly average and therefore payouts of EVERY order in the month.
SELECT pg_advisory_xact_lock(549005);
CREATE TEMP TABLE project5_expected ON COMMIT DROP AS
WITH orders AS (
 SELECT d.order_id,c.courier_key,c.courier_name,t.year,t.month,
        a.total_sum,d.rate,d.tip_sum,
        avg(d.rate::numeric) OVER(PARTITION BY c.id,t.year,t.month) AS monthly_rate
 FROM dds.project5_deliveries d
 JOIN dds.project5_couriers c ON c.id=d.courier_id
 JOIN dds.dm_orders o ON o.id=d.order_id
 JOIN dds.dm_timestamps t ON t.id=o.timestamp_id
 JOIN dds.project5_order_amounts a ON a.order_id=o.id
 WHERE t.ts < %(report_before)s
 AND date_trunc('month',t.ts)::timestamp = ANY(%(eligible_months)s::timestamp[])
), payments AS (
 SELECT *, CASE
  WHEN monthly_rate < 4 THEN greatest(total_sum*0.05,100)
  WHEN monthly_rate < 4.5 THEN greatest(total_sum*0.07,150)
  WHEN monthly_rate < 4.9 THEN greatest(total_sum*0.08,175)
  ELSE greatest(total_sum*0.10,200) END AS payout
 FROM orders
)
SELECT courier_key AS courier_id,courier_name,year AS settlement_year,
 month AS settlement_month,count(*) AS orders_count,
 sum(total_sum) AS orders_total_sum,avg(rate::numeric) AS rate_avg,
 round(sum(total_sum)*0.25,2) AS order_processing_fee,
 round(sum(payout),2) AS courier_order_sum,sum(tip_sum) AS courier_tips_sum,
 round(sum(payout)+sum(tip_sum)*0.95,2) AS courier_reward_sum
FROM payments GROUP BY courier_key,courier_name,year,month;
INSERT INTO cdm.dm_courier_ledger
(courier_id,courier_name,settlement_year,settlement_month,orders_count,
 orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
SELECT * FROM project5_expected
ON CONFLICT(courier_id,settlement_year,settlement_month) DO UPDATE SET
 courier_name=EXCLUDED.courier_name,orders_count=EXCLUDED.orders_count,
 orders_total_sum=EXCLUDED.orders_total_sum,rate_avg=EXCLUDED.rate_avg,
 order_processing_fee=EXCLUDED.order_processing_fee,courier_order_sum=EXCLUDED.courier_order_sum,
 courier_tips_sum=EXCLUDED.courier_tips_sum,courier_reward_sum=EXCLUDED.courier_reward_sum;
DELETE FROM cdm.dm_courier_ledger l
WHERE make_date(l.settlement_year,l.settlement_month,1) = ANY(%(eligible_months)s::timestamp[])
AND NOT EXISTS(SELECT 1 FROM project5_expected e WHERE e.courier_id=l.courier_id
 AND e.settlement_year=l.settlement_year AND e.settlement_month=l.settlement_month);
