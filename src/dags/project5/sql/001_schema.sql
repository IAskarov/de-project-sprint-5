-- Additive migration: existing order/bonus tables and DAGs are preserved.
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;
DO $$ BEGIN
 IF to_regclass('dds.dm_orders') IS NULL OR
    to_regclass('dds.dm_timestamps') IS NULL OR
    to_regclass('stg.ordersystem_orders') IS NULL THEN
   RAISE EXCEPTION 'First deploy the sprint order pipeline: dds.dm_orders, dds.dm_timestamps, stg.ordersystem_orders are required';
 END IF;
END $$;

CREATE TABLE IF NOT EXISTS stg.project5_couriers (
 object_id text PRIMARY KEY,
 object_value jsonb NOT NULL,
 loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS stg.project5_deliveries (
 object_id text PRIMARY KEY,
 object_value jsonb NOT NULL,
 delivery_ts timestamp NOT NULL,
 loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS project5_delivery_ts_idx ON stg.project5_deliveries(delivery_ts);
CREATE TABLE IF NOT EXISTS stg.project5_loads (
 id bigserial PRIMARY KEY, entity text NOT NULL,
 window_from timestamp, window_to timestamp, rows_count integer NOT NULL,
 loaded_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS dds.project5_couriers (
 id bigserial PRIMARY KEY,
 courier_key text NOT NULL UNIQUE,
 courier_name text NOT NULL
);
ALTER TABLE dds.dm_orders ADD COLUMN IF NOT EXISTS project5_courier_id bigint
 REFERENCES dds.project5_couriers(id);
CREATE TABLE IF NOT EXISTS dds.project5_deliveries (
 id bigserial PRIMARY KEY,
 delivery_key text NOT NULL UNIQUE,
 order_id integer NOT NULL REFERENCES dds.dm_orders(id),
 courier_id bigint NOT NULL REFERENCES dds.project5_couriers(id),
 delivery_ts timestamp NOT NULL,
 rate integer NOT NULL CHECK(rate BETWEEN 1 AND 5),
 tip_sum numeric(18,2) NOT NULL CHECK(tip_sum >= 0),
 UNIQUE(order_id)
);
-- One full order amount from MongoDB, independent of product-fact grain.
CREATE TABLE IF NOT EXISTS dds.project5_order_amounts (
 order_id integer PRIMARY KEY REFERENCES dds.dm_orders(id),
 total_sum numeric(18,2) NOT NULL CHECK(total_sum >= 0)
);
CREATE TABLE IF NOT EXISTS cdm.dm_courier_ledger (
 id bigserial PRIMARY KEY,
 courier_id text NOT NULL,
 courier_name text NOT NULL,
 settlement_year integer NOT NULL CHECK(settlement_year BETWEEN 1970 AND 9999),
 settlement_month integer NOT NULL CHECK(settlement_month BETWEEN 1 AND 12),
 orders_count bigint NOT NULL CHECK(orders_count >= 0),
 orders_total_sum numeric(18,2) NOT NULL,
 rate_avg numeric(18,8) NOT NULL,
 order_processing_fee numeric(18,2) NOT NULL,
 courier_order_sum numeric(18,2) NOT NULL,
 courier_tips_sum numeric(18,2) NOT NULL,
 courier_reward_sum numeric(18,2) NOT NULL,
 UNIQUE(courier_id, settlement_year, settlement_month)
);
