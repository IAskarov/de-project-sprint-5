-- Baseline order pipeline for a new trainer. No source table is dropped.
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS dds;
CREATE SCHEMA IF NOT EXISTS cdm;
CREATE TABLE IF NOT EXISTS stg.project5_state(source text PRIMARY KEY, value jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS stg.ordersystem_users (
 id bigserial PRIMARY KEY,object_id text NOT NULL UNIQUE,object_value jsonb NOT NULL,update_ts timestamp NOT NULL);
CREATE TABLE IF NOT EXISTS stg.ordersystem_restaurants (
 id bigserial PRIMARY KEY,object_id text NOT NULL UNIQUE,object_value jsonb NOT NULL,update_ts timestamp NOT NULL);
CREATE TABLE IF NOT EXISTS stg.ordersystem_orders (
 id bigserial PRIMARY KEY,object_id text NOT NULL UNIQUE,object_value jsonb NOT NULL,update_ts timestamp NOT NULL);
CREATE TABLE IF NOT EXISTS stg.project5_bonus_ranks(id integer PRIMARY KEY,object_value jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS stg.project5_bonus_users(id integer PRIMARY KEY,object_value jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS stg.project5_bonus_events (
 id integer PRIMARY KEY,event_ts timestamp NOT NULL,event_type text NOT NULL,event_value jsonb NOT NULL);
CREATE TABLE IF NOT EXISTS dds.dm_users (
 id serial PRIMARY KEY,user_id text NOT NULL UNIQUE,user_name text NOT NULL,user_login text NOT NULL);
CREATE TABLE IF NOT EXISTS dds.dm_restaurants (
 id serial PRIMARY KEY,restaurant_id text NOT NULL UNIQUE,restaurant_name text NOT NULL);
CREATE TABLE IF NOT EXISTS dds.dm_timestamps (
 id serial PRIMARY KEY,ts timestamp NOT NULL UNIQUE,year integer NOT NULL,month integer NOT NULL,
 day integer NOT NULL,time time NOT NULL,date date NOT NULL);
CREATE TABLE IF NOT EXISTS dds.dm_products (
 id serial PRIMARY KEY,product_id text NOT NULL,restaurant_id integer NOT NULL REFERENCES dds.dm_restaurants,
 product_name text NOT NULL,product_price numeric(18,2) NOT NULL,
 UNIQUE(product_id,restaurant_id));
CREATE TABLE IF NOT EXISTS dds.dm_orders (
 id serial PRIMARY KEY,order_key text NOT NULL UNIQUE,user_id integer NOT NULL REFERENCES dds.dm_users,
 restaurant_id integer NOT NULL REFERENCES dds.dm_restaurants,timestamp_id integer NOT NULL REFERENCES dds.dm_timestamps,
 order_status text NOT NULL);
CREATE TABLE IF NOT EXISTS dds.fct_product_sales (
 id bigserial PRIMARY KEY,order_id integer NOT NULL REFERENCES dds.dm_orders,
 line_no integer NOT NULL,product_id integer NOT NULL REFERENCES dds.dm_products,
 count bigint NOT NULL CHECK(count>0),price numeric(18,2) NOT NULL CHECK(price>=0),
 total_sum numeric(18,2) NOT NULL CHECK(total_sum>=0),UNIQUE(order_id,line_no));
CREATE TABLE IF NOT EXISTS dds.project5_bonus_ranks (
 id integer PRIMARY KEY,name text NOT NULL,bonus_percent numeric NOT NULL,min_payment_threshold numeric NOT NULL);
CREATE TABLE IF NOT EXISTS dds.project5_bonus_users (
 id integer PRIMARY KEY,order_user_id text NOT NULL);
CREATE TABLE IF NOT EXISTS dds.project5_bonus_events (
 id integer PRIMARY KEY,event_ts timestamp NOT NULL,event_type text NOT NULL,event_value jsonb NOT NULL);
