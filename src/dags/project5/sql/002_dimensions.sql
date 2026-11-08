INSERT INTO dds.project5_couriers(courier_key,courier_name)
SELECT object_id, object_value->>'name' FROM stg.project5_couriers
ON CONFLICT(courier_key) DO UPDATE SET courier_name=EXCLUDED.courier_name;
