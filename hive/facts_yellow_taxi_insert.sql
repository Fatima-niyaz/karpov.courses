INSERT INTO facts_yellow_taxi PARTITION(dt)
select vendor_id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance,
ratecode_id, store_and_fwd_flag, pulocation_id, dolocation_id, payment_type, fare_amount, extra, mta_tax,
tip_amount, tolls_amount, improvement_surcharge, total_amount, congestion_surcharge, DATE(tpep_pickup_datetime) as dt from yellow_taxi
WHERE tpep_pickup_datetime IS NOT NULL;
