CREATE EXTERNAL TABLE field_name (
    id INT,
    name STRING
)
COMMENT 'This is the field name table'
STORED AS PARQUET;

INSERT INTO field_name
SELECT 1, 'vendor_id'
union all
SELECT 2, 'tpep_pickup_datetime'
union all
SELECT 3, 'passenger_count'
union all
SELECT 4, 'trip_distance'
union all
SELECT 5, 'ratecode_id'
union all
SELECT 6, 'store_and_fwd_flag'
union all
SELECT 7, 'pulocation_id'
union all
SELECT 8, 'dolocation_id'
union all
SELECT 9, 'payment_type'
union all
SELECT 10, 'fare_amount'
union all
SELECT 11, 'extra'
union all
SELECT 12, 'mta_tax'
union all
SELECT 13, 'tip_amount'
union all
SELECT 14, 'tolls_amount'
union all
SELECT 15, 'improvement_surcharge'
union all
SELECT 16, 'total_amount'
union all
SELECT 17, 'congestion_surcharge';