CREATE EXTERNAL TABLE yellow_taxi (
    vendor_id INT,
    tpep_pickup_datetime TIMESTAMP,
    tpep_dropoff_datetime TIMESTAMP,
    passenger_count INT,
    trip_distance DOUBLE,
    ratecode_id INT,
    store_and_fwd_flag STRING,
    pulocation_id INT,
    dolocation_id INT,
    payment_type INT,
    fare_amount DOUBLE,
    extra DOUBLE,
    mta_tax DOUBLE,
    tip_amount DOUBLE,
    tolls_amount DOUBLE,
    improvement_surcharge DOUBLE,
    total_amount DOUBLE,
    congestion_surcharge DOUBLE
    )
COMMENT 'This is the yellow taxi trips table'
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
LOCATION 's3a://newtaxibacket/nytaxidata'
TBLPROPERTIES ("skip.header.line.count"="1");


