#!/bin/bash
hive -f db_yellow_taxi.sql;
set hive.execution.engine=tez;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;

hive -f field_name.sql;
hive -f yellow_taxi.sql;
hive -f facts_yellow_taxi.sql;
hive -f facts_yellow_taxi_insert.sql;
hive -f tips_avg_am_vw.sql;
hive -f tips_avg_am.sql;
hive -f tips_avg_am_insert.sql;


