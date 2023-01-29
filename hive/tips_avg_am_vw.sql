create external table payment
(
    id int,
    name string
)
stored as orc;

with t as (select 1, 'Credit card'
union all
select 2, 'Cash'
union all
select 3, 'No charge'
union all
select 4, 'Dispute'
union all
select 5, 'Unknown'
union all
select 6, 'Voided trip')
insert into payment select * from t;

CREATE VIEW tips_avg_am_vw
AS
SELECT name, dt, ROUND(AVG(tip_amount), 2) as average_tips_amount, SUM(passenger_count)
from facts_yellow_taxi
join payment on facts_yellow_taxi.payment_type = payment.id
GROUP BY name, dt;
