create database if not exists processed_db
location 'gs://gcp26/processed.db';

use processed_db;

create table if not exists processed_db.result_summary 
(
name string,
city string,
tot_purch_amt decimal(10,2)
)using parquet;


Insert Into processed_db.result_summary
select sale.name,sale.city,sum(coalesce(purch_amt,0)) as tot_purch_amt from
(select * from raw_db.customer_lo union select * from raw_db.customer_ny)cust
left outer join raw_db.orders order on cust.customer_id=order.customer_id
left outer join (select * from raw_db.salesman_lo union select * from raw_db.salesman_ny)sale
on sale.salesman_id=order.salesman_id
group by 1,2
order by 2,1;
