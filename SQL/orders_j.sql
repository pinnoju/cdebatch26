create or replace temporary view orders_v
using json
options (
path='gs://gcp26/jsonfiles/orders.json'
);

create table if not exists raw_db.orders
using parquet
select * from orders_v;