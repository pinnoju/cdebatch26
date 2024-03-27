create or replace temporary view salesman_ny_lo
using json
options(
	path='gs://gcp26/jsonfiles/salesman_ny.json'
);

create table if not exists raw_db.salesman_ny
using parquet
select * from salesman_ny_lo;