use raw_db;

create or replace temporary view salesman_lo_v
using json
options(
	path= 'gs://gcp26/jsonfiles/salesman_lo.json'
	);

create table if not exists raw_db.salesman_lo
using parquet
select * from salesman_lo_v