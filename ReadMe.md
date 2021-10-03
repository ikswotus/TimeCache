Spectare
-----------------------

Spectare is a simple proxy between grafana and a postgresql database.
It is intended specifically to work with time-series data.

What does it do?

1) Cache results of popular (or fixed) queries, so subsequent updates only need 'new' data.
2) Meta-Commands for data analysis
3) (Eventually)Perform filtering of query results, so multiple 'specific' queries can be resolved by a single more-expensive query.


Libraries/Programs used:
NPGSQL
Timescaledb
Postgresql
Grafana



Test Setup

Default installation of postgresql 12

// Setup timescaledb
create extension if not exists timescaledb;

// Create new database
CREATE DATABASE perftest
    WITH 
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;

// Create 2 users
spectare_user
grafana_reader

// Create new schema 'stats'
CREATE TABLE stats.timeseries_data
(
    sample_time timestamp with time zone NOT NULL,
    machine_name text COLLATE pg_catalog."default" NOT NULL,
    current_value real NOT NULL,
    category_name text COLLATE pg_catalog."default" NOT NULL,
    counter_name text COLLATE pg_catalog."default" NOT NULL,
    instance_name text COLLATE pg_catalog."default" NOT NULL
)

TABLESPACE pg_default;

ALTER TABLE stats.timeseries_data
    OWNER to spectare_user;
	
select create_hypertable('stats.timeseries_data', 'sample_time');

// Grafana permissions
grant usage on schema stats to grafana_reader;
grant select on table stats.timeseries_data to grafana_reader;

------------------
STAT COLLECTION
------------------

Sample data can be generated using one of two methods:
1) SimpleStatsGenerator - Creates 2 hours worth of random data
2) TODO: PerfCollector - service to collect live performance counter data.
