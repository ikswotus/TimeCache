TimeCache
-----------------------

TimeCache is a simple C# proxy server between grafana and postgresql/timescale.
It is intended specifically to work with time-series data.

What can it do?

1) Cache results of popular (or fixed) queries, so subsequent updates only need to hit the database for 'new' data. This frees up the database considerably when lots of dashboards are displaying information that is largely static (ie "old" data that is unlikely to change)
2) Meta-Commands for some very simple data analysis within grafana.
3) Decomposition of queries so multiple queries that may only differ in a predicate filter can share cached data. (Note, this is very experimental)


Libraries/Programs used:
NPGSQL
Postgresql with Timescaledb
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
timecache_user
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
    OWNER to timecache_user;
	
select create_hypertable('stats.timeseries_data', 'sample_time');

// Grafana permissions
grant usage on schema stats to grafana_reader;
grant select on table stats.timeseries_data to grafana_reader;

------------------
STAT COLLECTION
------------------

SimpleStatsGenerator - Create testing data

