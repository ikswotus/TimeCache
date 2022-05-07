

CREATE TYPE __timepoint as (time timestamp with time zone, val integer);

CREATE OR REPLACE FUNCTION public.generate_randomwalk_series(
	timestamp with time zone,
	timestamp with time zone,
	interval,
	integer)
    RETURNS SETOF __timepoint 
    LANGUAGE 'c'
    COST 1
    IMMUTABLE PARALLEL UNSAFE
    ROWS 1000

AS 'TimeCachePGExtensions', 'generate_randomwalk_series';

CREATE TABLE demo.timeseries
(
    metric_name text NOT NULL,
    sample_time timestamp with time zone NOT NULL,
    current_value numeric NOT NULL
) TABLESPACE pg_default;

ALTER TABLE demo.timeseries
    OWNER to postgres;

GRANT ALL ON TABLE demo.timeseries TO postgres;
GRANT SELECT ON TABLE demo.timeseries TO grafana_reader;

INSERT INTO demo.timeseries
select 'test', time, val from
generate_randomwalk_series(CURRENT_TIMESTAMP - interval '3 hours', 
                           CURRENT_TIMESTAMP, interval '1m', 1)

