# Examples

This section will demonstrate how timecache can be set up, walking through some of the various options and use-cases.

## Test Data
TODO: Add in link to PGExtensions for series.

Table used for demo data:

`CREATE TABLE demo.timeseries
(
    metric_name text  NOT NULL,
    sample_time timestamp with time zone NOT NULL,
    current_value numeric NOT NULL
)`

Make sure we have permissions
`grant select on demo.timeseries to cache_user`

Create some test series utilizing our series-generation extension
`INSERT INTO demo.timeseries
select 'sine',* from generate_sinewave_series(CURRENT_TIMESTAMP - interval '1 week', CURRENT_TIMESTAMP, interval '5 minutes', 1440)`

`INSERT INTO demo.timeseries
select 'rwalk-1',* from generate_randomwalk_series(CURRENT_TIMESTAMP - interval '1 week', CURRENT_TIMESTAMP, interval '5 minutes', 1)`

`INSERT INTO demo.timeseries
select 'rwalk-2',* from generate_randomwalk_series(CURRENT_TIMESTAMP - interval '1 week', CURRENT_TIMESTAMP, interval '5 minutes', 2)`

`INSERT INTO demo.timeseries
select 'rwalk-3',* from generate_randomwalk_series(CURRENT_TIMESTAMP - interval '1 week', CURRENT_TIMESTAMP, interval '5 minutes', 3)`
