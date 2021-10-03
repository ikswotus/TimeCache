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



