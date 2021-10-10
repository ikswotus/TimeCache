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

-----------------------------
Project Overview

TimeCacheGUI - Simple WPF application to allow starting/stopping a server and viewing logs.
TimeCacheService - Allows running the TimeCacheNetworkServer as a windows service. Logs periodically flushed to disk.
TimeCacheNetworkServer - Actual server/caching implementation, all the fun stuff is in here.
PostgresqlCommunicator - Handles postgresql wire format. Allows parsing the connection info from grafana's requests and translating the c# data table back into the raw psql format for grafana.

Other:
SLog - Extremely simple logging helper.
SimpleStatsGenerator - Small executable to generate testing data.
Utils - Helper classes, table manager for bulk inserting sample stats.

--------------------------------------
Getting started

To have grafana talk to a TimeCacheNetworkServer, it simply needs to be added as a postgresql data source. It mimics enough of the connection setup so grafana will recognize it as a valid postgresql db. 
Note: Currently it does NOT support any TLS/SSL modes.
