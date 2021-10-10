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

----------------------------
# Decomposition
### What is it?
A caching option that allows similar queries to share cached data.

#### Why would I want it?
* Minimize the actual number of queries issued to the database (to 1)
* Row data is filtered by the caching server, not the database so it further frees up DB resources.

#### Motivating example: (100% contrived)
Let's say we want to look at resource utilization from our sample data. One option is to use a single query and graph everything in the same chart:

``` 
   SELECT machine_name, $__timeGroup(sample_time, '30s', 0), avg(current_value)
   FROM stats.timeseries_data_id
   inner join stats.categories on categories.id = category_id and category_name = 'cpu'
   inner join stats.counters on counters.id = counter_id and counter_name = 'handles'
   inner join stats.instances on instances.id = instance_id and instance_name = 'devenv'
   inner join stats.machines on machines.id = machine_id
   where $__timeFilter(sample_time)
   --and machine_name = 'host-01'
   group by 1,2
   order by 2 asc
```

Resulting in:
![SingleChart](images/_demo_timecache_allhosts.png)

This is certainly simple, but has the unfortunate consequence that our machines might be on different scales entirely. One solution would be to change the chart to a log base 2/log base 10 scale for the y axis, but if we're looking for changes in an individual host, it can be hard to see on a scaled axis.
Another option is to duplicate the query for all machines (Grafana can do this a bit more easily using a templated variable + repeating options, but this example just hardcodes the query for each of the 3 charts)

![SeparateCharts](images/_demo_timecache_separatecharts.png)

The good news is now any signifcant change in any metric will be easier to spot, as each has in independent scale and won't be lost in the mix.

-----------------------------
Solution Overview

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
