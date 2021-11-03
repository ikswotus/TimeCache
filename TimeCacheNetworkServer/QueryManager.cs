using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Text.RegularExpressions;
using PostgresqlCommunicator;
using System.Threading;
using TimeCacheNetworkServer.Caching;
using System.IO;
using System.Diagnostics;

namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Handle query logic.
    /// 
    /// One QueryManager per connection string(but not limited to 1 connection)
    /// 
    /// 
    /// TODO: 
    /// Query Parsing/Interpreting
    /// Eviction (Memory Pressure / Age / Request frequency)
    /// -> Need to track stats per query/manager
    /// 
    /// </summary>
    public class QueryManager : SLog.SLoggableObject
    {
        #region Construct

        /// <summary>
        /// Constructor - require a connection string.
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="configFile">Contains configured queries that stay up to date</param>
        /// <param name="logger">Logging helper</param>
        private QueryManager(string connectionString, string configFile, SLog.ISLogger logger)
            : base("QueryManager", logger)
        {
            _connectionString = connectionString;
            _configFile = configFile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="connString"></param>
        /// <returns></returns>
        public static QueryManager GetManager(string connString, string configFile, SLog.ISLogger logger)
        {
            // TODO: Allow multiple managers for a single pairing?
            string key = connString + configFile;
            lock(_managers)
            {
                if (!_managers.ContainsKey(key))
                {
                    _managers[key] = new QueryManager(connString, configFile, logger);
                    _managers[key].Start();
                }
                return _managers[key];
            }

        }

        /// <summary>
        /// Pair connection string with a query manager.
        /// </summary>
        private static Dictionary<string, QueryManager> _managers = new Dictionary<string, QueryManager>();

        /// <summary>
        /// Configuration for cached queries.
        /// </summary>
        private string _configFile = null;

        /// <summary>
        /// Underlying connection string to the 'real' database.
        /// </summary>
        private string _connectionString;

        #endregion Construct

        #region Management

        /// <summary>
        /// Determines how cached data is released.
        /// </summary>
        private EvictionPolicy _evictPolicy = EvictionPolicy.USAGE;

        /// <summary>
        /// Flag for stop requests
        /// </summary>
        private volatile bool _stopped = false;

        /// <summary>
        /// Thread to manage evicting old data
        /// </summary>
        private Thread _evictionThread = null;

        /// <summary>
        /// Runs any configured cached queries so they stay up to date.
        /// </summary>
        private Thread _backgroundRefreshThread = null;

        /// <summary>
        /// Starts the manager
        /// </summary>
        public void Start()
        {
            Debug("Started");

            _stopped = false;

            _evictionThread = new Thread(() => EvictData());

            //_backgroundRefreshThread = new Thread(() => RefreshCache());

            // TODO: Start threads
            Debug("starting eviction thread.");

            _evictionThread.Start();
        }

        /// <summary>
        /// Stop
        /// </summary>
        public void DoStop()
        {
            Debug("DoStop() called");

            _stopped = true;

            if(_evictionThread != null)
            {
                if (!_evictionThread.Join(2000))
                {
                    Error("Failed to join eviction thread within 2s, attempting to abort");
                    try
                    {
                        _evictionThread.Abort();
                    }
                    catch { }
                }
            }
            if (_backgroundRefreshThread != null)
            {
                if (!_backgroundRefreshThread.Join(2000))
                {
                    try
                    {
                        _backgroundRefreshThread.Abort();
                    }
                    catch { }
                }
            }
        }

        /// <summary>
        /// Called on shutdown, informs any active managers we are stopping.
        /// </summary>
        public static void Stop()
        {
            lock(_managers)
            {
                foreach(string key in _managers.Keys)
                {
                    _managers[key].DoStop();
                }
            }
        }


        /// <summary>
        /// A background thread will refresh configured queries automatically
        /// to keep regularly queried data up to date. This allows typical
        /// caching requests to be fully satisfied, and not have to wait on/utilize the db.
        /// </summary>
        private void RefreshCache()
        {
            Dictionary<string, DateTime> _lastRefreshed = new Dictionary<string, DateTime>();

            while (!_stopped)
            {
                CachedQueryConfig cqc = null;
                try
                {
                    if (String.IsNullOrEmpty(_configFile) && File.Exists(_configFile))
                    {
                      cqc  = CachedQueryConfig.FromXmlFile(_configFile);
                    }

                    while(cqc != null && cqc.Queries != null && !_stopped)
                    {
                        foreach(CacheableQuery cq in cqc.Queries)
                        {
                            if(_lastRefreshed.ContainsKey(cq.RawQueryText) &&
                                _lastRefreshed[cq.RawQueryText].AddMinutes(cq.RefreshIntervalMinutes) > DateTime.UtcNow)
                            {
                                continue;
                            }

                            CachedQuery(cq.GetFormatted(), false);
                        }

                        for (int i = 0; i < 30 && !_stopped; i++)
                            Thread.Sleep(1000);
                    }

                }
                catch (Exception exc)
                {
                    Error("Failure in refresh thread: " + exc);
                }
                for (int i = 0; i < 30 && !_stopped; i++)
                    Thread.Sleep(1000);
            }
        }


        /// <summary>
        /// Eviction rules:
        /// 
        /// 1) Remove data that has not been utilized within the last X time period.
        /// 2) Remove oldest/least accessed data if memory pressure exceeds Y, until memory consumption
        /// falls below our threshold (or only 1 data chunk is cached...)
        /// </summary>
        private void EvictData()
        {
           

            while(!_stopped)
            {
                try
                {
                    switch (_evictPolicy)
                    {
                        case EvictionPolicy.USAGE:
                            {
                                TimeSpan age = GlobalOptions.GetOptionValue<TimeSpan>("EvictionTime", TimeSpan.FromHours(2));
                                Dictionary<string, DateTime> lastUsed = new Dictionary<string, DateTime>();
                                lock (_queryCache)
                                {
                                    foreach (string q in _queryCache.Keys)
                                    {
                                        CachedData cd = _queryCache[q];
                                        lock (cd)
                                        {
                                            if(cd.CacheUsage.Count > 0)
                                                lastUsed[q] = cd.CacheUsage.Last();
                                        }
                                    }
                                }

                                DateTime cutoff = DateTime.UtcNow.Subtract(age);
                                Debug("Eviction - cutoff time is: " + cutoff.ToString("O"));

                                foreach (KeyValuePair<string, DateTime> kvp in lastUsed)
                                {
                                    if (kvp.Value >= cutoff)
                                    {
                                        Debug("Key age does not exceed cutoff: " + kvp.Value.ToString("O") +"[" + kvp.Key +"]");
                                        continue;
                                    }
                                    Debug("Query has not been used since " + kvp.Value.ToString("O") + " and will be freed: [" + kvp.Key + "]");
                                    lock (_queryCache)
                                    {
                                        if (_queryCache.ContainsKey(kvp.Key))
                                        {
                                            CachedData cd = _queryCache[kvp.Key];

                                            lock (cd)
                                                cd.Clear();

                                            _queryCache.Remove(kvp.Key);
                                            Debug("Evicted key from cache to reclaim memory: [" + kvp.Key + "]");
                                        }
                                    }

                                }

                            }
                            break;
                        case EvictionPolicy.MEMORY:
                           
                            bool needToEvict = false;
                            long memoryAllowed = GlobalOptions.GetOptionValue<long>(GlobalOptions.AllowedMemoryUsage, -1);
                            // Evict based on memory footprint
                            // TODO: If we're pooling cached values...memory will only drop so far.
                            // Instead of looking at process memory, look at cached bytes free
                            if (memoryAllowed != -1)
                            {
                                needToEvict = memoryAllowed < Caching.MemoryUtils.GetMemoryUsageBytes();
                            }

                            // Look for inactive data
                            // TODO: Identify a memory threshold first, and only evict if that threshold is exceeded.
                            // TODO: Order by end date - evict the 'least' recently cached data
                            if (needToEvict)
                            {
                                Dictionary<string, int> _usage = new Dictionary<string, int>();
                                lock (_queryCache)
                                {
                                    foreach (string q in _queryCache.Keys)
                                    {
                                        CachedData cd = _queryCache[q];
                                        lock (cd)
                                        {
                                            _usage[q] = cd.CacheUsage.Count(r => r > DateTime.UtcNow.AddHours(-1));
                                        }
                                    }
                                }
                                if (_usage.Count > 0)
                                {
                                    string key = _usage.OrderBy(v => v.Value).First().Key;
                                    lock (_queryCache)
                                    {
                                        if (_queryCache.ContainsKey(key))
                                        {
                                            CachedData cd = _queryCache[key];

                                            lock (cd)
                                                cd.Clear();

                                            _queryCache.Remove(key);
                                            Debug("Evicted key from cache to reclaim memory: " + key);
                                        }
                                    }
                                }
                            }
                          
                            break;
                        default:
                            Error("No eviction policy - cannot evict data");
                            break;
                    }

                   
                }
                catch(Exception exc)
                {
                    // TODO: logging
                    Error("Fatal error in EvictData(): " + exc);
                }
                for (int i = 0; i < 60 && !_stopped; i++)
                    Thread.Sleep(500);
            }
        }

        #endregion Management

        #region Passthrough

        public DataTable SimpleQuery(string query, int timeout = 60)
        {
            Trace("SimpleQuery [{0}]", query);

            DataTable table = null;
            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(_connectionString))
            {
                conn.Open();

                table = new DataTable();
                table.TableName = "TempTable";

                NpgsqlCommand sc = new NpgsqlCommand(query, conn);
                sc.CommandTimeout = timeout;

                Npgsql.NpgsqlDataAdapter adapt = new NpgsqlDataAdapter(sc);

                adapt.Fill(table);
            }

            return table;
        }

        public DataTable SimpleQuery(StandardQuery query)
        {
            return SimpleQuery(query.UpdatedQuery, query.Timeout);
        }


        #endregion Passthrough

        #region Caching

        /// <summary>
        /// Trims a cache by removing all data before @startTime
        /// </summary>
        /// <param name="query"></param>
        /// <param name="startTime"></param>
        public void TrimCache(string normalizedQuery, DateTime startTime)
        {
            Caching.CachedData cache = null;
            lock (_queryCache)
            {
                if (!_queryCache.ContainsKey(normalizedQuery))
                {
                    return;
                }
                cache = _queryCache[normalizedQuery];
            }

            // If we're updating - make sure nobody else is
            lock (cache)
            {
                cache.Trim(startTime);
            }
        }

        public DataTable QueryToTable(StandardQuery query)
        {
            Match m = ParsingUtils.TimeFilterRegex.Match(query.RawQuery);
            if(!m.Success)
            {
                // TODO: SLOG
                query.UpdatedQuery = query.RawQuery;
                return SimpleQuery(query);
            }
            DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            string normalized = query.RawQuery.Replace(m.Groups["start_time"].Value, "###START###").Replace(m.Groups["end_time"].Value, "###END###");

            CachedData cache = null;
            lock (_queryCache)
            {
                if (!_queryCache.ContainsKey(normalized))
                {
                    // TODO: Friendly prefixes? 
                    string tag = String.IsNullOrEmpty(query.Tag) ? ParsingUtils.GetQueryTag(normalized) : query.Tag;
                    _queryCache[normalized] = new Caching.CachedData(tag, _logger);
                }
                cache = _queryCache[normalized];
            }

            lock (cache)
            {
                // Check overlaps

                CachedData.CacheStatus status = cache.HasCachedData(start, end);
                bool canUseCache = status != CachedData.CacheStatus.UNCACHED;

                Trace("Cache status was " + status.ToString());

                query.UpdatedQuery = query.RawQuery;


                if (!canUseCache)
                {
                    DataTable t = SimpleQuery(query);

                    cache.CacheUsage.Add(DateTime.UtcNow);

                    cache.UpdateResults(t, start, end, canUseCache, true, 1);
                }

                if (canUseCache)
                {
                    // TODO: allow overlap? X minutes of a 'refresh' window.

                    if (cache.FullySatisfied(start, end, query.UpdateInterval))
                    {
                        // If we're constantly refreshing, cache will be up to date (mostly)
                        Debug("Normalized query will be satisfied by cache.");
                        return cache.GetTable(start, end);
                    }

                    // Querying new data
                    // This means we need to make sure we use a good start time, accounting for:
                    // 1) Fuzzy data at the edge, in case we're querying a live table (that is, end time is utcnow)
                    // 2) bucket intervals - if we are grouping based on time, we want to avoid querying a partial bucket

                    if (status == CachedData.CacheStatus.EXTEND_END)
                    {
                        Trace("Adjusting start of query for EXTEND_END");
                        DateTime newStart = cache.DataEndTime;
                        newStart = newStart.Add(query.UpdateWindow.Negate());

                        if (query.CheckBucketDuration)
                        {
                            Match bucketMatch = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);
                            if (bucketMatch.Success)
                            {
                                newStart = ParsingUtils.RoundInterval(bucketMatch.Groups["duration"].Value, newStart);
                            }
                        }
                        query.UpdatedQuery = query.RawQuery.Replace(m.Groups["start_time"].Value, newStart.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");


                        DataTable t = SimpleQuery(query);

                        cache.CacheUsage.Add(DateTime.UtcNow);

                        cache.UpdateResults(t, start, end, canUseCache, true, 1);
                    }
                    else if (status == CachedData.CacheStatus.EXTEND_START)
                    {
                        Trace("Adjusting end of query for EXTEND_START");
                        DateTime newEnd = cache.DataStartTime;
                        newEnd = newEnd.Add(query.UpdateWindow);

                        if (query.CheckBucketDuration)
                        {
                            Match bucketMatch = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);
                            if (bucketMatch.Success)
                            {
                                newEnd = ParsingUtils.CeilingInterval(bucketMatch.Groups["duration"].Value, newEnd);
                            }
                        }
                        query.UpdatedQuery = query.RawQuery.Replace(m.Groups["end_time"].Value, newEnd.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");

                        DataTable t = SimpleQuery(query);

                        cache.CacheUsage.Add(DateTime.UtcNow);

                        cache.UpdateResults(t, start, end, canUseCache, false, 1);
                    }
                    else if(status == CachedData.CacheStatus.EXTEND_BOTH)
                    {
                        // For EXTEND_BOTH, we'll query both start and end
                        Trace("Adjusting start/end of query for EXTEND_BOTH");

                        string queryCopy = query.RawQuery;

                        DateTime newStart = cache.DataEndTime;
                        newStart = newStart.Add(query.UpdateWindow.Negate());

                        if (query.CheckBucketDuration)
                        {
                            Match bucketMatch = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);
                            if (bucketMatch.Success)
                            {
                                newStart = ParsingUtils.RoundInterval(bucketMatch.Groups["duration"].Value, newStart);
                            }
                        }
                        query.UpdatedQuery = query.RawQuery.Replace(m.Groups["start_time"].Value, newStart.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");

                        DataTable t = SimpleQuery(query);

                        cache.UpdateResults(t, newStart, end, true, false, 1);

                        DateTime newEnd = cache.DataStartTime;
                        newEnd = newEnd.Add(query.UpdateWindow);

                        if (query.CheckBucketDuration)
                        {
                            Match bucketMatch = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);
                            if (bucketMatch.Success)
                            {
                                newEnd = ParsingUtils.CeilingInterval(bucketMatch.Groups["duration"].Value, newEnd);
                            }
                        }
                        queryCopy = queryCopy.Replace(m.Groups["end_time"].Value, newEnd.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");

                        t = SimpleQuery(queryCopy);

                        cache.CacheUsage.Add(DateTime.UtcNow);

                        cache.UpdateResults(t, start, newEnd, true, false, 1);

                    }
                }



                return cache.GetTable(start, end);


            }
        }

        /// <summary>
        /// Executes a query utilizing our cache.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="wantResults">Return the results. False can be used for updates, so future queries that do want results have more data cached.</param>
        /// <returns></returns>
        public DataTable QueryToTable(string query)
        {
            // 'Key' will be the query with time part normalized.
            Match m = ParsingUtils.TimeFilterRegex.Match(query);
            if (!m.Success)
            {
                Error("Failed to identify start/end date for query - defaulting to passthrough: " + query);
                SimpleQuery(query);
            }
            DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            string normalized = query.Replace(m.Groups["start_time"].Value, "###START###").Replace(m.Groups["end_time"].Value, "###END###");

            Caching.CachedData cache = null;
            lock (_queryCache)
            {
                if (!_queryCache.ContainsKey(normalized))
                {
                    string tag = ParsingUtils.GetQueryTag(normalized);
                    _queryCache[normalized] = new Caching.CachedData(tag, _logger);
                }
                cache = _queryCache[normalized];
            }

            // If we're updating - make sure nobody else is
            lock (cache)
            {
                // Check overlaps
                bool canUseCache = (cache.DescriptorMessage != null &&
                                   (
                                        (start <= cache.DataEndTime && start >= cache.DataStartTime) ||
                                        (start < cache.DataStartTime && end >= cache.DataStartTime)
                                    ));



                string queryToExecute = query;

                if (canUseCache)
                {
                    // TODO: allow overlap? X minutes of a 'refresh' window.
                    bool within = start <= cache.DataEndTime && start >= cache.DataStartTime
                        && (end - cache.DataEndTime).TotalSeconds < 30;

                    if (within)
                    {
                        // If we're constantly refreshing, cache will be up to date (mostly)
                       Debug("Normalized query will be satisfied by cache.");
                        return cache.GetTable(start, end);
                    }

                    // Use our end date
                    queryToExecute = queryToExecute.Replace(m.Groups["start_time"].Value, cache.DataEndTime.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");

                    Debug("Cached data found - updating query: [" + queryToExecute + "]");
                }

                
                DataTable t = SimpleQuery(queryToExecute);

                cache.UpdateResults(t, start, end, false, false, 1);

                return t;

                
            }
        }

        private Stopwatch _timer = new Stopwatch();

        public IEnumerable<PGMessage> CachedQueryDecomp(Query.QueryUtils.NormalizedQuery query, bool wantResults = true)
        {
            _timer.Restart();

            try
            {
                Trace("Cached query: [{0}]", query.QueryText);


                // 'Key' will be the query with time part normalized.
                Match m = ParsingUtils.TimeFilterRegex.Match(query.QueryText);
                if (!m.Success)
                {
                    Error("Failed to identify start/end date for query - defaulting to passthrough: " + query.QueryText);
                    return Translator.BuildResponseFromData(SimpleQuery(query.QueryText));
                }
                DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);

                Match tb = ParsingUtils.TimeBucketRegex.Match(query.OriginalQueryText);
                if (tb.Success)
                {
                    // Need to adjust start to be a 'full' bucket
                    DateTime oldStart = start;
                    string dur = tb.Groups["duration"].Success ? tb.Groups["duration"].Value : tb.Groups["duration_sec"].Value + "s";
                    start = ParsingUtils.RoundInterval(dur, start);
                    query.BucketingInterval = dur;
                    Trace("TimeBucket found. Rounded start time from " + oldStart.ToString("O") + " to " + start.ToString("O"));
                }

                DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);


                query.AdjustedEnd = end;
                query.AdjustedStart = start;
                
                
                string normalized = query.QueryText.Replace(m.Groups["start_time"].Value, "###START###").Replace(m.Groups["end_time"].Value, "###END###");

                Caching.CachedData cache = null;
                long startMs = _timer.ElapsedMilliseconds;
                lock (_queryCache)
                {
                    if (!_queryCache.ContainsKey(normalized))
                    {
                        string tag = string.IsNullOrEmpty(query.QueryTag) ? ParsingUtils.GetQueryTag(normalized) : query.QueryTag;
                        _queryCache[normalized] = new Caching.CachedData(tag, _logger);
                    }
                    cache = _queryCache[normalized];
                }
                long lockTime = _timer.ElapsedMilliseconds - startMs;
                VTrace("Lock took " + lockTime);
                // If we're updating - make sure nobody else is
                lock (cache)
                {
                    CachedData.CacheStatus status = cache.HasCachedData(start, end);
                    bool canUseCache = status != CachedData.CacheStatus.UNCACHED;

                    Trace("Cache status was " + status.ToString());

                    string queryToExecute = query.QueryText;

                    if (!canUseCache)
                    {
                        _timer.Restart();
                        DataTable results = SimpleQuery(queryToExecute);
                        _timer.Stop();
                        VTrace("Simple query took: " + _timer.ElapsedMilliseconds);

                        cache.UpdateResults(results, start, end, canUseCache);

                        if (wantResults)
                            return cache.Get(start, end, query.RemovedPredicates);

                        return null;
                    }

                    if (canUseCache)
                    {
                        if (cache.FullySatisfied(start, end, TimeSpan.FromSeconds(30)))
                        {
                            // If we're constantly refreshing, cache will be up to date (mostly)
                            Debug("Normalized query will be satisfied by cache.");
                            cache.CacheUsage.Add(DateTime.UtcNow);
                            if (wantResults)
                                return cache.Get(start, end, query.RemovedPredicates);
                            return null;
                        }

                        Debug("Cached data found - updating query: [" + queryToExecute + "]");
                        if (status == CachedData.CacheStatus.EXTEND_END)
                        {
                            Trace("Adjusting start of query for EXTEND_END");
                            // Use our end date, modified to allow refreshing data
                            DateTime edge = cache.DataEndTime.AddMinutes(-5);
                            if (tb.Success)
                            {
                                // Need to adjust start to be a 'full' bucket
                                DateTime oldStart = edge;
                                string dur = tb.Groups["duration"].Success ? tb.Groups["duration"].Value : tb.Groups["duration_sec"].Value + "s";
                                edge = ParsingUtils.RoundInterval(dur, edge);
                                Trace("TimeBucket found. Rounded edge time from " + oldStart.ToString("O") + " to " + edge.ToString("O"));
                                queryToExecute = queryToExecute.Replace(m.Groups["start_time"].Value, edge.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");
                            }
                            _timer.Restart();
                            DataTable results = SimpleQuery(queryToExecute);
                            _timer.Stop();
                            VTrace("Simple query took: " + _timer.ElapsedMilliseconds);

                            cache.UpdateResults(results, start, end, canUseCache);

                            if (wantResults)
                                return cache.Get(start, end, query.RemovedPredicates);

                            return null;
                        }
                        else if (status == CachedData.CacheStatus.EXTEND_START)
                        {
                            Trace("Adjusting end of query for EXTEND_START");
                            // Use our end date, modified to allow refreshing data
                            DateTime edge = cache.DataStartTime.AddMinutes(5);
                            if (tb.Success)
                            {
                                // Need to adjust start to be a 'full' bucket
                                DateTime newEnd = edge;
                                string dur = tb.Groups["duration"].Success ? tb.Groups["duration"].Value : tb.Groups["duration_sec"].Value + "s";
                                edge = ParsingUtils.CeilingInterval(dur, edge);
                                Trace("TimeBucket found. Rounded edge time from " + newEnd.ToString("O") + " to " + edge.ToString("O"));
                                queryToExecute = queryToExecute.Replace(m.Groups["end_time"].Value, edge.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");
                            }
                            _timer.Restart();
                            DataTable results = SimpleQuery(queryToExecute);
                            _timer.Stop();
                            VTrace("Simple query took: " + _timer.ElapsedMilliseconds);

                            cache.UpdateResults(results, start, end, canUseCache);

                            if (wantResults)
                                return cache.Get(start, end, query.RemovedPredicates);

                            return null;
                        }
                        else if (status == CachedData.CacheStatus.EXTEND_BOTH)
                        {
                            // For EXTEND_BOTH, we'll query both start and end
                            Trace("Adjusting start/end of query for EXTEND_BOTH");

                            string queryCopy = queryToExecute;


                            DateTime edge = cache.DataEndTime.AddMinutes(-5);
                            if (tb.Success)
                            {
                                // Need to adjust start to be a 'full' bucket
                                DateTime oldStart = edge;
                                string dur = tb.Groups["duration"].Success ? tb.Groups["duration"].Value : tb.Groups["duration_sec"].Value + "s";
                                edge = ParsingUtils.RoundInterval(dur, edge);
                                Trace("TimeBucket found. Rounded edge time from " + oldStart.ToString("O") + " to " + edge.ToString("O"));
                                queryToExecute = queryToExecute.Replace(m.Groups["start_time"].Value, edge.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");
                            }
                            _timer.Restart();
                            DataTable results = SimpleQuery(queryToExecute);
                            _timer.Stop();
                            VTrace("Query 1/2 query took: " + _timer.ElapsedMilliseconds);

                            cache.UpdateResults(results, edge, end, canUseCache, false);

                            // Perform second query, this time getting data before the cache
                            edge = cache.DataStartTime.AddMinutes(5);
                            if (tb.Success)
                            {
                                // Need to adjust start to be a 'full' bucket
                                DateTime newEnd = edge;
                                string dur = tb.Groups["duration"].Success ? tb.Groups["duration"].Value : tb.Groups["duration_sec"].Value + "s";
                                edge = ParsingUtils.CeilingInterval(dur, edge);
                                Trace("TimeBucket found. Rounded edge time from " + newEnd.ToString("O") + " to " + edge.ToString("O"));
                                queryCopy = queryCopy.Replace(m.Groups["end_time"].Value, edge.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");
                            }
                            _timer.Restart();
                            results = SimpleQuery(queryCopy);
                            _timer.Stop();
                            VTrace("Query 2/2 query took: " + _timer.ElapsedMilliseconds);

                            cache.UpdateResults(results, start, edge, canUseCache, false);

                            if (wantResults)
                                return cache.Get(start, end, query.RemovedPredicates);

                            return null;
                        }
                        else if(status == CachedData.CacheStatus.FULL)
                        {
                            // If we're constantly refreshing, cache will be up to date (mostly)
                            Debug("Normalized query will be satisfied by cache.");
                            cache.CacheUsage.Add(DateTime.UtcNow);
                            if (wantResults)
                                return cache.Get(start, end, query.RemovedPredicates);
                            return null;
                        }
                        else
                        {
                            Critical("Cannot handle cache status: " + status.ToString());
                        }



                    }

                    return null;
                }
            }
            finally
            {
                _timer.Stop();
                VTrace("Query: " + _timer.Elapsed.ToString());
            }
        }

        /// <summary>
        /// Executes a query utilizing our cache.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="wantResults">Return the results. False can be used for updates, so future queries that do want results have more data cached.</param>
        /// <returns></returns>
        public IEnumerable<PGMessage> CachedQuery(string query, bool wantResults = true, string tag = null)
        {
            Query.QueryUtils.NormalizedQuery nq = new Query.QueryUtils.NormalizedQuery();
            nq.OriginalQueryText = query;
            nq.RemovedPredicates = null;
            nq.QueryText = query;
            nq.QueryTag = tag;

            return CachedQueryDecomp(nq, wantResults);
            //_timer.Restart();

            //try
            //{
            //    Trace("Cached query: [{0}]", query);


            //    // 'Key' will be the query with time part normalized.
            //    Match m = ParsingUtils.TimeFilterRegex.Match(query);
            //    if (!m.Success)
            //    {
            //        Error("Failed to identify start/end date for query - defaulting to passthrough: " + query);
            //        return Translator.BuildResponseFromData(SimpleQuery(query));
            //    }
            //    DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            //    DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            //    string normalized = query.Replace(m.Groups["start_time"].Value, "###START###").Replace(m.Groups["end_time"].Value, "###END###");

            //    Caching.CachedData cache = null;
            //    long startMs = _timer.ElapsedMilliseconds;
            //    lock (_queryCache)
            //    {
            //        if (!_queryCache.ContainsKey(normalized))
            //        {
            //            string tag = ParsingUtils.GetQueryTag(normalized);
            //            _queryCache[normalized] = new Caching.CachedData(tag,_logger);
            //        }
            //        cache = _queryCache[normalized];
            //    }
            //    long lockTime = _timer.ElapsedMilliseconds - startMs;
            //    VTrace("Lock took " + lockTime);
            //    // If we're updating - make sure nobody else is
            //    lock (cache)
            //    {
            //        // Check overlaps
            //        bool canUseCache = (cache.DescriptorMessage != null &&
            //                           (
            //                                (start <= cache.DataEndTime && start >= cache.DataStartTime) ||
            //                                (start < cache.DataStartTime && end >= cache.DataStartTime)
            //                            ));



            //        string queryToExecute = query;

            //        if (canUseCache)
            //        {
            //            // TODO: allow overlap? X minutes of a 'refresh' window.
            //            bool within = start <= cache.DataEndTime && start >= cache.DataStartTime
            //                && (end - cache.DataEndTime).TotalSeconds < 30;

            //            if (within)
            //            {
            //                cache.CacheUsage.Add(DateTime.UtcNow);
            //                // If we're constantly refreshing, cache will be up to date (mostly)
            //                Debug("Normalized query will be satisfied by cache.");
            //                return cache.Get(start, end);
            //            }

            //            // Use our end date
            //            queryToExecute = queryToExecute.Replace(m.Groups["start_time"].Value, cache.DataEndTime.ToString("yyyy-MM-ddTHH:mm:ss.fff") + "Z");

            //            Debug("Cached data found - updating query: [" + queryToExecute + "]");
            //        }

            //        DataTable results = SimpleQuery(queryToExecute);

            //        cache.UpdateResults(results, start, end, canUseCache);

            //        if (wantResults)
            //            return cache.Get(start, end);

            //        return null;
            //    }
            //}
            //finally
            //{
            //    _timer.Stop();
            //    VTrace("Query: " + _timer.Elapsed.ToString());
            //}
        }

      

        public IEnumerable<PostgresqlCommunicator.PGMessage> CachedQuery(StandardQuery query, bool wantResults = true)
        {
            if(query.RemovedPredicates == null || query.RemovedPredicates.Count == 0)
                return CachedQuery(query.RawQuery, wantResults, query.Tag);

            Query.QueryUtils.NormalizedQuery nq = new Query.QueryUtils.NormalizedQuery();
            nq.QueryText = query.RawQuery;
            nq.OriginalQueryText = query.RawQuery;
            nq.RemovedPredicates = query.RemovedPredicates;
            return CachedQueryDecomp(nq, wantResults);

            //// 'Key' will be the query with time part normalized.
            //Match m = ParsingUtils.TimeFilterRegex.Match(query);
            //if (!m.Success)
            //{
            //    Console.WriteLine("Failed to identify start/end date for query - defaulting to passthrough: " + query);
            //    return Translator.BuildResponseFromData(SimpleQuery(query));
            //}

            //DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            //DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
            //string normalized = query.Replace(m.Groups["start_time"].Value, "###START###").Replace(m.Groups["end_time"].Value, "###END###");
        }

        private Dictionary<string, Caching.CachedData> _queryCache = new Dictionary<string, Caching.CachedData>();

        #endregion Caching

    }
}
