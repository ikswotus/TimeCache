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
    public class QueryManager : SLog.SLoggableObject, Query.IQuerier
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

            _parser = new Query.QueryParser(logger);
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
        #region IQuerier

        public DataTable SimpleQuery(string query)
        {
            return Utils.Postgresql.TableManager.GetTable(_connectionString, query);
        }

        public DataTable CachedQuery(Query.NormalizedQuery normalized, QueryRange range)
        {
            string query = normalized.QueryToExecute(range);

            return Utils.Postgresql.TableManager.GetTable(_connectionString, query);
        }

        #endregion IQuerier


        private Query.QueryParser _parser = null;

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

            Query.QueryParser qp = new Query.QueryParser(_logger);

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

                            Query.NormalizedQuery nq = qp.Normalize(cq.GetFormatted());
                            CachedQuery(nq, false);
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
                                        SegmentManager cd = _queryCache[q];
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
                                            SegmentManager cd = _queryCache[kvp.Key];

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
                                        SegmentManager cd = _queryCache[q];
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
                                            SegmentManager cd = _queryCache[key];

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

        public DataTable SimpleQuery(Query.NormalizedQuery query)
        {
            return SimpleQuery(query.QueryText, query.Timeout);
        }


        #endregion Passthrough

        #region Caching

       
        public DataTable QueryToTable(string query)
        {
            Query.NormalizedQuery nq = _parser.Normalize(query);
            return QueryToTable(nq);
        }

        public DataTable QueryToTable(Query.NormalizedQuery query)
        {
            if (!query.ValidTimestamps)
            {
                Error("Failed to identify start/end date for query - defaulting to passthrough: " + query.QueryText);
                return SimpleQuery(query.QueryText);
            }

            SegmentManager cache = GetCache(query);        
            lock (cache)
            {
                return cache.GetTable(query);
            }
        }

        private Stopwatch _timer = new Stopwatch();

        public IEnumerable<PGMessage> CachedQueryDecomp(Query.NormalizedQuery query, bool wantResults = true)
        {
            _timer.Restart();

            try
            {
                Trace("Cached query: [{0}]", query.QueryText);
                if (!query.ValidTimestamps)
                {
                    Error("Failed to identify start/end date for query - defaulting to passthrough: " + query.QueryText);
                    return Translator.BuildResponseFromData(SimpleQuery(query.QueryText));
                }

                SegmentManager cache = GetCache(query);

                lock (cache)
                {
                    IEnumerable<PGMessage> res =  cache.Get(query);
                    if (wantResults)
                        return res;
                    return null;
                }
            }
            finally
            {
                _timer.Stop();
                VTrace("Query: " + _timer.Elapsed.ToString());
            }
        }

        private Caching.SegmentManager GetCache(Query.NormalizedQuery query)
        {
            Caching.SegmentManager cache = null;
            long startMs = _timer.ElapsedMilliseconds;
            lock (_queryCache)
            {
                if (!_queryCache.ContainsKey(query.NormalizedQueryText))
                {
                    string tag = string.IsNullOrEmpty(query.QueryTag) ? ParsingUtils.GetQueryTag(query.NormalizedQueryText) : query.QueryTag;
                    _queryCache[query.NormalizedQueryText] = new Caching.SegmentManager(this, _logger);
                }
                cache = _queryCache[query.NormalizedQueryText];
            }
            long lockTime = _timer.ElapsedMilliseconds - startMs;
            VTrace("Lock took " + lockTime);
            return cache;
        }

        /// <summary>
        /// Executes a query utilizing our cache.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="wantResults">Return the results. False can be used for updates, so future queries that do want results have more data cached.</param>
        /// <returns></returns>
        public IEnumerable<PGMessage> CachedQuery(Query.NormalizedQuery nq, bool wantResults = true)
        {
            return CachedQueryDecomp(nq, wantResults);
        }



        /// <summary>
        /// Pairs a normalized/decomposed query with the data manager
        /// </summary>
        private Dictionary<string, SegmentManager> _queryCache = new Dictionary<string, SegmentManager>();


       // private Dictionary<string, Caching.CachedData> _queryCache = new Dictionary<string, Caching.CachedData>();

        #endregion Caching

    }
}
