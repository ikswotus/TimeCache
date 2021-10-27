using PostgresqlCommunicator;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{

  
    /// <summary>
    /// 
    /// CachedData should be locked for usage.
    /// 
    /// TODO: Figure out storage for queries.
    /// 
    /// DataTable?
    /// DataRowMessage? -> nice because it's ready to transmit...
    /// </summary>
    public class CachedData : SLog.SLoggableObject
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CachedData(string key, SLog.ISLogger logger)
            : base(key, logger)
        {
            Debug("Allocated new cached data");

            DescriptorMessage = null;
            CurrentData = new List<CachedRow>();
        }

        /// <summary>
        /// Called when the cache is being evicted from memory - reset pooled objects
        /// </summary>
        public void Clear()
        {
            //DescriptorMessage = null;
            Debug("Clearing");

            foreach (CachedRow cr in CurrentData)
            {
                cr.TranslatedMessage.Release();
            }

            CurrentData.Clear();
        }

        public int Trim(DateTime start)
        {
            Debug("Trimming data from start: " + start.ToString("O"));
            int c = CurrentData.Count();
            while (CurrentData.Count > 0 && CurrentData[0].RawDate < start)
            {
                CurrentData.RemoveAt(0);
            }
            int d = CurrentData.Count();
            Debug("Trimmed from " + c + " to " + d);
            
            return c - d;
        }

        /// <summary>
        /// Determines if the period covered by start -> end overlaps
        /// any of our cached data.
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public CacheStatus HasCachedData(DateTime start, DateTime end)
        {
            if (DescriptorMessage == null)
                return CacheStatus.UNCACHED;
            /****
             * 
             * ------[DS---cached data---DE]-------
             * 1)        S----------E
             * 1)                   S------------E
             * 
             * ------[DS---cached data---DE]-------
             * 2)  S----E
             * 
             * ------[DS---cached data---DE]-------
             * 3)  S-------------------------E
             * 
             * 
             */
            // Possible cases
            // 1) Start is within cache, end is either within cache or outside DataEndTime
            if (start >= DataStartTime)
            {
                if (start >= DataEndTime || end < DataStartTime)
                    return CacheStatus.UNCACHED;

                if (end <= DataEndTime)
                    return CacheStatus.FULL;
                return CacheStatus.EXTEND_END;

            }
            //2) Start is before cache, end is within cache
            if(end <= DataEndTime)
            {
                if (end <= DataStartTime || start > DataEndTime)
                    return CacheStatus.UNCACHED;

                if (start <= DataEndTime)
                    return CacheStatus.FULL;
                return CacheStatus.EXTEND_START;
            }
            // 3) Overextend on both sides
            if (start < DataStartTime && end > DataEndTime)
            {
                return CacheStatus.EXTEND_BOTH;
            }
            return CacheStatus.UNCACHED;
        }

        /// <summary>
        /// Identify cached data
        /// </summary>
        public enum CacheStatus
        {
            /// <summary>
            /// No cached data
            /// </summary>
            UNCACHED,
            /// <summary>
            /// Fully cached data
            /// </summary>
            FULL,
            /// <summary>
            /// Start is covered by cache, but end exceeds cache
            /// </summary>
            EXTEND_END,
            /// <summary>
            /// End is covered by cache, but start preceeds cache
            /// </summary>
            EXTEND_START,
            /// <summary>
            /// Start preceeds cache, end exceeds cache
            /// </summary>
            EXTEND_BOTH
        }

        /// <summary>
        /// Returns true if the cache fully covers the requested range.
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="updateInterval"></param>
        /// <returns></returns>
        public bool FullySatisfied(DateTime start, DateTime end, TimeSpan updateInterval)
        {
            return start <= DataEndTime && start >= DataStartTime && (end - DataEndTime) < updateInterval;
        }

        /// <summary>
        /// Timer
        /// </summary>
        private System.Diagnostics.Stopwatch _timer = new Stopwatch();

        /// <summary>
        /// Cached data will identify when it is used, so the eviction mechanism
        /// can avoid removing active data.
        /// </summary>
        public List<DateTime> CacheUsage = new List<DateTime>();

        /// <summary>
        /// Pool the cached rows.
        /// </summary>
        private Utils.FixedSizeBytePool _pool = null;

        /// <summary>
        /// Merges in the results of a query.
        /// TODO: Caller must ensure there is overlap between any existing cache, otherwise our data will contain gaps.
        /// </summary>
        /// <param name="table"></param>
        public void UpdateResults(DataTable table, DateTime queryStart, DateTime queryEnd, bool usedCache, bool trim = true, int timeIndex = -1)
        {
            // Set this first, even with no results..
            if (DescriptorMessage == null)
            {
                DescriptorMessage = Translator.BuildRowDescription(table);
                // Find our time column

                for (int i = 0; i < DescriptorMessage.Fields.Count; i++)
                {
                    if (DescriptorMessage.Fields[i].ColumnName == "time\0")
                    {
                        _timeIndex = i;
                        break;
                    }
                }
            }
            if (_timeIndex == -1 && timeIndex != -1)
                _timeIndex = timeIndex;

            if (table.Rows.Count == 0)
            {
                return;
            }

            // CurrentData is ordered by time
            // Determine if the new data has any overlap
            // this is to be expected, especially if we set a 'RefreshWindow' so near-real time values
            // are updated by subsequent queries.
            if (DataStartTime == DateTime.MinValue)
                DataStartTime = queryStart;

            

            CacheUsage.Add(DateTime.UtcNow);

            // Keep 2 hours cache usage around.
            CacheUsage.RemoveAll(d => d < DateTime.UtcNow.AddHours(-2));

            if (_pool == null)
            {
                // Need to know the actual size to be able to construct the pool.
                DataRowMessage row = Translator.BuildRowMessage(table.Rows[0]);
                _pool = new Utils.FixedSizeBytePool(row.GetCompletedSize());
            }

            Type timeType = table.Columns[_timeIndex].DataType;
            DateTime start = Translator.GetDateTime(timeType, table.Rows[0], _timeIndex).ToUniversalTime();

            // Removal logic - check if we're caching older or newer
            bool removed = false;

            if (!usedCache)
            {
                // TODO: Support gaps in cached data. For now - if we get something outside our cached data, reset
                Debug("Query does not overlap cached data - clearing existing data");
                Clear();

                removed = true;
            }
            else
            {
                if (queryStart < DataStartTime)
                {
                    DateTime qend = Translator.GetDateTime(timeType, table.Rows[table.Rows.Count - 1], _timeIndex).ToUniversalTime();
                    VTrace("Existing query cache end: " + qend.ToString("O"));
                    while (CurrentData.Count > 0)
                    {
                        if (qend > CurrentData[0].RawDate)
                        {
                            Debug("Removing overlap date: " + CurrentData[0].RawDate.ToString("o"));
                            CurrentData.RemoveAt(0);
                            removed = true;
                        }
                        else
                        {
                            break;
                        }
                    }
                }
                if(queryEnd > DataEndTime)
                {
                    VTrace("query end exceed cache end time - merging results to end");
                    for (int i = CurrentData.Count() - 1; i >= 0; i--)
                    {
                        if (start > CurrentData[i].RawDate)
                        {
                            break;
                        }

                        // else overlap - remove the old data
                        CachedRow cr = CurrentData[i];
                        CurrentData.RemoveAt(i);
                        cr.TranslatedMessage.Release();
                        Debug("Removing overlap date: " + start.ToString("o"));
                        removed = true;
                    }

                }
            }

            if(trim)
                Trim(queryStart);

            // TODO: DEBUG - try to force re-pooling



            // Store raw data.
            List<CachedRow> rows = new List<CachedRow>(table.Rows.Count);
            foreach (DataRow dr in table.Rows)
            {
                CachedRow cd = new CachedRow();
                cd.Objects = dr.ItemArray;
                cd.RawDate = Translator.GetDateTime(timeType, dr, _timeIndex);
                cd.TranslatedMessage = Translator.BuildRowMessage(dr);
                cd.TranslatedMessage.Set(_pool);

                rows.Add(cd);
                
            }

            if (queryStart < DataStartTime)
                CurrentData.InsertRange(0, rows);
            else
                CurrentData.AddRange(rows);

            // Update stored dates to reflect added values
            if (queryStart < DataStartTime)
                DataStartTime = queryStart;

            if (queryEnd > DataEndTime)
                DataEndTime = queryEnd;

        }

        /// <summary>
        /// Retrieve all rows from the cache in the interval [start, end] inclusive
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <returns></returns>
        public IEnumerable<PGMessage> Get(DateTime start, DateTime end, bool inclusiveEnd = true)
        {
            if (DescriptorMessage == null)
                throw new Exception("No data - Update() must be called");

            List<PGMessage> matched = new List<PGMessage>();
            matched.Add(DescriptorMessage);

            if(inclusiveEnd)
                matched.AddRange(CurrentData.Where(r => r.RawDate >= start && r.RawDate <= end).Select(s => s.TranslatedMessage));
            else
                matched.AddRange(CurrentData.Where(r => r.RawDate >= start && r.RawDate < end).Select(s => s.TranslatedMessage));

            return matched;
        }

        /// <summary>
        /// Retrieve all rows from the cache in the interval [start, end] inclusive
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="predicates">List of predicates to filter on</param>
        /// <param name="inclusiveEnd"></param>
        /// <returns></returns>
        public IEnumerable<PGMessage> Get(DateTime start, DateTime end, List<Query.QueryUtils.PredicateGroup> predicates, bool inclusiveEnd = true)
        {
            if (DescriptorMessage == null)
                throw new Exception("No data - Update() must be called");

            List<PGMessage> matched = new List<PGMessage>();
            matched.Add(DescriptorMessage);

            _timer.Restart();
            int c = CurrentData.Count();

            // Filter by time first

            IEnumerable<CachedRow> filtered = inclusiveEnd ? CurrentData.Where(r => r.RawDate >= start && r.RawDate <= end)
                                                               : CurrentData.Where(r => r.RawDate >= start && r.RawDate < end);
            long timeFilterMs = _timer.ElapsedMilliseconds;
            VTrace("time filter took" + timeFilterMs);

            int fc = filtered.Count();
            Debug("Time filter reduced cachecount from " + c + " to " + fc);

            // Filter by any predicates
            long startMs = _timer.ElapsedMilliseconds;
            if (predicates != null)
            {
                foreach (Query.QueryUtils.PredicateGroup pg in predicates)
                {
                    string terminatedKey = pg.Key + "\0";
                    // Match by column
                    int columnIndex = -1;
                    for (int i = 0; i < DescriptorMessage.Fields.Count; i++)
                        if (DescriptorMessage.Fields[i].ColumnName.Equals(terminatedKey, StringComparison.OrdinalIgnoreCase))
                        {
                            columnIndex = i;
                            break;
                        }
                    if (columnIndex == -1)
                        throw new Exception("Unable to locate filter column: " + pg.Key);

                    Type t = DescriptorMessage.OriginalTypes[columnIndex];

                    object converted = ConvertPredicateValue(t, pg.Value);
                    filtered = filtered.Where(f => f.Filter(columnIndex, converted));

                    Debug("Filter predicate on [" + pg.Key + "=" + pg.Value + "] reduced count to" + filtered.Count());
                    long stopMs = _timer.ElapsedMilliseconds;
                    VTrace("Filter " + pg.Key + " took " + (stopMs - startMs));
                    startMs = stopMs;
                }
            }
             
            matched.AddRange(filtered.Select(s => s.TranslatedMessage));

            Debug("Filtered matches: " + matched.Count());

            return matched;
        }


        public object ConvertPredicateValue(Type t, string value)
        {
            if(t == typeof(string))
                return value;
            else if (t == typeof(int))
                return int.Parse(value);
            else if (t == typeof(double))
                return double.Parse(value);
            throw new Exception("Unsupported predicate filter type: " + t.ToString());
        }

        public DataTable GetTable(DateTime start, DateTime end, bool inclusiveEnd = true)
        {
            if (DescriptorMessage == null)
                throw new Exception("No data - Update() must be called");

            
            DataTable t = new DataTable();
            for (int i = 0; i < DescriptorMessage.Fields.Count; i++)
            {
                t.Columns.Add(new DataColumn(DescriptorMessage.Fields[i].ColumnName, DescriptorMessage.OriginalTypes[i]));
            }


            if (inclusiveEnd)
            {
                foreach (CachedRow cr in CurrentData.Where(r => r.RawDate >= start && r.RawDate <= end))
                {
                    t.Rows.Add(cr.Objects);
                }
            }
            else
            {
                foreach(CachedRow cr in CurrentData.Where(r => r.RawDate >= start && r.RawDate < end))
                {
                    t.Rows.Add(cr.Objects);
                } 
            }

            return t;
        }

        /// <summary>
        /// 'smart' filtering. If we're querying for data on an interval of now-x to now
        /// we'll leave more and more data cached that will never be utilized again.
        /// Tracking the start times will allow us to detect when older data is no longer being utilized
        /// or at least not being utilized well.
        /// </summary>
        private List<DateTime> _startTimes = new List<DateTime>();

        /// <summary>
        /// Identify the 'time' column so we can pull out the date to cache.
        /// </summary>
        private int _timeIndex = -1;

        /// <summary>
        /// Start of cached data
        /// </summary>
        public DateTime DataStartTime = DateTime.MinValue;

        /// <summary>
        /// End of cached data
        /// </summary>
        public DateTime DataEndTime = DateTime.MinValue;

        /// <summary>
        /// Header message identifying columns + types
        /// </summary>
        public RowDescription DescriptorMessage { get; set; }
        
        /// <summary>
        /// Actual cached data.
        /// </summary>
        public List<CachedRow> CurrentData { get; set; }
    }

    /// <summary>
    /// Pair the 'translated' row with the datetime.
    /// Having the row in the DataRowMessage format simplifies the return process,
    /// but the date makes caching simpler.
    /// </summary>
    public class CachedRow
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CachedRow()
        {

        }


        /// <summary>
        /// Allow simple filtering of a row
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool Filter(int index, object value)
        {
            if (index > Objects.Length)
                throw new IndexOutOfRangeException("Requested index: " + index + " exceeds row length: " + Objects.Length);

            return Objects[index].Equals(value);
        }
     
        /// <summary>
        /// Store 'row' objects
        /// </summary>
        public object[] Objects { get; set; }

        /// <summary>
        /// Date of the row
        /// </summary>
        public DateTime RawDate { get; set; }

        /// <summary>
        /// PG-ified data.
        /// </summary>
        public DataRowMessage TranslatedMessage { get; set; }
    }
}
