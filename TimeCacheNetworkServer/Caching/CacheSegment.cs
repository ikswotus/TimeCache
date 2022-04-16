using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// Allows caches to support multiple time ranges.
    ///
    /// A cache segment must be a continguous time period from
    /// @StartTime to @EndTime.
    /// 
    /// Segments must be locked when accessing/clearing/updating
    /// </summary>
    public class CacheSegment : SLog.SLoggableObject
    {
        public CacheSegment(string key, SLog.ISLogger logger, Utils.FixedSizeBytePool pool)
            : base(key, logger)
        {
            _pool = pool;
            CurrentData = new List<CachedRow>();
        }

        public long Count()
        {
            return CurrentData.LongCount();
        }

        public void Clear()
        {
            Trace("Clearing segment");

            foreach (CachedRow cr in CurrentData)
            {
                cr.TranslatedMessage.Release();
            }

            CurrentData.Clear();
        }

        /// <summary>
        /// Check to see if the cache segment times overlap the query
        ///
        /// --------[DS---cached data---DE]-------
        /// 1)          S----------E
        /// Contains: Query range is fully covered by the segment
        ///
        ///  ------[DS--cached data--DE]-------
        /// 2)   S--------------------------E
        /// Enveloped: Segment range is fully covered by the query
        /// 
        /// 
        /// ------[DS---cached data---DE]-------
        /// 3)                    S----------E
        /// OverlapStart: Partially cached, end of query is not covered
        /// 
        /// ---------[DS---cached data---DE]-------
        /// 4)  S-----------E
        /// OverlapEnd: PartiallyCached, start of query is not covered
        /// 
        /// ------[DS---cached data---DE]-------
        /// 5)                               S----E
        /// EMPTY: No overlap at all
        /// </summary>
        /// <param name="range"></param>
        /// <returns></returns>
        public bool Overlaps(QueryRange range)
        {
            return Contains(range) || Enveloped(range) || OverlapsStart(range) || OverlapsEnd(range);
        }

        /// <summary>
        /// Checks to see if the range is CONTAINED or OVERLAPPEDSTART
        /// Used for segment merging by ensuring our merges are in order
        /// </summary>
        /// <param name="range"></param>
        /// <returns></returns>
        public bool OverlapsOrdered(QueryRange range)
        {
            return Contains(range) || OverlapsStart(range);
        }

        public bool Enveloped(QueryRange range)
        {
            return (DataStartTime >= range.StartTime && DataEndTime <= range.EndTime);
        }

        public bool Contains(QueryRange range)
        {
            return (DataStartTime <= range.StartTime && range.EndTime <= DataEndTime);
        }

        /// <summary>
        /// True if range.start is within cache
        /// </summary>
        /// <param name="range"></param>
        /// <returns></returns>
        public bool OverlapsStart(QueryRange range)
        {
            return (DataStartTime <= range.StartTime && range.StartTime <= DataEndTime);
        }

        /// <summary>
        /// True if range.end is within cache
        /// </summary>
        /// <param name="range"></param>
        /// <returns></returns>
        public bool OverlapsEnd(QueryRange range)
        {
            return (DataStartTime <= range.EndTime && range.EndTime <= DataEndTime);
        }

        /// <summary>
        /// Retrieves the new list of ranges we need
        /// </summary>
        /// <param name="qr"></param>
        /// <returns></returns>
        public List<QueryRange> Intersect(QueryRange qr)
        {
            return Intersect(qr, TimeSpan.Zero, TimeSpan.Zero);
        }
        public List<QueryRange> Intersect(QueryRange qr, TimeSpan bucketInterval, TimeSpan updateInterval)
        {
            if (Contains(qr))
            {
                // Don't need any data
                // --------[DS---cached data---DE]-------
                //              S----------E
                return new List<QueryRange>();
            }
            else if (Enveloped(qr))
            {
                // Need to split the QueryRange into two subranges
                // overlapping our segment times
                // ------[DS--cached data--DE]------ -
                //   S---*-------------------*---E
                QueryRange before = new QueryRange(qr.StartTime, GetEnd(DataStartTime, bucketInterval, updateInterval));
                QueryRange after = new QueryRange(GetStart(DataEndTime, bucketInterval, updateInterval), qr.EndTime);

                return new List<QueryRange> { before, after };
            }
            else if (OverlapsStart(qr))
            {
                //------[DS---cached data---DE]-------
                //                    S-------*--E
                // Need to get data from CacheEnd to QueryEnd
                QueryRange after = new QueryRange(GetStart(DataEndTime, bucketInterval, updateInterval), qr.EndTime);
                return new List<QueryRange> { after };
            }
            else if (OverlapsEnd(qr))
            {
                //------[DS---cached data---DE]------ -
                // S----*---E
                // Need to get data from QueryStart to CacheStart
                QueryRange before = new QueryRange(qr.StartTime, GetEnd(DataStartTime, bucketInterval, updateInterval));
                return new List<QueryRange> { before };
            }

            // TODO: Throw? We should be calling overlaps first...but this should be harmless
            return new List<QueryRange>() { qr };
        }

        /// <summary>
        /// Helper to adjust start time by subtracting the update window
        /// and flooring to the nearest bucket
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="bucket"></param>
        /// <param name="update"></param>
        /// <returns></returns>
        private DateTime GetStart(DateTime start, TimeSpan bucket, TimeSpan update)
        {
            DateTime updated = start.Subtract(update);
            return ParsingUtils.RoundInterval(bucket, updated);
        }
        private DateTime GetEnd(DateTime end, TimeSpan bucket, TimeSpan update)
        {
            DateTime updated = end.Add(update);
            return ParsingUtils.CeilingInterval(bucket, updated);
        }

        /// <summary>
        /// Removes any cached data earlier than @start.
        /// StartTime will be adjusted to our new ealiest time.
        /// </summary>
        /// <param name="start"></param>
        /// <returns>Number of rows removed.</returns>
        private int TrimStart(DateTime start)
        {
            if (CurrentData.Count == 0)
                return 0;

            Debug("Trimming data from start: " + start.ToString("O"));
            int c = CurrentData.Count();
            while (CurrentData.Count > 0 && CurrentData[0].RawDate < start)
            {
                CurrentData.RemoveAt(0);
            }
            int removed = c - CurrentData.Count();
           
            if(CurrentData.Count == 0)
            {
                DataStartTime = DateTime.MinValue;
            }
            else
                DataStartTime = CurrentData[0].RawDate;

            Debug("TrimStart removed " + removed + " rows, adjusted start is now " + DataStartTime.ToString("O"));

            return removed;
        }

        /// <summary>
        /// Merges two segements
        /// Required that mergeTarget's start time is later, which means we only have 2 cases to merge:
        /// 1) Segment is entirely contained by the other (Note: This should never actually happen, since it would imply
        /// we queried for a subset of data that was already cached)
        /// 2) Segment extends our current segment. This should be the normal scenario - we will drop any overlapping
        /// rows from our data list, and take the merge target's rows.
        /// </summary>
        /// <param name="mergeTarget">Source of new data - Should not be used again after calling merge</param>
        public void MergeSegments(CacheSegment mergeTarget)
        {
            if (mergeTarget.DataStartTime == DateTime.MinValue || mergeTarget.DataEndTime == DateTime.MinValue)
                throw new Exception("Invalid cache segment to merge into");
            
            if(mergeTarget.CurrentData.Count == 0)
            {
                // This can happen if we're using a (now - x) query, and the data for now has not been databased
                // our query has fresh dates, but theres no new data to merge into. Maybe we dont expect more data
                // and this is ok. However, if we consistently dont have new data, its possible our update window
                // is too small and will introduce gaps into the data.
                Debug("Warning: Merge target did not contain any rows, update interval may need to be increased.");
            }

            // Determine where the overlap lies:
            QueryRange other = new QueryRange(mergeTarget.DataStartTime, mergeTarget.DataEndTime);
            
            if (!OverlapsOrdered(other))
                throw new Exception("Cannot merge segments - no overlap detected");


            if (Contains(other))
            {
                mergeTarget.Clear();

                return;
            }
            else if (OverlapsStart(other))
            {
                //------[DS---cached data---DE]-------
                //                    S-------*--E
                // Need to get data from CacheEnd to QueryEnd

                DateTime target = mergeTarget.CurrentData.Count > 0 ? mergeTarget.CurrentData[0].RawDate : mergeTarget.DataStartTime;

                for (int i = CurrentData.Count() - 1; i >= 0; i--)
                {
                    if (CurrentData[i].RawDate < target)
                    {
                        break;
                    }

                    // else overlap - remove the old data
                    CachedRow cr = CurrentData[i];
                    CurrentData.RemoveAt(i);
                    cr.TranslatedMessage.Release();
                    Trace("Removing overlap date: " + cr.RawDate.ToString("o"));

                    
                }
                CurrentData.AddRange(mergeTarget.CurrentData);
                mergeTarget.CurrentData.Clear();// reset target so we don't accidentally clear/release rows we've taken ownership of
                DataEndTime = mergeTarget.DataEndTime;
            }
            else
                throw new Exception("Merge does not support overlap type");


        }


        public void AddData(DataTable table, DateTime queryStart, DateTime queryEnd, int timeIndex)
        {
            Type timeType = table.Columns[timeIndex].DataType;
          //  DateTime start = PostgresqlCommunicator.Translator.GetDateTime(timeType, table.Rows[0], timeIndex).ToUniversalTime();

            foreach (DataRow dr in table.Rows)
            {
                CachedRow cd = new CachedRow();
                cd.Objects = dr.ItemArray;
                cd.RawDate = PostgresqlCommunicator.Translator.GetDateTime(timeType, dr, timeIndex);
                cd.TranslatedMessage = PostgresqlCommunicator.Translator.BuildRowMessage(dr);
                cd.TranslatedMessage.Set(_pool);

                CurrentData.Add(cd);
            }

            
            DataStartTime = queryStart;
            DataEndTime = queryEnd;
        }

        /// <summary>
        /// Retrieve all rows from the cache in the interval [start, end] inclusive
        /// </summary>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="predicates">List of predicates to filter on</param>
        /// <param name="inclusiveEnd"></param>
        /// <returns></returns>
        public IEnumerable<CachedRow> GetRows(PostgresqlCommunicator.RowDescription rowDescriptor, DateTime start, DateTime end, List<Query.PredicateGroup> predicates, bool inclusiveEnd = true)
        {
            List<CachedRow> matched = new List<CachedRow>();

            //_timer.Restart();
            int c = CurrentData.Count();

            // Filter by time first

            IEnumerable<CachedRow> filtered = inclusiveEnd ? CurrentData.Where(r => r.RawDate >= start && r.RawDate <= end)
                                                               : CurrentData.Where(r => r.RawDate >= start && r.RawDate < end);
            //long timeFilterMs = _timer.ElapsedMilliseconds;
            //   VTrace("time filter took" + timeFilterMs);

            int fc = filtered.Count();
            Debug("Time filter reduced cachecount from " + c + " to " + fc);

            // Filter by any predicates
            // long startMs = _timer.ElapsedMilliseconds;
            if (predicates != null)
            {
                foreach (Query.PredicateGroup pg in predicates)
                {
                    string terminatedKey = pg.Key + "\0";
                    // Match by column
                    int columnIndex = -1;
                    for (int i = 0; i < rowDescriptor.Fields.Count; i++)
                        if (rowDescriptor.Fields[i].ColumnName.Equals(terminatedKey, StringComparison.OrdinalIgnoreCase))
                        {
                            columnIndex = i;
                            break;
                        }
                    if (columnIndex == -1)
                        throw new Exception("Unable to locate filter column: " + pg.Key);

                    Type t = rowDescriptor.OriginalTypes[columnIndex];

                    object converted = ConvertPredicateValue(t, pg.Value);
                    filtered = filtered.Where(f => f.Filter(columnIndex, converted));

                    Debug("Filter predicate on [" + pg.Key + "=" + pg.Value + "] reduced count to" + filtered.Count());
                    //long stopMs = _timer.ElapsedMilliseconds;
                    //VTrace("Filter " + pg.Key + " took " + (stopMs - startMs));
                    //startMs = stopMs;
                }
            }

            matched.AddRange(filtered);

            Debug("Filtered matches: " + matched.Count());

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
        public IEnumerable<PostgresqlCommunicator.PGMessage> Get(PostgresqlCommunicator.RowDescription rowDescriptor, DateTime start, DateTime end, List<Query.PredicateGroup> predicates, bool inclusiveEnd = true)
        {
            List<PostgresqlCommunicator.PGMessage> matched = new List<PostgresqlCommunicator.PGMessage>();
            matched.Add(rowDescriptor);

            matched.AddRange(GetRows(rowDescriptor, start, end, predicates, inclusiveEnd).Select(s => s.TranslatedMessage));

            Debug("Filtered matches: " + matched.Count());

            return matched;
        }


        public object ConvertPredicateValue(Type t, string value)
        {
            if (t == typeof(string))
                return value;
            else if (t == typeof(int))
                return int.Parse(value);
            else if (t == typeof(double))
                return double.Parse(value);
            throw new Exception("Unsupported predicate filter type: " + t.ToString());
        }

        /// <summary>
        /// Pool the cached rows.
        /// </summary>
        private readonly Utils.FixedSizeBytePool _pool = null;

        /// <summary>
        /// Cached data will identify when it is used, so the eviction mechanism
        /// can avoid removing active data.
        /// </summary>
        public List<DateTime> CacheUsage = new List<DateTime>();

        /// <summary>
        /// actual cached data.
        /// </summary>
        protected List<CachedRow> CurrentData { get; set; }

        /// <summary>
        /// Start time of the cache
        /// </summary>
        public DateTime DataStartTime { get; protected set; }

        /// <summary>
        /// End time of the cached data
        /// </summary>
        public DateTime DataEndTime { get; protected set; }

        public QueryRange Range { get { return new QueryRange(DataStartTime, DataEndTime); } private set { } }
    }
}
