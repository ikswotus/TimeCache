using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// interface to cached segments.
    /// 
    /// Segmentation is hidden from caller
    /// 
    /// 
    /// TODO: 
    /// Rules for dropping/merging segements
    /// </summary>
    public class SegmentManager : SLog.SLoggableObject
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="querier"></param>
        /// <param name="logger"></param>
        /// <param name="tag">Friendly tag to identify query</param>
        public SegmentManager(Query.IQuerier querier, SLog.ISLogger logger, string tag)
            : base("SegmentManager", logger)
        {
            _tag = tag;
            _querier = querier;
        }

        private readonly string _tag;

        /// <summary>
        /// Cached data will identify when it is used, so the eviction mechanism
        /// can avoid removing active data.
        /// </summary>
        public List<DateTime> CacheUsage = new List<DateTime>();

        /// <summary>
        /// Remove all cached rows
        /// </summary>
        public void Clear()
        {
            foreach(CacheSegment segment in _segments)
            {
                segment.Clear();
                CacheUsage.Clear();
            }
            _segments.Clear();
        }

        /// <summary>
        /// Return data in the form of a DataTable object
        /// </summary>
        /// <param name="query">Our source query</param>
        /// <param name="inclusiveEnd">True if end timestamp should be included</param>
        /// <returns>Results of query</returns>
        /// <exception cref="Exception"></exception>
        public DataTable GetTable(Query.NormalizedQuery query, bool inclusiveEnd = true)
        {

            QueryRange normalRange = query.GetRange();

            CacheSegment cs = UpdateQuery(query, normalRange);

            if (DescriptorMessage == null)
                throw new Exception("No data - Update() must be called");

            DataTable t = new DataTable();
            for (int i = 0; i < DescriptorMessage.Fields.Count; i++)
            {
                t.Columns.Add(new DataColumn(DescriptorMessage.Fields[i].ColumnName, DescriptorMessage.OriginalTypes[i]));
            }
            foreach (CachedRow cr in cs.GetRows(DescriptorMessage, normalRange.StartTime, normalRange.EndTime, query.RemovedPredicates, inclusiveEnd))
            {
                t.Rows.Add(cr.Objects);
            }

            return t;
        }

        /// <summary>
        /// Retrieves cached data
        /// </summary>
        /// <param name="query"></param>
        /// <param name="inclusiveEnd"></param>
        /// <returns></returns>
        public IEnumerable<PostgresqlCommunicator.PGMessage> Get(Query.NormalizedQuery query, bool inclusiveEnd = true)
        {
            QueryRange normalRange = query.GetRange();

            CacheSegment cs = UpdateQuery(query, normalRange);

            return cs.Get(DescriptorMessage, normalRange.StartTime, normalRange.EndTime, query.RemovedPredicates, inclusiveEnd);
        }

        /// <summary>
        /// Updates/Merges segments and returns the segment that CONTAINS the full range of data.
        /// </summary>
        /// <param name="query"></param>
        /// <param name="normalRange"></param>
        /// <returns></returns>
        public CacheSegment UpdateQuery(Query.NormalizedQuery query, QueryRange normalRange)
        {
            CacheUsage.Add(DateTime.UtcNow);
            CacheUsage.RemoveAll(d => d < DateTime.UtcNow.AddHours(-1));

            List<QueryRange> needed = GetMissingRanges(normalRange, query.GetBucketTime(), query.UpdateWindow);

            if (needed.Count > 0)
            {
                Trace("Found: {0} missing ranges", needed.Count);
                foreach (QueryRange qr in needed)
                {
                    Trace("Missing: {0} -> {1}", qr.StartTime.ToString("O"), qr.EndTime.ToString("O"));
                }
                foreach (QueryRange need in needed)
                {
                    SetResults(_querier.CachedQuery(query, need), need.StartTime, need.EndTime);
                }

                // Ensure we have minimal segmenation
                MergeSegments();
            }
            else
            {
                Trace("Query Range: {0}->{1} was fully cached", query.StartTime.ToString("O"), query.EndTime.ToString("O"));
            }

            // Build list of data from existing segments
            CacheSegment cs = _segments.FirstOrDefault(f => f.Contains(normalRange));
            if (cs == null)
            {
                // This shouldn't happen unless we don't merge/query properly
                throw new Exception("Failed to find valid segement for range");
            }
            return cs;
        }

        /// <summary>
        /// Retrieve any gaps in @requested that is not currently cached.
        /// </summary>
        /// <param name="requested">Timespan of data to check for cached</param>
        /// <returns>0+ ranges</returns>
        private List<QueryRange> GetMissingRanges(QueryRange requested, TimeSpan bucketTime, TimeSpan updateTime)
        {
            List<QueryRange> needed = new List<QueryRange> { requested };

            for (int i = 0; i < _segments.Count; i++)
            {
                List<QueryRange> newRanges = new List<QueryRange>();
                for(int j =0; j< needed.Count; j++)
                {
                    newRanges.AddRange(_segments[i].Intersect(needed[j], bucketTime, updateTime));
                }
                //swap
                needed = newRanges;
                // Shortcut - if we fully cached everything, just return
                if (needed.Count == 0)
                    return needed;

               
            }

            return needed;
        }


        /// <summary>
        /// Object to manage actual query of new data
        /// </summary>
        private Query.IQuerier _querier;

      
        /// <summary>
        /// Checks to see if any segments overlap and can be merged
        /// </summary>
        public void MergeSegments()
        {
            if (_segments.Count < 2)
                return;

            List<CacheSegment> ordered = _segments.OrderBy(r => r.DataStartTime).ToList();
            for(int i = ordered.Count - 1; i > 0; i--)
            {
                if (ordered[i - 1].OverlapsOrdered(ordered[i].Range))
                {
                    Trace("Merging segments");
                    CacheSegment remove = ordered[i];
                    ordered.RemoveAt(i);
                    ordered[i - 1].MergeSegments(remove);
                }
            }


            _segments = ordered;
        }

        private void SetResults(DataTable table, DateTime queryStart, DateTime queryEnd, int timeIndex = -1)
        {
            // Set this first, even with no results..
            if (DescriptorMessage == null)
            {
                DescriptorMessage = PostgresqlCommunicator.Translator.BuildRowDescription(table);

                // Find our time column
                for (int i = 0; i < DescriptorMessage.Fields.Count; i++)
                {
                    if (DescriptorMessage.Fields[i].ColumnName == "time\0" ||
                        DescriptorMessage.Fields[i].ColumnName == "time")
                    {
                        _timeIndex = i;
                        break;
                    }
                }
            }
            if (_timeIndex == -1 && timeIndex != -1)
                _timeIndex = timeIndex;
            
            if(table.Rows.Count == 0)
            {
                Debug("0 row count?");
                return;
            }

            if (_timeIndex == -1)
                throw new Exception("Unable to identify time index");

            if (_pool == null)
            {
                // Need to know the actual size to be able to construct the pool.
                PostgresqlCommunicator.DataRowMessage row = PostgresqlCommunicator.Translator.BuildRowMessage(table.Rows[0]);
                

                _pool = new Utils.FixedSizeBytePool(row.GetCompletedSize());
            }

            // TODO: Best way to handle merging data
            // We could check first  for overlap and add data to an existing cache
            // but we still need to check to see if adding data causes any other caches to now overlap...
            // For now we will just do 1 loop at the end

            CacheSegment cs = new CacheSegment("CacheSegment", _logger, _pool);
            Trace("Adding cache segement for: {0}->{1}", queryStart.ToString("O"), queryEnd.ToString("O"));
            cs.AddData(table, queryStart, queryEnd, _timeIndex);

            _segments.Add(cs);

            
        }

        public List<SegmentSummary> GetSegmentSummaries()
        {
            return _segments.Select(s => new SegmentSummary(_tag, s.DataStartTime, s.DataEndTime, s.Count())).ToList();
        }

        /// <summary>
        /// Pool the cached rows.
        /// </summary>
        private Utils.FixedSizeBytePool _pool = null;

        /// <summary>
        /// Identify the 'time' column so we can pull out the date to cache.
        /// </summary>
        private int _timeIndex = -1;

        /// <summary>
        /// Header message identifying columns + types
        /// </summary>
        public PostgresqlCommunicator.RowDescription DescriptorMessage { get; set; }

        /// <summary>
        /// Current segments
        /// </summary>
        private List<CacheSegment> _segments = new List<CacheSegment>(1);
    }


    public class SegmentSummary
    {
        public SegmentSummary(string tag, DateTime start, DateTime end, long count)
        {
            Tag = tag;
            Start = start;
            End = end;
            Count = count;
        }

        public long Count { get; private set; }
        public DateTime End { get; private set; }
        public DateTime Start { get; private set; }
        public string Tag { get; private set; }

        public List<SegmentPoint> ToPoints(string tag)
        {
            return new List<SegmentPoint>(){new  SegmentPoint() {  Count = this.Count, Tag = tag, Timestamp = this.Start},
                 new SegmentPoint() {  Count = this.Count, Tag = tag, Timestamp = this.End} };
        }
    }

    public class SegmentPoint
    {
        public string Tag { get; set; }
        public long Count { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
