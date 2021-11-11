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
        public SegmentManager(Query.IQuerier querier, SLog.SLogger logger)
            : base("SegmentManager", logger)
        {
            _querier = querier;
        }

        public IEnumerable<PostgresqlCommunicator.PGMessage> Get(Query.NormalizedQuery query, bool inclusiveEnd = true)
        {

            List<QueryRange> needed = GetMissingRanges(new QueryRange(query.StartTime, query.EndTime), query.GetBucketTime(), query.UpdateWindow);
            
            if(needed.Count > 0)
            {
                // TODO: query new ranges + Merge/Create segments as needed
                Trace("Found: {0} missing ranges", needed.Count);
                foreach(QueryRange qr in needed)
                {
                    Trace("Missing: {0} -> {1}", qr.StartTime.ToString("O"), qr.EndTime.ToString("O"));

                    
                }
                foreach (QueryRange need in needed)
                {
                    SetResults(_querier.CachedQuery(query, need), need.StartTime, need.EndTime);
                }
                
            }
            else
            {
                Trace("Query Range: {0}->{1} was fully cached", query.StartTime.ToString("O"), query.EndTime.ToString("O"));
            }

            // Build list of data from existing segments


            return null;
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
}
