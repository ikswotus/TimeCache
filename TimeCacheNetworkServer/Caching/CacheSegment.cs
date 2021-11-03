using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
        public CacheSegment(string key, SLog.SLogger logger)
            : base(key, logger)
        {
            CurrentData = new List<CachedRow>();
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

        private bool Enveloped(QueryRange range)
        {
            return (StartTime >= range.StartTime && EndTime <= range.EndTime);
        }

        private bool Contains(QueryRange range)
        {
            return (StartTime <= range.StartTime && range.EndTime <= EndTime);
        }
        private bool OverlapsStart(QueryRange range)
        {
            return (StartTime >= range.StartTime && StartTime <= range.EndTime);
        }
        private bool OverlapsEnd(QueryRange range)
        {
            return (EndTime >= range.StartTime && EndTime <= range.EndTime);
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
                QueryRange before = new QueryRange();
                before.StartTime = qr.StartTime;
                before.EndTime = GetEnd(StartTime, bucketInterval, updateInterval);

                QueryRange after = new QueryRange();
                after.StartTime = GetStart(EndTime, bucketInterval, updateInterval);
                after.EndTime = qr.EndTime;

                return new List<QueryRange> { before, after };
            }
            else if (OverlapsStart(qr))
            {
                //------[DS---cached data---DE]-------
                //                    S-------*--E
                // Need to get data from CacheEnd to QueryEnd
                QueryRange after = new QueryRange();
                after.StartTime = GetStart(EndTime, bucketInterval, updateInterval);
                after.EndTime = qr.EndTime;
                return new List<QueryRange> { after };
            }
            else if (OverlapsEnd(qr))
            {
                //------[DS---cached data---DE]------ -
                // S----*---E
                // Need to get data from QueryStart to CacheStart
                QueryRange before = new QueryRange();
                before.StartTime = qr.StartTime;
                before.EndTime = GetEnd(StartTime, bucketInterval, updateInterval);
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
            DateTime updated = start.Subtract(bucket);
            return ParsingUtils.RoundInterval(bucket, updated);
        }
        private DateTime GetEnd(DateTime end, TimeSpan bucket, TimeSpan update)
        {
            DateTime updated = end.Add(bucket);
            return ParsingUtils.CeilingInterval(bucket, updated);
        }

        /// <summary>
        /// Removes any cached data earlier than @start.
        /// StartTime will be adjusted to our new ealiest time.
        /// </summary>
        /// <param name="start"></param>
        /// <returns>Number of rows removed.</returns>
        public int TrimStart(DateTime start)
        {
            Debug("Trimming data from start: " + start.ToString("O"));
            int c = CurrentData.Count();
            while (CurrentData.Count > 0 && CurrentData[0].RawDate < start)
            {
                CurrentData.RemoveAt(0);
            }
            int removed = c - CurrentData.Count();
            Debug("TrimStart removed " + removed + " rows, adjusted start is now " + StartTime.ToString("O"));

            StartTime = CurrentData[0].RawDate;

            return removed;
        }

        /// <summary>
        /// Cached data will identify when it is used, so the eviction mechanism
        /// can avoid removing active data.
        /// </summary>
        public List<DateTime> CacheUsage = new List<DateTime>();

        /// <summary>
        /// actual cached data.
        /// </summary>
        public List<CachedRow> CurrentData { get; set; }

        /// <summary>
        /// Start time of the cache
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// End time of the cached data
        /// </summary>
        public DateTime EndTime { get; set; }
    }
}
