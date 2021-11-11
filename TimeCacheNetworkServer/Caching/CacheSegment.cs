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

        public bool OverlapsOrdered(QueryRange range)
        {
            return Contains(range) || OverlapsStart(range);
        }

        private bool Enveloped(QueryRange range)
        {
            return (DataStartTime >= range.StartTime && DataEndTime <= range.EndTime);
        }

        private bool Contains(QueryRange range)
        {
            return (DataStartTime <= range.StartTime && range.EndTime <= DataEndTime);
        }
        private bool OverlapsStart(QueryRange range)
        {
            return (DataStartTime <= range.StartTime && range.StartTime <= DataEndTime);
        }
        private bool OverlapsEnd(QueryRange range)
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
            if (mergeTarget.CurrentData.Count == 0 || mergeTarget.DataStartTime == DateTime.MinValue || mergeTarget.DataEndTime == DateTime.MinValue)
                throw new Exception("Invalid cache segment to merge into");

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
                for (int i = CurrentData.Count() - 1; i >= 0; i--)
                {
                    if (CurrentData[i].RawDate <= mergeTarget.DataStartTime)
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
            DateTime start = PostgresqlCommunicator.Translator.GetDateTime(timeType, table.Rows[0], timeIndex).ToUniversalTime();

            //if (queryStart < DataStartTime)
            //{
            //    DateTime qend = PostgresqlCommunicator.Translator.GetDateTime(timeType, table.Rows[table.Rows.Count - 1], timeIndex).ToUniversalTime();
            //    VTrace("Existing query cache end: " + qend.ToString("O"));
            //    while (CurrentData.Count > 0)
            //    {
            //        if (qend > CurrentData[0].RawDate)
            //        {
            //            Debug("Removing overlap date: " + CurrentData[0].RawDate.ToString("o"));
            //            CurrentData.RemoveAt(0);
            //        }
            //        else
            //        {
            //            break;
            //        }
            //    }
            //}
            //if (queryEnd > DataEndTime)
            //{
            //    VTrace("query end exceed cache end time - merging results to end");
            //    for (int i = CurrentData.Count() - 1; i >= 0; i--)
            //    {
            //        if (start > CurrentData[i].RawDate)
            //        {
            //            break;
            //        }

            //        // else overlap - remove the old data
            //        CachedRow cr = CurrentData[i];
            //        CurrentData.RemoveAt(i);
            //        cr.TranslatedMessage.Release();
            //        Debug("Removing overlap date: " + start.ToString("o"));
            //    }

            //}

            //TrimStart(queryStart);

            // Store raw data.
           // List<CachedRow> rows = new List<CachedRow>(table.Rows.Count);
            foreach (DataRow dr in table.Rows)
            {
                CachedRow cd = new CachedRow();
                cd.Objects = dr.ItemArray;
                cd.RawDate = PostgresqlCommunicator.Translator.GetDateTime(timeType, dr, timeIndex);
                cd.TranslatedMessage = PostgresqlCommunicator.Translator.BuildRowMessage(dr);
                cd.TranslatedMessage.Set(_pool);

                CurrentData.Add(cd);
            }

            //if (queryStart < DataStartTime)
            //    CurrentData.InsertRange(0, rows);
            //else
            //    CurrentData.AddRange(rows);

            // Update stored dates to reflect added values
            //if (queryStart < DataStartTime)
                DataStartTime = queryStart;

            //if (queryEnd > DataEndTime)
                DataEndTime = queryEnd;
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
