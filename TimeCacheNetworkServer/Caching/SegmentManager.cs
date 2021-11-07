using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// interface to cached segments.
    /// 
    /// Segmentation is hidden from caller
    /// 
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
            List<QueryRange> needed = GetRanges(new QueryRange() { StartTime = query.StartTime, EndTime = query.EndTime });
            
            if(needed.Count > 0)
            {
                // TODO: query new ranges + Merge/Create segments as needed
            }

            // Build list of data from existing segments


            return null;
        }

        private List<QueryRange> GetRanges(QueryRange requested)
        {
            List<QueryRange> needed = new List<QueryRange> { requested };

            for (int i = 0; i < _segments.Count; i++)
            {
                List<QueryRange> newRanges = new List<QueryRange>();
                for(int j =0; j< needed.Count; j++)
                {
                    newRanges.AddRange(_segments[i].Intersect(needed[j]));
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
        /// Merges two adjacent segments
        /// </summary>
        /// <param name="first"></param>
        /// <param name="second"></param>
        /// <returns></returns>
        private CacheSegment JoinSegments(CacheSegment first, CacheSegment second)
        {
            return first;
        }

        /// <summary>
        /// Current segments
        /// </summary>
        private List<CacheSegment> _segments = new List<CacheSegment>(1);
    }
}
