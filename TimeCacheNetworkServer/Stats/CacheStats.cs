using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Stats
{
    /// <summary>
    /// Track stats on cache.
    /// 
    /// Things to consider:
    /// 
    /// How much space is used (Rows/Memory)
    /// -> # of rows, Width(start/end timestamps)
    /// How frequently the query is refreshed(bg)/accessed
    /// -> Last access, Last Refress + counts(track by time)
    /// How long refresh/full query takes
    /// --> Tricky if we only query 'edges' 
    /// 
    /// How long caching lookups take.
    /// 
    /// </summary>
    public class CacheStats
    {
        /// <summary>
        /// Last time the query was refreshed.
        /// This could be:
        /// 1) The initial query timestamp
        /// 2) An update for edge data (either automatic/background, or requested by a new query)
        /// </summary>
        public DateTime LastRefreshTime { get; set; }

        /// <summary>
        /// Track all queries that hit this 
        /// </summary>
        public List<DateTime> QueryActivity { get; set; }

        
    }
}
