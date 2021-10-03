using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    public class Eviction
    {

    }

    /// <summary>
    /// Determines how cached data is removed
    /// </summary>
    public enum EvictionPolicy
    {
        /// <summary>
        /// Queries removed when last usage exceeds configured interval
        /// Default is 1 hour
        /// </summary>
        USAGE,

        /// <summary>
        /// Cached data is only removed when memory pressure exceeds
        /// configured limit
        /// </summary>
        MEMORY,

        /// <summary>
        /// Only X queries can have cached results. Oldest removed
        /// </summary>
        CAPPED
    }
}
