using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Query wrapper
    /// 
    /// Contains any configured options included with a request
    /// </summary>
    public class StandardQuery
    {
        /// <summary>
        /// Constructor - Sets default values for options.
        /// </summary>
        public StandardQuery()
        {
            Timeout = 120;
            UpdateInterval = TimeSpan.FromMinutes(1);
            CheckBucketDuration = true;
            RawQuery = String.Empty;
            UpdateWindow = TimeSpan.FromMinutes(5);
            UpdatedQuery = String.Empty;
            AllowCache = true;
            MetaOnly = false;
            Replacements = new Dictionary<string, string>();
            RemovedPredicates = null;
            Tag = null;
        }

        /// <summary>
        /// Allows identification of the query.
        /// TODO: IF the tag already exists and the query does not match, use an id
        /// </summary>
        public string Tag { get; set; }

        /// <summary>
        /// For decomposition, store any removed predicates so they can be applied later
        /// </summary>
        public List<Query.QueryUtils.PredicateGroup> RemovedPredicates { get; set; }

        /// <summary>
        /// If true, retrieved data should be cached
        /// </summary>
        public bool AllowCache { get; set; }

        /// <summary>
        /// The normalized query that will actually be executed
        /// May be modified for various reasons: cached data, replacements, decomposition..etc
        /// </summary>
        public string UpdatedQuery { get; set; }
        
        /// <summary>
        /// Underlying unmodified query
        /// </summary>
        public string RawQuery { get; set; }
        /// <summary>
        /// DB timeout passed into the Npgsql command used to retrieve the data
        /// </summary>
        public int Timeout { get; set; }
        /// <summary>
        /// Allows replacement of string values within the raw query.
        /// </summary>
        public Dictionary<string, string> Replacements { get; set; }

        /// <summary>
        /// How often the query's cached data should be updated. If our cached data falls within
        /// this interval, no query will be issued to the db and only the cached data will be returned.
        /// </summary>
        public TimeSpan UpdateInterval { get; set; }

        /// <summary>
        /// If true, we check for time_bucket/$__timeGroup usage so the query window can be padded
        /// to account for the bucketing.
        /// </summary>
        public bool CheckBucketDuration { get; set; }

        /// <summary>
        /// Determines how large of a window the update should cover. Ensures the 'fuzzy edge' data
        /// is re-queried in case the db was not fully populated at the time of the initial query.
        /// </summary>
        public TimeSpan UpdateWindow { get; set; }

        /// <summary>
        /// If true, the actual query is not evaluated, only the associated meta-commands will be executed.
        /// </summary>
        public bool MetaOnly { get; set; }
    }
}
