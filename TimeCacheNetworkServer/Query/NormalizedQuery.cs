using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Query
{
    /// <summary>
    /// A normalized query will have:
    /// - the timestamps stripped
    /// - simple 'where' predicates removed (optional - only for decomp)
    /// </summary>
    public class NormalizedQuery
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public NormalizedQuery()
        {
            RemovedPredicates = new List<PredicateGroup>();
            BucketingInterval = null;

            MetaCommands = new List<SpecialQuery>();


            Timeout = 120;
            UpdateInterval = TimeSpan.FromMinutes(1);
            CheckBucketDuration = true;
          
            UpdateWindow = TimeSpan.FromMinutes(5);
          
            AllowCache = true;
            MetaOnly = false;
            Replacements = new Dictionary<string, string>();

            Tag = null;
        }



        /// <summary>
        /// Original query - unmodified. Will include any options/meta-commands
        /// </summary>
        public string OriginalQueryText { get; set; }

        /// <summary>
        /// Query with options/meta-commands removed, and timestamps/predicates
        /// replaced with placeholders
        /// </summary>
        public string NormalizedQueryText { get; set; }

        /// <summary>
        /// Actual query to execute
        /// </summary>
        public string QueryText { get; set; }

        /// <summary>
        /// Identifier for the query
        /// </summary>
        public string QueryTag { get; set; }

        /// <summary>
        /// If bucketing is detected, this will be the interval used.
        /// </summary>
        public string BucketingInterval { get; set; }

        /// <summary>
        /// Optional - identified predicatesto allow filtering
        /// </summary>
        public List<PredicateGroup> RemovedPredicates { get; set; }

        //public DateTime AdjustedStart { get; set; }
        //public DateTime AdjustedEnd { get; set; }

        /// <summary>
        /// Identified meta-commands for evaluation
        /// </summary>
        public List<SpecialQuery> MetaCommands { get; set; }



        /// <summary>
        /// Allows identification of the query.
        /// TODO: IF the tag already exists and the query does not match, use an id
        /// </summary>
        public string Tag { get; set; }


        /// <summary>
        /// If true, retrieved data should be cached
        /// </summary>
        public bool AllowCache { get; set; }

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

        /// <summary>
        /// True if we identified timestamps
        /// </summary>
        public bool ValidTimestamps { get; set; }

        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }

        public string QueryStartTime { get; set; }
        public string QueryEndTime { get; set; }


    }

    public class PredicateGroup
    {
        public string QueryText { get; set; }
        public string Key { get; set; }
        public string Value { get; set; }
    }
}
