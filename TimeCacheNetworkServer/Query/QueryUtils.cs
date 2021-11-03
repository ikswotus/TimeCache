using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Text.RegularExpressions;

namespace TimeCacheNetworkServer.Query
{
    public static class QueryUtils
    {
        /// <summary>
        /// Does not look at timegroupings/time columns.
        /// </summary>
        /// <param name="rawQuery"></param>
        /// <param name="allowDecomp">If true, simple where predicates are also removed in the normalization process</param>
        /// <returns></returns>
        public static NormalizedQuery NormalizeWherePredicate(string rawQuery)
        {
            NormalizedQuery nq = new NormalizedQuery();
            nq.OriginalQueryText = rawQuery;
            nq.QueryText = rawQuery;

            Match m = WhereRegex.Match(rawQuery);
            if (m.Success)
            {
                int start = m.Groups[0].Index + m.Groups[0].Length;
                if (start < rawQuery.Length)
                {
                    int gb = rawQuery.IndexOf("group by");
                    if (gb > 0 && gb < rawQuery.Length)
                    {
                        m = PredicateRegex.Match(rawQuery, start);
                        while (m.Success)
                        {
                            PredicateGroup pg = new PredicateGroup();
                            pg.QueryText = m.Groups["predicate"].Value;
                            pg.Key = m.Groups["key"].Value;
                            pg.Value = m.Groups["value"].Value.Trim('\'');

                            nq.RemovedPredicates.Add(pg);

                            start = m.Index + m.Length;
                            if (start >= gb)
                                break;
                            m = PredicateRegex.Match(rawQuery, start);
                        }
                    }
                }
            }
            foreach(PredicateGroup pg in nq.RemovedPredicates)
            {
                nq.QueryText = nq.QueryText.Replace(pg.QueryText, "");
            }

            return nq;
        }

        /// <summary>
        /// Format a query so it is single-spaced. Newlines and tabs are converted to spaces.
        /// 
        /// TODO: This can alter queries with commented out lines...
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public static string NormalizeWhitespace(string query)
        {
            // First replace with a space
            string replaced = NewLineTabs.Replace(query, " ");
            // Remove any duplicate spaces
            replaced = MultiSpace.Replace(replaced, " ");
            
            return replaced;
        }

        /// <summary>
        /// Look for multiple spaces in a row.
        /// </summary>
        public static Regex MultiSpace = new Regex(@"(\s{2,})", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Look for tabs/new lines/returns
        /// </summary>
        public static Regex NewLineTabs = new Regex(@"[\r\n\t]+?", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Locate the 'where' filter to extract timestamp
        /// </summary>
        public static Regex WhereRegex = new Regex(@"where[\s(\n|\r|\r\n)]+(?<time_col>[\w\W]+?) BETWEEN '[\w\W]+?' AND '[\w\W]+?'[\s(\n|\r|\r\n)]+", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Identify predicate groupings
        /// </summary>
        public static Regex PredicateRegex = new Regex(@"(?<predicate>AND (?<key>[\w\W]+?) = (?<value>[\w\W]+?)[\s(\n|\r|\r\n)]+)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        
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
            }
            /// <summary>
            /// Original query
            /// </summary>
            public string OriginalQueryText { get; set; }

            /// <summary>
            /// Normalized query
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

            public DateTime AdjustedStart { get; set; }
            public DateTime AdjustedEnd { get; set; }

        }

        public class PredicateGroup
        {
            public string QueryText { get; set; }
            public string Key { get; set; }
            public string Value { get; set; }
        }
    }
}
