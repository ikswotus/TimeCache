using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Text.RegularExpressions;

namespace TimeCacheNetworkServer.Query
{
    /// <summary>
    /// Handles parsing of a query
    /// </summary>
    public class QueryParser: SLog.SLoggableObject
    {
        /// <summary>
        /// Constructor for logging
        /// </summary>
        /// <param name="log"></param>
        public QueryParser(SLog.ISLogger log) : base("QueryParser", log)
        {

        }

        /// <summary>
        /// Takes a string query, and 'normalizes' it for use with a cache.
        /// 
        /// Performs the following steps:
        /// 1) Identify any meta-commands
        /// 2) Identify any cache options
        /// 3) Normalize the query for timestamps
        /// 4) Normalize the query for predicates
        /// 5) 'Tag' the query with a unique identifier
        /// </summary>
        /// <param name="rawQuery"></param>
        /// <returns></returns>
        public NormalizedQuery Normalize(string rawQuery, string tag = null)
        {
            NormalizedQuery query = new NormalizedQuery();
            query.OriginalQueryText = rawQuery;
            query.QueryText = rawQuery;

            query.Tag = tag;
            if (String.IsNullOrEmpty(tag))
            {
                query.Tag = ParsingUtils.GetQueryTag(query.QueryText);
            }

            DateTime startTime = DateTime.UtcNow.AddHours(-6);
            DateTime endTime = DateTime.UtcNow;

            Match tm = ParsingUtils.TimeFilterRegex.Match(query.QueryText);
            if (tm.Success)
            {
                query.ValidTimestamps = true;

                startTime = DateTime.Parse(tm.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AdjustToUniversal);
                endTime = DateTime.Parse(tm.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AdjustToUniversal);

                query.StartTime = startTime;
                query.EndTime = endTime;

                query.QueryStartTime = tm.Groups["start_time"].Value;
                query.QueryEndTime = tm.Groups["end_time"].Value;

            }

            /**
             * Overall format should be:
             * [METACOMMANDS]
             * {OPTIONS}
             *  QUERY
             * 
             * Where:
             * 
             * Meta-command format
             * [{command,(params)}]
             * 
             * 
             */
            if (query.QueryText.StartsWith("["))
            {
                Match m = ParsingUtils.SpecialQueryRegex.Match(query.QueryText);
                if (m.Success)
                {
                    string meta = m.Groups["meta"].Value.Trim('[').TrimEnd(']');

                    query.QueryText = query.QueryText.Replace(m.Groups["meta"].Value, "").TrimStart();

                    string[] parts = meta.Split(new string[] { "},{" }, StringSplitOptions.None).Select(p => p.TrimStart('{').TrimEnd('}')).ToArray();

                    foreach (string part in parts)
                    {
                        string[] optionParts = part.Split(',');

                        SpecialQuery sp = new SpecialQuery(optionParts[0], query.QueryText);
                        sp.Start = startTime;
                        sp.End = endTime;

                        if (optionParts.Length > 1)
                        {
                            for (int p = 1; p < optionParts.Length; p++)
                            {
                                string[] namedOpts = optionParts[p].Split('=');
                                if (namedOpts.Length == 2)
                                {
                                    if (namedOpts[0].Equals("start", StringComparison.OrdinalIgnoreCase))
                                    {
                                        sp.Start = DateTime.Parse(namedOpts[1]);
                                    }
                                    else if (namedOpts[0].Equals("end", StringComparison.OrdinalIgnoreCase))
                                    {
                                        sp.End = DateTime.Parse(namedOpts[1]);
                                    }
                                    else
                                    {
                                        sp.Options.Add(namedOpts[0], namedOpts[1]);
                                    }
                                }
                                else
                                {
                                    Error("Skipping invalid option: " + optionParts[p]);
                                }
                            }
                        }
                        // else
                        {
                            query.MetaCommands.Add(sp);
                        }
                    }
                }
                else
                {
                    Error("Unparsed special? " + query.QueryText);
                }
            }

            /**
             * Options
             * {optionkey=optionvalue} query
             */
            if (query.QueryText.StartsWith("{"))
            {
                // Options
                Match optMatch = ParsingUtils.OptionsRegex.Match(query.QueryText);

                if (optMatch.Success)
                {
                    // comma separated key=value pairs
                    string[] values = optMatch.Groups["options"].Value.TrimStart('{').TrimEnd('}').Split(',');

                    foreach (string v in values)
                    {
                        string[] namedOpts = v.Split('=');
                        if (namedOpts.Length != 2)
                        {
                            Error("Skipping invalid option: " + v);
                            continue;
                        }
                        try
                        {
                            switch (namedOpts[0].ToLower())
                            {
                                case "allowcache":
                                case "cache":
                                    query.AllowCache = bool.Parse(namedOpts[1]);
                                    break;
                                case "decomp":
                                case "decompose":
                                case "allowdecomp":
                                    query.AllowDecomposition = bool.Parse(namedOpts[1]);
                                    break;
                                case "timeout":
                                    query.Timeout = int.Parse(namedOpts[1]);
                                    break;
                                case "updateinterval":
                                    query.UpdateInterval = TimeSpan.Parse(namedOpts[1]);
                                    break;
                                case "checkbucket":
                                case "checkbucketduration":
                                    query.CheckBucketDuration = bool.Parse(namedOpts[1]);
                                    break;
                                case "metaonly":
                                case "metadataonly":
                                    query.ReturnMetaOnly = bool.Parse(namedOpts[1]);
                                    query.ExecuteMetaOnly = query.ReturnMetaOnly;
                                    break;
                                
                                case "replace":
                                    // expect 2 'values' comma-separated
                                    string[] vals = namedOpts[1].Split(',');
                                    if (vals.Length != 2)
                                    {
                                        Error("Invalid replacement option: " + namedOpts[1]);
                                        break;
                                    }
                                    query.Replacements[vals[0]] = vals[1];
                                    break;
                                case "tag":
                                    query.Tag = namedOpts[1];
                                    break;
                                default:
                                    Error("Unknown option: " + namedOpts[0]);
                                    break;
                            }
                        }
                        catch (Exception exc)
                        {
                            Error("Failure handing option: " + v + ", " + exc);
                        }
                    }
                    query.QueryText = query.QueryText.Replace(optMatch.Groups["options"].Value, "");
                }
                else
                {
                    Error("Unparsed options: " + query.QueryText);
                }

            }


            // Normalize the query timestamps
            if (tm.Success)
            {
                query.NormalizedQueryText = query.QueryText.Replace(tm.Groups["start_time"].Value, TimePlaceholderStart).Replace(tm.Groups["end_time"].Value, TimePlaceholderEnd);
            }

            // Normalize the query predicates
            if (query.AllowDecomposition)
            {
                Match wm = WhereRegex.Match(query.NormalizedQueryText);
                if (wm.Success)
                {
                    int start = wm.Groups[0].Index + wm.Groups[0].Length;
                    if (start < rawQuery.Length)
                    {
                        int gb = rawQuery.IndexOf("group by");
                        if (gb > 0 && gb < rawQuery.Length)
                        {
                            wm = PredicateRegex.Match(query.NormalizedQueryText, start);
                            while (wm.Success)
                            {
                                PredicateGroup pg = new PredicateGroup();
                                pg.QueryText = wm.Groups["predicate"].Value;
                                pg.Key = wm.Groups["key"].Value;
                                pg.Value = wm.Groups["value"].Value.Trim('\'');

                                query.RemovedPredicates.Add(pg);

                                start = wm.Index + wm.Length;
                                if (start >= gb)
                                    break;
                                wm = PredicateRegex.Match(query.NormalizedQueryText, start);
                            }
                        }
                    }
                }

                foreach (PredicateGroup pg in query.RemovedPredicates)
                {
                    query.QueryText = query.QueryText.Replace(pg.QueryText, "");
                }
            }

            Match bucketMatch = ParsingUtils.TimeBucketRegex.Match(query.OriginalQueryText);
            if (bucketMatch.Success)
            {
                if(bucketMatch.Groups["duration"].Success)
                    query.BucketingInterval = bucketMatch.Groups["duration"].Value;
                if (bucketMatch.Groups["duration_sec"].Success)
                    query.BucketingInterval = bucketMatch.Groups["duration_sec"].Value + "s";
            }


            return query;
        }

        /// <summary>
        /// Static method for normalization
        /// </summary>
        /// <param name="rawQuery"></param>
        /// <returns></returns>
        public static NormalizedQuery NormalizeQuery(string rawQuery)
        {
            QueryParser qp = new QueryParser(SLog.EmptySLogger.Instance);
            return qp.Normalize(rawQuery);
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
        /// "yyyy-MM-ddTHH:mm:ss.fffZ"
        /// </summary>
        public const string TimestampToStringFormat = "yyyy-MM-ddTHH:mm:ss.fffZ";

        /// <summary>
        /// "###START###"
        /// </summary>
        public const string TimePlaceholderStart = "###START###";

        /// <summary>
        /// "###END###"
        /// </summary>
        public const string TimePlaceholderEnd = "###END###";

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

    }
}
