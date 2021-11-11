using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Text.RegularExpressions;

namespace TimeCacheNetworkServer
{
    public class ParsingUtils
    {
        /// <summary>
        /// Looks for custom meta keyword indicating a special server command.
        /// Format should be:
        /// [{command1(,options)},{command2(,options)}(,meta_options)] actualquery
        /// </summary>
        public static Regex SpecialQueryRegex = new Regex(@"^(?<meta>^\[(?<command>{[\w\W]+?},?)+?(?<options>,[^{]+?)?\])", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Extract bucket interval and column name //  floor(extract(epoch from sample_time)/60)*60
        /// </summary>
        public static Regex TimeBucketRegex = new Regex(@"(?<bucket>time_bucket\('(?<duration>\d{1,}[^']+?)'\,(?<time_column>[^\)]+?)\))|(?<bucket>floor\(extract\(epoch from(?<time_column>[^\)]+?)\)/(?<duration_sec>\d{1,})\))", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Regular expression to pull out start and end dates from the query.
        /// 
        /// UGH - Grafana can sometimes only use 1-2 digits for millis
        /// </summary>
        public static Regex TimeFilterRegex = new Regex(@"between '(?<start_time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z)' and '(?<end_time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,3})?Z)'", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// Pull out table to use as 'key' to identify a query for stat tracking
        /// </summary>
        public static Regex QueryTagRegex = new Regex(@"FROM (?<source>[\w\W]+?)[\s\r\n]", RegexOptions.IgnoreCase | RegexOptions.Compiled);

        /// <summary>
        /// return a friendly tag name
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public static string GetQueryTag(string query)
        {
            Match m = QueryTagRegex.Match(query);
            if (m.Success)
            {
                lock (_tagSync)
                {
                    return m.Groups["source"].Value + "_" + _queryCount++.ToString("D2");
                }
            }
            lock (_tagSync)
            {
                return "unknown_" + _queryCount++.ToString("D2");
            }
        }

        /// <summary>
        /// TODO: Find a better way to tag queries with a log name.
        /// </summary>
        private static object _tagSync = new object();
        private static int _queryCount = 0;

        /// <summary>
        /// Look for  column named 'time' for bucketing
        /// </summary>
        public static Regex TimeColumnRegex = new Regex(@"[\s,](?<time_column>[\w\W]+?) as time", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        public static List<Predicate> ParsePredicates(string query)
        {
            List<Predicate> p = new List<Predicate>();

            Match m = _timeFilterRegex.Match(query);
            if (!m.Success)
                return p;

            m = _predicateRegex.Match(query, m.Index + m.Length);

            while (m.Success)
            {
                Predicate pred = new Predicate();
                pred.ColumnName = m.Groups["column_name"].Value;
                pred.Value = m.Groups["value"].Value;
                p.Add(pred);

                m = _predicateRegex.Match(query, m.Index + m.Length);
            }

            return p;
        }

        public static TimeSpan ParseInterval(string duration)
        {
            int res = 0;
            int di = -1;
            for (int i = 0; i < duration.Length; i++)
            {
                if (Char.IsDigit(duration[i]))
                    di = i;

            }
            if (di == -1)
                throw new Exception("Invalid digits");
            for (int i = 0; i <= di; i++)
                res += (int)(duration[i] - '0') * (int)Math.Pow(10, (di - i));
            if (di + 1 == duration.Length)
                throw new Exception("No format specifier");// TODO: Assume something?
            string durationType = duration.Substring(di + 1).ToLower().Trim();
            switch (durationType)
            {
                case "h":
                case "hours":
                case "hour":
                    return TimeSpan.FromHours(res);
                case "m":
                case "minutes":
                case "minute":
                    return TimeSpan.FromMinutes(res);
                case "s":
                case "second":
                case "seconds":
                    return TimeSpan.FromSeconds(res);
                case "d":
                case "days":
                case "day":
                    return TimeSpan.FromDays(res);
                case "w":
                case "week":
                case "weeks":
                    return TimeSpan.FromDays(res * 7);
                default:
                    throw new Exception("Unsupported durationType: " + durationType);

            }
        }


        public static DateTime RoundInterval(string duration, DateTime start)
        {
            TimeSpan d = ParsingUtils.ParseInterval(duration);
            return RoundInterval(d, start);
        }

        public static DateTime RoundInterval(TimeSpan duration, DateTime start)
        {
            if (duration.Ticks == 0)
                return start;

            long ticks = start.Ticks / duration.Ticks;
            return new DateTime(ticks * duration.Ticks);
        }

        public static DateTime CeilingInterval(string duration, DateTime time)
        {
            TimeSpan d = ParsingUtils.ParseInterval(duration);
            return CeilingInterval(d, time);
        }
        public static DateTime CeilingInterval(TimeSpan duration, DateTime time)
        {
            if (duration.Ticks == 0)
                return time;

            long ticks = (time.Ticks + duration.Ticks) / duration.Ticks;
            return new DateTime(ticks * duration.Ticks);
        }

        public static double ExpandNumeric(string numeric)
        {
            int res = 0;
            int di = -1;
            for (int i = 0; i < numeric.Length; i++)
            {
                if (Char.IsDigit(numeric[i]))
                    di = i;

            }
            if (di == -1)
                throw new Exception("Invalid digits");
            for (int i = 0; i <= di; i++)
                res += (int)(numeric[i] - '0') * (int)Math.Pow(10, (di - i));

            if (di + 1 == numeric.Length)
            {
                return res;
                //throw new Exception("No format specifier");// TODO: Assume something?
            }

            string nt = numeric.Substring(di + 1).ToLower().Trim();

            switch (nt)
            {
                case "k":
                    return res * 1000.0;
                case "m":
                    return res * 1000000.0;
                case "b":
                    return res * 1000000000.0;
                default:
                    throw new Exception("Unsupported numeric expander: " + nt);
            }
        }




        /// <summary>
        /// Parsed predicate
        /// </summary>
        public class Predicate
        {
            public Predicate()
            {
                ComparisonType = PredicateComparison.EQUALS;
            }

            /// <summary>
            /// Determines how filtering is performed.
            /// </summary>
            public PredicateComparison ComparisonType { get; set; }

            /// <summary>
            /// Column parsed from filter.
            /// </summary>
            public string ColumnName { get; set; }

            /// <summary>
            /// Value parsed from filter.
            /// </summary>
            public string Value { get; set; }

            public override string ToString()
            {
                return ColumnName + ComparisonType.ToString() + Value;
            }
        }

        /// <summary>
        /// Options for predicate comparisons
        /// </summary>
        public enum PredicateComparison
        {
            EQUALS,
            LIKE,
            IN
        }

        /// <summary>
        /// Identify predicates. Start with the end of the time range.
        /// This enforces the order of where predicates to be 
        /// 1) __timeFilter(time_column)
        /// 2+) remaining filter predicates.
        /// </summary>
        public static Regex _predicateRegex = new Regex(@"(?:and)\s?(?<column_name>[\w\W]+?)\s?=\s?'?(?<value>[^']+)['\s]", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        /// <summary>
        /// First identify the end time range to get position.
        /// </summary>
        public static Regex _timeFilterRegex = new Regex(@"AND '(?<end_time>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)'\s*", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        public static void Parse(string query)
        {
            //BETWEEN '2020-07-04T16:45:14.471Z' AND '2020-07-04T22:45:14.471Z'

            Regex r = new Regex(@"between '(?<start_time>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}Z)");
        }

        public static Regex OptionsRegex = new Regex(@"^(?<options>{[\w\W]+?})", RegexOptions.Compiled | RegexOptions.IgnoreCase);

    }



}
