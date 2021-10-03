using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;
using System.Text.RegularExpressions;

using PostgresqlCommunicator;

namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Contains methods for dealing with meta commands
    /// </summary>
    public static class MetaCommands
    {
        /// <summary>
        /// Identify the special query and return data points.
        /// 
        /// Most commands begin by separating the data into logical series based on a named metric column
        /// </summary>
        /// <param name="query"></param>
        /// <param name="qm"></param>
        /// <returns></returns>
        public static IEnumerable<PGMessage> HandleSpecial(SpecialQuery query, QueryManager qm)
        {
            if (string.Equals(query.Command, "regress", StringComparison.OrdinalIgnoreCase))
            {
                TimeCollection data = GetSeriesCollection(query, qm);

                // options: maxPoints, difference
                int ptok = -1;
                if (query.Options.ContainsKey("points"))
                    ptok = int.Parse(query.Options["points"]);

                bool useDiff = false;
                if (query.Options.ContainsKey("difference"))
                    useDiff = bool.Parse(query.Options["difference"]);

                List<PGMessage> ret = new List<PGMessage>();
                Match m = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(d => d.SampleTime);
                    if (ptok != -1)
                        points = points.Skip(points.Count() - ptok).Take(ptok);
                    Maths.RegressionLine line = Maths.SimpleLinearRegression.GetLine(points);

                    string reportedValue = useDiff ? Math.Round(line.End.Value - line.Start.Value, 6).ToString() : Math.Round(line.Regression.Slope, 6).ToString();
                    string textKey = "regress_" + ts.Key + "_" + ((ptok == -1) ? "" : "(" + ptok.ToString() + ")") + "_" + reportedValue;

                    // if we are timebucketing, we need to pretend the line is complete, otherwise grafana will add fill values for us
                    //  TODO: Still ugliness around end points...might need further adjustments for time_bucketing
                    if (m.Success)
                    {
                        double startvalue = line.Start.Value;

                        int i = 0;
                        double adjSlope = (line.End.Value - line.Start.Value) / points.Count();

                        foreach (DataPointDouble point in points)
                        {
                            ret.Add(Translator.BuildRowMessage(new object[] { textKey, point.SampleTime, startvalue + i++ * adjSlope }));
                        }
                    }
                    else
                    {
                        // Simple straight line
                        ret.Add(Translator.BuildRowMessage(new object[] { textKey, line.Start.SampleTime, line.Start.Value }));
                        ret.Add(Translator.BuildRowMessage(new object[] { textKey, line.End.SampleTime, line.End.Value }));
                    }
                }

                return ret;

            }
            else if (string.Equals(query.Command, "agg_bucket", StringComparison.OrdinalIgnoreCase))
            {
                TimeCollection data = GetSeriesCollection(query, qm);

                string opInt = query.Options.ContainsKey("interval") ? query.Options["interval"] : "1h";
                TimeSpan interval = ParsingUtils.ParseInterval(opInt);

                List<PGMessage> ret = new List<PGMessage>();

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    List<DataPointDouble> aggPoints = new List<DataPointDouble>();

                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime);

                    DataPointDouble start = points.First();

                    DateTime end = points.Last().SampleTime;

                    DateTime curr = start.SampleTime;
                    while (curr < end)
                    {
                        DateTime intEnd = curr.Add(interval);

                        DataPointDouble agg = new DataPointDouble() { SampleTime = curr };
                        agg.Value = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd).Average(v => v.Value);

                        // want a flat line, so add 2 points
                        aggPoints.Add(agg);
                        aggPoints.Add(new DataPointDouble() { SampleTime = intEnd, Value = agg.Value });

                        curr = curr.Add(interval);
                    }

                    foreach (DataPointDouble agp in aggPoints)
                    {
                        ret.Add(Translator.BuildRowMessage(new object[] { "agg_bucket_" + ts.Key + "_" + opInt, agp.SampleTime, agp.Value }));
                    }
                }

                return ret;
            }
            else if (string.Equals(query.Command, "stddev", StringComparison.OrdinalIgnoreCase))
            {
                // TODO: merge with 'lines'? Similar behavior
                // TODO: replace with postgresql stddev?
                int deviations = query.Options.ContainsKey("deviations") ? int.Parse(query.Options["deviations"]) : 2;

                TimeCollection series = GetSeriesCollection(query, qm);

                List<PGMessage> ret = new List<PGMessage>();
                foreach (KeyValuePair<string, TimeSeries> ts in series.SeriesData)
                {
                    // Skip for now, TODO: Log 
                    if (ts.Value.Data.Count <= 2)
                        continue;

                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime);

                    DateTime start = points.First().SampleTime;
                    DateTime end = points.Last().SampleTime;
                    double avg = points.Average(v => v.Value);
                    double std = Maths.Algorithms.StandardDeviation(avg, points);


                    double high = avg + deviations * std;
                    double low = avg - deviations * std;
                    string devKey = deviations.ToString() + "std";

                    // line 1 = avg
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, start, avg }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, end, avg }));
                    // line 2 +std
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, start, high }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, end, high }));
                    // line 3 -std
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, start, low }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, end, low }));
                }
                return ret;
            }
            else if (string.Equals(query.Command, "offset", StringComparison.OrdinalIgnoreCase))
            {
                string opInt = query.Options.ContainsKey("interval") ? query.Options["interval"] : "1h";

                TimeSpan interval = ParsingUtils.ParseInterval(opInt);

                Match m = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);
                if (!m.Success)
                    m = ParsingUtils.TimeColumnRegex.Match(query.RawQuery);
                if (!m.Success)
                {
                    qm.Error("Failed to identify time column for offset command");
                    return null;
                }
                // Only replace first instance of the time column, it may be repeated throughout the query
                string updated = query.RawQuery.Substring(0, m.Groups["time_column"].Index) +
                    m.Groups["time_column"].Value + " + interval '" + opInt + "'" +
                    query.RawQuery.Substring(m.Groups["time_column"].Index + m.Groups["time_column"].Length);
                // Next modify the where() filter
                m = ParsingUtils.TimeFilterRegex.Match(updated);
                if (!m.Success)
                {
                    qm.Error("Failed to match timefilter regex against query. Cannot perform offset command");
                    return null;
                }
                DateTime start = DateTime.Parse(m.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
                DateTime end = DateTime.Parse(m.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);

                updated = updated.Replace("'" + m.Groups["start_time"].Value + "'", "'" + m.Groups["start_time"].Value + "'::timestamptz - interval '" + opInt + "'");
                updated = updated.Replace("'" + m.Groups["end_time"].Value + "'", "'" + m.Groups["end_time"].Value + "'::timestamptz - interval '" + opInt + "'");

                qm.Trace("Updated Offset query: [" + updated + "]");
                DataTable t = qm.SimpleQuery(updated);
                TimeCollection series = GetSeriesCollection(t);
                List<PGMessage> ret = new List<PGMessage>();
                foreach (KeyValuePair<string, TimeSeries> ts in series.SeriesData)
                {
                    string key = "offset_" + ts.Key + "_" + opInt;
                    foreach (DataPointDouble point in ts.Value.Data)
                    {
                        ret.Add(Translator.BuildRowMessage(new object[] { key, point.SampleTime, point.Value }));
                    }
                }
                return ret;
            }
            else if (String.Equals(query.Command, "project", StringComparison.OrdinalIgnoreCase))
            {
                string opInt = query.Options.ContainsKey("interval") ? query.Options["interval"] : "1h";
                TimeSpan interval = ParsingUtils.ParseInterval(opInt);

                int ptok = query.Options.ContainsKey("points") ? int.Parse(query.Options["points"]) : -1;

                List<PGMessage> ret = new List<PGMessage>();

                TimeCollection series = GetSeriesCollection(query, qm);

                foreach (KeyValuePair<string, TimeSeries> ts in series.SeriesData)
                {
                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(d => d.SampleTime);
                    if (ptok != -1)
                        points = points.Skip(points.Count() - ptok).Take(ptok);

                    Maths.RegressionLine line = Maths.SimpleLinearRegression.GetLine(points);
                    string textKey = "project_" + ts.Key + "_" + ((ptok == -1) ? "" : "(" + ptok.ToString() + ")") + "_" + opInt;

                    // Take the last X points in reverse and project them into the future, adjusting the value based on our regression.
                    DataPointDouble last = points.Last();
                    DateTime start = last.SampleTime.Add(interval.Negate());
                    foreach(DataPointDouble ppoint in points.Where( c => c.SampleTime > start).Reverse())
                    {
                        // TODO: Need units for slope, dont force minutes here...
                        double minutes = (last.SampleTime - ppoint.SampleTime).TotalMinutes;
                        DateTime newTimestamp = last.SampleTime.AddMinutes(minutes);
                        double change = (minutes * line.Regression.Slope);
                        ret.Add(Translator.BuildRowMessage(new object[] { textKey, newTimestamp, ppoint.Value + change }));
                    }
                }

                    return ret;
            }
            else if(string.Equals(query.Command, "lines", StringComparison.OrdinalIgnoreCase))
            {
                TimeCollection series = GetSeriesCollection(query, qm);
                List<PGMessage> ret = new List<PGMessage>();

                if (query.Options.Count == 0 || (query.Options.ContainsKey("min") && query.Options["min"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = series.SeriesData.Values.Min(ts => ts.Data.Min(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_min", query.Start, value }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_min", query.End, value }));
                }

                if (query.Options.Count == 0 || (query.Options.ContainsKey("max") && query.Options["max"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = series.SeriesData.Values.Min(ts => ts.Data.Max(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_max", query.Start, value }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_max", query.End, value }));
                }

                if (query.Options.Count == 0 || (query.Options.ContainsKey("avg") && query.Options["avg"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = series.SeriesData.Values.Min(ts => ts.Data.Average(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_avg", query.Start, value }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_avg", query.End, value }));
                }

                if (query.Options.ContainsKey("fixed"))
                {
                    double value = ParsingUtils.ExpandNumeric(query.Options["fixed"]);
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_fixed_" + query.Options["fixed"], query.Start, value }));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_fixed_" + query.Options["fixed"], query.End, value }));
                }

                return ret;
            }


            //else if(string.Equals(query.Command, "cluster", StringComparison.OrdinalIgnoreCase))
            //{
            //    int k = query.Options.ContainsKey("k") ? int.Parse(query.Options["k"]) : 3;

            //    List<PGMessage> ret = new List<PGMessage>();
            //    TimeCollection series = GetSeriesCollection(query, qm);

            //    foreach(KeyValuePair<string, TimeSeries> ts in series.SeriesData)
            //    {
            //        List<DataPointDouble> points = ts.Value.Data.OrderBy(d => d.SampleTime).ToList();
// CLUSTER1D
            //    }
            //}

            return null;
        }

        /// <summary>
        /// Separate data into 'series' based on a 'metric'
        /// </summary>
        /// <param name="query"></param>
        /// <param name="qm"></param>
        /// <returns></returns>
        public static TimeCollection GetSeriesCollection(SpecialQuery query, QueryManager qm)
        {
            DataTable table = qm.QueryToTable(new StandardQuery() { RawQuery = query.RawQuery });

            return GetSeriesCollection(table);
        }

        public static TimeCollection GetSeriesCollection(DataTable table)
        {
            //identify columns
            int timeCol = -1;
            int valCol = -1;
            int metCol = -1;
            // TODO: Determine based on column name, not type?
            foreach(DataColumn dc in table.Columns)
            {
                if (dc.DataType == typeof(DateTime) && timeCol == -1)
                    timeCol = dc.Ordinal;
                else if (dc.DataType != typeof(string) && valCol == -1)
                    valCol = dc.Ordinal;
                else if (dc.DataType == typeof(string) && metCol == -1)
                    metCol = dc.Ordinal;
                
                if (timeCol != -1 && valCol != -1 && metCol != -1)
                    break;
            }

            return ConvertToSeriesCollection(table, timeCol, valCol, metCol);
        }

        public static TimeCollection ConvertToSeriesCollection(DataTable table, int timeIndex, int valIndex, int metIndex)
        {
            TimeCollection ret = new TimeCollection();

            foreach(DataRow dr in table.Rows)
            {
                string key = dr.ItemArray[metIndex] as string;
                if (!ret.SeriesData.ContainsKey(key))
                    ret.SeriesData[key] = new TimeSeries(key);
                ret.SeriesData[key].Data.Add(new DataPointDouble() { SampleTime = (dr.ItemArray[timeIndex] as DateTime?).Value, Value = Convert.ToDouble(dr.ItemArray[valIndex]) });
            }

            return ret;
        }

    }
}
