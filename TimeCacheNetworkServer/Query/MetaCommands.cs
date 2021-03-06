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
        public static IEnumerable<PGMessage> HandleSpecial(SpecialQuery query, QueryManager qm, Query.NormalizedQuery sourceQuery)
        {
            List<PGMessage> ret = new List<PGMessage>();

            // Special Case - avoid GetSeriesCollection()
            if (string.Equals(query.Command, "cache_segments", StringComparison.OrdinalIgnoreCase))
            {
                /**
                 * Retrieves an overview of the current cached segments.
                 * For each segment, a line is returned from DataStart to DataEnd.
                 * The height (value) of the line will be the # of rows within the segment.
                 * 
                 * For now, all segments will be returned. Additional filtering by tag/source
                 * would be useful at some point...
                 */
                List<Caching.SegmentSummary> segments = qm.GetSegmentSummaries();

                bool separate = false;
                if (query.Options.ContainsKey("separate"))
                    separate = bool.Parse(query.Options["separate"]);

                if (sourceQuery.ExecuteMetaOnly && sourceQuery.ReturnMetaOnly)
                {
                    List<Translator.NamedColumns> cols = new List<Translator.NamedColumns>();
                    cols.Add(new Translator.NamedColumns() { Name = "tag", ValueType = typeof(string) });
                    cols.Add(new Translator.NamedColumns() { Name = "time", ValueType = typeof(DateTime) });
                    cols.Add(new Translator.NamedColumns() { Name = "value", ValueType = typeof(Int64) });
                    RowDescription rd = Translator.BuildRowDescription(cols);
                    ret.Add(rd);
                }


                // This may result in 'data out of range' messages in grafana
                // We could strictly limit to c.start >= query.Start && c.end <= query.End
                // however this will remove/hide segments that cover 99% of the window if we're off by even a tiny amount...
                // Ideally cache_segments will be used as ExecuteMetaOnly && ReturnMetaOnly above...otherwise
                // assume we'll want to see overlapping segments on the chart
                int seg = 0;
                List<Caching.SegmentPoint> points = new List<Caching.SegmentPoint>();
                foreach (Caching.SegmentSummary cs in segments.Where(c => UtilityMethods.Between(query.Start, query.End, c.Start) || UtilityMethods.Between(query.Start, query.End, c.End)).OrderBy(c => c.Start))
                {
                    string t = cs.Tag;
                    if (separate)
                        t += "_" + seg++.ToString();
                    points.AddRange(cs.ToPoints(t));
                    
                }
                foreach(Caching.SegmentPoint sp in points.OrderBy(p => p.Timestamp))
                {
                    ret.Add(Translator.BuildRowMessage(new object[] { sp.Tag, sp.Timestamp, sp.Count }));
                }
                return ret;

            }

            TimeCollection data = GetSeriesCollection(query, qm);

            // TODO: Verify ALL meta-commands only return these columns?
            if (sourceQuery.ExecuteMetaOnly && sourceQuery.ReturnMetaOnly)
            {
                ret.Add(data.DescriptorMessage);
            }


            if (string.Equals(query.Command, "regress", StringComparison.OrdinalIgnoreCase))
            {
                // options: maxPoints, difference
                int ptok = -1;
                if (query.Options.ContainsKey("points"))
                    ptok = int.Parse(query.Options["points"]);

                bool useDiff = false;
                if (query.Options.ContainsKey("difference"))
                    useDiff = bool.Parse(query.Options["difference"]);

               
                Match m = ParsingUtils.TimeBucketRegex.Match(query.RawQuery);

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(d => d.SampleTime);
                    if (ptok != -1)
                        points = points.Skip(points.Count() - ptok).Take(ptok);
                    Maths.RegressionLine line = Maths.SimpleLinearRegression.GetLine(points);

                    string reportedValue = useDiff ? Math.Round(line.End.Value - line.Start.Value, 6).ToString() : Math.Round(line.Regression.Slope, 6).ToString();
                    string textKey = "regress_" + ts.Key + "_" + ((ptok == -1) ? "*" : "(" + ptok.ToString() + ")") + "_" + reportedValue + (useDiff ? "(d)" : "(s)");

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
              

                string opInt = query.Options.ContainsKey("interval") ? query.Options["interval"] : "1h";
                TimeSpan interval = ParsingUtils.ParseInterval(opInt);

                bool separate = query.Options.ContainsKey("separate") ? bool.Parse(query.Options["separate"]) : false;

                string method = query.Options.ContainsKey("method") ? query.Options["method"] : "avg";

                AggMethod am = AggMethod.AVG;
                if(!Enum.TryParse(method.ToUpper(), out am))
                {
                    qm.Error("Failed to parse aggregate method, provided: " + method);
                }

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime);

                    DataPointDouble start = points.First();

                    DateTime end = points.Last().SampleTime;

                    DateTime curr = start.SampleTime;

                    if (separate)
                    {
                        int aggBucket = 0;
                        while (curr < end)
                        {
                            DateTime intEnd = curr.Add(interval);

                            double value = 0.0;

                            switch (am)
                            {
                                case AggMethod.AVG:
                                    value = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd).Average(v => v.Value);
                                    break;
                                case AggMethod.SUM:
                                    value = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd).Sum(v => v.Value);
                                    break;
                                default:
                                    qm.Error("Unsupported AggMethod: " + am.ToString());
                                    break;
                            }
                           

                            ret.Add(Translator.BuildRowMessage(new object[] { "agg_bucket_" + method.ToString() + "_" + ts.Key + "_" + aggBucket.ToString("D2") + "_" + opInt, curr, value }));
                            ret.Add(Translator.BuildRowMessage(new object[] { "agg_bucket_" + method.ToString() + "_" + ts.Key + "_" + aggBucket.ToString("D2") + "_" + opInt, intEnd, value }));

                            aggBucket++;
                            curr = curr.Add(interval);
                        }
                    }
                    else
                    {
                        List<DataPointDouble> aggPoints = new List<DataPointDouble>();
                        while (curr < end)
                        {
                            DateTime intEnd = curr.Add(interval);

                            DataPointDouble agg = new DataPointDouble() { SampleTime = curr };

                            agg.Value = 0.0;

                            switch (am)
                            {
                                case AggMethod.AVG:
                                    agg.Value = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd).Average(v => v.Value);
                                    break;
                                case AggMethod.SUM:
                                    agg.Value = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd).Sum(v => v.Value);
                                    break;
                                default:
                                    qm.Error("Unsupported AggMethod: " + am.ToString());
                                    break;
                            }
                           

                            // want a flat line, so add 2 points
                            aggPoints.Add(agg);
                            aggPoints.Add(new DataPointDouble() { SampleTime = intEnd, Value = agg.Value });

                            curr = curr.Add(interval);
                        }

                        foreach (DataPointDouble agp in aggPoints)
                        {
                            ret.Add(Translator.BuildRowMessage(new object[] { "agg_bucket_" + method.ToString() + "_" + ts.Key + "_" + opInt, agp.SampleTime, agp.Value }));
                        }
                    }
                }

                return ret;
            }
            else if (string.Equals(query.Command, "stddev", StringComparison.OrdinalIgnoreCase))
            {
                // TODO: merge with 'lines'? Similar behavior
                // TODO: replace with postgresql built-in stddev function?
                int deviations = query.Options.ContainsKey("deviations") ? int.Parse(query.Options["deviations"]) : 2;

                bool highlighOutliers = query.Options.ContainsKey("highlight") ? bool.Parse(query.Options["highlight"]) : false;

                bool useInterval = query.Options.ContainsKey("interval"); // Rolling STDDEV

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    // Skip for now, TODO: Log 
                    if (ts.Value.Data.Count <= 2)
                        continue;

                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime);

                    DateTime start = points.First().SampleTime;
                    DateTime end = points.Last().SampleTime;

                    TimeSpan interval = query.Options.ContainsKey("interval") ? ParsingUtils.ParseInterval(query.Options["interval"])
                        : (end - start);

                    DataPointDouble firstPoint = points.First();

                    DateTime curr = start;

                    while (curr < end)
                    {
                        DateTime intEnd = curr.Add(interval);
                        IEnumerable<DataPointDouble> inRange = points.Where(p => p.SampleTime >= curr && p.SampleTime < intEnd);

                        if (!inRange.Any()) //TODO:  count < 2?
                            continue;

                        double avg = inRange.Average(v => v.Value);
                        double std = Maths.Algorithms.StandardDeviation(avg, inRange);


                        double high = avg + deviations * std;
                        double low = avg - deviations * std;

                        if (highlighOutliers)
                        {
                            // Collect all points outside:
                            string k = "stddev_outlier_" + ts.Key;
                            foreach (DataPointDouble dpd in inRange)
                            {
                                if (dpd.Value < low || dpd.Value > high)
                                {
                                    ret.Add(Translator.BuildRowMessage(new object[] { k, dpd.SampleTime, dpd.Value }));
                                }
                            }

                        }
                        else // Show fixed lines only
                        {

                            string devKey = deviations.ToString() + "std";

                            // line 1 = avg
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, curr, avg }));
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, intEnd, avg }));
                            // line 2 +std
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, curr, high }));
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, intEnd, high }));
                            // line 3 -std
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, curr, low }));
                            ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, intEnd, low }));
                        }

                        curr = curr.Add(interval);
                    }
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
               

              
                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
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

              
                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    IEnumerable<DataPointDouble> points = ts.Value.Data.OrderBy(d => d.SampleTime);
                    if (ptok != -1)
                        points = points.Skip(points.Count() - ptok).Take(ptok);

                    Maths.RegressionLine line = Maths.SimpleLinearRegression.GetLine(points);
                    string textKey = "project_" + ts.Key + "_" + ((ptok == -1) ? "" : "(" + ptok.ToString() + ")") + "_" + opInt;

                    // Take the last X points in reverse and project them into the future, adjusting the value based on our regression.
                    DataPointDouble last = points.Last();
                    DateTime start = last.SampleTime.Add(interval.Negate());
                    foreach (DataPointDouble ppoint in points.Where(c => c.SampleTime > start).Reverse())
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
            else if (string.Equals(query.Command, "lines", StringComparison.OrdinalIgnoreCase))
            {

                bool fill = query.Options.ContainsKey("fill");
                TimeSpan interval = new TimeSpan();
                if(fill)
                {
                    interval = ParsingUtils.ParseInterval(query.Options["fill"]);
                }

                if (!query.Options.ContainsKey("fixed") || (query.Options.ContainsKey("min") && query.Options["min"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = data.SeriesData.Values.Min(ts => ts.Data.Min(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_min", query.Start, value }, query.Start));
                    
                    if (fill)
                        ret.AddRange(FillData("line_min", value, query.Start, query.End, interval));

                    ret.Add(Translator.BuildRowMessage(new object[] { "line_min", query.End, value }, query.End));
                }

                if (!query.Options.ContainsKey("fixed") || (query.Options.ContainsKey("max") && query.Options["max"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = data.SeriesData.Values.Min(ts => ts.Data.Max(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_max", query.Start, value }, query.Start));
                    if (fill)
                        ret.AddRange(FillData("line_max", value, query.Start, query.End, interval));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_max", query.End, value }, query.End));
                }

                if (!query.Options.ContainsKey("fixed") || (query.Options.ContainsKey("avg") && query.Options["avg"].Equals("true", StringComparison.OrdinalIgnoreCase)))
                {
                    double value = data.SeriesData.Values.Min(ts => ts.Data.Average(d => d.Value));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_avg", query.Start, value }, query.Start));
                    if (fill)
                        ret.AddRange(FillData("line_avg", value, query.Start, query.End, interval));
                    ret.Add(Translator.BuildRowMessage(new object[] { "line_avg", query.End, value },  query.End ));
                }

                if(query.Options.ContainsKey("fixed"))
                {
                    double value = ParsingUtils.ExpandNumeric(query.Options["fixed"]);
                    string k = "line_fixed_" + query.Options["fixed"];
                    ret.Add(Translator.BuildRowMessage(new object[] {k , query.Start, value }, query.Start));
                    if (fill)
                        ret.AddRange(FillData(k, value, query.Start, query.End, interval));
                    ret.Add(Translator.BuildRowMessage(new object[] { k, query.End, value }, query.End));
                }

                return ret;
            }
            else if (string.Equals(query.Command, "rolling", StringComparison.OrdinalIgnoreCase))
            {
                /* Rolling average + std
                 * Welfords Algorithm
                 * https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
                 */
                int deviations = query.Options.ContainsKey("deviations") ? int.Parse(query.Options["deviations"]) : 2;

                int ptok = query.Options.ContainsKey("points") ? int.Parse(query.Options["points"]) : 5;
                if (ptok < 5)
                    ptok = 5;
                //bool highlighOutliers = query.Options.ContainsKey("highlight") ? bool.Parse(query.Options["highlight"]) : false;
               
                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    List<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime).ToList();
                    if (points.Count < 5)
                        continue;
                    double avg = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        avg += points[i].Value;
                    }
                    avg = avg / 5;
                    double m = Maths.Algorithms.SumSquares(avg, points, ptok);
                    for (int i = ptok; i < points.Count(); i++)
                    {
                        double newAvg = avg + ((points[i].Value - avg) / ptok);
                        // Standard deviation
                        m = m + (points[i].Value - avg) * (points[i].Value - newAvg);
                        double std = Math.Sqrt(m / i - 1);

                        avg = newAvg;

                        double high = avg + deviations * std;
                        double low = avg - deviations * std;

                        string devKey = deviations.ToString() + "std";

                        // line 1 = avg
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, points[i].SampleTime, avg }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, points[i].SampleTime, avg }));
                        // line 2 +std
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, points[i].SampleTime, high }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, points[i].SampleTime, high }));
                        // line 3 -std
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, points[i].SampleTime, low }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, points[i].SampleTime, low }));

                    }
                }
                return ret;
            }
            else if (string.Equals(query.Command, "ema", StringComparison.OrdinalIgnoreCase))
            {
                /*
                 * Exponential Moving Average
                 * https://en.wikipedia.org/wiki/Moving_average
                 */
                int deviations = query.Options.ContainsKey("deviations") ? int.Parse(query.Options["deviations"]) : 2;

                int ptok = query.Options.ContainsKey("points") ? int.Parse(query.Options["points"]) : 5;
                if (ptok < 5)
                    ptok = 5;

                double smooth = query.Options.ContainsKey("smooth") ? double.Parse(query.Options["smooth"]) : 0.9;

                foreach (KeyValuePair<string, TimeSeries> ts in data.SeriesData)
                {
                    List<DataPointDouble> points = ts.Value.Data.OrderBy(c => c.SampleTime).ToList();
                    if (points.Count < 5)
                        continue;

                    // Initialize our average to the first 5 values
                    double avg = 0;
                    for (int i = 0; i < 5; i++)
                    {
                        avg += points[i].Value;
                    }
                    avg = avg / 5;

                    double ema = avg;
                    double emvar = 0;
                    
                    for (int i = ptok; i < points.Count(); i++)
                    {
                        double delta = points[i].Value - ema;
                        ema = ema + smooth * delta;
                        emvar = (1 - smooth) * (emvar + smooth * Math.Pow(delta, 2));

                        // line 1 = avg
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, points[i].SampleTime, ema }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key, points[i].SampleTime, ema }));

                        double sq = Math.Sqrt(emvar);
                        double high = ema + deviations * sq;
                        double low = ema - deviations * sq;

                        string devKey = deviations.ToString() + "std";

                        // line 2 +std
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, points[i].SampleTime, high }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_+" + devKey, points[i].SampleTime, high }));
                        // line 3 -std
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, points[i].SampleTime, low }));
                        ret.Add(Translator.BuildRowMessage(new object[] { "avg_" + ts.Key + "_-" + devKey, points[i].SampleTime, low }));

                    }
                }
                return ret;
            }
            


            // TODO: Support clustering (C# or extension)? Both?
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

        public static IEnumerable<PGMessage> FillData(string metric, double value, DateTime start, DateTime end, TimeSpan interval)
        {

            DateTime next = start + interval;
            while (next < end)
            {
                yield return Translator.BuildRowMessage(new object[] { metric, next, value }, next);
                next = next.Add(interval);
            }
        }

    /// <summary>
    /// Separate data into 'series' based on a 'metric'
    /// </summary>
    /// <param name="query"></param>
    /// <param name="qm"></param>
    /// <returns></returns>
    public static TimeCollection GetSeriesCollection(SpecialQuery query, QueryManager qm)
        {
            DataTable table = qm.QueryToTable(query.RawQuery);

            return GetSeriesCollection(table);
        }

        public static TimeCollection GetSeriesCollection(DataTable table)
        {
            //identify columns
            int timeCol = -1;
            int valCol = -1;
            int metCol = -1;
            // TODO: Determine based on column name, not type?
            foreach (DataColumn dc in table.Columns)
            {
                // Allow for datetime (regular timestamp) or double (epoch time - bucketed)
                if (dc.DataType == typeof(DateTime) && timeCol == -1 || dc.ColumnName.Equals("time", StringComparison.OrdinalIgnoreCase) || dc.ColumnName.Equals("time\0", StringComparison.OrdinalIgnoreCase))
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

            ret.DescriptorMessage = PostgresqlCommunicator.Translator.BuildRowDescription(table);


            bool doubleTime = table.Columns[timeIndex].DataType != typeof(DateTime);

            foreach (DataRow dr in table.Rows)
            {
                string key = dr.ItemArray[metIndex] as string;
                if (!ret.SeriesData.ContainsKey(key))
                    ret.SeriesData[key] = new TimeSeries(key);
                if (!doubleTime)
                    ret.SeriesData[key].Data.Add(new DataPointDouble() { SampleTime = (dr.ItemArray[timeIndex] as DateTime?).Value, Value = Convert.ToDouble(dr.ItemArray[valIndex]) });
                else
                    ret.SeriesData[key].Data.Add(new DataPointDouble() { SampleTime = _Epoch.AddSeconds(Convert.ToDouble(dr.ItemArray[timeIndex])), Value = Convert.ToDouble(dr.ItemArray[valIndex]) });
            }

            return ret;
        }

        // TODO: Put in common lib
        private static DateTime _Epoch = new DateTime(1970, 01, 01);
    }

  
    public enum AggMethod
    {
        AVG,
        SUM
    }
}
