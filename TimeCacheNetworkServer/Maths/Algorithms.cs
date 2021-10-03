using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Maths
{
    /// <summary>
    /// Math Helper stuff for special queries
    /// </summary>
    public class Algorithms
    {
        public static double StandardDeviation(double avg, IEnumerable<DataPointDouble> values)
        {
            if (values == null)
                return 0;
            return Math.Sqrt(values.Sum(v => Math.Pow(v.Value - avg, 2)) / values.Count());
        }
    }

    public static class SimpleLinearRegression
    {
        public static Coefficients Regress(IEnumerable<DataPointDouble> points)
        {
            Coefficients c = new Coefficients();

            double x = points.Sum(v => v.EpochTime);
            double y = points.Sum(v => v.Value);
            double xy = points.Sum(v => v.EpochTime * v.Value);
            double xs = points.Sum(v => Math.Pow(v.EpochTime, 2));

            c.Slope = (points.Count() * xy - x * y) / (points.Count() * xs - Math.Pow(x, 2));

            c.Intercept = (y - c.Slope * x) / points.Count();

            return c;
        }


        public static RegressionLine GetLine(IEnumerable<DataPointDouble> points)
        {
            RegressionLine ret = new RegressionLine();
            if (!points.Any())
                return ret;

            ret.Regression = Regress(points);


            ret.Start = new DataPointDouble();
            ret.Start.SampleTime = points.OrderBy(p => p.SampleTime).First().SampleTime;
            ret.Start.Value = ret.Regression.Slope * ret.Start.EpochTime + ret.Regression.Intercept;

            ret.End = new DataPointDouble();
            ret.End.SampleTime = points.OrderByDescending(p => p.SampleTime).First().SampleTime;
            ret.End.Value = ret.Regression.Slope * ret.End.EpochTime + ret.Regression.Intercept;

            return ret;
        }
    }


    public class Coefficients
    {
        public double Slope { get; set; }
        public double Intercept { get; set; }
    }

    public class RegressionLine
    {
        public DataPointDouble Start { get; set; }
        public DataPointDouble End { get; set; }

        public Coefficients Regression { get; set; }
    }
}
