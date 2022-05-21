using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace TC_Tests
{
    internal class TestData
    {
        private static Random _rand = new Random();

        public static DataTable GetEmptyTestData()
        {
            DataTable table = new DataTable();
            table.Columns.Add("metric", typeof(string));
            table.Columns.Add("time", typeof(DateTime));
            table.Columns.Add("value", typeof(double));

            return table;
        }

        public static int TimeColumnIndex = 1;

        public static DataTable GetTestData(DateTime start, DateTime end, TimeSpan interval)
        {
            DataTable tt = GetEmptyTestData();
            if (interval.TotalMilliseconds < 0)
                throw new Exception("Interval must be postive step");
            if (start >= end)
                throw new Exception("Start must be before end");

            while (start < end)
            {
                tt.Rows.Add("test", start, _rand.NextDouble());

                start = start.Add(interval);
            }

            return tt;
        }

    }
}
