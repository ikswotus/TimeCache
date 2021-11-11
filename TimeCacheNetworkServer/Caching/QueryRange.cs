using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// Represents a range of time we need to query
    /// </summary>
    public class QueryRange
    {
        public QueryRange(DateTime start, DateTime end)
        {
            StartTime = start;
            EndTime = end;

            if (StartTime > EndTime)
                throw new Exception("Invalid query range, start cannot be after end");
        }


        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
    }
}
