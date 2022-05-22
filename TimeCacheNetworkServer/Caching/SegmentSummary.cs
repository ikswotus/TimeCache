using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    public class SegmentSummary
    {
        public SegmentSummary(string tag, DateTime start, DateTime end, long count)
        {
            Tag = tag;
            Start = start;
            End = end;
            Count = count;
        }

        public long Count { get; private set; }
        public DateTime End { get; private set; }
        public DateTime Start { get; private set; }
        public string Tag { get; private set; }

        public List<SegmentPoint> ToPoints(string tag)
        {
            return new List<SegmentPoint>(){new  SegmentPoint() {  Count = this.Count, Tag = tag, Timestamp = this.Start},
                 new SegmentPoint() {  Count = this.Count, Tag = tag, Timestamp = this.End} };
        }
    }

    public class SegmentPoint
    {
        public string Tag { get; set; }
        public long Count { get; set; }
        public DateTime Timestamp { get; set; }
    }
}
