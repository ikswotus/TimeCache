using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{

    public class TimeCollection
    {
        public TimeCollection()
        {
            SeriesData = new Dictionary<string, TimeSeries>();
        }

        /// <summary>
        /// In case we're doing a meta-only query, allow the timecollection to retrieve/build the descriptor.
        /// </summary>
        public PostgresqlCommunicator.RowDescription DescriptorMessage { get; set; }
        
        public Dictionary<string, TimeSeries> SeriesData { get; set; }
    }

    public class TimeSeries
    {
        public TimeSeries()
        {
            Metric = String.Empty;
            Data = new List<DataPointDouble>();
            Outliers = new List<DataPointDouble>();
        }

        public TimeSeries(string metric)
        {
            Metric = metric;
            Data = new List<DataPointDouble>();
            Outliers = new List<DataPointDouble>();
        }

        public TimeSeries(string metric, List<DataPointDouble> points)
        {
            Metric = metric;
            Data = points;
            Outliers = new List<DataPointDouble>();
        }


        public String Metric { get; set; }
        public List<DataPointDouble> Data { get; set; }

        public List<DataPointDouble> Outliers { get; set; }
    }


        public class DataPointDouble
        {
            public DataPointDouble()
            {

            }

            public double EpochTime
            {
                get
                {
                    if (_evalue == -1.0)
                        _evalue = (SampleTime - _Epoch).TotalMinutes;
                    return _evalue;
                }
                private set
                {

                }
            }

            public static DateTime _Epoch = new DateTime(1970, 01, 01);

            private double _evalue = -1.0;
            private DateTime _sampleTime = DateTime.MinValue;

            public DateTime SampleTime
            {
                get
                {
                    return _sampleTime;
                }
                set
                {
                    _sampleTime = value;
                    _evalue = -1.0;
                }
            }

            public double Value { get; set; }
        }
 
}
