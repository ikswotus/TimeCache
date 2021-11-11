using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utils.Test
{
    /// <summary>
    /// Helper for generating sample data
    /// </summary>
    public class SampleDataGenerator
    {
        /// <summary>
        /// Set of machine names to use for stat creation.
        /// </summary>
        private static List<string> _defaultMachines = new List<string>()
        {
            "client-01",
            "tablet-02",
            "host-01",
            "host-02",
            "upload-77",
            "proxy-08",
            "webserver-01",
            "webserver-02"
        };

        /// <summary>
        /// Category names
        /// </summary>
        private static List<string> _defaultCategories = new List<string>()
        {
            "memory",
            "cpu",
            "codestats"
        };

        /// <summary>
        /// Counters names
        /// </summary>
        private static List<string> _defaultCounters = new List<string>()
        {
            "threads",
            "handles",
            "pages",
            "bytes sent",
            "errors",
            "active"
        };

        /// <summary>
        /// instance names
        /// </summary>
        private static List<string> _defaultInstances = new List<string>
        {
            "_total",
            "devenv",
            "statsgenerator",
            "timecache_server-01",
            "timecache_server-02",
            "timecache_client-01"
        };

        public SampleDataGenerator()
        {

        }

        private List<String> _instances;
        private List<String> _machines;
        private List<String> _counters;
        private List<String> _categories;


        public void SetInstances(List<string> instances)
        {
            _instances = instances;
        }
        public void SetMachines(List<string> machines)
        {
            _machines = machines;
        }
        public void SetCounters(List<string> counters)
        {
            _counters = counters;
        }
        public void SetCategories(List<string> categories)
        {
            _categories = categories;

        }

        /// <summary>
        /// new random instance
        /// </summary>
        /// <returns></returns>
        public DummyCounter GetInstance()
        {
            DummyCounter di = new DummyCounter();

            List<String> t = _machines ?? _defaultMachines;
            di.MachineID = _rand.Next(t.Count);
            di.Machine = t[di.MachineID];

            t = _categories ?? _defaultCategories;
            di.CategoryID = _rand.Next(t.Count);
            di.Category = t[di.CategoryID];

            t = _counters ?? _defaultCounters;
            di.CounterID = _rand.Next(t.Count);
            di.Counter = t[di.CounterID];

            t = _instances ?? _defaultInstances;
            di.InstanceID = _rand.Next(t.Count);
            di.Instance = t[di.InstanceID];

            di.Anchor = new Random((int)DateTime.UtcNow.Ticks).Next(1000, 20000);

            return di;
        }

        public TestData.TimeseriesDataIDDataTable GenerateTestData(int hours)
        {
            List<Utils.Test.SampleDataGenerator.DummyCounter> instances = AllInstances();

            TestData.TimeseriesDataIDDataTable tdt = new TestData.TimeseriesDataIDDataTable();
            tdt.TableName = "stats.timeseries_data_id";


            DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
            DateTime end = DateTime.UtcNow.AddHours(1 * hours);

            while (start < end)
            {
                foreach (Utils.Test.SampleDataGenerator.DummyCounter di in instances)
                {
                    tdt.AddTimeseriesDataIDRow(start, (double)(_rand.Next(di.Anchor) + _rand.NextDouble()), di.MachineID, di.CategoryID, di.CounterID, di.InstanceID);
                }
                start = start.AddSeconds(1);
            }
            return tdt;
        }

        public List<DummyCounter> AllInstances()
        {
            Random r = new Random((int)DateTime.UtcNow.Ticks);
            List<DummyCounter> dummies = new List<DummyCounter>();

            List<String> mach = _machines ?? _defaultMachines;
            List<String> cat = _categories ?? _defaultCategories;
            List<String> count = _counters ?? _defaultCounters;
            List<String> inst = _instances ?? _defaultInstances;

            for (int m = 0; m < mach.Count; m++)
            {
                for (int c = 0; c < cat.Count; c++)
                    for (int n = 0; n < count.Count; n++)
                        for (int i = 0; i < inst.Count; i++)
                        {
                            DummyCounter di = new DummyCounter();
                            di.MachineID = (m + 1);
                            di.Machine = mach[m];
                            di.CategoryID = (c + 1);
                            di.Category = cat[c];
                            di.CounterID = (n + 1);
                            di.Counter = count[n];
                            di.InstanceID = (i + 1);
                            di.Instance = inst[i];
                            di.Anchor = r.Next(1000, 20000);
                            dummies.Add(di);
                        }
            }
            return dummies;
        }

        public class DummyCounter
        {
            public DummyCounter()
            {

            }

           

            public int Anchor = 0;

            public string Machine { get; set; }
            public string Category { get; set; }
            public string Counter { get; set; }
            public string Instance { get; set; }

            public int MachineID { get; set; }
            public int CategoryID { get; set; }
            public int CounterID { get; set; }
            public int InstanceID { get; set; }

        }

        /// <summary>
        /// Randomly choose stat names
        /// </summary>
        private static Random _rand = new Random(DateTime.UtcNow.Millisecond);
    }
}
