using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Threading;

namespace SimpleStatsGenerator
{
    /// <summary>
    /// Simple stats generator creates dummy values for testing
    /// 
    /// Inserted into: stats.timeseries_data_id
    /// 
    /// Modes are:
    /// live - add new values, default is a bulk insert every 30s
    /// bulk - add values at 1s intervals for -1  hour to +1 hour based on current timestamp, then exit
    /// 
    /// 
    /// Values are fabricated and look roughly like windows performance counter values.
    /// 
    /// </summary>
    class Program
    {
        /// <summary>
        /// Set of machine names to use for stat creation.
        /// </summary>
        private static List<string> _machines = new List<string>()
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
        private static List<string> _categories = new List<string>()
        {
            "memory",
            "cpu",
            "codestats"
        };

        /// <summary>
        /// Counters names
        /// </summary>
        private static List<string> _counters = new List<string>()
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
        private static List<string> _instances = new List<string>
        {
            "_total",
            "devenv",
            "statsgenerator",
            "timecache_server-01",
            "timecache_server-02",
            "timecache_client-01"
        };

        private class DummyInstance
        {
            public DummyInstance()
            {

            }

            /// <summary>
            /// new random instance
            /// </summary>
            /// <returns></returns>
            public static DummyInstance GetInstance()
            {
                DummyInstance di = new DummyInstance();

                di.MachineID = _rand.Next(_machines.Count);
                di.Machine = _machines[di.MachineID];
                di.CategoryID = _rand.Next(_categories.Count );
                di.Category = _categories[di.CategoryID];
                di.CounterID = _rand.Next(_counters.Count );
                di.Counter = _counters[di.CounterID];
                di.InstanceID = _rand.Next(_instances.Count);
                di.Instance = _instances[di.InstanceID];

                di.Anchor = new Random((int)DateTime.UtcNow.Ticks).Next(1000, 20000);

                return di;
            }

            public static List<DummyInstance> AllInstances()
            {
                Random r = new Random((int)DateTime.UtcNow.Ticks);
                List<DummyInstance> dummies = new List<DummyInstance>();
                for(int m = 0; m < _machines.Count; m++)
                {
                    for (int c = 0; c < _categories.Count; c++)
                        for (int n = 0; n < _counters.Count; n++)
                            for (int i = 0; i < _instances.Count; i++)
                            {
                                DummyInstance di = new DummyInstance();
                                di.MachineID = (m + 1);
                                di.Machine = _machines[m];
                                di.CategoryID = (c + 1);
                                di.Category = _categories[c];
                                di.CounterID = (n + 1);
                                di.Counter = _counters[n];
                                di.InstanceID = (i + 1);
                                di.Instance = _instances[i];
                                di.Anchor = r.Next(1000, 20000);
                                dummies.Add(di);
                            }
                }
                return dummies;
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

        /// <summary>
        /// Insert values.
        /// TODO: Change this to a bulk insert.
        /// </summary>
        private static string insertq = @"INSERT INTO stats.timeseries_data_id(
	sample_time, machine_id, current_value, category_id, counter_id, instance_id)
	VALUES (@sample_time, @machine_name, @current_value, @category_name, @counter_name, @instance_name);";


        /// <summary>
        /// Args[0] = connection string
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                if(args == null || args.Length < 2)
                {
                    Console.Error.WriteLine("Usage: args[0] = connection string, args[1] = mode (live|bulk), args[2] = # of hours(if bulk)");
                    return;
                }

                string mode = args[1];
                if(String.IsNullOrEmpty(mode))
                {
                    Console.Error.WriteLine("No valid mode given, defaulting to bulk +- 1 hour");
                    mode = "bulk";
                }

                int hours = 1;
                if (mode == "bulk" && args.Length >= 3)
                {
                    if (!int.TryParse(args[2], out hours))
                        hours = 1;
                }

                // Construct a set of dummy instances to use for value generation.
                //List<DummyInstance> instances = new List<DummyInstance>(10);

                //for (int i = 0; i< 10;i++)
                //{
                //    instances.Add(DummyInstance.GetInstance());
                //}

                List<DummyInstance> instances = DummyInstance.AllInstances();

                TestData.TimeseriesDataIDDataTable tdt = new TestData.TimeseriesDataIDDataTable();
                tdt.TableName = "stats.timeseries_data_id";

                Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), args[0]);


                //// Generate 2h of data at 1s intervals (7200 data points)
                //// Go into the future so we can test querying through now().

                if (mode == "bulk")
                {
                    DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
                    DateTime end = DateTime.UtcNow.AddHours(1 * hours);
                    Console.WriteLine("Adding values: " + start + " to " + end);

                 
                    while (start < end)
                    {
                        foreach (DummyInstance di in instances)
                        {
                            tdt.AddTimeseriesDataIDRow(start, (double)(_rand.Next(di.Anchor) + _rand.NextDouble()), di.MachineID, di.CategoryID, di.CounterID, di.InstanceID);
                        }
                        start = start.AddSeconds(1);

                        if (tdt.Rows.Count > 10000)
                        {
                            tm.BulkInsert(tdt, "stats.timeseries_data_id");
                            tdt = new TestData.TimeseriesDataIDDataTable();
                            tdt.TableName = "stats.timeseries_data_id";
                        }
                    }
                    if(tdt.Rows.Count > 0)
                    {
                        tm.BulkInsert(tdt, "stats.timeseries_data_id");
                        tdt = null;
                    }
                }
                else
                {
                    while (true)
                    {
                        for (int i = 0; i < 30; i++)
                        {
                            foreach (DummyInstance di in instances)
                            {
                                tdt.AddTimeseriesDataIDRow(DateTime.UtcNow, (double)(_rand.Next(di.Anchor) + _rand.NextDouble()), di.MachineID, di.CategoryID, di.CounterID, di.InstanceID);
                            }
                            Thread.Sleep(1000);
                        }

                        tm.BulkInsert(tdt, "stats.timeseries_data_id");
                        tdt = new TestData.TimeseriesDataIDDataTable();
                        tdt.TableName = "stats.timeseries_data_id";
                    }
                }
            }
            catch(Exception exc)
            {
                Console.WriteLine("Failed:" + exc);
            }
            Console.WriteLine("Press key to exit");
            Console.ReadKey();
        }
    }
}
