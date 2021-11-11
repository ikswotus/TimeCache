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

                Utils.Test.SampleDataGenerator sdg = new Utils.Test.SampleDataGenerator();

                List<Utils.Test.SampleDataGenerator.DummyCounter> instances = sdg.AllInstances();

                TestData.TimeseriesDataIDDataTable tdt = new TestData.TimeseriesDataIDDataTable();
                tdt.TableName = "stats.timeseries_data_id";

                Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), args[0]);

                Random _rand = new Random(DateTime.UtcNow.Millisecond);
                
                //// Generate 2h of data at 1s intervals (7200 data points)
                //// Go into the future so we can test querying through now().

                if (mode == "bulk")
                {
                    DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
                    DateTime end = DateTime.UtcNow.AddHours(1 * hours);
                    Console.WriteLine("Adding values: " + start + " to " + end);

                 
                    while (start < end)
                    {
                        foreach (Utils.Test.SampleDataGenerator.DummyCounter di in instances)
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
                            foreach (Utils.Test.SampleDataGenerator.DummyCounter di in instances)
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
