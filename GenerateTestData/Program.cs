using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GenerateTestData
{
    /**
     * This program is just for generating test series
     * for examples
     * 
     */
    class Program
    {
        /// <summary>
        /// Generate data that increases over time
        /// </summary>
        public static void IncreasingData(string connString)
        {
            DateTime start = DateTime.UtcNow.AddHours(-1);
            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "test.simple_test";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);
            DateTime end = DateTime.UtcNow.AddHours(1);

            string metricName = "test_metric";
            double core_value = 500.0;
            Random r = new Random();

            while (start < end)
            {
                if (r.NextDouble() > 0.96)
                    core_value++;

                else if (r.NextDouble() > 0.99)
                    core_value--;

                double value = core_value;
                double add = r.NextDouble();
                if (add > 0.8)
                {
                    value += r.NextDouble() * 500;
                }
                else if (add < 0.2)
                {
                    value -= r.NextDouble() * 500;
                }

                tdt.AddSimpleTestRow(metricName, start, value);

              

                start = start.AddSeconds(1);
            }
            tm.BulkInsert(tdt, "test.simple_test");
        }

        static void Main(string[] args)
        {
            try
            {
                if(args.Length != 2)
                {
                    Console.WriteLine("Arguments: [conn_string] [test_name]");
                    return;
                }
                if (string.IsNullOrEmpty(args[0]))
                {
                    Console.WriteLine("Invalid connection string");
                    return;
                }
                if (string.IsNullOrEmpty(args[1]))
                {
                    Console.WriteLine("Invalid test name");
                    return;
                }

                string test = args[1].ToLower();

                if (String.Equals(test, "increase"))
                {
                    IncreasingData(args[0]);
                }


            }
            catch(Exception exc)
            {
                Console.WriteLine("Failed: " + exc);
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
