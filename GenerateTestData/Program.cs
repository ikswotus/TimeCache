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
        public static void DeleteTest(string cs)
        {
            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(cs))
            {
                conn.Open();
                Npgsql.NpgsqlCommand cm = new Npgsql.NpgsqlCommand("delete from test.simple_test", conn);
                cm.ExecuteNonQuery();
            }
        }

        public static void CyclicalTrending(string connString, double trend, double anchor, double outlierProbability)
        {

            double magnitude = 100.0;

            DateTime start = DateTime.UtcNow.AddHours(-4);
            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "test.simple_test";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);
            DateTime end = DateTime.UtcNow.AddHours(4);

            string metricName = "test_metric";
            double core_value = anchor;
            Random r = new Random();

            double a = 0;

            while (start < end)
            {
                if (r.NextDouble() > 0.96)
                    core_value += trend;

                a = start.Minute * 6;
                if (a < 180) a = 360 - a;
                a = a - 180;

                double value = core_value + (Math.Sin(a) * magnitude);


                if (r.NextDouble() < outlierProbability)
                {
                    if (r.NextDouble() > 0.5)
                        value *= (r.NextDouble() + 1.5);// between 1.5x and 2.5x
                    else
                        value /= (r.NextDouble() + 1.5);
                }

                tdt.AddSimpleTestRow(metricName, start, value);



                start = start.AddSeconds(1);
            }
            tm.BulkInsert(tdt, "test.simple_test");
        }

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
                //if(args.Length != 2)
                //{
                //    Console.WriteLine("Arguments: [conn_string] [test_name]");
                //    return;
                //}
                //if (string.IsNullOrEmpty(args[0]))
                //{
                //    Console.WriteLine("Invalid connection string");
                //    return;
                //}
                //if (string.IsNullOrEmpty(args[1]))
                //{
                //    Console.WriteLine("Invalid test name");
                //    return;
                //}
                //
                //string test = args[1].ToLower();
                string test = "cycle";
                string connString = "Host=localhost;Port=5432;Database=stats;User ID=stats_user;Password=L5z$8322ions;";


                if (String.Equals(test, "increase"))
                {
                    IncreasingData(connString);
                }
                else if(string.Equals(test, "cycle"))
                {
                    DeleteTest(connString);

                    CyclicalTrending(connString, 1, 500.0, 0.001);
                }
                else
                {
                    Console.WriteLine("Unknown test: " + test);
                    Console.WriteLine("Known: " + "increase");
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
