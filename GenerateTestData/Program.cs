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
     * Existing data is deleted by default. A command line option 'nodelete' can override to keep existing
     */
    class Program
    {
        /// <summary>
        /// Delete test data points.
        /// </summary>
        /// <param name="cs"></param>
        public static void DeleteTestData(string cs)
        {
            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(cs))
            {
                conn.Open();
                Npgsql.NpgsqlCommand cm = new Npgsql.NpgsqlCommand("delete from demo.generated_data;", conn);
                cm.ExecuteNonQuery();
            }
        }

        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="connString"></param>
        /// <param name="hours"></param>
        /// <param name="shift">Probability for changing direction</param>
        public static void RandomWalk(string connString, string metric_name, int hours = 2, double shift = 0.5, double startValue = 500)
        {
            DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
            DateTime end = DateTime.UtcNow.AddHours(hours);

            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "demo.generated_data";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);

            Random r = new Random();

            double value = startValue;

            short direction = 1;

            while (start < end)
            {
                if (r.NextDouble() < shift)
                    direction *= -1;

                value += 1 * direction;

                start = start.AddSeconds(1);
                tdt.AddSimpleTestRow(metric_name, start, value);
            }
            tm.BulkInsert(tdt, "demo.generated_data");
        }
        public static void CyclicalTrending(string connString, string metric_name, int hours = 2, double trend = 1.0, double anchor = 500.0, double outlierProbability = 0.00, int pm = 60)// double sinAmp = 6.0, double modFactor = 1.0)
        {

            double magnitude = 100.0;

            DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "demo.generated_data";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);
            DateTime end = DateTime.UtcNow.AddHours(hours);

            // 

            TestSeries.SimpleTestDataTable sinDT = new TestSeries.SimpleTestDataTable();
            TestSeries.SimpleTestDataTable adt = new TestSeries.SimpleTestDataTable();

            double core_value = anchor;
            Random r = new Random();

            //double ten = pm / 10; //12.0
            double modFactor = pm / 60.0;
            double sinAmp = 6.0 / modFactor;

            Console.WriteLine("Period: " + pm);
            Console.WriteLine("SinAmp: " + sinAmp);
            Console.WriteLine("ModFactor: " + modFactor);
            
            double a = 0;
            double factor = Math.PI / 180.0;
            while (start < end)
            {
                //if (r.NextDouble() > 0.96)
                //    core_value += trend;

                double modHour = start.Hour % modFactor;

                //if(start.Hour > 18)
                //{
                //    int diff = (24 - start.Hour);
                //    modHour += (1.0 / diff);
                //}

                a =  (modHour * (360 / modFactor)) + start.Minute * sinAmp;

                adt.AddSimpleTestRow("a", start, a);

                double sin = Math.Sin(a * factor);
                // Peaks are 0.85,1.0
                // Valleys: -0.85,-1.0

                double adjust = Math.Pow((1 - Math.Abs(sin)), 2);

                double asin = (sin < 0) ? sin - adjust : sin + adjust;
              

                double value = core_value + asin * magnitude;
              

                if (r.NextDouble() < outlierProbability)
                {
                    if (r.NextDouble() > 0.5)
                        value *= (r.NextDouble() + 1.5);// between 1.5x and 2.5x
                    else
                        value /= (r.NextDouble() + 1.5);
                }

                tdt.AddSimpleTestRow(metric_name, start, value);
                sinDT.AddSimpleTestRow("sin", start, Math.Sin(a * factor));
                sinDT.AddSimpleTestRow("asin", start, asin);

                start = start.AddSeconds(1);
            }
            tm.BulkInsert(tdt, "demo.generated_data");
            tm.BulkInsert(sinDT, "demo.generated_data");
            tm.BulkInsert(adt, "demo.generated_data");
        }

        /// <summary>
        /// Generate data that increases over time but has wild swings in value
        /// 
        /// </summary>
        public static void TrendingData(string connString, string metricName, double startValue = 500.0, double increaseLiklihood = 0.04, double decreaseLikelihood = 0.01, double magnitude = 1.0)
        {
            DateTime start = DateTime.UtcNow.AddHours(-1);
            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "demo.generated_data";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);
            DateTime end = DateTime.UtcNow.AddHours(1);

            //string metricName = "test_metric";
            double core_value = startValue;
            Random r = new Random();

            double ic = 1.0 - increaseLiklihood;
            double dc = 1.0 - decreaseLikelihood;

            while (start < end)
            {
                if (r.NextDouble() > ic)
                    core_value += magnitude;

                else if (r.NextDouble() > dc)
                    core_value -= magnitude;

                double value = core_value;
                double add = r.NextDouble();

                if (add > 0.8)
                {
                    value += r.NextDouble() * r.Next(1, 500);
                }
                else if (add < 0.1)
                {
                    value -= r.NextDouble() * r.Next(1, 500);
                }

                tdt.AddSimpleTestRow(metricName, start, value);

              

                start = start.AddSeconds(1);
            }
            tm.BulkInsert(tdt, "demo.generated_data");
        }

        private static string _argStringHelp = @"
        tests: 'trend', 'walk', 'cycle'
        options:
            global: 
                'metric_name': string value for naming the series, default is test name
                'start_value': integer - initial value for the series.
                'hours' : integer - number of hours of data to generate
            'walk':
            'trend'
                'increase_prob': Decimal 0.0-1.0, probability of core value increase
                'decrease_prob': Decimal 0.0-1.0, probability of core value decrease
                'magnitude' : amount of increase per change, default 1.0
        ";

        static void Main(string[] args)
        {
            try
            {
                if (args.Length < 2)
                {
                    // Require conn_string and test_name
                    // Optional  key:value pairings for tests/data retention
                    throw new Exception("Invalid Arguments: [conn_string] [test_name] ('metric_name':test_name) (start_value:50) (hours|1+) (no_delete:true|false) (walk_stickiness:0-100)");
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

                bool skipdelete = false;
                Dictionary<string, string> options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
                if (args.Length > 2)
                {
                    for(int i = 2; i < args.Length; i++)
                    {
                        string[] kvp = args[i].Split(':');
                        if(kvp.Length != 2)
                        {
                            Console.Error.WriteLine("Skipping invalid option: " + args[i]);
                        }
                        else
                        {
                            options[kvp[0].Trim()] = kvp[1].Trim();
                        }
                    }
                }

                string connString = args[0];
                string test = args[1];

                if(options.ContainsKey("no_delete"))
                {
                    if (!bool.TryParse(options["no_delete"], out skipdelete))
                        Console.Error.WriteLine("Failed to parse boolean value for no_delete option: " + options["no_delete"]);
                }

                int hours = 1;
                if (options.ContainsKey("hours"))
                {
                    if (!int.TryParse(options["hours"], out hours))
                        Console.Error.WriteLine("Failed to parse integer value for hours option: " + options["hours"]);
                    if (hours < 1)
                    {
                        throw new Exception("Invalid hours: must be at least 1, provided: " + hours);
                    }
                }

                double startValue = 500;
                if (options.ContainsKey("start_value"))
                {
                    if (!double.TryParse(options["start_value"], out startValue))
                        Console.Error.WriteLine("Failed to parse double value for start_value option: " + options["start_value"]);
                }

                string metric_name = test;
                if (options.ContainsKey("metric_name"))
                {
                    metric_name = options["metric_name"];
                }

                if (!skipdelete)
                    DeleteTestData(connString);

                if (String.Equals(test, "trend"))
                {
                    double ic = 0.04;
                    double dc = 0.04;
                    double mag = 1.0;
                    if (options.ContainsKey("increase_prob"))
                    {
                        if (!double.TryParse(options["increase_prob"], out ic))
                            Console.Error.WriteLine("Failed to parse double value for increase_prob option: " + options["increase_prob"]);
                        if (0.0 >= ic || 1.0 <= ic)
                        {
                            throw new Exception("Invalid increase_prob: must be between 0.0 and 1.0 provided: " + ic);
                        }
                    }
                    if (options.ContainsKey("decrease_prob"))
                    {
                        if (!double.TryParse(options["decrease_prob"], out dc))
                            Console.Error.WriteLine("Failed to parse double value for decrease_prob option: " + options["decrease_prob"]);
                        if (0.0 >= dc || 1.0 <= dc)
                        {
                            throw new Exception("Invalid decrease_prob: must be between 0.0 and 1.0 provided: " + ic);
                        }
                    }
                    if (options.ContainsKey("magnitude"))
                    {
                        if (!double.TryParse(options["magnitude"], out mag))
                            Console.Error.WriteLine("Failed to parse double value for magnitude option: " + options["magnitude"]);
                        if (0.0 >= mag)
                        {
                            throw new Exception("Invalid mag: must be greater than 0.0 " + mag);
                        }
                    }


                    TrendingData(connString, metric_name, startValue, ic, dc, mag);
                }
                else if(string.Equals(test, "cycle"))
                {
                    int pm = 60;
                    if (options.ContainsKey("period_minutes"))
                    {
                        if (!int.TryParse(options["period_minutes"], out pm))
                            Console.Error.WriteLine("Failed to parse integer value for period_minutes option: " + options["period_minutes"]);
                        if (0.0 > pm)
                        {
                            throw new Exception("Invalid period_minutes: must be greater than 0.0: " + pm);
                        }
                    }

                    //double sa = 6.0;
                    //if (options.ContainsKey("sin_amp"))
                    //{
                    //    if (!double.TryParse(options["sin_amp"], out sa))
                    //        Console.Error.WriteLine("Failed to parse double value for sin_amp option: " + options["sin_amp"]);
                    //    if (0.0 > sa)
                    //    {
                    //        throw new Exception("Invalid sin_amp: must be greater than 0.0: " + sa);
                    //    }
                    //}
                    //double mf = 1.0;
                    //if (options.ContainsKey("mod_factor"))
                    //{
                    //    if (!double.TryParse(options["mod_factor"], out mf))
                    //        Console.Error.WriteLine("Failed to parse double value for mod_factor option: " + options["mod_factor"]);
                    //    if (0.0 > mf || 24.0 < mf)
                    //    {
                    //        throw new Exception("Invalid mod_factor: must be greater than 0.0: " + mf);
                    //    }
                    //}

                    CyclicalTrending(connString, metric_name, hours, 1, 500.0, 0.000, pm);
                }
                else if(string.Equals(test, "walk"))
                {
                    double stickiness = 0.5;
                    if (options.ContainsKey("walk_stickiness"))
                    {
                        if (!double.TryParse(options["walk_stickiness"], out stickiness))
                            Console.Error.WriteLine("Failed to parse double value for walk_stickiness option: " + options["walk_stickiness"]);
                        if(stickiness < 0.0 || stickiness > 100.0)
                        {
                            throw new Exception("Invalid stickiness: must be between 0.0 and 100.0, provided: " + stickiness);
                        }    
                    }
                    RandomWalk(connString, metric_name, hours, stickiness, startValue);
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
            //Console.ReadKey();
        }
    }
}
