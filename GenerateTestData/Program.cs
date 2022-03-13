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
     * 
     * 
     * Examples:
     * 
     * .\GenerateTestData.exe 'conn_string' cycle hours:72 no_delete:false period_minutes:1440 metric_name:daily interval_seconds:60
     * 
     * .\GenerateTestData.exe 'conn_string' walk hours:72 start_value:1000 walk_stickiness:0.7 metric_name:random_walk_test
     * 
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
        public static void RandomWalk(string connString, string metric_name, TimeSpan interval, int hours = 2, double shift = 0.5, double startValue = 500)
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

                start = start.Add(interval);
                tdt.AddSimpleTestRow(metric_name, start, value);
            }
            tm.BulkInsert(tdt, "demo.generated_data");
        }
        
        
        /// <summary>
        /// Generate data that follows a cyclical pattern. This is accomplished
        /// by using a sine wave as the root value. Allows inclusion of trends/randomized values
        /// </summary>
        /// <param name="connString"></param>
        /// <param name="metric_name"></param>
        /// <param name="hours"></param>
        /// <param name="trend"></param>
        /// <param name="anchor"></param>
        /// <param name="pointValueFlux"></param>
        /// <param name="outlierProbability"></param>
        /// <param name="pm"></param>
        public static void CyclicalTrending(string connString, string metric_name, TimeSpan interval, int hours = 2, double trend = 0.001, double anchor = 500.0, int pointValueFlux = 15, double outlierProbability = 0.00, int pm = 60)// double sinAmp = 6.0, double modFactor = 1.0)
        {

            double magnitude = 100.0;

            DateTime start = DateTime.UtcNow.AddHours(-1 * hours);
            TestSeries.SimpleTestDataTable tdt = new TestSeries.SimpleTestDataTable();
            tdt.TableName = "demo.generated_data";

            Utils.Postgresql.TableManager tm = new Utils.Postgresql.TableManager(new Utils.Postgresql.ManagedBulkWriter(), connString);
            DateTime end = DateTime.UtcNow.AddHours(hours);

            double core_value = anchor;
            Random r = new Random();

            double modFactor = pm / 60.0;
            double sinAmp = 6.0 / modFactor;

            Console.WriteLine("Period: " + pm);
            Console.WriteLine("SinAmp: " + sinAmp);
            Console.WriteLine("ModFactor: " + modFactor);
            
            double fixedTrend = 0.0;

            long looped = 0;

            double a = 0;
            double factor = Math.PI / 180.0;
            while (start < end)
            {
               
                double modHour = start.Hour % modFactor;

               
                a =  (modHour * (360 / modFactor)) + start.Minute * sinAmp;

                double sin = Math.Sin(a * factor);
                
              //  double adjust = Math.Pow((1 - Math.Abs(sin)), 2);

              //  double asin = (sin < 0) ? sin - adjust : sin + adjust;
              

                double value = core_value + sin * magnitude;

                // Adjust the point randomly by pointValueFlux
                if(pointValueFlux > 0)
                {
                    if (r.NextDouble() > 0.5)
                        value += r.Next(0, pointValueFlux);
                    else
                        value -= r.Next(0, pointValueFlux);

                    //value += randomTrend;
                }

                // Extreme point values - magnify value by 1.5x-2.5x
                if (r.NextDouble() < outlierProbability)
                {
                    if (r.NextDouble() > 0.5)
                        value *= (r.NextDouble() + 1.5);// between 1.5x and 2.5x
                    else
                        value /= (r.NextDouble() + 1.5);
                }

               

                value += fixedTrend;

                fixedTrend += trend;
                

                tdt.AddSimpleTestRow(metric_name, start, value);

                looped++;
                start = start.Add(interval);
            }
            tm.BulkInsert(tdt, "demo.generated_data");
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
                int intervalSeconds = 5;
                if (options.ContainsKey("interval_seconds"))
                {
                    if (!int.TryParse(options["interval_seconds"], out intervalSeconds))
                        Console.Error.WriteLine("Failed to parse integer for interval seconds option: " + options["interval_seconds"]);
                }
                TimeSpan interval = TimeSpan.FromSeconds(intervalSeconds);

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

                    CyclicalTrending(connString, metric_name, interval, hours, 0.01, 500.0, 15, 0.000, pm);
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
                    RandomWalk(connString, metric_name, interval, hours, stickiness, startValue);
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
