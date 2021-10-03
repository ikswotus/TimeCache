using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;


namespace TimeCacheNetworkServer
{
    class Program
    {
        /// <summary>
        /// Runs the simple network server.
        /// </summary>
        /// <param name="args"></param>
        static void Main(string[] args)
        {
            try
            {
                if(args.Length <= 2)
                {
                    Console.WriteLine("Usage: {0}= username, {1}=password");
                    args = new string[2];
                    Console.WriteLine("Enter username: ");
                    args[0] = Console.ReadLine();
                    Console.WriteLine("Enter password: ");
                    args[1] = Console.ReadLine();
                }

                string connectionString = String.Format(Properties.Settings.Default.DatabaseConnectionString, args);

                NetworkServer ns = new NetworkServer(connectionString, 9876);
                ns.Start();

                Console.WriteLine("Server started - press key to stop");
                Console.ReadKey();

                ns.Stop();
            }
            catch (Exception exc)
            {
                Console.WriteLine("Error:" + exc);
            }
            Console.WriteLine("Finished - Press key to exit.");
            Console.ReadKey();
        }
    }
}
