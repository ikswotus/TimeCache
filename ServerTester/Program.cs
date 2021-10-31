using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using System.Net.Sockets;


namespace ServerTester
{
    /// <summary>
    /// Test program for running a server
    /// </summary>
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                string ip = "192.168.1.7";
                IPAddress serverIP = IPAddress.Parse(ip);
                TimeCacheNetworkServer.NetworkServer tns = new TimeCacheNetworkServer.NetworkServer(serverIP, 5433, 5432);

                tns.Start();

                while(true)
                {
                    System.Threading.Thread.Sleep(1000);
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
