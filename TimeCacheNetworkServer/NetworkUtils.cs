using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net;
using System.Net.NetworkInformation;

namespace TimeCacheNetworkServer
{
    public abstract class NetworkUtils
    {
        public static System.Net.IPAddress GetIP(bool ignoreLocal = true)
        {
            foreach(NetworkInterface ni in NetworkInterface.GetAllNetworkInterfaces())
            {
                foreach(UnicastIPAddressInformation uProp in ni.GetIPProperties().UnicastAddresses.Where(a => a.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork))
                {
                    string address = uProp.Address.ToString();

                    if (String.Equals(address, "127.0.0.1") && ignoreLocal)
                        continue;
                    return uProp.Address;
                }
            }
            return null;
        }
    }
}
