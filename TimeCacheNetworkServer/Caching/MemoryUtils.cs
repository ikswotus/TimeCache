using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Diagnostics;

namespace TimeCacheNetworkServer.Caching
{
    public static class MemoryUtils
    {
        /// <summary>
        /// Retrieve current memory usage
        /// </summary>
        /// <returns></returns>
        public static long GetMemoryUsageBytes()
        {
            using (Process p = Process.GetCurrentProcess())
            {
                return p.WorkingSet64;
            }
        }
    }
}
