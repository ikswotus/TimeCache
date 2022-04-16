using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{
    public static class UtilityMethods
    {
        public static bool Between(DateTime start, DateTime end, DateTime date)
        {
            return (date >= start && date <= end);
        }
    }
}
