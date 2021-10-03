using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{
    public class ActiveConnectionInfo
    {
        public ActiveConnectionInfo()
        {
            RemoteAddress = String.Empty;
            QueriesEvaluated = 0;
            ConnectedAt = DateTime.UtcNow;
            LastActivity = DateTime.MinValue;
        }

        public string RemoteAddress { get; set; }
        public int QueriesEvaluated { get; set; }
        public DateTime ConnectedAt { get; set; }
        public DateTime LastActivity { get; set; }
    }
}
