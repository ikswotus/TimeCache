using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Wrapper class for MetaCommand queries
    /// </summary>
    public class SpecialQuery
    {
        public SpecialQuery()
        {
            Options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            Command = String.Empty;
            RawQuery = String.Empty;

            Start = DateTime.MinValue;
            End = DateTime.MaxValue;
        }

        public SpecialQuery(string command, string query)
        {
            Options = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            Command = command;
            RawQuery = query;

            Start = DateTime.MinValue;
            End = DateTime.MaxValue;
            
        }

        
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
        public String Command { get; set; }
        public Dictionary<string, string> Options { get; set; }
        public String RawQuery { get; set; }
    }
}
