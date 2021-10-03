using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Xml;
using System.IO;
using System.Xml.Serialization;
using System.ComponentModel;

namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Configuration for stored queries.
    /// </summary>
    public class CachedQueryConfig
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CachedQueryConfig()
        {
            Queries = new List<CacheableQuery>();
        }

        /// <summary>
        /// List of queries to periodically evaluate.
        /// </summary>
        public List<CacheableQuery> Queries { get; set; }

        /// <summary>
        /// How much memory can our process use before evicting cached data
        /// </summary>
        [Category("Options")]
        public long AllowedMemory { get; set; }

        /// <summary>
        /// Reads a config option from a file
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static CachedQueryConfig FromXmlFile(string file)
        {
            CachedQueryConfig config = null;
            XmlSerializer xs = new XmlSerializer(typeof(CachedQueryConfig));
            using (FileStream fs = File.OpenRead(file))
                config = (CachedQueryConfig)xs.Deserialize(fs);
            return config;
        }

        /// <summary>
        /// Write the config to a file
        /// </summary>
        /// <param name="file"></param>
        public void ToXmlFile(string file)
        {
            XmlSerializer xs = new XmlSerializer(typeof(CachedQueryConfig));
            using (FileStream fs = File.Create(file))
                xs.Serialize(fs, this);
        }
    }

    /// <summary>
    /// Options for periodic queries.
    /// </summary>
    public class CacheableQuery
    {
        /// <summary>
        /// Constructor - set defaults
        /// </summary>
        public CacheableQuery()
        {
            WindowInHours = 6;
            RefreshIntervalMinutes = 5;
            UpdateWindowMinutes = 10;
            RawQueryText = String.Empty;
        }

        /// <summary>
        /// Actual query to run.
        /// Should contain placeholders for timestamps: ###START### and ###END###
        /// so query can be easily reused.
        /// </summary>
        public string RawQueryText { get; set; }

        /// <summary>
        /// How big is our time window.
        /// </summary>
        public int WindowInHours { get; set; }

        /// <summary>
        /// How often this query is run in the background to get latest results.
        /// </summary>
        public int RefreshIntervalMinutes { get; set; }

        /// <summary>
        /// Overlap between start of refresh query and end of last query.
        /// Allows 'fuzzy edge' to be updated with latest values.
        /// </summary>
        public int UpdateWindowMinutes { get; set; }

        /// <summary>
        /// Retrieve a formatted query
        /// </summary>
        /// <returns></returns>
        public string GetFormatted()
        {
            string cfgd = RawQueryText;
            string queryStart = DateTime.UtcNow.AddHours(WindowInHours * -1).ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            string queryEnd = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss.fffZ");
            cfgd = cfgd.Replace("###START###", queryStart).Replace("###END###", queryEnd);
            return cfgd;
        }
    }
}
