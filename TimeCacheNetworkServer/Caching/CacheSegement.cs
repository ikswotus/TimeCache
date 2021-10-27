using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// Allows caches to support multiple time ranges.
    ///
    /// A cache segment must be a continguous time period from
    /// @StartTime to @EndTime.
    /// 
    /// Segments must be locked when accessing/clearing/updating
    /// </summary>
    public class CacheSegement : SLog.SLoggableObject
    {
        public CacheSegement(string key, SLog.SLogger logger)
            : base(key, logger)
        {
            CurrentData = new List<CachedRow>();
        }

        public void Clear()
        {
            Trace("Clearing segment");

            foreach (CachedRow cr in CurrentData)
            {
                cr.TranslatedMessage.Release();
            }

            CurrentData.Clear();
        }

        /// <summary>
        /// Removes any cached data earlier than @start.
        /// StartTime will be adjusted to our new ealiest time.
        /// </summary>
        /// <param name="start"></param>
        /// <returns>Number of rows removed.</returns>
        public int TrimStart(DateTime start)
        {
            Debug("Trimming data from start: " + start.ToString("O"));
            int c = CurrentData.Count();
            while (CurrentData.Count > 0 && CurrentData[0].RawDate < start)
            {
                CurrentData.RemoveAt(0);
            }
            int removed = c - CurrentData.Count();
            Debug("TrimStart removed " + removed + " rows, adjusted start is now " + StartTime.ToString("O"));

            StartTime = CurrentData[0].RawDate;

            return removed;
        }

        /// <summary>
        /// Cached data will identify when it is used, so the eviction mechanism
        /// can avoid removing active data.
        /// </summary>
        public List<DateTime> CacheUsage = new List<DateTime>();

        /// <summary>
        /// actual cached data.
        /// </summary>
        public List<CachedRow> CurrentData { get; set; }

        /// <summary>
        /// Start time of the cache
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// End time of the cached data
        /// </summary>
        public DateTime EndTime { get; set; }
    }
}
