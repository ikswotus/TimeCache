using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{
    /// <summary>
    /// Simple Logger to keep a fixed amount of logs in memory.
    /// </summary>
    public class SLogger : ISLogger
    {
        public  SLogger(string owner, int capacity = 2000)
        {
            Owner = owner;
            Records = new CappedQueue<SLogRecord>(capacity);
        }

        public void AddRecord(string component, string message, DateTime timestamp, LogLevel level)
        {
            lock (_recordLock)
                Records.Add(new SLogRecord(component, message, timestamp, level));
        }

        public void Flush()
        {
            lock (_recordLock)
                Records.Clear();
        }

        public List<SLogRecord> GetAllRecords(bool clear = true)
        {
            lock (_recordLock)
                return Records.FlushToList(clear);
        }

        private readonly object _recordLock = new object();

        public readonly string Owner;

        public CappedQueue<SLogRecord> Records { get; private set; }

        #region HelperLevels

        public void VTrace(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.VTRACE);
        }

        public void Trace(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.TRACE);
        }
        public void Debug(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.DEBUG);
        }
        public void Error(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.ERROR);
        }
        public void Critical(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.CRITICAL);
        }

        public void VTrace(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.VTRACE);
        }

        public void Trace(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.TRACE);
        }
        public void Debug(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.DEBUG);
        }
        public void Error(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.ERROR);
        }
        public void Critical(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.CRITICAL);
        }

        #endregion HelperLevels

    }

}
