using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{



    /// <summary>
    /// Record definition of a log message
    /// </summary>
    public class SLogRecord
    {
        public SLogRecord(string component, string message, DateTime timestamp, LogLevel level)
        {
            Component = component;
            Message = message;
            Timestamp = timestamp;
            Level = level;
        }

        public SLogRecord()
        {
            Component = String.Empty;
            Message = String.Empty;
            Timestamp = DateTime.UtcNow;
            Level = LogLevel.VTRACE;
        }

        public string Component { get; private set; }
        public string Message { get; private set; }
        public DateTime Timestamp { get; private set; }
        public LogLevel Level { get; private set; }

    }

    public enum LogLevel
    {
        VTRACE,
        TRACE,
        DEBUG,
        ERROR,
        CRITICAL
    }
}
