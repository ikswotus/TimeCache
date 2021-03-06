using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{
    /// <summary>
    /// Simple logger, but writes all messages to the console.
    ///
    /// Intended for debugging/local testing only
    /// </summary>
    public class ConsoleSLogger : ISLogger
    {
        public void AddRecord(string component, string message, DateTime timestamp, LogLevel level)
        {
            Console.WriteLine(timestamp.ToString("O") + "[" + component + "](" + level.ToString() + ") " + message);
        }
        public void Flush() { }
        public List<SLogRecord> GetAllRecords(bool clear)
        {
            return new List<SLogRecord>();
        }

        public void VTrace(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.VTRACE);
        }
        public void VTrace(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.VTRACE);
        }
        public void Trace(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.TRACE);
        }
        public void Trace(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.TRACE);
        }
        public void Debug(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.DEBUG);
        }
        public void Debug(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.DEBUG);
        }
        public void Error(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.ERROR);
        }
        public void Error(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.ERROR);
        }
        public void Critical(string component, string message)
        {
            AddRecord(component, message, DateTime.UtcNow, LogLevel.CRITICAL);
        }
        public void Critical(string component, string messageFormat, params object[] args)
        {
            AddRecord(component, String.Format(messageFormat, args), DateTime.UtcNow, LogLevel.CRITICAL);
        }
    }
}
