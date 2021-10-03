using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{
    /// <summary>
    /// Interface for Slogger, defining common level method helpers.
    /// </summary>
    public interface ISLogger
    {
        void AddRecord(string component, string message, DateTime timestamp, LogLevel level);
        void Flush();
        List<SLogRecord> GetAllRecords(bool clear);

        void VTrace(string component, string message);
        void VTrace(string component, string messageFormat, params object[] args);
        void Trace(string component, string message);
        void Trace(string component, string messageFormat, params object[] args);
        void Debug(string component, string message);
        void Debug(string component, string messageFormat, params object[] args);
        void Error(string component, string message);
        void Error(string component, string messageFormat, params object[] args);
        void Critical(string component, string message);
        void Critical(string component, string messageFormat, params object[] args);
    }
}
