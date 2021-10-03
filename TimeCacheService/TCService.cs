using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;

using System.Threading;
using System.IO;


namespace TimeCacheService
{
    /// <summary>
    /// Windows service to run the time cache
    /// </summary>
    public partial class TCService : ServiceBase
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public TCService()
        {
            InitializeComponent();
        }

        /// <summary>
        /// Log shared with all subcomponents
        /// </summary>
        private SLog.SLogger _slog = null;

        /// <summary>
        /// Stop flag
        /// </summary>
        private volatile bool _stop = false;

        /// <summary>
        /// Actual server
        /// </summary>
        private TimeCacheNetworkServer.NetworkServer _server = null;

        /// <summary>
        /// Stat/Log thread
        /// </summary>
        private Thread _runThread = null;

        /// <summary>
        /// Called on service start
        /// Initialize threads, start network server
        /// </summary>
        /// <param name="args"></param>
        protected override void OnStart(string[] args)
        {
            _slog = new SLog.SLogger("TimeCacheService_" + Environment.MachineName);
            _slog.Debug("TCService", "OnStart() called. Launching new server.");

            _runThread = new Thread(() => RunLoop());
            _runThread.Start();

            _server = new TimeCacheNetworkServer.NetworkServer(TCSettings.Default.ConnectionString, TCSettings.Default.Port, _slog);
            _server.Start();
        }

        /// <summary>
        /// Service stopped
        /// </summary>
        protected override void OnStop()
        {
            _slog.Debug("TCService", "OnStop() called. Shutting down server");

            HandleStop();

        }

        /// <summary>
        /// System is shutting down
        /// </summary>
        protected override void OnShutdown()
        {
            base.OnShutdown();
            _slog.Debug("TCService", "OnShutdown() called. Shutting down server");
            
            HandleStop();
        }

        /// <summary>
        /// Perform service shutdown procedure
        /// </summary>
        private void HandleStop()
        {
            _stop = true;

            if (_server != null)
            {
                _server.Stop();
                _server = null;
            }

            _runThread.Join(5000);
        }

        /// <summary>
        /// Handle stats/logs on periodic basis while the service is running.
        /// </summary>
        private void RunLoop()
        {
            try
            {
                _slog.Debug("TCService", "Run loop starting");

                // Initial setup
                string logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");
                Directory.CreateDirectory(logDir);

                _slog.Debug("TCService", "Starting new server");
                _server = new TimeCacheNetworkServer.NetworkServer(Properties.TimeCacheSettings.Default.ConnectionString, Properties.TimeCacheSettings.Default.Port, _slog);
                _server.Start();

                while (!_stop)
                {
                    for (int i = 0; i < 300 && !_stop; i++)
                        Thread.Sleep(1000);


                    DumpLogs(logDir);

                }
                // Final flush
                DumpLogs(logDir);
            }
            catch(Exception exc)
            {
                _slog.Error("TCService", "Failure in run loop: " + exc.ToString());
            }

        }

        /// <summary>
        /// Flush logs to disk
        /// </summary>
        /// <param name="logDir"></param>
        private void DumpLogs(string logDir)
        {
            try
            {
                List<SLog.SLogRecord> records = _slog.GetAllRecords(true);

                if (records.Count > 0)
                {
                    string logFile = Path.Combine(logDir, "TimeCacheService_" + DateTime.UtcNow.ToString("yyyy_MM_dd HH.mm.ss.fff") + "z.log");

                    new SLog.SLogWrapper(records).WriteXml(logFile);
                }

            }
            catch (Exception exc)
            {
                // Write here too just in case it's an intermittent failure...
                _slog.Error("TCService", "Failed to flush logs to disk: " + exc);

                EventLog.WriteEntry("TCService", "Failure writing logs to disk: " + exc.ToString(), EventLogEntryType.Error);
            }
        }
    }
}
