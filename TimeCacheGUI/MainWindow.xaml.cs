using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;

using System.IO;
using System.Threading;

using System.Collections.ObjectModel;

namespace TimeCacheGUI
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window, ILogRefresh
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public MainWindow()
        {
            InitializeComponent();

            PortTB.Text = Properties.Settings.Default.Port.ToString();

            Connections = new ObservableCollection<TimeCacheNetworkServer.ActiveConnectionInfo>();

            ConnectionGrid.SelectionUnit = DataGridSelectionUnit.FullRow;
            ConnectionGrid.DataContext = this;
            ConnectionGrid.ItemsSource = Connections;

            ConnectionGrid.KeyDown += ConnectionGrid_KeyDown;
            ConnectionGrid.SelectionChanged += ConnectionGrid_SelectionChanged;

            this.Closing += MainWindow_Closing;
            
            _logthread = new Thread(() => DumpLogsToDisk());
            _logthread.Start();


            _flushLogs = FlushLogs.IsChecked.Value;

            FlushLogs.Checked += FlushLogs_Checked;
            FlushLogs.Unchecked += FlushLogs_Unchecked;

        }

        private void FlushLogs_Unchecked(object sender, RoutedEventArgs e)
        {
            _flushLogs = false;
        }

        private volatile bool _flushLogs = false;

        private void FlushLogs_Checked(object sender, RoutedEventArgs e)
        {
            _flushLogs = true;
        }

        /// <summary>
        /// Shutdown
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MainWindow_Closing(object sender, System.ComponentModel.CancelEventArgs e)
        {

            _stop = true;

            if (_server != null)
            {
                _server.Stop();
            }

            if (_logthread != null)
                _logthread.Join();
        }

        private void ConnectionGrid_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if (ConnectionGrid.SelectedItems == null || ConnectionGrid.SelectedItems.Count == 0)
                CloseConnection.IsEnabled = false;
            else
                CloseConnection.IsEnabled = true;
        }

        private void ConnectionGrid_KeyDown(object sender, KeyEventArgs e)
        {
            // Do nothing
        }


        public List<SLog.SLogRecord> GetRecords()
        {
            return _logger.GetAllRecords(true);
        }
        public ObservableCollection<TimeCacheNetworkServer.ActiveConnectionInfo> Connections { get; set; }

        private TimeCacheNetworkServer.NetworkServer _server = null;
        private SLog.SLogger _logger = new SLog.SLogger("TimeCacheGUI");

        /// <summary>
        /// stop flag
        /// </summary>
        private volatile bool _stop = false;

        /// <summary>
        /// Periodically flush logs to disk.
        /// </summary>
        private Thread _logthread = null;




        private void Start_Click(object sender, RoutedEventArgs e)
        {
            if (String.Equals("START", Start.Content.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                if (_server == null)
                {
                    _logger.Debug("MainWindow", "Starting server");

                    int port = 5433;
                    if (String.IsNullOrEmpty(PortTB.Text) || !int.TryParse(PortTB.Text, out port))
                        port = 5433;
                    _logger.Debug("MainWindow", "Chosen port: " + port.ToString());
                    _server = new TimeCacheNetworkServer.NetworkServer(Properties.Settings.Default.DBConnectionString, port, _logger);

                    _server.Start();

                    Start.Content = "Stop";
                }
            }
            else if(String.Equals("STOP", Start.Content.ToString(), StringComparison.OrdinalIgnoreCase))
            {
                if(_server != null)
                {
                    _logger.Debug("MainWindow", "Stopping service");
                    _server.Stop();
                    _logger.Debug("MainWindow", "Service stopped");
                    _server = null;
                }

                Start.Content = "Start";
            }
            else
            {
                _logger.Critical("MainWindow", "Unknown button state for start/stop: " + Start.Content.ToString());
            }
        }

        private void DumpLogsToDisk()
        {
            try
            {
                // Initial setup
                string logDir = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "logs");
                Directory.CreateDirectory(logDir);

                while (!_stop)
                {
                    for (int i = 0; i < 300 && !_stop; i++)
                        Thread.Sleep(1000);

                    if (_stop)
                        break;

                    if (_flushLogs)
                    {
                        List<SLog.SLogRecord> records = _logger.GetAllRecords(true);

                        if (records.Count > 0)
                        {
                            string logFile = Path.Combine(logDir, DateTime.UtcNow.ToString("yyyy_MM_dd HH.mm.ss.fff") + "z.log");
                            new SLog.SLogWrapper(records).WriteXml(logFile);
                        }
                    }
                }

                if (_flushLogs)
                {
                    List<SLog.SLogRecord> records = _logger.GetAllRecords(true);

                    if (records.Count > 0)
                    {
                        string logFile = Path.Combine(logDir, DateTime.UtcNow.ToString("yyyy_MM_dd HH.mm.ss.fff") + "z.log");
                        new SLog.SLogWrapper(records).WriteXml(logFile);
                    }
                }
            }
            catch(Exception exc)
            {
                _logger.Error("MainWindow", "Failed to flush logs to disk: " + exc);
            }
        }


        private void CloseConnection_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if(_server != null && ConnectionGrid.SelectedItems != null)
                {
                    foreach (TimeCacheNetworkServer.ActiveConnectionInfo conn in ConnectionGrid.SelectedItems)
                    {
                        _logger.Debug("MainWindow", "Attempting to kill connection: " + conn.RemoteAddress);
                        _server.KillConnection(conn);
                    }
                }
            }
            catch(Exception exc)
            {
                MessageBox.Show("Failure closing connection: " + exc);
            }
        }

        private void LogBtn_Click(object sender, RoutedEventArgs e)
        {
            List<SLog.SLogRecord> records = _logger.GetAllRecords(true);
            LogViewer lv = new LogViewer(this, records);
            lv.Show();
        }

        private void RefreshConn_Click(object sender, RoutedEventArgs e)
        {
            if(_server != null)
            {
                Connections.Clear();
                foreach (TimeCacheNetworkServer.ActiveConnectionInfo conn in _server.GetConnectionInfo())
                    Connections.Add(conn);
            }
        }

        private void CacheStats_Click(object sender, RoutedEventArgs e)
        {

        }
    }
}
