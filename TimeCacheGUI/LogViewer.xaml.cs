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
using System.Windows.Shapes;

using System.Collections.ObjectModel;

namespace TimeCacheGUI
{
    /// <summary>
    /// Interaction logic for LogViewer.xaml
    /// </summary>
    public partial class LogViewer : Window
    {
        public LogViewer(ILogRefresh source, List<SLog.SLogRecord> records)
        {
            InitializeComponent();

            LogGrid.DataContext = this;
            

            foreach(SLog.SLogRecord r in records)
            {
                RecordCollection.Add(r);
            }
            LogGrid.ItemsSource = RecordCollection;

            _refresher = source;
        }

        private System.Collections.ObjectModel.ObservableCollection<SLog.SLogRecord> RecordCollection = new ObservableCollection<SLog.SLogRecord>();

        private ILogRefresh _refresher = null;

        private void Refresh_Click(object sender, RoutedEventArgs e)
        {
            foreach (SLog.SLogRecord r in _refresher.GetRecords())
            {
                RecordCollection.Add(r);
            }
        }

        private void Clear_Click(object sender, RoutedEventArgs e)
        {
            RecordCollection.Clear();
        }
    }
}
