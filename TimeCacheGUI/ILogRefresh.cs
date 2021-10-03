using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheGUI
{
    public interface ILogRefresh
    {
        List<SLog.SLogRecord> GetRecords();
    }
}
