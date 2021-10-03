using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


using System.IO;
using System.Xml.Serialization;

namespace SLog
{
    /// <summary>
    /// Helper class for saving/loading logs to disk as xml via serialization
    /// </summary>
    public class SLogWrapper
    {
        public SLogWrapper()
        {
            Records = new List<SLogRecord>();
        }

        public SLogWrapper(List<SLog.SLogRecord> records)
        {
            Records = records;
        }

        public void WriteXml(string file)
        {
            using (FileStream fs = File.Create(file))
            {
                XmlSerializer xs = new XmlSerializer(typeof(SLogWrapper));
                xs.Serialize(fs, this);
            }
        }

        public static SLogWrapper FromXmlFile(string file)
        {
            using (FileStream fs = File.OpenRead(file))
            {
                XmlSerializer xs = new XmlSerializer(typeof(SLogWrapper));
                return (SLogWrapper)xs.Deserialize(fs);
            }
        }

        public List<SLog.SLogRecord> Records { get; set; }
    }
}
