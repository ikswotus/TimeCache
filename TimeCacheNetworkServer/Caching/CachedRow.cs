using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer.Caching
{
    /// <summary>
    /// Pair the 'translated' row with the datetime.
    /// Having the row in the DataRowMessage format simplifies the return process,
    /// but the date makes caching simpler.
    /// </summary>
    public class CachedRow
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public CachedRow()
        {

        }


        /// <summary>
        /// Allow simple filtering of a row
        /// </summary>
        /// <param name="index"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool Filter(int index, object value)
        {
            if (index > Objects.Length)
                throw new IndexOutOfRangeException("Requested index: " + index + " exceeds row length: " + Objects.Length);

            return Objects[index].Equals(value);
        }

        /// <summary>
        /// Store 'row' objects
        /// </summary>
        public object[] Objects { get; set; }

        /// <summary>
        /// Date of the row
        /// </summary>
        public DateTime RawDate { get; set; }

        /// <summary>
        /// PG-ified data.
        /// </summary>
        public PostgresqlCommunicator.DataRowMessage TranslatedMessage { get; set; }
    }
}
