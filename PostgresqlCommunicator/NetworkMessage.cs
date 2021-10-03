using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Translated PGMessages into byte[] for sending over the network.
    /// </summary>
    public class NetworkMessage
    {
        public NetworkMessage()
        {
            Bytes = new List<ByteWrapper>();
        }

        public NetworkMessage(int count)
        {
            Bytes = new List<ByteWrapper>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public long Send(Socket s)
        {
            long total = 0;
            foreach (ByteWrapper bw in Bytes)
            {
                total += bw.Send(s);
            }

            foreach (ByteWrapper bw in Bytes)
            {
                ByteWrapper.Reset(bw);
            }
            Bytes.Clear();

            return total;
        }

        public List<ByteWrapper> Bytes { get; set; }
    }
}
