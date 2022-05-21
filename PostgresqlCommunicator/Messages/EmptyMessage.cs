using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator.Messages
{
    /// <summary>
    /// For messages that dont need params - Just specify the type 
    /// </summary>
    public class EmptyMessage : PGMessage
    {
        public EmptyMessage(byte type)
        {
            this.MessageType = type;
            this.Length = 4;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
        }

        public override byte[] GetMessageBytes()
        {
            return _empty;
        }
        public static byte[] _empty = new byte[0];

        protected override int GetPayloadLength()
        {
            return 0;
        }
    }
}
