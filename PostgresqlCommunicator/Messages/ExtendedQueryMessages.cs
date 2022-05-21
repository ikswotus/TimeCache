using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator.Messages
{
     public class ParseExtMessage : PGMessage
    {
        public ParseExtMessage(byte[] bytes, ref int index, int length)
        {
            Statement = MessageParser.ReadString(bytes, ref index, length);
            Query = MessageParser.ReadString(bytes, ref index, length);
            Params = MessageParser.ReadShort(bytes, ref index);
        }
        public short Params { get; set; }
        public string Query { get; set; }
        public string Statement { get; set; }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }
        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }
    }
}
