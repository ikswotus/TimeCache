using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net.WebSockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Schema;
using System.Security.Cryptography;
using Utils;

using System.Net.Sockets;
using System.IO;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Handles building a response message
    /// </summary>
    public abstract class ProtocolBuilder
    {
        /// <summary>
        /// Prepares PGMessages for transport over a socket by converting them to a byte[].
        /// 
        /// TODO: Allow streaming, by writing PGMessages directly to a socket to avoid needing
        /// large byte[] in the first place...
        /// 
        /// </summary>
        /// <param name="messages"></param>
        /// <returns></returns>
        public static NetworkMessage BuildResponseMessage(IEnumerable<PGMessage> messages)
        {
            long expected = messages.Count() + messages.Sum(m => m.GetLength());
            NetworkMessage nm = new NetworkMessage( expected < 65500 ? 1 : (int)expected / 65500);

            ByteWrapper current = ByteWrapper.Get(65000);

            foreach (PGMessage mess in messages)
            {
                if (mess._completedMessage == null)
                {
                    int req = mess.GetLength() + 1;
                    if(current.AvailableSpace() < req)
                    {
                        nm.Bytes.Add(current);
                        current = ByteWrapper.Get(65000);
                    }
                    current.Write(mess.MessageType);
                    
                    int encodedLength = mess.GetLength();

                    current.Write((byte)((encodedLength & 0xFF000000) >> 24));
                    current.Write((byte)((encodedLength & 0x00FF0000) >> 16));
                    current.Write((byte)((encodedLength & 0x0000FF00) >> 8));
                    current.Write((byte)((encodedLength & 0x000000FF)));

                    byte[] mb = mess.GetMessageBytes();
                    if (mb.Length != (encodedLength - 4))
                        throw new Exception("Invalid length message");

                    current.Write(mess.GetMessageBytes());
                }
                else
                {
                    if (current.AvailableSpace() < mess._completedMessage.Length)
                    {
                        nm.Bytes.Add(current);
                        current = ByteWrapper.Get(65000);
                    }
                    current.Write(mess._completedMessage);
                }
            }
            if (current.UsedSpace() > 0)
                nm.Bytes.Add(current);
            

            return nm;
        }

        /// <summary>
        /// Single message
        /// </summary>
        /// <param name="message"></param>
        /// <returns></returns>
        public static byte[] BuildResponseMessage(PGMessage message)
        {
            long expected =  5 + message.GetLength();
            byte[] ret = new byte[expected];




            ret[0] = message.MessageType;

            int encodedLength = message.GetLength() + 4;

            ret[1] = (byte)((encodedLength & 0xFF000000) >> 24);
            ret[2] = (byte)((encodedLength & 0x00FF0000) >> 16);
            ret[3] = (byte)((encodedLength & 0x0000FF00) >> 8);
            ret[4] = (byte)((encodedLength & 0x000000FF));

            byte[] b = message.GetMessageBytes();
            Buffer.BlockCopy(b, 0, ret, 5, b.Length);


            return ret;
        }


        // DEPRECATED - Use NetworkMessage since it pools byte[]
        /// <summary>
        /// Converts messages directly to a byte[]
        /// Deprecated in fav
        /// </summary>
        /// <param name="messages"></param>
        /// <returns></returns>
        //public static byte[] BuildResponse(IEnumerable<PGMessage> messages)
        //{
        //    // 5 byte overhead (type + 32bit length) + actual message length
        //    List<byte> response = new List<byte>(messages.Count() * 5 + messages.Sum(m => m.GetLength()));
        //    foreach (PGMessage mess in messages)
        //    {
        //        if (mess._completedMessage == null)
        //        {
        //            response.Add(mess.MessageType);

        //            int encodedLength = mess.GetLength() + 4;

        //            response.Add((byte)((encodedLength & 0xFF000000) >> 24));
        //            response.Add((byte)((encodedLength & 0x00FF0000) >> 16));
        //            response.Add((byte)((encodedLength & 0x0000FF00) >> 8));
        //            response.Add((byte)((encodedLength & 0x000000FF)));

        //            response.AddRange(mess.GetMessageBytes());
        //        }
        //        else
        //        {
        //            response.AddRange(mess._completedMessage);
        //        }
        //    }


        //    return response.ToArray();
        //}

    }


    /// <summary>
    /// DataRowMessage contains the actual query results.
    /// This will be the bulk of the messages.
    /// To minimize allocations, bytes are pooled.
    /// </summary>
    public class DataRowMessage : PGMessage, IDisposable
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="fieldCount"></param>
        public DataRowMessage(int fieldCount = 1)
        {
            MessageType = PGTypes.DataRow;
            Fields = new List<PGField>(fieldCount);
        }

        private DataRowMessage()
        {

        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            return Fields.Sum(f => f.ColumnLength + 4) + 2;
        }

        /// <summary>
        /// Data
        /// </summary>
        public List<PGField> Fields { get; set; }

        public override byte[] GetMessageBytes()
        {
            int totalSize = Fields.Sum(f => f.ColumnLength + 4);
            totalSize += 2; // Field Count
            byte[] b = new byte[totalSize];
            int index = 0;
            MessageParser.WriteShort(b, ref index, (short)Fields.Count());
            

            foreach (PGField rdf in Fields)
            {
                MessageParser.WriteInt(b, ref index, rdf.ColumnLength);
            
                Buffer.BlockCopy(rdf.Data, 0, b, index, rdf.ColumnLength);
                index += rdf.ColumnLength;
            }

            return b;
        }

        public static DataRowMessage FromBuffer(byte[] buffer, int index, int length)
        {
            short fc = MessageParser.ReadShort(buffer, ref index);
            DataRowMessage drm = new DataRowMessage();
            drm.Fields = new List<PGField>(fc);
            for(int i =0; i< fc; i++)
            {
                int start = index;

                drm.Fields.Add(PGField.FromBuffer(buffer, ref index, length));
                length -= (index - start);
            }


            return drm;
        }


        public int GetCompletedSize()
        {
            int totalSize = Fields.Sum(f => f.ColumnLength + 4);
            totalSize += 2; // Field Count
            totalSize += 5; // Type + length
            return totalSize;
        }

        public void Set(FixedSizeBytePool pool)
        {
            _pool = pool;

            int totalSize = Fields.Sum(f => f.ColumnLength + 4);
            totalSize += 2; // Field Count
            _completedMessage = new byte[totalSize + 5];

            _completedMessage[0] = MessageType;

            int encodedLength = totalSize + 4;

            _completedMessage[1] = (byte)((encodedLength & 0xFF000000) >> 24);
            _completedMessage[2] = (byte)((encodedLength & 0x00FF0000) >> 16);
            _completedMessage[3] = (byte)((encodedLength & 0x0000FF00) >> 8);
            _completedMessage[4] =  (byte)(encodedLength & 0x000000FF);

            int index = 5;
            MessageParser.WriteShort(_completedMessage, ref index, (short)Fields.Count());
            foreach (PGField rdf in Fields)
            {
                MessageParser.WriteInt(_completedMessage, ref index, rdf.ColumnLength);

                Buffer.BlockCopy(rdf.Data, 0, _completedMessage, index, rdf.ColumnLength);
                index += rdf.ColumnLength;
            }
        }

        public void Dispose()
        {
            if (_pool != null)
            {
                //Console.WriteLine("Returning datarowmessage array to cache");
                _pool.Return(_completedMessage);
            }
            _completedMessage = null;
            _pool = null;
        }

        public FixedSizeBytePool _pool = null;
    }



   
}
