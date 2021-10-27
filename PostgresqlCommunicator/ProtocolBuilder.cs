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

                    mess.WriteTo(current);
                    //current.Write(mess.MessageType);
                    
                    //int encodedLength = mess.GetLength();

                    //current.Write((byte)((encodedLength & 0xFF000000) >> 24));
                    //current.Write((byte)((encodedLength & 0x00FF0000) >> 16));
                    //current.Write((byte)((encodedLength & 0x0000FF00) >> 8));
                    //current.Write((byte)((encodedLength & 0x000000FF)));

                    //byte[] mb = mess.GetMessageBytes();
                    //if (mb.Length != (encodedLength - 4))
                    //    throw new Exception("Invalid length message");

                    //current.Write(mess.GetMessageBytes());
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


}
