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
            long expected = messages.Count() * 5 + messages.Sum(m => m.GetLength());
            NetworkMessage nm = new NetworkMessage( expected < 65500 ? 1 : (int)expected / 65500);

            ByteWrapper current = ByteWrapper.Get(65000);

            foreach (PGMessage mess in messages)
            {
                if (mess._completedMessage == null)
                {
                    int req = mess.GetLength() + 4 + 1;
                    if(current.AvailableSpace() < req)
                    {
                        nm.Bytes.Add(current);
                        current = ByteWrapper.Get(65000);
                    }
                    current.Write(mess.MessageType);
                    
                    int encodedLength = mess.GetLength() + 4;

                    current.Write((byte)((encodedLength & 0xFF000000) >> 24));
                    current.Write((byte)((encodedLength & 0x00FF0000) >> 16));
                    current.Write((byte)((encodedLength & 0x0000FF00) >> 8));
                    current.Write((byte)((encodedLength & 0x000000FF)));

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
    /// Handle reading client messages
    /// </summary>
    public static class MessageParser
    {
        /// <summary>
        /// Consume a null terminated string.
        /// </summary>
        /// <param name="buffer">Source data</param>
        /// <param name="index">Current position</param>
        /// <param name="maxLength">Last index to allow data, may be less than buffer.Length</param>
        /// <returns></returns>
        public static string ReadString(byte[] buffer, ref int index, int maxLength)
        {
            int nullTermIndex = -1;
            for (int i = index; i < maxLength; i++)
            {
                if (buffer[i] == 0x00)
                {
                    nullTermIndex = i;
                    break;
                }
            }
            if (nullTermIndex == -1)
                throw new Exception("Failed to locate column name null terminator");

            

            string ret = Encoding.ASCII.GetString(buffer, index, nullTermIndex - index);
            index = nullTermIndex + 1;
            return ret;
        }

        /// <summary>
        /// Convert 4 byte field into an integer.
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static int ReadInt(byte[] buffer, ref int index)
        {
            int ret = (buffer[index] << 24 | buffer[index + 1] << 16 | buffer[index + 2] << 8 | buffer[index + 3]);
            index += 4;
            return ret;
        }

        /// <summary>
        /// Convert 2 byte field into a short
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static short ReadShort(byte[] buffer, ref int index)
        {
            short s = (short)(buffer[index] << 8 | buffer[index + 1]);
            index += 2;
            return s;
        }

        public static void WriteInt(byte[] buffer, ref int index, int value)
        {
            buffer[index] = (byte)((value & 0xFF000000) >> 24);
            buffer[index + 1] = (byte)((value & 0x00FF0000) >> 16);
            buffer[index + 2] = (byte)((value & 0x0000FF00) >> 8);
            buffer[index + 3] = (byte)(value & 0x000000FF);
            index += 4;
        }

        public static void WriteShort(byte[] buffer, ref int index, short value)
        {
            buffer[index] = (byte)((value & 0xFF00) >> 8);
            buffer[index + 1] = (byte)(value & 0x00FF);
            index += 2;
        }

        public static void WriteBytes(byte[] buffer, ref int index, byte[] source, int count)
        {
            if (index + count > buffer.Length)
            {
                throw new Exception(String.Format("Unable to copy buffer, size requested exceeds dest buffer. Index={0}, Count={1}, buff.Length={2}", index, count, buffer.Length));
            }
            Buffer.BlockCopy(source, 0, buffer, index, count);
            index += count;
        }
    }

    public class StartupMessage : PGMessage
    {
        public StartupMessage()
        {
            MajorVersion = 3;
            MinorVersion = 0;
            Params = new List<StartupParam>();
        }

        public static StartupMessage ParseMessage(byte[] buffer, int index)
        {
            StartupMessage sm = new StartupMessage();

            sm.Length = MessageParser.ReadInt(buffer, ref index);
            if (buffer.Length < sm.Length)
                throw new Exception("Length does not match startup packet length.");
            sm.MajorVersion = MessageParser.ReadShort(buffer, ref index);
            sm.MinorVersion = MessageParser.ReadShort(buffer, ref index);
            // TODO: For now only allow 3.0.
            // TODO: Make this a configuration option? 3.1 may 'just work'
            if (sm.MajorVersion != 3 || sm.MinorVersion != 0)
                throw new Exception("Only protocol verion 3.0 is currently supported");
           
            while(index < sm.Length - 1)
            {
                StartupParam p = new StartupParam();
                p.Name = MessageParser.ReadString(buffer, ref index, sm.Length);
                p.Value = MessageParser.ReadString(buffer, ref index, sm.Length);
                sm.Params.Add(p);
            }
            if(index != sm.Length-1 || buffer[index] != 0x00)
            {
                throw new Exception("Failed to locate null terminator of startup message");
            }

            return sm;
        }

        public override byte[] GetMessageBytes()
        {
            int paramSize = Params.Sum(p => p.Name.Length + p.Value.Length + 2);

            Length = 8 + paramSize + 1;

            byte[] ret = new byte[Length];
            int index = 0;
            
            MessageParser.WriteInt(ret, ref index, Length);
            MessageParser.WriteShort(ret, ref index, MajorVersion);
            MessageParser.WriteShort(ret, ref index, MinorVersion);
            foreach(StartupParam sp in Params)
            {
                byte[] bytes = Encoding.UTF8.GetBytes(sp.Name);
                Buffer.BlockCopy(bytes, 0, ret, index, bytes.Length);
                index += bytes.Length;
                ret[index++] = 0x00;
                bytes = Encoding.UTF8.GetBytes(sp.Value);
                Buffer.BlockCopy(bytes, 0, ret, index, bytes.Length);
                index += bytes.Length;
                ret[index++] = 0x00;
            }
            ret[index++] = 0x00;

            return ret;
        }

        public short MajorVersion { get; set; }
        public short MinorVersion { get; set; }

        /// <summary>
        /// Parameters: should include db, username, etc
        /// </summary>
        public List<StartupParam> Params { get; set; }
    }

    public class StartupParam
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }

    public class PGField
    {
        public PGField()
        {

        }

        public static PGField BuildField(byte[] data)
        {
            PGField f = new PGField()
            {
                Data = data,
                ColumnLength = data.Length
            };
            return f;
        }

        public int ColumnLength;
        
        public byte[] Data;
    }

    public class RowDescriptionField
    {
        public RowDescriptionField(string columnName)
        {
            ColumnName = columnName;
            TypeModifier = -1;

            TableOID = 0;
            TypeModifier = -1;
            Format = (short)FieldFormats.Text;
        }

        public RowDescriptionField(string columnName, int typeOid, short length)
        {
            ColumnName = columnName;

            TypeOID = typeOid;
            ColumnLength = length;
            
            TableOID = 0;
            TypeModifier = -1;

            Format = (short)FieldFormats.Text;
        }

        public RowDescriptionField(byte[] buffer, int index, int length)
        {
            if (length <= 0)
                throw new Exception("invalid length");

            int nullTermIndex = -1;
            for (int i =0; i< length;i++)
            {
                if(buffer[index + i] == 0x00)
                {
                    nullTermIndex = i;
                    break;
                }
            }
            if (nullTermIndex == -1)
                throw new Exception("Failed to locate column name null terminator");
            ColumnName = Encoding.ASCII.GetString(buffer, index, nullTermIndex - index);
            // Remaining length should be 18
            nullTermIndex++;
            length -= (nullTermIndex - index);
            if (length != 18)
                throw new Exception("INvalid index after identifying column name");
            TableOID = MessageParser.ReadInt(buffer,ref  nullTermIndex);
            ColumnIndex = MessageParser.ReadShort(buffer, ref nullTermIndex);
            TypeOID = MessageParser.ReadInt(buffer, ref nullTermIndex);
            ColumnLength = MessageParser.ReadShort(buffer, ref nullTermIndex);           
            TypeModifier = MessageParser.ReadInt(buffer, ref nullTermIndex);
            Format = MessageParser.ReadShort(buffer, ref nullTermIndex);
            
        }

        public string ColumnName { get; set; }
        public int TableOID { get; set; }
        public short ColumnIndex { get; set; }
        public int TypeOID { get; set; }
        public short ColumnLength { get; set; }
        public int TypeModifier { get; set; }
        public short Format { get; set; }

        /// <summary>
        /// 18+ ColumnName length
        /// </summary>
        /// <returns></returns>
        public int GetLength()
        {
            return 4 + 2 + 4 + 2 + 4 + 2 + ColumnName.Length;
        }

        public void CopyToBuffer(byte[] dest, ref int index)
        {
            if (dest.Length - index < GetLength())
                throw new Exception("Not enough space to copy to dest");
            byte[] columnName = Encoding.ASCII.GetBytes(ColumnName);
            Buffer.BlockCopy(columnName, 0, dest, index, columnName.Length);
            index += columnName.Length;

            MessageParser.WriteInt(dest, ref index, TableOID);
            MessageParser.WriteShort(dest, ref index, ColumnIndex);           
            MessageParser.WriteInt(dest, ref index, TypeOID);
            MessageParser.WriteShort(dest, ref index, ColumnLength);
            MessageParser.WriteInt(dest, ref index, TypeModifier);
            MessageParser.WriteShort(dest, ref index, Format);
        }


    }

    /// <summary>
    ///  Types for fields
    /// </summary>
    public enum FieldFormats
    {
        Text = 0, // 0x00 0x00
    }


    public abstract class PGMessage
    {
        public byte MessageType { get; set; }
        protected int Length { get; set; }

        public byte[] _completedMessage = null;

        public virtual int GetLength()
        {
            return Length;
        }

        public byte[] GetMessagePayload()
        {
            byte[] payload = new byte[5 + GetLength()];
            payload[0] = MessageType;
            int index = 1;
            MessageParser.WriteInt(payload, ref index, GetLength() + 4);
            MessageParser.WriteBytes(payload, ref index, GetMessageBytes(), GetLength());

            return payload;
        }

        public abstract byte[] GetMessageBytes();
    }

    /// <summary>
    /// Contains column descriptions for subsequent rows.
    /// </summary>
    public class RowDescription : PGMessage
    {
        /// <summary>
        /// Default constructor.
        /// </summary>
        public RowDescription(int fieldCount)
        {
            MessageType = PGTypes.RowDescription;
            Fields = new List<RowDescriptionField>(fieldCount);
            OriginalTypes = new List<Type>(fieldCount);
        }

        public List<RowDescriptionField> Fields { get; set; }

        public List<Type> OriginalTypes { get; set; }

        public override int GetLength()
        {
            return Fields.Sum(f => f.GetLength()) + 2;
        }

        public override byte[] GetMessageBytes()
        {
            int totalSize = Fields.Sum(f => f.GetLength());
            totalSize += 2; // Field Count
            byte[] b = new byte[totalSize];
            int index = 0;
            MessageParser.WriteShort(b, ref index, (short)Fields.Count());

            foreach(RowDescriptionField rdf in Fields)
            {
                rdf.CopyToBuffer(b, ref index);
            }

            return b;
        }
    }

    /// <summary>
    /// TODO: Readonly
    /// </summary>
    public class ReadyForQuery : PGMessage
    {
        public ReadyForQuery()
        {
            MessageType = PGTypes.ReadyForQuery;

            Length = 1;
        }

        private static readonly byte[] _readyByte = new byte[] { 0x49 };

        public override byte[] GetMessageBytes()
        {
            return _readyByte;
        }
    }

    /// <summary>
    /// Indicate query success
    /// </summary>
    public class CommandCompletion : PGMessage
    {
        /// <summary>
        /// Command completed constructor
        /// </summary>
        /// <param name="tag">Query (Full?)</param>
        public CommandCompletion(string tag):  base()
        {
            MessageType = PGTypes.CommandCompletion;
            Tag = tag;
            if(Tag[Tag.Length - 1] != '\0')
            {
                Tag += '\0';
            }
            Length = Tag.Length;
        }
        public string Tag { get; set; }

        public override byte[] GetMessageBytes()
        {
            return Encoding.ASCII.GetBytes(Tag);
        }
    }

    public class SimpleQuery : PGMessage
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="index"></param>
        /// <param name="length"></param>
        public SimpleQuery(byte[] bytes, int index, int length)
        {
            if (bytes[index + length - 1] != 0x00)
                throw new Exception("Query was not null terminated");

            Bytes = new byte[length];
            Buffer.BlockCopy(bytes, index, Bytes, 0, length);

            Query = Encoding.ASCII.GetString(bytes, index, length - 1);
        }

        private byte[] Bytes = null;

        /// <summary>
        /// Query extracted
        /// </summary>
        public string Query { get; set; }

        public override byte[] GetMessageBytes()
        {
            return Bytes;
        }
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

        public override int GetLength()
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


    public class AuthenticationMD5Password : PGMessage
    {
        public AuthenticationMD5Password() : base()
        {
            MessageType = PGTypes.AuthenticationRequest;
            
            Salt = new byte[4];
            
            Length = 8;
        }

        /// <summary>
        /// parse from byte array
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static AuthenticationMD5Password FromBytes(byte[] bytes, int index)
        {
            if (bytes[index] != 0x52)
                throw new Exception("not an authenticationmd5request");
            if(bytes.Length - index < 12)
                throw new Exception("Not enough bytes for authenticationmd5request");
            AuthenticationMD5Password mess = new AuthenticationMD5Password();

            Buffer.BlockCopy(bytes, index + 9, mess.Salt, 0, 4);

            return mess;
        }

        /// <summary>
        /// Always 5 for md5
        /// </summary>
        public static readonly int AuthenticationType = 5;

        /// <summary>
        /// Random bytes
        /// </summary>
        public byte[] Salt { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override byte[] GetMessageBytes()
        {
            byte[] bytes = new byte[8];
            int index = 0;
            MessageParser.WriteInt(bytes, ref index, AuthenticationType);
            MessageParser.WriteBytes(bytes, ref index, Salt, Salt.Length);
            return bytes;
        }
    }

    public class AuthenticationMD5Response
    {
        public AuthenticationMD5Response()
        {

        }

        public static AuthenticationMD5Response FromBytes(byte[] bytes, int index)
        {
            if (bytes[index] != 0x70)
                throw new Exception("not a password message");

            if (bytes.Length < 35 || index + 35 > bytes.Length)
                throw new Exception("Not enough bytes for md5 response.");
            
            // First 3 should be 'md5'
            if(bytes[index+5] != 0x6D || // 'm'
               bytes[index+6] != 0x64 || // 'd'
               bytes[index+7] != 0x35)   // '5'
            {
                throw new Exception("Not md5");
            }
            index += 8;

            AuthenticationMD5Response amr = new AuthenticationMD5Response();
            amr.EncryptedDigest =  new byte[32];
            Buffer.BlockCopy(bytes, index, amr.EncryptedDigest, 0, 32);

            return amr;
        }

        public byte[] EncryptedDigest { get; set; }
    }

    public class AuthenticationOK : PGMessage
    {
        /// <summary>
        /// Always 0
        /// </summary>
        private static readonly byte[] _okBytes = new byte[4]{0x00, 0x00, 0x00, 0x00};

        
        public AuthenticationOK()
        {
            MessageType = 0x52; // 'R'
            
            Length = 4;
        }


        public override byte[] GetMessageBytes()
        {
            return _okBytes;
        }
    }


    public class PasswordMessage : PGMessage
    {
        public PasswordMessage()
        {
            MessageType = PGTypes.PasswordMessage;

        }

        public static PasswordMessage GetPasswordMessage(string pass, string user_name, byte[] salt)
        {
            
            byte[] userbytes = Encoding.UTF8.GetBytes(user_name);
            byte[] passbytes = Encoding.UTF8.GetBytes(pass);

            byte[] cryptBuf = new byte[passbytes.Length + userbytes.Length];

            passbytes.CopyTo(cryptBuf, 0);
            userbytes.CopyTo(cryptBuf, passbytes.Length);


            MD5 m = MD5.Create();
            byte[] hash1 = m.ComputeHash(cryptBuf);

            StringBuilder sb = new StringBuilder();
            foreach (byte b in hash1)
                sb.Append(b.ToString("x2"));
            string prehash = sb.ToString();
           // Console.WriteLine("Hash1: " + prehash);


            byte[] prehashbytes = Encoding.UTF8.GetBytes(prehash);


            // Raw bytes
            cryptBuf = new byte[prehashbytes.Length + 4];


            Buffer.BlockCopy(salt, 0, cryptBuf, prehashbytes.Length, 4);
            prehashbytes.CopyTo(cryptBuf, 0);

            //                Buffer.BlockCopy(prehashbytes, 0, cryptBuf, 0, prehashbytes.Length);

            byte[] hash2 = m.ComputeHash(cryptBuf);
            sb.Clear();
            sb.Append("md5");

            foreach (byte b in hash2)
                sb.Append(b.ToString("x2"));

            string computedHash2 = sb.ToString();
          //  Console.WriteLine("Hash RR: " + computedHash2);

            PasswordMessage pm = new PasswordMessage();

            pm.Hash = new byte[computedHash2.Length];

            Encoding.UTF8.GetBytes(computedHash2, 0, computedHash2.Length, pm.Hash, 0);

            
            return pm;
        }

        public byte[] Hash { get; set; }

        public override byte[] GetMessageBytes()
        {
            Length = 40;

            byte[] b = new byte[Length + 1];
            b[0] = MessageType;
            
            int index = 1;
            MessageParser.WriteInt(b, ref index, Length);

            MessageParser.WriteBytes(b, ref index, Hash, Hash.Length);

            b[b.Length - 1] = 0x00;

            return b;
        }
    }

   // public c

}
