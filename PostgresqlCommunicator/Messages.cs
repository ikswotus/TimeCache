using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Encapsulates error messages
    /// </summary>
    public class ErrorResponseMessage : PGMessage
    {
        public ErrorResponseMessage()
        {

        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }
        public override byte[] GetMessageBytes()
        {
            return null;
        }

        /// <summary>
        /// Ugh - For now just dump the entire message as a blob
        /// </summary>
        public string ErrorText { get; set; }
    }

    /// <summary>
    /// A startup message for initiating a connection
    /// </summary>
    public class StartupMessage : PGMessage
    {
        public StartupMessage()
        {
            MajorVersion = 3;
            MinorVersion = 0;
            Params = new List<StartupParam>();
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
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

            while (index < sm.Length - 1)
            {
                StartupParam p = new StartupParam();
                p.Name = MessageParser.ReadString(buffer, ref index, sm.Length);
                p.Value = MessageParser.ReadString(buffer, ref index, sm.Length);
                sm.Params.Add(p);
            }
            if (index != sm.Length - 1 || buffer[index] != 0x00)
            {
                throw new Exception("Failed to locate null terminator of startup message");
            }

            return sm;
        }

        protected override int GetPayloadLength()
        {
            int paramSize = Params.Sum(p => p.Name.Length + p.Value.Length + 2);

            return 4 + paramSize + 1;
        }

        public override byte[] GetMessageBytes()
        {
            int length = GetPayloadLength();

            byte[] ret = new byte[length];
            
            int index = 0;
            MessageParser.WriteShort(ret, ref index, MajorVersion);
            MessageParser.WriteShort(ret, ref index, MinorVersion);
            foreach (StartupParam sp in Params)
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

        public static PGField FromBuffer(byte[] buffer, ref int index, int length)
        {
            PGField f = new PGField();
            int l = MessageParser.ReadInt(buffer, ref index);
            f.ColumnLength = l;
            if (l > length - 4)
                throw new Exception("Invalid length");
            f.Data = new byte[l];
            Buffer.BlockCopy(buffer, index, f.Data, 0, l);
            index += l;
            return f;
        }

        public object As(Type t)
        {
            string s = Encoding.UTF8.GetString(Data);
            if (t == typeof(string))
                return s;
            else if (t == typeof(DateTime))
                return DateTime.Parse(s);
            else if (t == typeof(float))
                return float.Parse(s);
            throw new Exception("Unsupported type: " + t);
        }



        public int ColumnLength;

        public byte[] Data;
    }

    public class RowDescriptionField
    {
        private RowDescriptionField()
        {
        }

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
            for (int i = 0; i < length; i++)
            {
                if (buffer[index + i] == 0x00)
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
            TableOID = MessageParser.ReadInt(buffer, ref nullTermIndex);
            ColumnIndex = MessageParser.ReadShort(buffer, ref nullTermIndex);
            TypeOID = MessageParser.ReadInt(buffer, ref nullTermIndex);
            ColumnLength = MessageParser.ReadShort(buffer, ref nullTermIndex);
            TypeModifier = MessageParser.ReadInt(buffer, ref nullTermIndex);
            Format = MessageParser.ReadShort(buffer, ref nullTermIndex);

        }

        public static RowDescriptionField FromBuffer(byte[] buffer, ref int index, int length)
        {
            if (length <= 0)
                throw new Exception("invalid length");

            RowDescriptionField rdf = new RowDescriptionField();

            int nullTermIndex = -1;
            for (int i = 0; i < length; i++)
            {
                if (buffer[index + i] == 0x00)
                {
                    nullTermIndex = index + i;
                    break;
                }
            }
            if (nullTermIndex == -1)
                throw new Exception("Failed to locate column name null terminator");
            rdf.ColumnName = Encoding.ASCII.GetString(buffer, index, nullTermIndex - index);
            // Remaining length should be 18
            nullTermIndex++;
            length -= (nullTermIndex - index);
            if (length < 18)
                throw new Exception("INvalid index after identifying column name");
            rdf.TableOID = MessageParser.ReadInt(buffer, ref nullTermIndex);
            rdf.ColumnIndex = MessageParser.ReadShort(buffer, ref nullTermIndex);
            rdf.TypeOID = MessageParser.ReadInt(buffer, ref nullTermIndex);
            rdf.ColumnLength = MessageParser.ReadShort(buffer, ref nullTermIndex);
            rdf.TypeModifier = MessageParser.ReadInt(buffer, ref nullTermIndex);
            rdf.Format = MessageParser.ReadShort(buffer, ref nullTermIndex);

            index = nullTermIndex;

            return rdf;
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

        private  RowDescription()
        {
            MessageType = PGTypes.RowDescription;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }


        public List<RowDescriptionField> Fields { get; set; }

        public List<Type> OriginalTypes { get; set; }

        protected override int GetPayloadLength()
        {
            return Fields.Sum(f => f.GetLength()) + 2;
        }

        public static RowDescription FromBuffer(byte[] buffer, int index, int length)
        {
            RowDescription rd = new RowDescription();

            short fieldCount = MessageParser.ReadShort(buffer, ref index);
            rd.Fields = new List<RowDescriptionField>(fieldCount);
            for (int i = 0; i < fieldCount; i++)
            {
                rd.Fields.Add(RowDescriptionField.FromBuffer(buffer, ref index, length));
            }

            return rd;
        }

        

        public override byte[] GetMessageBytes()
        {
            int totalSize = GetPayloadLength();
            byte[] b = new byte[totalSize];
            
            int index = 0;
            MessageParser.WriteShort(b, ref index, (short)Fields.Count());

            foreach (RowDescriptionField rdf in Fields)
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
        }

        private static readonly byte[] _readyByte = new byte[] { 0x49 };

        public static ReadyForQuery FromBuffer(byte[] buffer, int index, int length)
        {
            if (length != 1)
                throw new Exception("Unsupported length for status: " + length);
            ReadyForQuery refq = new ReadyForQuery();
            refq.Status = buffer[index];
            index++;
            return refq;
        }

        protected override int GetPayloadLength()
        {
            return 1;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            dest.Write(Status);
        }

        public byte Status { get; set; }

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
        public CommandCompletion(string tag) : base()
        {
            MessageType = PGTypes.CommandCompletion;
            Tag = tag;
            Length = Tag.Length + _baseLength;
        }
        public CommandCompletion() : base()
        { }

        /// <summary>
        /// Null terminated tag string.
        /// </summary>
        public string Tag { get; set; }


        public override byte[] GetMessageBytes()
        {
            return GetStringBytes(Tag);
        }

        protected override int GetPayloadLength()
        {
            return GetEncodedStringLength(Tag);
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            dest.Write(GetStringBytes(Tag));
            
        }

        public static CommandCompletion FromBuffer(byte[] bytes, int index, int length)
        {
            CommandCompletion cc = new CommandCompletion();

            cc.Tag = PGMessage.ParseNullTerminatedString(bytes, length, ref index, "Tag");

            return cc;
        }
    }


    /// <summary>
    /// Simple querey
    /// </summary>
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

            //Bytes = new byte[length];
            //Buffer.BlockCopy(bytes, index, Bytes, 0, length);

            Query = Encoding.ASCII.GetString(bytes, index, length - 1);
        }

        public SimpleQuery(string s)
        {
            MessageType = PGTypes.SimpleQuery;
            Query = s;


        }
        protected override int GetPayloadLength()
        {          
           return GetEncodedStringLength(Query);
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            dest.Write(GetStringBytes(Query));
        }

        /// <summary>
        /// Query extracted
        /// </summary>
        public string Query { get; set; }


        public override byte[] GetMessageBytes()
        {
            int messLength = GetPayloadLength();
            byte[] bytes = new byte[messLength];

            byte[] b = Encoding.UTF8.GetBytes(EnsureNullTerminated(Query));
            Buffer.BlockCopy(b, 0, bytes, 0, b.Length);

            return bytes;
        }
    }

    /// <summary>
    /// Deal with unhandled messages by storing the raw byttes
    /// </summary>
    public class UnhandledMessage : PGMessage
    {

        public UnhandledMessage(byte type, int length)
        {
            MessageType = type;
            Length = length;
        }

        public byte[] ConsumedBytes { get; set; }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }
    }


}
