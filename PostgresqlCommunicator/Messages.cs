using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Encapsulates error messages
    /// 
    /// Seems to be a series of null terminated strings followed by a null byte
    /// </summary>
    public class ErrorResponseMessage : PGMessage
    {
        public ErrorResponseMessage()
        {
            MessageType = PGTypes.ErrorResponse;

            Text = "ERROR";
        }

        public string Severity { get; set; }
        public string Text { get; set; }
        public string Code { get; set; }
        public string Message { get; set; }
        public string Position { get; set; }
        public string File { get; set; }
        public string Line { get; set; }
        public string Routine { get; set; }
        

        protected override void DoWriteTo(ByteWrapper dest)
        {
            dest.Write(GetStringBytes2("S" + Severity));
            dest.Write(GetStringBytes2("V" + Text));
            dest.Write(GetStringBytes2("C" + Code));
            dest.Write(GetStringBytes2("M" + Message));
            dest.Write(GetStringBytes2("P" + Position));
            dest.Write(GetStringBytes2("F" + File));
            dest.Write(GetStringBytes2("L" + Line));
            dest.Write(GetStringBytes2("R" + Routine));

            dest.Write(_nullByte);
        }

        protected override int GetPayloadLength()
        {
            return GetEncodedStringLength(Severity) + 1 +
                   GetEncodedStringLength(Text) + 1 +
                   GetEncodedStringLength(Code) + 1 +
                   GetEncodedStringLength(Message) + 1 +
                   GetEncodedStringLength(Position) + 1 +
                   GetEncodedStringLength(File) + 1 +
                   GetEncodedStringLength(Line) + 1 +
                   GetEncodedStringLength(Routine) + 1 +
                 1;
        }
        public override byte[] GetMessageBytes()
        {
            int length = GetPayloadLength();

            byte[] ret = new byte[length];

            int index = 0;

            ret[index++] = (byte)'S';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Severity), false);
            ret[index++] = (byte)'V';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Text), false);
            ret[index++] = (byte)'C';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Code), false);
            ret[index++] = (byte)'M';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Message), false);
            ret[index++] = (byte)'P';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Position), false);
            ret[index++] = (byte)'F';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(File), false);
            ret[index++] = (byte)'L';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Line), false);
            ret[index++] = (byte)'R';
            MessageParser.WriteString(ret, ref index, EnsureNullTerminated(Routine), false);

            ret[length - 1] = _nullByte;
            return ret;
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
            //if (sm.MajorVersion != 3 || sm.MinorVersion != 0)
            //    throw new Exception("Only protocol version 3.0 is currently supported: " + sm.MajorVersion + "." + sm.MinorVersion);

            while (index < sm.Length - 1)
            {
                StartupParam p = new StartupParam();
                p.Name = MessageParser.ReadString(buffer, ref index, sm.Length);
                p.Value = MessageParser.ReadString(buffer, ref index, sm.Length);
                sm.Params.Add(p);
            }
            if (sm.Length > 8 && index != sm.Length - 1 || buffer[index] != 0x00)
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

        /// <summary>
        ///  TODO: Make this a base memeber
        /// </summary>
        /// <param name="s"></param>
        public void SendTo(Socket s)
        {
            int length = GetPayloadLength() + 4;

            byte[] ret = new byte[length];

            int index = 0;
            MessageParser.WriteInt(ret, ref index, length);
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

            s.Send(ret);
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
            int l = (data != null) ? data.Length : -1;

            PGField f = new PGField()
            {
                Data = data,
                ColumnLength = l
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

        public RowDescriptionField(string columnName, int typeOid, short length, int colIdx)
        {
            ColumnName = columnName;

            if (!ColumnName.EndsWith("\0"))
                ColumnName = ColumnName + "\0";

            ColumnIndex = (short)colIdx;
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

        public void WriteTo(ByteWrapper dest)
        {
            byte[] columnName = Encoding.ASCII.GetBytes(ColumnName);
            dest.Write(columnName);
            dest.Write(TableOID);
            dest.Write(ColumnIndex);
            dest.Write(TypeOID);
            dest.Write(ColumnLength);
            dest.Write(TypeModifier);
            dest.Write(Format);
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
            
            // Use MinValue - this needs to be first
            Time = DateTime.MinValue;
        }

        private  RowDescription()
        {
            Time = DateTime.MinValue;
            MessageType = PGTypes.RowDescription;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            dest.Write((short)Fields.Count());

            foreach (RowDescriptionField rdf in Fields)
            {
                rdf.WriteTo(dest);
            }

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
            Status = _readyByte[0];
            Time = DateTime.MaxValue;
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
            Time = DateTime.MaxValue.AddSeconds(-1);
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

        protected SimpleQuery()
        {

        }

        protected override int GetPayloadLength()
        {          
           return GetEncodedStringLength(Query);
        }

        public static SimpleQuery FromBytes(byte[] buffer, int index, int length)
        {
            SimpleQuery sq = new SimpleQuery();
            sq.Query = MessageParser.ReadString(buffer, ref index, length);
            return sq;
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

        
        /// <summary>
        /// Look for ';' characters and split a query into separate queries.
        /// 
        /// This was exposed by NPGSQL's connection initialization where they send several queries
        /// in one 'SimpleQuery'
        /// 
        /// TODO: Perhaps we should maintain SimpleQuery as a single message, and have 'Query' be a list instead?
        /// </summary>
        /// <returns></returns>
        public List<SimpleQuery> Split()
        {
            int semiIndex = Query.IndexOf(';');

            if(semiIndex == -1)
            {
                return new List<SimpleQuery> { this };
            }
            List<SimpleQuery> queries = new List<SimpleQuery>();
            string sourceQuery = Query;
            while(semiIndex != -1)
            {
                string sub = sourceQuery.Substring(0, semiIndex);

                queries.Add(new SimpleQuery(sub));

                sourceQuery = sourceQuery.Substring(semiIndex + 1);
                semiIndex = sourceQuery.IndexOf(';');
            }

            return queries;
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

    /// <summary>
    /// DataRowMessage contains the actual query results.
    /// This will be the bulk of the messages.
    /// To minimize allocations, bytes are pooled.
    /// </summary>
    public class DataRowMessage : PGMessage
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
            dest.Write((short)Fields.Count());

            foreach (PGField rdf in Fields)
            {
                dest.Write(rdf.ColumnLength);

                dest.Write(rdf.Data);
            }
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
            for (int i = 0; i < fc; i++)
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

        public void Set(Utils.FixedSizeBytePool pool)
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
            _completedMessage[4] = (byte)(encodedLength & 0x000000FF);

            int index = 5;
            MessageParser.WriteShort(_completedMessage, ref index, (short)Fields.Count());
            foreach (PGField rdf in Fields)
            {
                MessageParser.WriteInt(_completedMessage, ref index, rdf.ColumnLength);

                Buffer.BlockCopy(rdf.Data, 0, _completedMessage, index, rdf.ColumnLength);
                index += rdf.ColumnLength;
            }
        }

        /// <summary>
        /// Called to re-pool when no longer needed in the cache
        /// </summary>
        public void Release()
        {
            if (_pool != null)
            {
                //Console.WriteLine("Returning datarowmessage array to cache");
                _pool.Return(_completedMessage);
            }
            _completedMessage = null;
            _pool = null;
        }

        public Utils.FixedSizeBytePool _pool = null;
    }





}
