using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Base class for a postgresql message, either sent or received.
    /// </summary>
    public abstract class PGMessage
    {
        public PGMessage()
        {
            // TODO: What to use for messages that aren't time-specific?
            Time = DateTime.UtcNow;
        }

        /// <summary>
        /// Type of message
        /// </summary>
        public byte MessageType { get; set; }

        /// <summary>
        /// Length of message. Note: Length includes size of length field (+4)
        /// </summary>
        protected int Length { get; set; }

        /// <summary>
        /// TODO: Revisit this.
        /// For caching, we keep the constructed message around to reduce work
        /// </summary>
        public byte[] _completedMessage = null;

        /// <summary>
        /// 4 bytes for Length field
        /// </summary>
        protected const int _baseLength = 4;

        /// <summary>
        /// Derived classes that need to calculate a length dynamically can do so here
        /// Otherwise we return Length
        /// </summary>
        /// <returns></returns>
        public int GetLength()
        {
            return _baseLength + GetPayloadLength();
        }

        /// <summary>
        /// Time column for sending results back to grafana
        /// TODO: what if we dont have one???
        /// </summary>
        public DateTime Time { get; set; }

        protected abstract int GetPayloadLength();

        /// <summary>
        /// Returns the message as a byte[]
        /// 
        /// TODO: Avoid this whenver possible and use the WriteTo() methods
        /// </summary>
        /// <returns></returns>
        public byte[] GetMessagePayload()
        {
            byte[] payload = new byte[1 + GetLength()];
            payload[0] = MessageType;
            int index = 1;
            MessageParser.WriteInt(payload, ref index, GetLength());
            MessageParser.WriteBytes(payload, ref index, GetMessageBytes());

            return payload;
        }

        /// <summary>
        /// Helper method for reading strings
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="maxLength"></param>
        /// <param name="index"></param>
        /// <param name="pname"></param>
        /// <returns></returns>
        public static string ParseNullTerminatedString(byte[] buffer, int maxLength, ref int index, string pname)
        {
            if (maxLength + index > buffer.Length)
                throw new Exception("Max length exceeds buffer");

            int nullIdx = -1;
            for (int i = index; i < buffer.Length; i++)
            {
                if (buffer[i] == 0x00)
                {
                    nullIdx = i;
                    break;
                }
            }
            if (nullIdx == -1)
                throw new Exception("Could not parse: " + pname);
            string s = Encoding.UTF8.GetString(buffer, index, nullIdx - index);
            index = nullIdx + 1;
            return s;
        }

        /// <summary>
        /// Derived classes will implement this to convert to a byte[] representation of a message, excluding the type and length fields
        /// </summary>
        /// <returns></returns>
        public abstract byte[] GetMessageBytes();

        /// <summary>
        /// Writes the message to the destination bytewrapper
        /// </summary>
        /// <param name="dest"></param>
        public void WriteTo(ByteWrapper dest)
        {
            dest.Write(MessageType);
            int length = GetLength();
            dest.Write((byte)((length & 0xFF000000) >> 24));
            dest.Write((byte)((length & 0x00FF0000) >> 16));
            dest.Write((byte)((length & 0x0000FF00) >> 8));
            dest.Write((byte)((length & 0x000000FF)));

            DoWriteTo(dest);
        }

        /// <summary>
        /// Derived classes will implement this to write data to the bytewrapper, excluding type and length fields
        /// </summary>
        /// <param name="dest"></param>
        protected abstract void DoWriteTo(ByteWrapper dest);


        /// <summary>
        /// Checks to see if a string has a null-terminating character
        /// </summary>
        /// <param name="s"></param>
        /// <returns>The string, or a new string with an appended null terminator</returns>
        protected static string EnsureNullTerminated(string s)
        {
            if (s == null || s.Length == 0)
                return _nullTermString;
            if (s[s.Length - 1] == '\0')
                return s;
            // Ugh, this is an unfortunate copy
            // It may be better to never call this, and instead handle it during
            // the write
            return s + _nullTermString;
        }

        protected static bool IsNullTerminated(string s)
        {
            if (s == null || s.Length == 0)
                return false;
            return (s[s.Length - 1] == '\0');
                
        }

        protected int GetEncodedStringLength(string s)
        {
            if (s == null || s.Length == 0)
                return 1;
            return s.Length + (IsNullTerminated(s) ? 0 : 1);
        }

        public static byte[] GetStringBytes(string s)
        {
            return GetStringBytes(s, Encoding.UTF8);
        }

        public static byte[] GetStringBytes(string s, Encoding encoding)
        {
            return encoding.GetBytes(EnsureNullTerminated(s));
        }

        public static byte[] GetStringBytes2(string s)
        {
            byte[] sb =  Encoding.UTF8.GetBytes(s);
            if (sb.Length == 0)
                return _empty;
            if (sb[s.Length - 1] == 0x00)
                return sb;

            byte[] b = new byte[s.Length + 1];
            Array.Copy(sb, b, sb.Length);
            b[s.Length] = 0x00;
            return b;
        }

        protected static byte[] _empty = new byte[1] { 0x00 };
        protected static byte _nullByte = 0x00;

        protected static string _nullTermString = "\0";
    }


}
