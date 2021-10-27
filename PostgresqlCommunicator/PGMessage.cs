﻿using System;
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

        protected abstract int GetPayloadLength();

        /// <summary>
        /// Returns the message as a byte[]
        /// 
        /// TODO: Avoid this and use helpers that write to existing buffers
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

        protected static string EnsureNullTerminated(string s)
        {
            if (s == null || s.Length == 0)
                return _nullTermString;
            if (s[s.Length - 1] == '\0')
                return s;
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

        protected static string _nullTermString = "\0";
    }


}