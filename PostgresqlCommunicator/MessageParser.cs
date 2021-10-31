using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Handle reading/writing postgresql messages from byte buffers.
    /// </summary>
    public static class MessageParser
    {
        /// <summary>
        /// Helper method to read a message from a buffer and return an interpreted type
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index">index into buffer</param>
        /// <param name="buffLength"></param>
        /// <param name="readBytes">read position</param>
        /// <returns></returns>
        public static PGMessage ReadMessage(byte[] buffer, int index, int buffLength, out int readPosition, Type expectedType, bool allowUnknown = false )
        {
            // Expect a type byte and a length
            if (buffer == null || buffer.Length - index < 5 || buffLength < 5)
            {
                throw new Exception("Invalid buffer: Null or <5 bytes");
            }

            byte mType = buffer[index];
            int length = (buffer[index + 1] << 24 | buffer[index + 2] << 16 | buffer[index + 3] << 8 | buffer[index + 4]);

            int buffPosition = index + 5;

            readPosition = index + length + 1;

            int payloadLength = length - 4;




            switch (mType)
            {
                case PGTypes.ParameterStatus:
                    return ParameterStatus.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.BackendKeyData:
                    return BackendKeyData.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.CommandCompletion:
                    return CommandCompletion.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.ReadyForQuery:
                    return ReadyForQuery.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.RowDescription:
                    return RowDescription.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.DataRow:
                    return DataRowMessage.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.ErrorResponse:
                    ErrorResponseMessage erm = new ErrorResponseMessage();
                    erm.ErrorText = Encoding.ASCII.GetString(buffer, buffPosition, payloadLength);
                    return erm;
                case PGTypes.SimpleQuery:
                    return SimpleQuery.FromBytes(buffer, buffPosition, payloadLength);
                case PGTypes.SASLInitialResponse:
                    if(expectedType == typeof(AuthenticationSASLInitialResponse))
                        return AuthenticationSASLInitialResponse.FromBuffer(buffer, buffPosition, payloadLength);
                    return AuthenticationSASLResponse.FromBuffer(buffer, buffPosition, payloadLength);
                case PGTypes.AuthenticationRequest:
                    // Need type
                    int authType = ReadInt(buffer, ref buffPosition);

                    if (authType == AuthenticationTypes.Success)
                    {
                        AuthenticationSuccess authSucc = new AuthenticationSuccess();
                        return authSucc;
                    }
                    else if (authType < AuthenticationTypes.SASLRequest)
                    {
                        return null;// Unsupported
                    }
                    if (authType == AuthenticationTypes.SASLRequest)
                    {
                        AuthenticationRequestSASL asl = new AuthenticationRequestSASL();
                        // Local testing only shows 1, but presumably this is are null-separated list
                        while (buffPosition < buffLength)
                        {
                            int start = buffPosition;
                            int next = buffPosition;
                            for (int i = next; i < buffLength; i++)
                            {
                                if (buffer[i] == 0x00)
                                {
                                    next = i;
                                    break;
                                }
                            }

                            if (next != buffPosition)
                            {
                                string authMech = Encoding.ASCII.GetString(buffer, start, next - start);
                                asl.AuthenticationMechanisms.Add(authMech);
                                if (buffer[buffPosition + 1] == 0x00)
                                    break;
                            }
                            buffPosition = next + 1;
                        }
                        return asl;
                    }
                    else if(authType == AuthenticationTypes.SASLContinue)
                    {
                        AuthenticationSASLContinue ac = new AuthenticationSASLContinue();


                        ac.Parse(buffer, buffPosition, payloadLength);


                        return ac;
                    }
                    else if(authType == AuthenticationTypes.SASLComplete)
                    {
                        AuthenticationSASLComplete comp = new AuthenticationSASLComplete();

                        comp.SASLAuthData = new byte[payloadLength -4];
                        Buffer.BlockCopy(buffer, buffPosition, comp.SASLAuthData, 0, payloadLength - 4);

                        return comp;
                    }

                    break;
            }

            if(!allowUnknown)
                return null;

            UnhandledMessage mess = new UnhandledMessage(mType, length);
            mess.ConsumedBytes = new byte[payloadLength];
            Buffer.BlockCopy(buffer, buffPosition, mess.ConsumedBytes, 0, payloadLength);
            return mess;
        }

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
            for (int i = index; i < (index + maxLength); i++)
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

        /// <summary>
        /// Write a 4-byte integer into the buffer
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="value"></param>
        public static void WriteInt(byte[] buffer, ref int index, int value)
        {
            buffer[index] = (byte)((value & 0xFF000000) >> 24);
            buffer[index + 1] = (byte)((value & 0x00FF0000) >> 16);
            buffer[index + 2] = (byte)((value & 0x0000FF00) >> 8);
            buffer[index + 3] = (byte)(value & 0x000000FF);
            index += 4;
        }

        /// <summary>
        /// Write a 2-byte integer into the buffer
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="value"></param>
        public static void WriteShort(byte[] buffer, ref int index, short value)
        {
            buffer[index] = (byte)((value & 0xFF00) >> 8);
            buffer[index + 1] = (byte)(value & 0x00FF);
            index += 2;
        }

        /// <summary>
        /// Copy bytes into the buffer
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="source"></param>
        /// <param name="count"></param>
        public static void WriteBytes(byte[] buffer, ref int index, byte[] source)
        {
            WriteBytes(buffer, ref index, source, source.Length);
        }

        /// <summary>
        /// Copy bytes into the buffer
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="source"></param>
        /// <param name="count"></param>
        public static void WriteBytes(byte[] buffer, ref int index, byte[] source, int count)
        {
            if (index + count > buffer.Length)
            {
                throw new Exception(String.Format("Unable to copy buffer, size requested exceeds dest buffer. Index={0}, Count={1}, buff.Length={2}", index, count, buffer.Length));
            }
            Buffer.BlockCopy(source, 0, buffer, index, count);
            index += count;
        }

        /// <summary>
        /// Write a string into the buffer
        /// </summary>
        /// <param name="buffer"></param>
        /// <param name="index"></param>
        /// <param name="s"></param>
        /// <param name="nullTerminate">True if a null terminator should be added.</param>
        public static void WriteString(byte[] buffer, ref int index, string s, bool nullTerminate = true)
        {
            int req =  index + s.Length + (nullTerminate ? 1 : 0);
            if (req > buffer.Length)
                throw new Exception(String.Format("Unable to write string to buffer. Size exceeds buffer length: Index={0}, req={1}, buff.Length={2}", index, req, buffer.Length));

            byte[] m = Encoding.ASCII.GetBytes(s);
            Buffer.BlockCopy(m, 0, buffer, index, s.Length);
            index += s.Length;
            if (nullTerminate)
                buffer[index++] = 0x00;
        }
    }
}
