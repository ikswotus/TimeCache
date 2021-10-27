using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Allows reading messages from a network socket.
    /// 
    /// Semi-circular buffer...shifts data from end of buffer to start 
    /// when buffer is full and more data needs to be read.
    /// </summary>
    public class MessageReader
    {
        /// <summary>
        /// Constructor - require a socket to read from.
        /// </summary>
        /// <param name="s"></param>
        public MessageReader(Socket s)
        {
            _socket = s;
            _buffer = new byte[65535];
        }

        /// <summary>
        /// Attempt to read a message of type T.
        /// Exceptions are thrown if there is a type mismatch, or if an error message is returned.
        /// </summary>
        /// <typeparam name="T">Expected message type</typeparam>
        /// <returns>A message object of the expected type T</returns>
        public T ReadMessage<T>() where T : PGMessage
        {
            PGMessage message = ReadMessage();

            PostgresqlCommunicator.ErrorResponseMessage erm = message as PostgresqlCommunicator.ErrorResponseMessage;

            if (erm != null)
            {
                throw new Exception("Error Message:" + erm.ErrorText);
            }

            T expectedMessage = message as T;

            if (expectedMessage == null)
                throw new Exception("Did not receive expected message: " + typeof(T) + ". Received: " + message.GetType());

            return expectedMessage;
        }

        /// <summary>
        /// Read a message from the socket.
        /// Blocks until byte are received
        /// </summary>
        /// <returns></returns>
        public PGMessage ReadMessage()
        {
            // Do we need to read?
            if (_remainingBufferBytes == 0)
            {
                _bufferPosition = 0;
                _remainingBufferBytes = _socket.Receive(_buffer);
            }
            while (_remainingBufferBytes < 5 || ReadLength() > _remainingBufferBytes + 1)
            {
                CopyBuffer();
                _bufferPosition = 0;

                int read = _buffer.Length - _remainingBufferBytes - 1;
                _remainingBufferBytes += _socket.Receive(_buffer, _remainingBufferBytes, read, SocketFlags.None);
            }

            int consumed = 0;
            PGMessage message = MessageParser.ReadMessage(_buffer, _bufferPosition, _remainingBufferBytes, out consumed, true);

            int readBytes = (consumed - _bufferPosition);
            if (_remainingBufferBytes < readBytes)
                throw new Exception("Invalid remainder?");
            
            _remainingBufferBytes -= readBytes;
            _bufferPosition += readBytes;

           
            return message;
        }

        /// <summary>
        /// Attempts to read the length field of the next pg message
        /// </summary>
        /// <returns></returns>
        private int ReadLength()
        {
            if (_bufferPosition + 4 > _buffer.Length)
                throw new Exception("Invalid length read");

            int l = (_buffer[_bufferPosition + 1] << 24 | _buffer[_bufferPosition + 2] << 16 | _buffer[_bufferPosition + 3] << 8 | _buffer[_bufferPosition + 4]);

            // Some sanity checks on the length
            // Need space for message + type + length field
            if (l > _buffer.Length - 5)
                throw new Exception("Expected length of message exceeds buffer size: " + l);
            // If we got confused, we may have a garbage length. It will be very hard to tell, but a negative length is a good indicator.
            if(l < 0)
                throw new Exception("Invalid message length: " + l);
            // TODO: Verify position 0 is a valid message type?

            return l;
        }

        /// <summary>
        /// Shift remaining bytes within our buffer
        /// </summary>
        private void CopyBuffer()
        {
            for (int i = 0; i < _remainingBufferBytes; i++)
            {
                _buffer[i] = _buffer[_bufferPosition + i];
            }
        }

        /// <summary>
        /// # of bytes left in the buffer available for reading
        /// </summary>
        private int _remainingBufferBytes = 0;

        /// <summary>
        /// Current position within buffer
        /// </summary>
        private int _bufferPosition = 0;

        /// <summary>
        /// Buffer to store reads
        /// </summary>
        private byte[] _buffer = null;


        /// <summary>
        /// A connected socket to read from
        /// </summary>
        private Socket _socket = null;
    }
}

