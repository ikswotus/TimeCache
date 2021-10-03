using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;
using System.IO;

using Utils;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Helper class for dealing with responses to be sent over the wire.
    /// Pools large byte[] so we do do not have to constantly allocate new ones.
    /// </summary>
    public class ByteWrapper
    {
        /// <summary>
        /// Retreives a ByteWrapper of at least @size length
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public static ByteWrapper Get(int size)
        {
            ByteWrapper bw = new ByteWrapper();
            bw.Buffer = _messagePool.Get(size);
            return bw;
        }

        public static Utils.BytePool _messagePool = new BytePool();

        /// <summary>
        /// Return our underlying byte[] to the storage pool
        /// and sets to null;
        /// 
        /// Should only be called when we no longer need access
        /// </summary>
        /// <param name="bw"></param>
        public static void Reset(ByteWrapper bw)
        {
            _messagePool.Return(bw.Buffer);

            bw.Buffer = null;

        }

        /// <summary>
        /// Private constructor - Only the static Get() should
        /// be used to initialize a new ByteWrapper object
        /// </summary>
        private ByteWrapper()
        {

        }

        /// <summary>
        /// Intended for DEBUGGING - Allocates a new byte[] that
        /// contains only the used bytes.
        /// </summary>
        /// <returns></returns>
        public byte[] GetBytes()
        {
            byte[] copy = new byte[_currentPosition];
            Array.Copy(Buffer, 0, copy, 0, _currentPosition);
            return copy;
        }

        /// <summary>
        /// Send()s the current data over the socket
        /// </summary>
        /// <param name="s"></param>
        /// <returns></returns>
        public int Send(Socket s)
        {
            return s.Send(Buffer, _currentPosition, SocketFlags.None);
        }

        /// <summary>
        /// Copies populated data to the provided @dest array
        /// </summary>
        /// <param name="dest"></param>
        public void CopyTo(byte[] dest)
        {
            if (dest.Length < _currentPosition)
                throw new Exception("Destination buffer too small");

            Array.Copy(Buffer, 0, dest, 0, _currentPosition);
        }

        /// <summary>
        /// Copies populated data to the provided stream
        /// </summary>
        /// <param name="s"></param>
        public void CopyTo(Stream s)
        {
            s.Write(Buffer, 0, _currentPosition);
        }

        /// <summary>
        /// Our underlying byte array. Actual length of data
        /// is tracked by a position pointer, not Buffer.Length
        /// </summary>
        private byte[] Buffer { get; set; }

        /// <summary>
        /// Write data to the buffer
        /// </summary>
        /// <param name="data"></param>
        public void Write(byte[] data)
        {
            if (data.Length + _currentPosition >= Buffer.Length)
                throw new Exception("Not enough space in buffer");

            Array.Copy(data, 0, Buffer, _currentPosition, data.Length);

            _currentPosition += data.Length;
        }

        /// <summary>
        /// Write a single byte
        /// </summary>
        /// <param name="data"></param>
        public void Write(byte data)
        {
            if (_currentPosition >= Buffer.Length)
                throw new Exception("Not enough space in buffer");

            Buffer[_currentPosition++] = data;
        }

        /// <summary>
        /// position tracker - used for determining how much data has been written
        /// to our buffer.
        /// </summary>
        private int _currentPosition = 0;

        /// <summary>
        /// Returns the amount of free space left in our buffer.
        /// </summary>
        /// <returns></returns>
        public int AvailableSpace()
        {
            return Buffer.Length - _currentPosition - 1;
        }

        /// <summary>
        /// Essentially a .Length(). Returns the number of bytes actually
        /// written to the underlying buffer
        /// </summary>
        /// <returns></returns>
        public int UsedSpace()
        {
            return _currentPosition;
        }

    }
}
