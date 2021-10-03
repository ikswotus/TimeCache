using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utils
{
    /// <summary>
    /// Bytepool to get an array of at least @size. New array is allocated if necessary
    /// </summary>
    public class BytePool
    {
        public BytePool(int pooled = 10)
        {
            _pool = new Stack<byte[]>(pooled);
        }

        /// <summary>
        /// Simple return
        /// - does not clear memory
        /// </summary>
        /// <param name="barr"></param>
        public void Return(byte[] barr)
        {
            lock (_pool)
                _pool.Push(barr);
        }

        /// <summary>
        /// Loop through available pools, returning a valid one if found, allocating a new one if not found.
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public byte[] Get(int size)
        {
            lock (_pool)
            {
                int c = _pool.Count;
                while( c > 0)
                {
                    byte[] pool = _pool.Pop();
                    if (pool.Length >= size)
                        return pool;
                    c--;
                    _pool.Push(pool);
                }
            }
            return new byte[size];
        }

        private Stack<Byte[]> _pool = null;
    }
}
