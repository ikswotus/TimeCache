using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Utils
{
    /// <summary>
    /// Simple pool for byte[]
    /// </summary>
    public class FixedSizeBytePool
    {

        public FixedSizeBytePool(int size, int startingCapacity = 10)
        {
            _size = size;
            _pool = new Stack<byte[]>(startingCapacity);
        }

        private int _size;

        public void Return(byte[] barr)
        {
            lock (_pool)
                _pool.Push(barr);
        }

        public byte[] Get()
        {
            lock(_pool)
            {
                if (_pool.Count > 0)
                    return _pool.Pop();
            }
            return new byte[_size];
        }

        private Stack<Byte[]> _pool = null;
    }
}
