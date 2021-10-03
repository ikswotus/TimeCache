using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{
    /// <summary>
    /// Simple wrapper around a queue allowing a fixed number
    /// of values to be kept.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class CappedQueue<T>
    {
        public CappedQueue(int capacity = 500)
        {
            MaxCapacity = capacity;

            _valueQueue = new Queue<T>(MaxCapacity);
        }

        /// <summary>
        /// Adds a value, return true if at capacity
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public bool Add(T val)
        {
            _valueQueue.Enqueue(val);

            bool popped = false;
            while(_valueQueue.Count > MaxCapacity)
            {
                _valueQueue.Dequeue();

                popped = true;
            }
            return popped;
        }

        /// <summary>
        /// Remove all values from the queue
        /// </summary>
        public void Clear()
        {
            _valueQueue.Clear();
        }

        /// <summary>
        /// Return all queued values in a list and optionally clear the queue.
        /// </summary>
        /// <returns></returns>
        public List<T> FlushToList(bool clear)
        {
            List<T> vals = new List<T>(_valueQueue);
            if(clear)
                _valueQueue.Clear();
            return vals;
        }

        /// <summary>
        /// Underlying queue of values
        /// </summary>
        private readonly Queue<T> _valueQueue = null;

        /// <summary>
        /// Max capacity allowed.
        /// </summary>
        public readonly int MaxCapacity = 500;
    }
}
