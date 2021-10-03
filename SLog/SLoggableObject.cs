using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SLog
{
    /// <summary>
    /// A sloggable object provides convenient access to a logger.
    /// A ComponentName is set during constuction, and all helper methods
    /// log using that value.
    /// </summary>
    public abstract class SLoggableObject
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="componentName"></param>
        /// <param name="logger"></param>
        public SLoggableObject(string componentName, ISLogger logger)
        {
            _logger = logger;
            ComponentName = componentName;
        }

        /// <summary>
        /// Log object
        /// </summary>
        protected readonly ISLogger _logger;

        /// <summary>
        /// Name of the component used for logging
        /// </summary>
        public readonly string ComponentName;


        public void VTrace(string message)
        {
            _logger.VTrace(ComponentName, message);
        }

        public void Trace( string message)
        {
            _logger.Trace(ComponentName, message);
        }
        public void Debug(string message)
        {
            _logger.Debug(ComponentName, message);
        }
        public void Error(string message)
        {
            _logger.Error(ComponentName, message);
        }
        public void Critical( string message)
        {
            _logger.Critical(ComponentName, message);
        }

        public void VTrace( string messageFormat, params object[] args)
        {
            _logger.VTrace(ComponentName, messageFormat, args);
        }

        public void Trace(string messageFormat, params object[] args)
        {
            _logger.Trace(ComponentName, messageFormat, args);
        }
        public void Debug( string messageFormat, params object[] args)
        {
            _logger.Debug(ComponentName, messageFormat, args);
        }
        public void Error(string messageFormat, params object[] args)
        {
            _logger.Error(ComponentName, messageFormat, args);
        }
        public void Critical(string messageFormat, params object[] args)
        {
            _logger.Critical(ComponentName, messageFormat, args);
        }
    }
}
