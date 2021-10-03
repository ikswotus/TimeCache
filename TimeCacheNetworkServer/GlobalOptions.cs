using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TimeCacheNetworkServer
{
    public static class GlobalOptions
    {
        private static Dictionary<string, object> _options = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        public static bool HasOption(string optionName)
        {
            lock (_options)
                return _options.ContainsKey(optionName);
        }

        public static bool TryGetValue(string optionName, out object value)
        {
            lock (_options)
                return _options.TryGetValue(optionName, out value);
        }

        public static void SetOption(string optionName, object value, bool allowOverride = false)
        {
            lock(_options)
            {
                if (_options.ContainsKey(optionName) && !allowOverride)
                    throw new Exception("Option named: " + optionName + " already existed");
                _options[optionName] = value;
            }
        }
        public static bool SetOptionIfNotExist(string optionName, object value)
        {
            lock (_options)
            {
                if (!_options.ContainsKey(optionName))
                {
                    _options[optionName] = value;
                    return true;
                }
                return false;
            }
        }

        public static T GetOptionValue<T>(string optionName, T defaultValue)
        {
            object ret;
            if(TryGetValue(optionName, out ret))
            {
                return (T)ret;
            }
            return defaultValue;
        }


        /// <summary>
        /// MemoryUsage
        /// </summary>
        public static string AllowedMemoryUsage = "MemoryUsage";
    }
}
