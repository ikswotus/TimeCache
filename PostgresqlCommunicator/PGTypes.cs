using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Known/supported message command types.
    /// </summary>
    public abstract class PGTypes
    {
        public const byte ParseCompletion = 0x31;
        public const byte BindCompletion = 0x32;

        public const byte Bind = 0x42;
        public const byte CommandCompletion = 0x43;
        public const byte Describe = 0x44;
        public const byte DataRow = 0x44;
        public const byte Execute = 0x45;
        public const byte BackendKeyData = 0x4b;
       
        public const byte Parse = 0x50;
        public const byte SimpleQuery = 0x51;
        public const byte AuthenticationRequest = 0x52;
        public const byte Sync = 0x53;
        public const byte ParameterStatus = 0x53;
        public const byte RowDescription = 0x54;
        public const byte ReadyForQuery = 0x5a;

        public const byte PasswordMessage = 0x70;

        public static string GetType(byte b)
        {
            switch (b)
            {
                case 0x31:
                    return "ParseCompletion";
                case 0x32:
                    return "BindCompletion";
                case 0x42:
                    return "Bind";
                case 0x43:
                    return "CommandCompletion";
                case 0x44:
                    return "DataRow(Server)|Describe(Client)";
                case 0x45:
                    return "Execute";
                case 0x4b:
                    return "BackendKeyData";

                case 0x50:
                    return "Parse";
                case 0x51:
                    return "SimpleQuery";
                case 0x52:
                    return "AuthenticationRequest";
                case 0x53:
                    return "Sync|ParameterStatus";
                case 0x54:
                    return "RowDescription";
                case 0x5A:
                    return "ReadyForQuery";
                case 0x70:
                    return "PasswordMessage";


                default:
                    return "UNKNOWN::" + b.ToString("X2");
            }
        }
    }

}
