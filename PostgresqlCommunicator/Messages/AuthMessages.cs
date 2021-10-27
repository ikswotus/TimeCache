using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Security.Cryptography;

namespace PostgresqlCommunicator
{

    /// <summary>
    /// Base for authentication messages
    /// </summary>
    public abstract class AuthenticationMessage : PGMessage
    {
        /// <summary>
        /// Identifies Auth type
        /// </summary>
        public int AuthenticationType { get; set; }
    }

    public class AuthenticationRequestSASL : AuthenticationMessage
    {
        public AuthenticationRequestSASL()
        {
            MessageType = PGTypes.AuthenticationRequest;
            AuthenticationMechanisms = new List<string>(1);
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }


        public List<string> AuthenticationMechanisms { get; set; }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }


        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
    }
    public class AuthenticationSuccess : AuthenticationMessage
    {
        public AuthenticationSuccess()
        {
            AuthenticationType = AuthenticationTypes.Success;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

      
        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
    }
    public class AuthenticationSASLContinue : AuthenticationMessage
    {
        public AuthenticationSASLContinue()
        {
            AuthenticationType = AuthenticationTypes.SASLContinue;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

        public void Parse(byte[] buffer, int index)
        {
            FullText = Encoding.UTF8.GetString(buffer, index, buffer.Length - index);
            string[] parts = FullText.Split(',');
            for (int i = 0; i < parts.Length; i++)
            {
                if (parts[i].StartsWith("r=", StringComparison.Ordinal))
                {
                    ClientNonce = parts[i].Substring(2);
                }
                else if (parts[i].StartsWith("s=", StringComparison.Ordinal))
                {
                    Salt = parts[i].Substring(2);
                }
                else if (parts[i].StartsWith("i=", StringComparison.Ordinal))
                {
                    Iterations = int.Parse(parts[i].Substring(2));
                }
            }
        }
        // Keep this since we need it for auth valid
        public string FullText { get; set; }

        public string ClientNonce { get; set; }
        public string Salt { get; set; }
        public int Iterations { get; set; }

        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
    }
    public class AuthenticationSASLComplete : AuthenticationMessage
    {
        public AuthenticationSASLComplete()
        {
            AuthenticationType = AuthenticationTypes.SASLContinue;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

        public byte[] SASLAuthData { get; set; }

       
        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }
    }
    public class AuthenticationSASLInitialResponse : PGMessage
    {
        public AuthenticationSASLInitialResponse()
        {
            MessageType = PGTypes.SASLInitialResponse;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }
        public string Mechanism { get; set; }

        public byte[] SASLAuthData { get; set; }

        protected override int GetPayloadLength()
        {
            return GetEncodedStringLength(Mechanism) + 4 + SASLAuthData.Length;
        }

        public override byte[] GetMessageBytes()
        {
            byte[] ret = new byte[Mechanism.Length + 1 + SASLAuthData.Length + 4];

            int pos = 0;
            MessageParser.WriteString(ret, ref pos, Mechanism, true);
            MessageParser.WriteInt(ret, ref pos, SASLAuthData.Length);
            Buffer.BlockCopy(SASLAuthData, 0, ret, pos, SASLAuthData.Length);

            return ret;
        }
    }

    public class AuthenticationSASLResponse : PGMessage
    {
        public AuthenticationSASLResponse()
        {
            MessageType = PGTypes.SASLInitialResponse;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        public byte[] SASLAuthData { get; set; }

        protected override int GetPayloadLength()
        {
            return SASLAuthData.Length;
        }

        public override byte[] GetMessageBytes()
        {
            byte[] ret = new byte[SASLAuthData.Length];

            int pos = 0;

            Buffer.BlockCopy(SASLAuthData, 0, ret, pos, SASLAuthData.Length);

            return ret;
        }
    }

    public class AuthenticationMD5Password : PGMessage
    {
        public AuthenticationMD5Password() : base()
        {
            MessageType = PGTypes.AuthenticationRequest;

            Salt = new byte[4];

            Length = 8;
        }

        /// <summary>
        /// parse from byte array
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static AuthenticationMD5Password FromBytes(byte[] bytes, int index)
        {
            if (bytes[index] != 0x52)
                throw new Exception("not an authenticationmd5request");
            if (bytes.Length - index < 12)
                throw new Exception("Not enough bytes for authenticationmd5request");
            AuthenticationMD5Password mess = new AuthenticationMD5Password();

            Buffer.BlockCopy(bytes, index + 9, mess.Salt, 0, 4);

            return mess;
        }



        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }


        /// <summary>
        /// Always 5 for md5
        /// </summary>
        public static readonly int AuthenticationType = 5;

        protected override int GetPayloadLength()
        {
            return 4 + Salt.Length;           
        }

        /// <summary>
        /// Random bytes
        /// </summary>
        public byte[] Salt { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override byte[] GetMessageBytes()
        {
            byte[] bytes = new byte[4 + Salt.Length];
            int index = 0;
            MessageParser.WriteInt(bytes, ref index, AuthenticationType);
            MessageParser.WriteBytes(bytes, ref index, Salt, Salt.Length);
            return bytes;
        }
    }

    public class AuthenticationMD5Response : PGMessage
    {
        public AuthenticationMD5Response()
        {

        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

        public override byte[] GetMessageBytes()
        {
            throw new NotImplementedException();
        }

        public static AuthenticationMD5Response FromBytes(byte[] bytes, int index)
        {
            if (bytes[index] != 0x70)
                throw new Exception("not a password message");

            if (bytes.Length < 35 || index + 35 > bytes.Length)
                throw new Exception("Not enough bytes for md5 response.");

            // First 3 should be 'md5'
            if (bytes[index + 5] != 0x6D || // 'm'
               bytes[index + 6] != 0x64 || // 'd'
               bytes[index + 7] != 0x35)   // '5'
            {
                throw new Exception("Not md5");
            }
            index += 8;

            AuthenticationMD5Response amr = new AuthenticationMD5Response();
            amr.EncryptedDigest = new byte[32];
            Buffer.BlockCopy(bytes, index, amr.EncryptedDigest, 0, 32);

            return amr;
        }

        public byte[] EncryptedDigest { get; set; }
    }

    public class AuthenticationOK : PGMessage
    {
        /// <summary>
        /// Always 0
        /// </summary>
        private static readonly byte[] _okBytes = new byte[4] { 0x00, 0x00, 0x00, 0x00 };


        public AuthenticationOK()
        {
            MessageType = 0x52; // 'R'
        }

        protected override int GetPayloadLength()
        {
            return 4;
        }

        public override byte[] GetMessageBytes()
        {
            return _okBytes;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }
    }

    public class ParameterStatus : PGMessage
    {
        public ParameterStatus()
        {
            MessageType = PGTypes.ParameterStatus;
        }

        public static ParameterStatus FromBuffer(byte[] buffer, int index, int length)
        {
            ParameterStatus ps = new ParameterStatus();

            ps.ParameterName = PGMessage.ParseNullTerminatedString(buffer, length - 1, ref index, nameof(ps.ParameterName));
            ps.ParameterValue = PGMessage.ParseNullTerminatedString(buffer, length, ref index, nameof(ps.ParameterValue));

            return ps;
        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }
        public override byte[] GetMessageBytes()
        {
            Length = GetLength();

            byte[] buff = new byte[1 + Length];

            buff[0] = MessageType;

            int index = 1;
            MessageParser.WriteInt(buff, ref index, Length);
            MessageParser.WriteString(buff, ref index, ParameterName, true);
            MessageParser.WriteString(buff, ref index, ParameterValue, true);

            return buff;
        }

        protected override int GetPayloadLength()
        {
            return  GetEncodedStringLength(ParameterName) + GetEncodedStringLength(ParameterValue);
        }

        public string ParameterName { get; set; }
        public string ParameterValue { get; set; }
    }
    public class BackendKeyData : PGMessage
    {
        public BackendKeyData()
        {
            MessageType = PGTypes.BackendKeyData;
        }

        public static BackendKeyData FromBuffer(byte[] buffer, int index, int length)
        {
            BackendKeyData bkd = new BackendKeyData();

            bkd.PID = MessageParser.ReadInt(buffer, ref index);
            bkd.Key = new byte[length - 4];
            Buffer.BlockCopy(buffer, index, bkd.Key, 0, length - 4);

            return bkd;
        }

        

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        public override byte[] GetMessageBytes()
        {
            Length = GetLength();

            byte[] buff = new byte[1 + Length];

            buff[0] = MessageType;

            int index = 1;
            MessageParser.WriteInt(buff, ref index, Length);

            MessageParser.WriteInt(buff, ref index, PID);

            MessageParser.WriteBytes(buff, ref index, Key, Key.Length);

            return buff;
        }

        protected override int GetPayloadLength()
        {
            return 4 + Key.Length;
        }
        public byte[] Key { get; set; }
        public int PID { get; set; }
    }

    public class PasswordMessage : PGMessage
    {
        public PasswordMessage()
        {
            MessageType = PGTypes.PasswordMessage;

        }

        protected override void DoWriteTo(ByteWrapper dest)
        {
            throw new NotImplementedException();
        }

        protected override int GetPayloadLength()
        {
            throw new NotImplementedException();
        }

        public static PasswordMessage GetPasswordMessage(string pass, string user_name, byte[] salt)
        {

            byte[] userbytes = Encoding.UTF8.GetBytes(user_name);
            byte[] passbytes = Encoding.UTF8.GetBytes(pass);

            byte[] cryptBuf = new byte[passbytes.Length + userbytes.Length];

            passbytes.CopyTo(cryptBuf, 0);
            userbytes.CopyTo(cryptBuf, passbytes.Length);


            MD5 m = MD5.Create();
            byte[] hash1 = m.ComputeHash(cryptBuf);

            StringBuilder sb = new StringBuilder();
            foreach (byte b in hash1)
                sb.Append(b.ToString("x2"));
            string prehash = sb.ToString();
            // Console.WriteLine("Hash1: " + prehash);


            byte[] prehashbytes = Encoding.UTF8.GetBytes(prehash);


            // Raw bytes
            cryptBuf = new byte[prehashbytes.Length + 4];


            Buffer.BlockCopy(salt, 0, cryptBuf, prehashbytes.Length, 4);
            prehashbytes.CopyTo(cryptBuf, 0);

            //                Buffer.BlockCopy(prehashbytes, 0, cryptBuf, 0, prehashbytes.Length);

            byte[] hash2 = m.ComputeHash(cryptBuf);
            sb.Clear();
            sb.Append("md5");

            foreach (byte b in hash2)
                sb.Append(b.ToString("x2"));

            string computedHash2 = sb.ToString();
            //  Console.WriteLine("Hash RR: " + computedHash2);

            PasswordMessage pm = new PasswordMessage();

            pm.Hash = new byte[computedHash2.Length];

            Encoding.UTF8.GetBytes(computedHash2, 0, computedHash2.Length, pm.Hash, 0);


            return pm;
        }

        public byte[] Hash { get; set; }

        public override byte[] GetMessageBytes()
        {
            Length = 40;

            byte[] b = new byte[Length + 1];
            b[0] = MessageType;

            int index = 1;
            MessageParser.WriteInt(b, ref index, Length);

            MessageParser.WriteBytes(b, ref index, Hash, Hash.Length);

            b[b.Length - 1] = 0x00;

            return b;
        }
    }
}
