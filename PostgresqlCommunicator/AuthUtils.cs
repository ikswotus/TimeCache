using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Security.Cryptography;

namespace PostgresqlCommunicator
{
    public class AuthUtils
    {
        /// <summary>
        /// https://datatracker.ietf.org/doc/html/rfc5802#section-5
        /// Hi(str, salt, i):

        /// U1   := HMAC(str, salt + INT(1))
        /// U2   := HMAC(str, U1)
        /// ...
        /// Ui-1 := HMAC(str, Ui-2)
        /// Ui   := HMAC(str, Ui-1)
        /// Hi := U1 XOR U2 XOR ... XOR Ui
        /// 
        /// </summary>
        /// <param name="message"></param>
        /// <param name="salt"></param>
        /// <param name="iterations"></param>
        /// <returns></returns>
        public static byte[] Hi(string str, byte[] salt, int count)
        {
            using (HMACSHA256 hmac = new HMACSHA256(Encoding.UTF8.GetBytes(str)))
            {
                byte[] salt1 = new byte[salt.Length + 4];
                byte[] hi, u1;

                Buffer.BlockCopy(salt, 0, salt1, 0, salt.Length);
                salt1[salt1.Length - 1] = 1;

                hi = u1 = hmac.ComputeHash(salt1);

                for (int i = 1; i < count; i++)
                {
                    byte[] u2 = hmac.ComputeHash(u1);
                    Xor(hi, u2);
                    u1 = u2;
                }

                return hi;
            }
        }

        /// <summary>
        /// XOR two buffers into the first
        /// </summary>
        /// <param name="buffer1"></param>
        /// <param name="buffer2"></param>
        /// <returns></returns>
        public static byte[] Xor(byte[] buffer1, byte[] buffer2)
        {
            for (int i = 0; i < buffer1.Length; i++)
            {
                buffer1[i] ^= buffer2[i];
            }
            return buffer1;
        }
        /// <summary>
        /// HMACSHA256 
        /// </summary>
        /// <param name="bytes"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        public static byte[] HMAC(byte[] bytes, string message)
        {
            using (var hmacsha256 = new HMACSHA256(bytes))
                return hmacsha256.ComputeHash(Encoding.UTF8.GetBytes(message));
        }

        /// <summary>
        /// SHA256 hash
        /// </summary>
        /// <param name="bytes"></param>
        /// <returns></returns>
        public static byte[] SHA(byte[] bytes)
        {
            using (SHA256 sha = SHA256.Create())
            {
                return sha.ComputeHash(bytes);
            }
        }
    }
}
