using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Security.Cryptography;

namespace ConsoleApp1
{
    class Program
    {
        static void Main(string[] args)
        {
            try
            {
                byte[] clientNonceBytes = new byte[]
                {
                     0x2f ,0x69 ,0x76
                    ,0x44 ,0x49 ,0x32 ,0x7a ,0x31 ,0x69 ,0x34 ,0x4a ,0x69 ,0x47 ,0x53 ,0x39 ,0x61 ,0x30,0x34 ,0x62
                    ,0x74 ,0x35 ,0x51 ,0x3d ,0x3d
                };

                byte[] serverNonceBytes = new byte[]
                {
                    0x2f ,0x69 ,0x76
                    ,0x44 ,0x49 ,0x32 ,0x7a ,0x31 ,0x69 ,0x34 ,0x4a ,0x69 ,0x47 ,0x53 ,0x39 ,0x61 ,0x30,0x34 ,0x62
                    ,0x74 ,0x35 ,0x51 ,0x3d ,0x3d,
                    0x39 ,0x34 ,0x33 ,0x42 ,0x79 ,0x4b ,0x38 ,0x35 ,0x55 ,0x42 ,0x4d ,0x38 ,0x48
                    ,0x68 ,0x53 ,0x43 ,0x35 ,0x45 ,0x6b ,0x76 ,0x4a ,0x4f ,0x63, 0x64
                };

                byte[] salt = new byte[]
                {
                    0x31, 0x67 , 0x4d , 0x5a , 0x62 , 0x74 , 0x51 , 0x44 , 0x76 , 0x58 , 0x48 , 0x6a , 0x76 , 0x79 , 0x4b , 0x62 , 0x42 , 0x76
                    , 0x66 , 0x61 , 0x6f , 0x67, 0x3d , 0x3d
                };

                string user_name = "grafana_reader";
                string password = "gr12345";


                PostgresqlCommunicator.AuthenticationSASLContinue firstServerMsg = new PostgresqlCommunicator.AuthenticationSASLContinue();
                firstServerMsg.ClientNonce = "/ivDI2z1i4JiGS9a04bt5Q==+JF66C/V+ii7A4/lTfdqLTLX";
                    //Encoding.UTF8.GetString(serverNonceBytes);
                firstServerMsg.Salt = Encoding.UTF8.GetString(salt);
                firstServerMsg.Iterations = 4096;

                // string clientNonce = Encoding.UTF8.GetString(clientNonceBytes);
                string clientNonce = "/ivDI2z1i4JiGS9a04bt5Q==";

                byte[] saltBytes = Convert.FromBase64String(firstServerMsg.Salt);

                
                byte[] saltedPassword = PostgresqlCommunicator.AuthUtils.Hi(password.Normalize(NormalizationForm.FormKC), saltBytes, firstServerMsg.Iterations);
                
                byte[] clientKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Client Key");

                byte[] storedKey;
                using (var sha256 = SHA256.Create())
                    storedKey = sha256.ComputeHash(clientKey);
                // Build new messages
                string clientFirstMessageBar = "n=*,r=" + clientNonce;
                string serverFirstMessage = "r=" + firstServerMsg.ClientNonce + ",s=" + firstServerMsg.Salt + ",i=" + firstServerMsg.Iterations;
                string clientFinalMessageWithoutProof = "c=biws,r=" + firstServerMsg.ClientNonce;

                string authMessage = clientFirstMessageBar + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
                Console.WriteLine("AuthMessage:" + authMessage);
               // string authMessage = "n=*,r=" + clientNonce + "," + asc.FullText + "," + "c=biws,r=" + asc.ClientNonce;

                byte[] clientSig = PostgresqlCommunicator.AuthUtils.HMAC(storedKey, authMessage);
                byte[] proofBytes = PostgresqlCommunicator.AuthUtils.Xor(clientKey, clientSig);
                string proof = Convert.ToBase64String(proofBytes);



                byte[] serverKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Server Key");
                byte[] serverSig = PostgresqlCommunicator.AuthUtils.HMAC(serverKey, authMessage);

                // SASL Response
                string saslRespMessage = clientFinalMessageWithoutProof+ ",p=" + proof;
                Console.WriteLine(saslRespMessage);
                System.IO.File.WriteAllText(@"D:\data\me.txt", saslRespMessage);
                PostgresqlCommunicator.AuthenticationSASLResponse saslResp = new PostgresqlCommunicator.AuthenticationSASLResponse();
                saslResp.SASLAuthData = Encoding.UTF8.GetBytes(saslRespMessage);
                byte[] b = PostgresqlCommunicator.ProtocolBuilder.BuildResponseMessage(saslResp);

                //System.IO.File.WriteAllBytes(@"D:\data\b.bin", b);
                string s = Encoding.UTF8.GetString(b);
                Console.WriteLine(s);

            }
            catch(Exception exc)
            {
                Console.WriteLine("Failure: " + exc);
            }
            
            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
