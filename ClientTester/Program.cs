using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

using System.Net;
using System.Net.Sockets;

using System.Data;
using System.Security.Cryptography;

namespace ClientTester
{
    /**
     * Simple test framework for PG message flow
     */
    class Program
    {
        static void Main(string[] args)
        {
            try
            {



                string ip = null;
                int port = 5432;
#if DEBUG
                ip = "::1";
#else 
    ip = args[0];
#endif

                Socket clientSock = new Socket(AddressFamily.InterNetworkV6, SocketType.Stream, ProtocolType.Tcp);

                IPEndPoint pgEndPoint = new IPEndPoint(IPAddress.Parse(ip), port);

                clientSock.Connect(pgEndPoint);


                // 1) Send a startup Message
                PostgresqlCommunicator.StartupMessage message = new PostgresqlCommunicator.StartupMessage();
                message.MajorVersion = 3;
                message.MinorVersion = 0;
                message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "user", Value = "grafana_reader" });
                message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "client_encoding", Value = "UTF8" });
                message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "database", Value = "perftest" });
                //message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "datestyle", Value = "ISO, MDY" });

                byte[] m = message.GetMessageBytes();



            

                PostgresqlCommunicator.Auth.SCRAM.Authenticate(clientSock, "grafana_reader", "gr12345", "perftest", @"D:\data");

                Console.WriteLine("Connected + Authenticated");

                // Try a query
                string q = @"SELECT metric_name, sample_time as time, current_value FROM test.simple_test WHERE sample_time BETWEEN '2021-10-24T15:03:37.474Z' AND '2021-10-24T16:03:37.474Z' ORDER BY 2 ASC";

                DataTable ret = PostgresqlCommunicator.QueryHelper.SimpleQuery(clientSock, q);

                Console.WriteLine("Table:" + ret.Rows.Count);

                //  // Startup Message
                //  PostgresqlCommunicator.StartupMessage message = new PostgresqlCommunicator.StartupMessage();
                //  message.MajorVersion = 3;
                //  message.MinorVersion = 0;
                //  message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "user", Value = "grafana_reader" });
                //  message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "client_encoding", Value = "UTF8" });
                //  message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "database", Value = "perftest" });
                //  //message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "datestyle", Value = "ISO, MDY" });

                //  byte[] m = message.GetMessageBytes();

                //  Console.WriteLine(m.Length);

                //  int sent = clientSock.Send(m);
                //  Console.WriteLine("Sent: " + sent);

                //  byte[] buffer = new byte[2048];

                //  int ret = clientSock.Receive(buffer);
                //  Console.WriteLine("Auth:" + ret);

                //  // Expecting SASL Authentication
                //  // https://www.postgresql.org/docs/14/sasl-authentication.html

                //  int bufferPosition = 0;

                //  // 1) Expect an AuthenticationSASL message
                //  PostgresqlCommunicator.AuthenticationRequestSASL ars = HandleMessage<PostgresqlCommunicator.AuthenticationRequestSASL>(ret, buffer, out bufferPosition, 0, @"D:\data");
                //  if (ars == null)
                //      throw new Exception("Did not receive SASL request");

                //  string chosen = "SCRAM-SHA-256";
                //  if (!ars.AuthenticationMechanisms.Contains(chosen))
                //  {
                //      throw new Exception("Cannot authenticate: SCRAM-SHA-256 not supported");
                //  }

                //  // 2) Client (us) chooses mechanism + sends SASLInitialResponse message
                //  string user = "grafana_reader";
                //  // Generate a random nonce
                //  Random r = new Random();
                //  byte[] nonce = new byte[18];
                //  // 97-122
                //  for(int i =0; i< 18; i++)
                //  {
                //     nonce[i] = (byte)r.Next(97, 122);
                //  }
                //  PostgresqlCommunicator.AuthenticationSASLInitialResponse air = new PostgresqlCommunicator.AuthenticationSASLInitialResponse();

                //  string clientNonce = Convert.ToBase64String(nonce);

                //  air.SASLAuthData = Encoding.UTF8.GetBytes("n,,n=" + user + ",r=" +clientNonce);

                //  air.Mechanism = chosen;

                //  // Send it!
                //  Console.WriteLine("Sending SCRAM SASL initial Response");
                //  byte[] b = PostgresqlCommunicator.ProtocolBuilder.BuildResponseMessage(air);
                //  clientSock.Send(b);

                //  // Await response
                //  ret = clientSock.Receive(buffer);
                //  Console.WriteLine("Received response for SASL: " + ret);


                //  PostgresqlCommunicator.AuthenticationSASLContinue firstServerMsg = HandleMessage<PostgresqlCommunicator.AuthenticationSASLContinue>(ret, buffer, out bufferPosition, 0, @"D:\data");
                //  if (firstServerMsg == null)
                //      throw new Exception("Failed to receive SASL continue from server");

                //  // Validate client nonce
                //  if (!firstServerMsg.ClientNonce.StartsWith(clientNonce))
                //      throw new Exception("Client nonce mismatch");




                //  byte[] saltBytes = Convert.FromBase64String(firstServerMsg.Salt);


                //  byte[] saltedPassword = PostgresqlCommunicator.AuthUtils.Hi("gr12345".Normalize(NormalizationForm.FormKC), saltBytes, firstServerMsg.Iterations);

                //  byte[] clientKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Client Key");

                //  byte[] storedKey = PostgresqlCommunicator.AuthUtils.SHA(clientKey);

                //  // UGH- so somehow npgsql gets away with n=*,r=..." here
                //  // No idea how that works for them, but it fails spectacularly for me.
                //  // RFC says n=user, and that works so thats what we're going with
                //  string clientFirstMessageBare = "n=grafana_reader,r=" + clientNonce;
                //  string serverFirstMessage = "r=" + firstServerMsg.ClientNonce + ",s=" + firstServerMsg.Salt + ",i=" + firstServerMsg.Iterations;
                //  string clientFinalMessageWithoutProof = "c=biws,r=" + firstServerMsg.ClientNonce;

                //  string authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;
                //  //Console.WriteLine("AuthMessage:" + authMessage);
                //  // string authMessage = "n=*,r=" + clientNonce + "," + asc.FullText + "," + "c=biws,r=" + asc.ClientNonce;

                //  byte[] clientSig = PostgresqlCommunicator.AuthUtils.HMAC(storedKey, authMessage);
                //  byte[] proofBytes = PostgresqlCommunicator.AuthUtils.Xor(clientKey, clientSig);
                //  string proof = Convert.ToBase64String(proofBytes);



                //  byte[] serverKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Server Key");
                //  byte[] serverSig = PostgresqlCommunicator.AuthUtils.HMAC(serverKey, authMessage);

                // // Console.WriteLine("ServerSig: " + );

                //  // SASL Response
                //  string saslRespMessage = "c=biws,r=" + firstServerMsg.ClientNonce + ",p=" + proof;
                //  Console.WriteLine(saslRespMessage);
                ////File.WriteAllText(@"D:\data\me_msg.txt", saslRespMessage);

                //  PostgresqlCommunicator.AuthenticationSASLResponse saslResp = new PostgresqlCommunicator.AuthenticationSASLResponse();
                //  saslResp.SASLAuthData = Encoding.UTF8.GetBytes(saslRespMessage);
                //  b = PostgresqlCommunicator.ProtocolBuilder.BuildResponseMessage(saslResp);
                //  Console.WriteLine("Sending SASL response");
                //  clientSock.Send(b);

                //  ret = clientSock.Receive(buffer);
                //  Console.WriteLine("Received response for SASL: " + ret);

                //  // Expected response should be SASL Complete + AUTH SUCCESS + Params* + ReadyForQuery
                //  PostgresqlCommunicator.AuthenticationSASLComplete comp = HandleMessage<PostgresqlCommunicator.AuthenticationSASLComplete>(ret, buffer, out bufferPosition, 0, @"D:\data");
                //  string serverSideSig = Encoding.UTF8.GetString(comp.SASLAuthData);
                //  string expectedSig = Convert.ToBase64String(serverSig);
                //  if (!String.Equals(serverSideSig, expectedSig))
                //      throw new Exception("Signature mismatch");


                //  int index = bufferPosition;
                //  PostgresqlCommunicator.AuthenticationSuccess authSuccess = HandleMessage<PostgresqlCommunicator.AuthenticationSuccess>(ret, buffer, out bufferPosition, index, @"D:\data");
                //  Console.WriteLine("Got auth Success!");

                //  while(bufferPosition < ret)
                //  {
                //      index = bufferPosition;
                //      PostgresqlCommunicator.PGMessage unknownMessage = HandleMessage<PostgresqlCommunicator.PGMessage>(ret, buffer, out bufferPosition, index, @"D:\data");
                //      Console.WriteLine("Received: " + unknownMessage.MessageType.ToString());
                //      if(unknownMessage is PostgresqlCommunicator.ParameterStatus)
                //      {
                //          PostgresqlCommunicator.ParameterStatus ps = unknownMessage as PostgresqlCommunicator.ParameterStatus;
                //          Console.WriteLine(ps.ParameterName + ":->:" + ps.ParameterValue);
                //      }
                //      if(unknownMessage is PostgresqlCommunicator.ReadyForQuery)
                //      {
                //          Console.WriteLine("Ready for query");
                //          if(bufferPosition != ret)
                //          {
                //              throw new Exception("Bytes left in buffer");
                //          }
                //      }
                //  }


            }
            catch (Exception exc)
            {
                Console.WriteLine("Failure: " + exc);
            }

            Console.WriteLine("Done");
            Console.ReadKey();
        }


        //public static T HandleMessage<T>(int ret, byte[] buffer, out int bufferPosition, int index = 0, string saveDir = null) where T : PostgresqlCommunicator.PGMessage
        //{
        //    bufferPosition = 0;
        //    PostgresqlCommunicator.PGMessage responseMessage = PostgresqlCommunicator.MessageParser.ReadMessage(buffer, index, ret, out bufferPosition);
        //    if (responseMessage == null)
        //    {
        //        byte[] response = new byte[ret];
        //        Buffer.BlockCopy(buffer, index, response, 0, ret);

        //        if (saveDir != null)
        //        {
        //            File.WriteAllBytes(Path.Combine(saveDir, "response.bin"), response);
        //        }
        //        throw new Exception("Failed to parse response message");
        //    }

        //    //Console.WriteLine("Received: " + responseMessage.GetType());
        //    PostgresqlCommunicator.ErrorResponseMessage erm = responseMessage as PostgresqlCommunicator.ErrorResponseMessage;

        //    if (erm != null)
        //    {
        //        throw new Exception("Error Message:" + erm.ErrorText);
        //    }

        //    T expectedMessage = responseMessage as T;
            
        //    if (expectedMessage == null)
        //        throw new Exception("Did not receive expected message: " + typeof(T) +". Received: " + responseMessage.GetType());

        //    return expectedMessage;
        //}
    }
}
