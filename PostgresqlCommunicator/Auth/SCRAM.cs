using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.IO;

using System.Net;
using System.Net.Sockets;

using System.Security.Cryptography;

namespace PostgresqlCommunicator.Auth
{
    /***
     * Helper class for establishing a new connection
     * 
     *  https://www.postgresql.org/docs/14/sasl-authentication.html
     */
    public static class SCRAM
    {
        /// <summary>
        /// Only supported auth for now
        /// </summary>
        private static string _defaultAuthMechanism = "SCRAM-SHA-256";

        /// <summary>
        /// Handle authenticating using SCRAM-SHA-256 SASL
        /// 
        /// TODO:
        /// Binding
        /// SCRAM-SHA-256-PLUS
        /// Add SLOG for debugging
        /// </summary>
        /// <param name="socket"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <param name="database"></param>
        /// <param name="errorDir"></param>
        public static void Authenticate(Socket socket, string username, string password, string database, string errorDir = null)
        {
            if (socket == null || !socket.Connected)
                throw new ArgumentException("Invalid socket", nameof(socket));
            if (String.IsNullOrEmpty(username))
                throw new ArgumentException("Invalid username", nameof(username));
            if (String.IsNullOrEmpty(password))
                throw new ArgumentException("Invalid password", nameof(password));
            if (String.IsNullOrEmpty(database))
                throw new ArgumentException("Invalid database", nameof(database));

            // 1) Send a startup Message
            PostgresqlCommunicator.StartupMessage message = new PostgresqlCommunicator.StartupMessage();
            message.MajorVersion = 3;
            message.MinorVersion = 0;
            message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "user", Value = username });
            message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "client_encoding", Value = "UTF8" });
            message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "database", Value = database });
            //message.Params.Add(new PostgresqlCommunicator.StartupParam() { Name = "datestyle", Value = "ISO, MDY" });

            message.SendTo(socket);

            //byte[] m = message.GetMessageBytes();

            byte[] buffer = new byte[2048];

            int sent = 0;// socket.Send(m);
            int ret = socket.Receive(buffer);

            int bufferPosition = 0;

            // 2) Response should be an AuthenticationSASL message
            PostgresqlCommunicator.AuthenticationRequestSASL ars = HandleMessage<PostgresqlCommunicator.AuthenticationRequestSASL>(ret, buffer, out bufferPosition, 0, errorDir);

            if (!ars.AuthenticationMechanisms.Contains(_defaultAuthMechanism))
            {
                throw new Exception("Cannot authenticate: SCRAM-SHA-256 not supported");
            }
            // 3) Send SASLInitialResponse message

            // Generate a random nonce
            Random r = new Random();
            byte[] nonce = new byte[18];
            for (int i = 0; i < 18; i++)
            {
                nonce[i] = (byte)r.Next(97, 122);
            }
            string clientNonce = Convert.ToBase64String(nonce);

            PostgresqlCommunicator.AuthenticationSASLInitialResponse air = new PostgresqlCommunicator.AuthenticationSASLInitialResponse()
            {
                SASLAuthData = Encoding.UTF8.GetBytes("n,,n=" + username + ",r=" + clientNonce),
                Mechanism = _defaultAuthMechanism,
            };

            byte[] b = PostgresqlCommunicator.ProtocolBuilder.BuildResponseMessage(air);
            socket.Send(b);
            ret = socket.Receive(buffer);

            // 4) Expect a continue message
            PostgresqlCommunicator.AuthenticationSASLContinue firstServerMsg = HandleMessage<PostgresqlCommunicator.AuthenticationSASLContinue>(ret, buffer, out bufferPosition, 0, errorDir);

            // Validate client nonce
            if (!firstServerMsg.ClientNonce.StartsWith(clientNonce))
                throw new Exception("Client nonce mismatch");

            byte[] saltBytes = Convert.FromBase64String(firstServerMsg.Salt);
            byte[] saltedPassword = PostgresqlCommunicator.AuthUtils.Hi(password.Normalize(NormalizationForm.FormKC), saltBytes, firstServerMsg.Iterations);
            byte[] clientKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Client Key");
            byte[] storedKey = PostgresqlCommunicator.AuthUtils.SHA(clientKey);

            // UGH - so somehow npgsql gets away with n=*,r=..." here
            // No idea how that works for them, but it fails spectacularly for me.
            // RFC says n=user, and that works so thats what we're going with
            string clientFirstMessageBare = "n=" + username + ",r=" + clientNonce;
            string serverFirstMessage = "r=" + firstServerMsg.ClientNonce + ",s=" + firstServerMsg.Salt + ",i=" + firstServerMsg.Iterations;
            string clientFinalMessageWithoutProof = "c=biws,r=" + firstServerMsg.ClientNonce;

            string authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

            byte[] clientSig = PostgresqlCommunicator.AuthUtils.HMAC(storedKey, authMessage);
            byte[] proofBytes = PostgresqlCommunicator.AuthUtils.Xor(clientKey, clientSig);
            string proof = Convert.ToBase64String(proofBytes);


            byte[] serverKey = PostgresqlCommunicator.AuthUtils.HMAC(saltedPassword, "Server Key");
            byte[] serverSig = PostgresqlCommunicator.AuthUtils.HMAC(serverKey, authMessage);

            string saslRespMessage = "c=biws,r=" + firstServerMsg.ClientNonce + ",p=" + proof;

            PostgresqlCommunicator.AuthenticationSASLResponse saslResp = new PostgresqlCommunicator.AuthenticationSASLResponse()
            {
                SASLAuthData = Encoding.UTF8.GetBytes(saslRespMessage)
            };

            b = PostgresqlCommunicator.ProtocolBuilder.BuildResponseMessage(saslResp);

            socket.Send(b);

            ret = socket.Receive(buffer);

            // Expected response should be SASL Complete + AUTH SUCCESS + Params* + ReadyForQuery
            PostgresqlCommunicator.AuthenticationSASLComplete comp = HandleMessage<PostgresqlCommunicator.AuthenticationSASLComplete>(ret, buffer, out bufferPosition, 0, errorDir);

            string serverSideSig = Encoding.UTF8.GetString(comp.SASLAuthData);
            string expectedSig = Convert.ToBase64String(serverSig);

            if (!String.Equals(serverSideSig, expectedSig))
                throw new Exception("Signature mismatch");

            int index = bufferPosition;
            PostgresqlCommunicator.AuthenticationSuccess authSuccess = HandleMessage<PostgresqlCommunicator.AuthenticationSuccess>(ret, buffer, out bufferPosition, index, @"D:\data");

            // Expect 1+ ParameterStatus messages, Keydata, and a ReadyForQuery
            while (bufferPosition < ret)
            {
                index = bufferPosition;
                PostgresqlCommunicator.PGMessage unknownMessage = HandleMessage<PostgresqlCommunicator.PGMessage>(ret, buffer, out bufferPosition, index, @"D:\data");
                //if (unknownMessage is PostgresqlCommunicator.ParameterStatus)
                //{
                //    PostgresqlCommunicator.ParameterStatus ps = unknownMessage as PostgresqlCommunicator.ParameterStatus;
                //}
                if (unknownMessage is PostgresqlCommunicator.ReadyForQuery)
                {
                    if (bufferPosition != ret)
                    {
                        throw new Exception("Bytes left in buffer");
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="serverSocket">Postgresql socket</param>
        /// <param name="clientSocket">client socket (ie grafana)</param>
        /// <param name="errorDir"></param>
        public static void ForwardAuthenticate(Socket serverSocket, Socket clientSocket, PostgresqlCommunicator.StartupMessage startMessage, string errorDir = null, bool handleSimple = true)
        {
            if (serverSocket == null || !serverSocket.Connected)
                throw new ArgumentException("Invalid server socket", nameof(serverSocket));

            if (clientSocket == null || !clientSocket.Connected)
                throw new ArgumentException("Invalid client socket", nameof(clientSocket));

            if(startMessage == null)
                throw new ArgumentException("Invalid start messsage", nameof(startMessage));

            // Send the startup message to the server 
            //byte[] m = startMessage.GetMessageBytes();
          
            byte[] buffer = new byte[2048];

            startMessage.SendTo(serverSocket);
            int sent = 0;// serverSocket.Send(m);
            int ret = serverSocket.Receive(buffer);

            int bufferPosition = 0;

            // 2) Response should be an AuthenticationSASL message
            PostgresqlCommunicator.AuthenticationRequestSASL ars = HandleMessage<PostgresqlCommunicator.AuthenticationRequestSASL>(ret, buffer, out bufferPosition, 0, errorDir);

            if (!ars.AuthenticationMechanisms.Contains(_defaultAuthMechanism))
            {
                throw new Exception("Cannot authenticate: SCRAM-SHA-256 not supported");
            }

            Console.WriteLine("Received SASL auth request - forwarding to client");

            ByteWrapper bw = ByteWrapper.Get(65535);

            ars.WriteTo(bw);

            bw.Send(clientSocket);



            // 3) Handle SASLInitialResponse message
            ret = clientSocket.Receive(buffer);
            PostgresqlCommunicator.AuthenticationSASLInitialResponse air = HandleMessage<PostgresqlCommunicator.AuthenticationSASLInitialResponse>(ret, buffer, out bufferPosition, 0, errorDir);
            Console.WriteLine("Got sasl initial response from client - forwarding to server");

            bw.Clear();
            air.WriteTo(bw);

            bw.Send(serverSocket);

            ret = serverSocket.Receive(buffer);
            PostgresqlCommunicator.AuthenticationSASLContinue firstServerMsg = HandleMessage<PostgresqlCommunicator.AuthenticationSASLContinue>(ret, buffer, out bufferPosition, 0, errorDir);

            Console.WriteLine("Received SASL continue from server- forwarding to client");


            bw.Clear();

            firstServerMsg.WriteTo(bw);

            bw.Send(clientSocket);

            ret = clientSocket.Receive(buffer);
            PostgresqlCommunicator.AuthenticationSASLResponse saslResp = HandleMessage<PostgresqlCommunicator.AuthenticationSASLResponse>(ret, buffer, out bufferPosition, 0, errorDir);

            Console.WriteLine("Got sasl response - forwarding to server");

            bw.Clear();

            saslResp.WriteTo(bw);
            bw.Send(serverSocket);

            ret = serverSocket.Receive(buffer);

            // Expected response should be SASL Complete + AUTH SUCCESS + Params* + ReadyForQuery
            PostgresqlCommunicator.AuthenticationSASLComplete comp = HandleMessage<PostgresqlCommunicator.AuthenticationSASLComplete>(ret, buffer, out bufferPosition, 0, errorDir);

            Console.WriteLine("Got sasl auth complete");
            
            int index = bufferPosition;
            PostgresqlCommunicator.AuthenticationSuccess authSuccess = HandleMessage<PostgresqlCommunicator.AuthenticationSuccess>(ret, buffer, out bufferPosition, index, @"D:\data");

            Console.WriteLine("Got auth success");

            List<PGMessage> messages = new List<PGMessage>();
            messages.Add(comp);
            messages.Add(authSuccess);

            //// Expect 1+ ParameterStatus messages, Keydata, and a ReadyForQuery
            while (bufferPosition < ret)
            {
                index = bufferPosition;
                PostgresqlCommunicator.PGMessage unknownMessage = HandleMessage<PostgresqlCommunicator.PGMessage>(ret, buffer, out bufferPosition, index, @"D:\data");
                //if (unknownMessage is PostgresqlCommunicator.ParameterStatus)
                //{
                //    PostgresqlCommunicator.ParameterStatus ps = unknownMessage as PostgresqlCommunicator.ParameterStatus;
                //}
                if (unknownMessage is PostgresqlCommunicator.ReadyForQuery)
                {
                    if (bufferPosition != ret)
                    {
                        throw new Exception("Bytes left in buffer");
                    }
                }
                messages.Add(unknownMessage);
            }
            // Forward final block to client
            NetworkMessage successBlock = ProtocolBuilder.BuildResponseMessage(messages);
            Console.WriteLine("Sending final success block");
            successBlock.Send(clientSocket);

            // Receive simple query
            if (handleSimple)
            {
                ret = clientSocket.Receive(buffer);
                // PostgresqlCommunicator.SimpleQuery simple = HandleMessage<PostgresqlCommunicator.SimpleQuery>(ret, buffer, out bufferPosition, 0, errorDir);
                //  Console.WriteLine("Received simple query: " + simple.Query);
                serverSocket.Send(buffer, ret, SocketFlags.None);
                ret = serverSocket.Receive(buffer);
                clientSocket.Send(buffer, ret, SocketFlags.None);
            }
        }


        public static T HandleMessage<T>(int ret, byte[] buffer, out int bufferPosition, int index = 0, string saveDir = null) where T : PostgresqlCommunicator.PGMessage
        {
            bufferPosition = 0;
            PostgresqlCommunicator.PGMessage responseMessage = PostgresqlCommunicator.MessageParser.ReadMessage(buffer, index, ret, out bufferPosition, typeof(T));
            if (responseMessage == null)
            {
                byte[] response = new byte[ret];
                Buffer.BlockCopy(buffer, index, response, 0, ret);

                if (saveDir != null)
                {
                    File.WriteAllBytes(Path.Combine(saveDir, "response.bin"), response);
                }
                throw new Exception("Failed to parse response message");
            }

            //Console.WriteLine("Received: " + responseMessage.GetType());
            PostgresqlCommunicator.ErrorResponseMessage erm = responseMessage as PostgresqlCommunicator.ErrorResponseMessage;

            if (erm != null)
            {
                throw new Exception("Error Message:" + erm.ErrorText);
            }

            T expectedMessage = responseMessage as T;

            if (expectedMessage == null)
                throw new Exception("Did not receive expected message: " + typeof(T) + ". Received: " + responseMessage.GetType());

            return expectedMessage;
        }
    }
}
