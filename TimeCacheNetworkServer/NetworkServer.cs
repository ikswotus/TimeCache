using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Threading;

using System.Net;
using System.Net.Sockets;
using PostgresqlCommunicator;
using System.IO;
using System.Text.RegularExpressions;

using System.Data;


namespace TimeCacheNetworkServer
{
    /// <summary>
    /// Simple network server to handle listening for incoming connections. 
    /// Initial communication verifies the incoming postgresql request.
    /// 
    /// Requires:
    /// Listening port (Default is 5433)
    /// Connection String (Npgsql is used for actual db communication)
    /// </summary>
    public class NetworkServer : SLog.SLoggableObject
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Postgresql connection string.</param>
        /// <param name="port">Port to listen on, defaults to 5433</param>
        public NetworkServer(string connectionString, int port = 5433)
            : this(connectionString, port, new SLog.SLogger("TimeCacheNetworkServer"))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">Postgresql connection string.</param>
        /// <param name="port">Port to listen on, defaults to 5433</param>
        /// <param name="logger">Log instance to use</param>
        public NetworkServer(string connectionString, int port, SLog.ISLogger logger)
            : this(connectionString, null, -1, port, logger)
        {
        }

        public NetworkServer(IPAddress pgIP, int port, int pgPort)
            : this(null, pgIP, pgPort, port, new SLog.SLogger("TimeCacheNetworkServer"))
        {
        }


        /// <summary>
        /// Full connection string
        /// </summary>
        /// <param name="connectionString"></param>
        /// <param name="pgAuthIP"></param>
        /// <param name="pgPort"></param>
        /// <param name="serverPort"></param>
        /// <param name="logger"></param>
        public NetworkServer(string connectionString, IPAddress pgAuthIP, int pgPort, int serverPort, SLog.ISLogger logger)
            : base("TimeCacheNetworkServer", logger)
        {

            _postgresPort = pgPort;
            _postgresIP = pgAuthIP;

            _connectionString = connectionString;
            _port = serverPort;
        }

        /// <summary>
        /// Port for postgresql auth connections
        /// </summary>
        private readonly int _postgresPort = 5432;

        /// <summary>
        /// IP of actual postgresql server
        /// </summary>
        private readonly IPAddress _postgresIP = null;

        /// <summary>
        /// Port server will be listening on
        /// </summary>
        private readonly int _port = 5433;

        /// <summary>
        /// Connection string to pass to our connection manager to perform actual queries
        /// </summary>
        private readonly string _connectionString = null;

        /// <summary>
        /// Stop flag - set by program when shutdown is requested.
        /// </summary>
        private volatile bool _stop = false;

        /// <summary>
        /// Active sockets will be pushed onto new threads.
        /// </summary>
        private List<Thread> handlers = new List<Thread>();

        /// <summary>
        /// Main thread runner
        /// </summary>
        private Thread _runner;

        /// <summary>
        /// Main server socket
        /// </summary>
        private Socket _serverSock;

        /// <summary>
        /// Stop handler - called when server is being shutdown.
        /// </summary>
        public void Stop()
        {
            Debug("Stopping");

            _stop = true;

            QueryManager.Stop();

            // TODO: Join on QM instead of simply waiting for cleaner shutdown?
            Thread.Sleep(2000);

            lock (handlers)
            {
                foreach (Thread t in handlers)
                    try { t.Abort(); } catch { }
            }

            if (_serverSock != null)
                _serverSock.Close();

            if (_runner != null)
            {
                if (!_runner.Join(2000))
                {
                    try
                    {
                        _runner.Abort();

                    }
                    catch { }
                }
            }
        }

        /// <summary>
        /// Starts a thread to handle new incoming connections.
        /// </summary>
        public void Start()
        {
            Debug("Starting server");
            _stop = false;

            if (_runner == null)
            {
                _runner = new Thread(() => RunLoop());
                _runner.Start();
            }
        }


        /// <summary>
        /// Super simple Main loop - until Stop() is called, listen for new incoming tcp connections and push them onto a handler thread
        /// </summary>
        private void RunLoop()
        {
            Debug("Run loop starting");
            try
            {
                _serverSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                IPAddress ipAddress = NetworkUtils.GetIP(true);

                IPEndPoint localEndPoint = new IPEndPoint(ipAddress, _port);

                _serverSock.Bind(localEndPoint);
                _serverSock.Listen(10);

                while (!_stop)
                {
                    Socket actualSocket = _serverSock.Accept();

                    Debug("New connection: " + actualSocket.RemoteEndPoint.ToString());

                    // Push the new connection onto a handler thread and resume accepting.
                    Thread handler = new Thread(() => HandleConnect(actualSocket));
                    handler.Start();

                    lock (handlers)
                        handlers.Add(handler);
                }
            }

            catch (Exception exc)
            {
                Critical("Fatal Error in main loop: " + exc);
            }
        }

        /// <summary>
        /// Maintain our list of active connections to report to UI
        /// </summary>
        private List<ActiveConnectionInfo> _connections = new List<ActiveConnectionInfo>();

        /// <summary>
        /// Retrieve a copy of current set of connections.
        /// </summary>
        /// <returns></returns>
        public List<ActiveConnectionInfo> GetConnectionInfo()
        {
            lock (_connections)
                return _connections.ToList();
        }

        /// <summary>
        /// Pair sockets with remote ips
        /// </summary>
        private Dictionary<string, Socket> _connHandlerMap = new Dictionary<string, Socket>();

        /// <summary>
        /// Allow terminating a connection by ip
        /// </summary>
        /// <param name="conn"></param>
        /// <returns></returns>
        public bool KillConnection(ActiveConnectionInfo conn)
        {
            lock (_connHandlerMap)
            {
                if (_connHandlerMap.ContainsKey(conn.RemoteAddress))
                {
                    _connHandlerMap[conn.RemoteAddress].Shutdown(SocketShutdown.Both);
                    return true;
                }
            }
            return false;
        }


        /// <summary>
        /// Called when a new socket connection is received.
        /// 
        /// This will perform the startup synchronization with the client, then handle queries
        /// </summary>
        /// <param name="s">A connected socket.</param>
        private void HandleConnect(Socket s)
        {
            string re = null;
            ActiveConnectionInfo connInfo = new ActiveConnectionInfo();
            try
            {
                re = s.RemoteEndPoint.ToString();

                connInfo.RemoteAddress = re;

                lock (_connections)
                    _connections.Add(connInfo);
                lock (_connHandlerMap)
                    _connHandlerMap[re] = s;

                // Socket setup for initial sync, require a timeout.
                // inside the loop we remain connected...
                s.ReceiveTimeout = 2000;

                byte[] buffer = new byte[1024];

                #region Startup

                // Authenticate
                int bytes = s.Receive(buffer);

                if (bytes <= 4)
                    throw new Exception("invalid startup packet. Length <= 4");

                StartupMessage sm = StartupMessage.ParseMessage(buffer, 0);

                long sent = 0;
                if (_postgresIP != null)
                {
                    Debug("Attempting to forward authentication to : " + _postgresIP);
                    Socket serverSock = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);

                    IPEndPoint pgEndPoint = new IPEndPoint(_postgresIP, _postgresPort);

                    serverSock.Connect(pgEndPoint);


                    PostgresqlCommunicator.Auth.SCRAM.ForwardAuthenticate(serverSock, s, sm, null, false);

                    Debug("Authentication successful - accepting queries");
                }
                else // Dummy auth
                {
                    Debug("Dummy authentication only");
                    AuthenticationMD5Password am = new AuthenticationMD5Password();
                    Random r = new Random();
                    r.NextBytes(am.Salt);

                    byte[] authRequest = am.GetMessagePayload();
                    s.Send(authRequest);

                    // TODO: Perform some actual password validation to ensure authenticity

                    bytes = s.Receive(buffer);

                    AuthenticationMD5Response amr = AuthenticationMD5Response.FromBytes(buffer, 0);

                    List<PGMessage> response = new List<PGMessage>()
                    {
                        new AuthenticationOK(),
                        new ReadyForQuery()
                    };

                    // Send 
                    Debug("sending authok + rfq");
                    NetworkMessage nm = ProtocolBuilder.BuildResponseMessage(response);
                    //byte[] complete = ProtocolBuilder.BuildResponse(response);
                    sent = nm.Send(s);

                    VTrace("sent: " + sent);
                    // Connected and Authenticated
                    // Now wait and respond to queries.
                }


                #endregion Startup

                QueryManager qm = QueryManager.GetManager(_connectionString, null, _logger);

                Query.QueryParser qp = new Query.QueryParser(_logger);

                while (!_stop)
                {
                    try
                    {
                        while (s.Available <= 0 && s.Connected)
                            Thread.Sleep(500);

                        Trace("Receiving");
                        bytes = s.Receive(buffer);


                        connInfo.LastActivity = DateTime.UtcNow;
                        connInfo.QueriesEvaluated++;// TODO: Might be more than one in the payload.

                        if (bytes == 0)
                        {
                            Debug("Exiting connection loop: 0 bytes received");
                            break;// TODO: Weird loops when grafana gets hung up on something...
                        }

                        Debug("Received {0} bytes: " + DateTime.UtcNow.ToString("O"), bytes);
                        // Simple query
                        for (int i = 0; i < bytes; i++)
                        {
                            byte pType = buffer[i];

                            if (pType == PGTypes.SimpleQuery)
                            {
                                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                                sw.Start();

                                // Get Length
                                int length = (buffer[i + 1] << 24 | buffer[i + 2] << 16 | buffer[i + 3] << 8 | buffer[i + 4]);
                                length -= 4;
                                //Console.WriteLine("Length: " + length);
                                int index = i + 5;

                                // Parsing Test
                                PostgresqlCommunicator.SimpleQuery sq = new PostgresqlCommunicator.SimpleQuery(buffer, index, length);
                                Debug("Received query: [" + sq.Query + "]");

                                Query.NormalizedQuery query = qp.Normalize(sq.Query);


                                List<PGMessage> messageList = new List<PGMessage>();

                                if (query.AllowCache)
                                {
                                    if (!query.ReturnMetaOnly)
                                    {
                                        IEnumerable<PGMessage> res = qm.CachedQuery(query);
                                        if (res == null)
                                        {
                                            Error("Cached query returned null?");
                                        }
                                        else
                                        {
                                            messageList.AddRange(res);
                                        }
                                    }
                                    else
                                    {
                                        Debug("Meta-Only query received, executing: " + query.ExecuteMetaOnly);
                                        if (!query.ExecuteMetaOnly)
                                            qm.CachedQuery(query, false);
                                    }
                                }
                                else
                                {
                                    Trace("Not allowed to cache: Simple query");
                                    messageList.AddRange(PostgresqlCommunicator.Translator.BuildResponseFromData(qm.SimpleQuery(query)));
                                }

                                if (query.MetaCommands.Count == 0)
                                {
                                    messageList.Add(new CommandCompletion("SELECT " + (messageList.Count - 1)));
                                    messageList.Add(new ReadyForQuery());
                                }
                                else
                                {
                                    List<PGMessage> spList = new List<PGMessage>();

                                    foreach (SpecialQuery special in query.MetaCommands)
                                    {
                                        IEnumerable<PGMessage> messages = MetaCommands.HandleSpecial(special, qm, query);
                                        if (messages == null)
                                            continue;
                                        spList.AddRange(messages);
                                    }

                                    spList.Add(new CommandCompletion("SELECT 1"));
                                    spList.Add(new ReadyForQuery());

                                    NetworkMessage sPmess = ProtocolBuilder.BuildResponseMessage(spList);
                                    long sb = sPmess.Send(s);
                                    // byte[] payload = ProtocolBuilder.BuildResponse(spList);

                                    // Send select result
                                    Debug("Sending special response");
                                    //int sb = s.Send(payload);
                                    //Trace("Sent: " + sb + ", " + payload.Length);
                                    Trace("Sent: " + sb);
                                }

                                NetworkMessage message = ProtocolBuilder.BuildResponseMessage(messageList);
                                sent = message.Send(s);
                                message = null;

                                sw.Stop();
                                VTrace("TotalTime: " + sw.ElapsedMilliseconds.ToString());

                                i += (length + 5);
                            }
                            else
                            {
                                Critical("Unhandled type: " + PGTypes.GetType(pType));
                            }




                        }
                        Trace("Bytes handled: " + bytes);
                        s.ReceiveTimeout = 0;
                    }

                    catch (Npgsql.NpgsqlException npgExc)
                    {
                        Error("Failure: " + npgExc.ToString());
                        // Attempt to forward message details
                        try
                        {
                            PostgresqlCommunicator.ErrorResponseMessage erm = new ErrorResponseMessage();
                            erm.Severity = npgExc.Data["Severity"] as string;
                            erm.Code = npgExc.Data["SqlState"].ToString();
                            erm.Message = npgExc.Data["MessageText"] as string;
                            erm.Position = npgExc.Data["Position"].ToString();
                            erm.File = npgExc.Data["File"] as string;
                            erm.Line = npgExc.Data["Line"].ToString();
                            erm.Routine = npgExc.Data["Routine"] as string;

                            List<PGMessage> response = new List<PGMessage>() { erm, new ReadyForQuery() };
                            NetworkMessage errorResp = ProtocolBuilder.BuildResponseMessage(response);
                            errorResp.Send(s);
                        }
                        catch (Exception exc)
                        {
                            Critical("Failed to send error response message:" + exc.ToString());
                            throw exc;
                        }
                    }
                }


            }
            catch (ThreadAbortException)
            {
                // Ignore
                if (connInfo != null)
                    Trace("Thread abort: " + connInfo.RemoteAddress);
            }

            catch (Exception exc)
            {
                Error("Failure in socket handler: " + exc);
            }
            finally
            {
                if (connInfo != null)
                    lock (_connections)
                        _connections.Remove(connInfo);
                lock (_connHandlerMap)
                    _connHandlerMap.Remove(re);

                try
                {
                    s.Shutdown(SocketShutdown.Both);
                    s.Close();
                }
                catch (Exception exc)
                {
                    Error("Failure closing socket(" + (connInfo != null ? connInfo.RemoteAddress + ")" : "unknown)") + exc);
                }
            }
        }


    }
}
