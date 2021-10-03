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
            : base("TimeCacheNetworkServer", logger)
        {
            _connectionString = connectionString;
            _port = port;
        }

        /// <summary>
        /// Port listening on
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
                long sent = nm.Send(s);

                VTrace("sent: " + sent);
                // Connected and Authenticated
                // Now wait and respond to queries.

                #endregion Startup

                QueryManager qm = QueryManager.GetManager(_connectionString, null, _logger);

                List<SpecialQuery> specialQueries = new List<SpecialQuery>();

                while (!_stop)
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

                            StandardQuery query = new StandardQuery();

                            DateTime startTime = DateTime.UtcNow.AddHours(-6);
                            DateTime endTime = DateTime.UtcNow;

                            Match tm = ParsingUtils.TimeFilterRegex.Match(sq.Query);
                            if (tm.Success)
                            {
                                startTime = DateTime.Parse(tm.Groups["start_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
                                endTime = DateTime.Parse(tm.Groups["end_time"].Value, null, System.Globalization.DateTimeStyles.AssumeUniversal);
                            }

                            /**
                             * Meta-command format
                             * [{command,(params)}] query
                             * 
                             * 
                             */
                            if (sq.Query.StartsWith("["))
                            {
                                Match m = ParsingUtils.SpecialQueryRegex.Match(sq.Query);
                                if (m.Success)
                                {
                                    string meta = m.Groups["meta"].Value.Trim('[').TrimEnd(']');

                                    sq.Query = sq.Query.Replace(m.Groups["meta"].Value, "").TrimStart();

                                    string[] parts = meta.Split(new string[] { "},{" }, StringSplitOptions.None).Select(p => p.TrimStart('{').TrimEnd('}')).ToArray();

                                    foreach (string part in parts)
                                    {
                                        string[] optionParts = part.Split(',');

                                        SpecialQuery sp = new SpecialQuery(optionParts[0], sq.Query);
                                        sp.Start = startTime;
                                        sp.End = endTime;

                                        if (optionParts.Length > 1)
                                        {
                                            for (int p = 1; p < optionParts.Length; p++)
                                            {
                                                string[] namedOpts = optionParts[p].Split('=');
                                                if (namedOpts.Length == 2)
                                                {
                                                    if (namedOpts[0].Equals("start", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        sp.Start = DateTime.Parse(namedOpts[1]);
                                                    }
                                                    else if (namedOpts[0].Equals("end", StringComparison.OrdinalIgnoreCase))
                                                    {
                                                        sp.End = DateTime.Parse(namedOpts[1]);
                                                    }
                                                    else
                                                    {
                                                        sp.Options.Add(namedOpts[0], namedOpts[1]);
                                                    }
                                                }
                                                else
                                                {
                                                    Error("Skipping invalid option: " + optionParts[p]);
                                                }
                                            }
                                        }
                                       // else
                                        {
                                            specialQueries.Add(sp);
                                        }
                                    }
                                }
                                else
                                {
                                    Error("Unparsed special? " + sq.Query);
                                }
                            }

                            /**
                             * Options
                             * {optionkey=optionvalue} query
                             */
                            if (sq.Query.StartsWith("{"))
                            {
                                // Options
                                Match optMatch = ParsingUtils.OptionsRegex.Match(sq.Query);

                                if (optMatch.Success)
                                {
                                    // comma separated key=value pairs
                                    string[] values = optMatch.Groups["options"].Value.TrimStart('{').TrimEnd('}').Split(',');

                                    foreach (string v in values)
                                    {
                                        string[] namedOpts = v.Split('=');
                                        if (namedOpts.Length != 2)
                                        {
                                            Error("Skipping invalid option: " + v);
                                            continue;
                                        }
                                        try
                                        {
                                            switch (namedOpts[0].ToLower())
                                            {
                                                case "allowcache":
                                                case "cache":
                                                    query.AllowCache = bool.Parse(namedOpts[1]);
                                                    break;
                                                case "decomp":
                                                case "decompose":
                                                case "allowdecomp":
                                                    bool allowDecomp = bool.Parse(namedOpts[1]);
                                                    if(allowDecomp)
                                                        query.RemovedPredicates = new List<Query.QueryUtils.PredicateGroup>();
                                                    break;
                                                case "timeout":
                                                    query.Timeout = int.Parse(namedOpts[1]);
                                                    break;
                                                case "updateinterval":
                                                    query.UpdateInterval = TimeSpan.Parse(namedOpts[1]);
                                                    break;
                                                case "checkbucket":
                                                case "checkbucketduration":
                                                    query.CheckBucketDuration = bool.Parse(namedOpts[1]);
                                                    break;
                                                case "metaonly":
                                                case "metadataonly":
                                                    query.MetaOnly = bool.Parse(namedOpts[1]);
                                                    break;
                                                case "replace":
                                                    // expect 2 'values' comma-separated
                                                    string[] vals = namedOpts[1].Split(',');
                                                    if (vals.Length != 2)
                                                    {
                                                        Error("Invalid replacement option: " + namedOpts[1]);
                                                        break;
                                                    }
                                                    query.Replacements[vals[0]] = vals[1];
                                                    break;
                                                case "tag":
                                                    query.Tag = namedOpts[1];
                                                    break;
                                                default:
                                                    Error("Unknown option: " + namedOpts[0]);
                                                    break;
                                            }
                                        }
                                        catch (Exception exc)
                                        {
                                            Error("Failure handing option: " + v + ", " + exc);
                                        }
                                    }
                                    sq.Query = sq.Query.Replace(optMatch.Groups["options"].Value, "");
                                }
                                else
                                {
                                    Error("Unparsed options: " + sq.Query);
                                }

                            }



                            List<PGMessage> messageList = new List<PGMessage>();


                            query.RawQuery = sq.Query;

                            // TODO: Use dedicated flag??
                            if(query.RemovedPredicates != null)
                            {
                                Debug("Checking for predicates");
                                Query.QueryUtils.NormalizedQuery nq = Query.QueryUtils.NormalizeWherePredicate(query.RawQuery);
                                query.RemovedPredicates = nq.RemovedPredicates;
                                query.RawQuery = nq.QueryText;
                                Debug("Found predicates:" + query.RemovedPredicates.Count);
                            }

                            if (query.AllowCache)
                            {
                                if (!query.MetaOnly)
                                    messageList.AddRange(qm.CachedQuery(query));
                                else
                                    qm.CachedQuery(query, false);
                            }
                            else
                            {
                                // TODO: Trace
                                query.UpdatedQuery = query.RawQuery;
                                messageList.AddRange(PostgresqlCommunicator.Translator.BuildResponseFromData(qm.SimpleQuery(query)));
                            }

                            if (specialQueries.Count == 0)
                            {
                                messageList.Add(new CommandCompletion("SELECT " + (messageList.Count - 1)));
                                messageList.Add(new ReadyForQuery());
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

                        // Handled all queries.

                        if (specialQueries.Count > 0)
                        {
                            List<PGMessage> spList = new List<PGMessage>();

                            foreach (SpecialQuery special in specialQueries)
                            {
                                IEnumerable<PGMessage> messages = MetaCommands.HandleSpecial(special, qm);
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

                            specialQueries.Clear();
                        }


                    }
                    Trace("Bytes handled: " + bytes);
                    s.ReceiveTimeout = 0;
                }
            
            }
            catch (ThreadAbortException)
            {
                // Ignore
                if(connInfo != null)
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
                catch(Exception exc)
                {
                    Error("Failure closing socket(" + (connInfo != null ? connInfo.RemoteAddress + ")" : "unknown)") + exc);
                }
            }
        }

      
    }
}
