using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Net.Sockets;
using System.Data;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Helper class for sending queries + receiving data
    /// </summary>
    public static class QueryHelper
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="s">Socket - Already authenticated</param>
        /// <param name="query">Query to execute</param>
        /// <returns></returns>
        public static DataTable SimpleQuery(Socket s, string query)
        {
            DataTable results = new DataTable();

            SimpleQuery sq = new SimpleQuery(query);

            ByteWrapper bw = ByteWrapper.Get(65535);

            sq.WriteTo(bw);

            //byte[] b = sq.GetMessageBytes();

            int sent = bw.Send(s);

            MessageReader mr = new MessageReader(s);

            RowDescription rd = mr.ReadMessage<RowDescription>();
            foreach(RowDescriptionField rdf in rd.Fields)
            {
                // Convert type
                results.Columns.Add(rdf.ColumnName, Translator.ReverseOIDLookup(rdf.TypeOID));
            }

            PGMessage mess = mr.ReadMessage();
            while(mess != null)
            {
               // Console.WriteLine("Recieved: " + mess.GetType().ToString());
               
                if(mess is CommandCompletion)
                {
                    Console.WriteLine("Command completed");
                    break;
                }
                // Should only get drms (or error...but that would throw on RowDescrptor read)
                DataRowMessage drm = mess as DataRowMessage;
                if(drm == null)
                {
                    throw new Exception("Unknown message received: " + mess.GetType().ToString());
                }
                if (drm.Fields.Count != rd.Fields.Count)
                    throw new Exception("Field count mismatch");

                object[] ret = new object[rd.Fields.Count];
                for(int i =0; i< drm.Fields.Count;i++)
                {
                    ret[i] = drm.Fields[i].As(Translator.ReverseOIDLookup(rd.Fields[i].TypeOID));
                }
                results.Rows.Add(ret);

                mess = mr.ReadMessage();
            }
            ReadyForQuery rfq = mr.ReadMessage<ReadyForQuery>();
            Console.WriteLine("Ready for query");
            // Convert to results


            ////byte[] buffer = new byte[2048];

            ////// TODO: Need a 'command reader'
            ////List<byte> ret = new List<byte>();
            ////int rec = s.Receive(buffer);
            ////while(rec > 0)
            ////{
            ////    for(int i =0; i < rec; i++)
            ////    {
            ////        ret.Add(buffer[i]);
            ////    }
            ////    rec = s.Receive(buffer);
            ////}
            ////Console.WriteLine("Recevied: " + ret.Count + " bytes");
            return results;
        }
    }
}
