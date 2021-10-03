using System;
using System.CodeDom;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace PostgresqlCommunicator
{
    /// <summary>
    /// Handles converting from a structured data table to raw postgresql wire format
    /// </summary>
    public class Translator
    {

        public class NamedColumns
        {
            public string Name { get; set; }
            public Type ValueType { get; set; }
        }

        public static  RowDescription BuildRowDescription(DataTable table)
        {
            RowDescription rd = new RowDescription(table.Columns.Count);

            foreach (DataColumn dc in table.Columns)
            {
                Type lookup = dc.DataType;
                
                if (!TypeOIDMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to oid");
                if (!ColumnLengthMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to length");
                short cl = ColumnLengthMap[lookup];

                RowDescriptionField rdf = new RowDescriptionField(dc.ColumnName + "\0", TypeOIDMap[lookup], cl);
                rd.Fields.Add(rdf);
                rd.OriginalTypes.Add(lookup);
            }
            return rd;
        }

        public static RowDescription BuildRowDescription(List<NamedColumns> cols)
        {
            RowDescription specialRes = new RowDescription(cols.Count);
            foreach(NamedColumns nc in cols)
            {
                if (!TypeOIDMap.ContainsKey(nc.ValueType))
                    throw new Exception("Unable to map type: " + nc.ValueType + " to oid");
                if (!ColumnLengthMap.ContainsKey(nc.ValueType))
                    throw new Exception("Unable to map type: " + nc.ValueType + " to length");
                short cl = ColumnLengthMap[nc.ValueType];

                RowDescriptionField rdf = new RowDescriptionField(nc.Name + "\0", TypeOIDMap[nc.ValueType], cl);
                specialRes.Fields.Add(rdf);
            }


                return specialRes;
        }

        /// <summary>
        /// Converts a datatable to pgmessages
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static IEnumerable<PGMessage> BuildResponseFromData(DataTable dt)
        {
            List<PGMessage> mess = new List<PGMessage>();

            // First - Need a row descriptor identifying the columns
            RowDescription rd = new RowDescription(dt.Columns.Count);

            foreach(DataColumn dc in dt.Columns)
            {
                Type lookup = dc.DataType;
                if (!TypeOIDMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to oid");
                if (!ColumnLengthMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to length");
                short cl = ColumnLengthMap[lookup];
                    
                RowDescriptionField rdf = new RowDescriptionField(dc.ColumnName + "\0", TypeOIDMap[lookup], cl);
                rd.Fields.Add(rdf);
            }
            mess.Add(rd);
            // Next - add row data

            foreach(DataRow dr in dt.Rows)
            {
                DataRowMessage drm = new DataRowMessage(rd.Fields.Count);

                foreach (object item in dr.ItemArray)
                {
                    // Convert object to bytes
                    PGField field = PGField.BuildField(ConvertObject(item));
                    drm.Fields.Add(field);
                }

                mess.Add(drm);
            }

            return mess;
        }

        public static DateTime GetDateTime(Type t, DataRow row, int index)
        {
            if (t == typeof(DateTime))
            {
                return (DateTime)row[index];
            }
            else if (t == typeof(double))
            {
                return _epoch.AddSeconds((Double)row[index]);
            }
            else
                throw new Exception("Unsupported time column type: " + t);
        }

        public static DataRowMessage BuildRowMessage(DataRow dr)
        {
            return BuildRowMessage(dr.ItemArray);
        }

        public static DataRowMessage BuildRowMessage(object[] items)
        {
            DataRowMessage drm = new DataRowMessage(items.Length);

            foreach (object item in items)
            {
                // Convert object to bytes
                PGField field = PGField.BuildField(ConvertObject(item));
                drm.Fields.Add(field);
            }
            return drm;
        }

        public static byte[] ConvertObject(object o)
        {
            Type t = o.GetType();
            if(t == typeof(string))
            {
                return Encoding.ASCII.GetBytes(o as string);
            }
            else if(t == typeof(Int32))
            {
                Int32 i = (Int32)o;
                return Encoding.ASCII.GetBytes(i.ToString("G20"));
            }
            else if(t == typeof(double))
            {
                // Eww. Convert to string, then string to bytes.
                Double d = (Double)o;
                return Encoding.ASCII.GetBytes(d.ToString("G20"));
            }
            else if(t == typeof(DateTime))
            {
                DateTime d = (DateTime)o;
                return Encoding.ASCII.GetBytes((d - _epoch).TotalMilliseconds.ToString("G20"));
            }
            else if(t == typeof(Decimal))
            {
                Decimal d = (Decimal)o;
                return Encoding.ASCII.GetBytes(d.ToString("G20"));
            }
            throw new Exception("Unsupported conversion type: " + t);
        }

        private static DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);


        //public static Type GetTypeFromOID(int oid)
        //{
            
        //}

        /// <summary>
        /// Pairs c# column type to integer oid.
        /// </summary>
        public static Dictionary<Type, int> TypeOIDMap = new Dictionary<Type, int>()
        {
            {typeof(String), 25 },
            {typeof(Double), 701},
            {typeof(int), 23 },
            // Lie - we'll convert datetime to doubles
            {typeof(DateTime), 701},


            //TESTING
            {typeof(Decimal), 701 }
        };

        /// <summary>
        /// Pairs c# column type to integer oid.
        /// </summary>
        public static Dictionary<Type, short> ColumnLengthMap = new Dictionary<Type, short>()
        {
            {typeof(String), -1 },
            {typeof(Double), 8},
            {typeof(int), 4 },
            // Lie - we'll convert datetime to doubles
            {typeof(DateTime), 8},

            {typeof(Decimal), 8}
        };

    }
}
