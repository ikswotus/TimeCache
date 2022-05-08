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

            for(int i =0; i< table.Columns.Count; i++) 
            {
                DataColumn dc = table.Columns[i];
                
                Type lookup = dc.DataType;
                
                if (!TypeOIDMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to oid");
                if (!ColumnLengthMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to length");
                short cl = ColumnLengthMap[lookup];

                RowDescriptionField rdf = new RowDescriptionField(dc.ColumnName, TypeOIDMap[lookup], cl, i);
                rd.Fields.Add(rdf);
                rd.OriginalTypes.Add(lookup);
            }
            return rd;
        }

        public static RowDescription BuildRowDescription(List<NamedColumns> cols)
        {
            RowDescription specialRes = new RowDescription(cols.Count);
            for(int i =0; i< cols.Count; i++)
            {
                NamedColumns nc = cols[i];
                if (!TypeOIDMap.ContainsKey(nc.ValueType))
                    throw new Exception("Unable to map type: " + nc.ValueType + " to oid");
                if (!ColumnLengthMap.ContainsKey(nc.ValueType))
                    throw new Exception("Unable to map type: " + nc.ValueType + " to length");
                short cl = ColumnLengthMap[nc.ValueType];

                RowDescriptionField rdf = new RowDescriptionField(nc.Name, TypeOIDMap[nc.ValueType], cl, i);
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

            short timeIndex = -1;
            Type timeType = typeof(double);

            for(int i =0; i< dt.Columns.Count; i++)
            {
                DataColumn dc = dt.Columns[i];
                Type lookup = dc.DataType;
                if (!TypeOIDMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to oid");
                if (!ColumnLengthMap.ContainsKey(lookup))
                    throw new Exception("Unable to map type: " + lookup + " to length");
                short cl = ColumnLengthMap[lookup];
                    
                RowDescriptionField rdf = new RowDescriptionField(dc.ColumnName, TypeOIDMap[lookup], cl, i);
                rd.Fields.Add(rdf);

                if (dc.ColumnName.StartsWith("time", StringComparison.OrdinalIgnoreCase))
                {
                    timeIndex = (short)i;
                    timeType = lookup;
                }
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
                if(timeIndex != -1)
                    drm.Time = GetDateTime(timeType, dr, timeIndex);
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

                //drm.Time = time;
            }
            return drm;
        }

        public static DataRowMessage BuildRowMessage(object[] items, DateTime time)
        {
            DataRowMessage drm = new DataRowMessage(items.Length);

            foreach (object item in items)
            {
                // Convert object to bytes
                PGField field = PGField.BuildField(ConvertObject(item));
                drm.Fields.Add(field);

                drm.Time = time;
            }
            return drm;
        }

        public static byte[] ConvertObject(object o)
        {
            Type t = o.GetType();
            if (t == typeof(string))
            {
                return Encoding.ASCII.GetBytes(o as string);
            }
            else if (t == typeof(Int32))
            {
                Int32 i = (Int32)o;
                return Encoding.ASCII.GetBytes(i.ToString("G20"));
            }
            else if (t == typeof(double))
            {
                // Eww. Convert to string, then string to bytes.
                Double d = (Double)o;
                return Encoding.ASCII.GetBytes(d.ToString("G20"));
            }
            else if (t == typeof(DateTime))
            {
                DateTime d = (DateTime)o;
                return Encoding.ASCII.GetBytes((d - _epoch).TotalMilliseconds.ToString("G20"));
            }
            else if (t == typeof(Decimal))
            {
                Decimal d = (Decimal)o;
                return Encoding.ASCII.GetBytes(d.ToString("G20"));
            }
            else if (t == typeof(Int64))
            {
                Int64 i = (Int64)o;
                return Encoding.ASCII.GetBytes(i.ToString("G20"));
            }
            else if (t == typeof(UInt32))
            {
                UInt32 i = (UInt32)o;
                return Encoding.ASCII.GetBytes(i.ToString("G20"));
            }
            else if (t == typeof(char))
            {
                return Encoding.ASCII.GetBytes(o.ToString());
            }
            else if (t == typeof(bool))
            {
                bool b = Boolean.Parse(o.ToString());
                if (b)
                    return new byte[] { 0x01 };
                return new byte[] { 0x00 };

            }
            else if (t == typeof(DBNull))
                return null;

            throw new Exception("Unsupported conversion type: " + t);
        }

        private static DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static Type ReverseOIDLookup(int oid)
        {
            if (!ReverseTypeOIDMap.ContainsKey(oid))
            {
                throw new Exception("Unknown oid: " + oid);
            }
            return ReverseTypeOIDMap[oid];
        }

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
            {typeof(char), 18},
            {typeof(bool), 16},
            {typeof(int), 23 },
            {typeof(long), 20},
            {typeof(UInt32), 23 },
           // {typeof(Int64), 20 },
            // Lie - we'll convert datetime to doubles
            {typeof(DateTime), 701},


            //TESTING
            {typeof(Decimal), 701 }
        };

        /// <summary>
        /// Pairs integer oid to c# column type
        /// </summary>
        public static Dictionary<int,Type> ReverseTypeOIDMap = new Dictionary<int,Type>()
        {
            {25, typeof(String) },
            {701, typeof(Double)},
            {20, typeof(long) },
            {23, typeof(int)}, 
            {1184, typeof(DateTime)},
            {1700, typeof(float) },

            {18, typeof(char)},
            

            //TESTING
           // {typeof(Decimal), 701 }
        };

        /// <summary>
        /// Pairs c# column type to integer oid.
        /// </summary>
        public static Dictionary<Type, short> ColumnLengthMap = new Dictionary<Type, short>()
        {
            {typeof(String), -1 },
            {typeof(Double), 8},
            {typeof(int), 4 },
            {typeof(long), 8 },
            {typeof(char), 1 },
            {typeof(bool), 1 },
            {typeof(UInt32), 4 },
           // {typeof(Int64), 8 },
            // Lie - we'll convert datetime to doubles
            {typeof(DateTime), 8},

            {typeof(Decimal), 8}
        };

    }
}

