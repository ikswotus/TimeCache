using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace Utils.Postgresql
{
    /// <summary>
    /// Helper class for simple bulk inserts of DataTables.
    /// 
    /// Types are determined via mapping of .net type to postgres type (May not always be desired...)
    /// </summary>
    public class ManagedBulkWriter : IPostGresCopy
    {
        /// <summary>
        /// Constructor
        /// </summary>
        public ManagedBulkWriter() { }

        /// <summary>
        /// Write data
        /// </summary>
        /// <param name="importer"></param>
        /// <param name="table"></param>
        public void WriteData(Npgsql.NpgsqlBinaryImporter importer, DataTable table)
        {
            lock (_mappedTables)
            {
                if (!_mappedTables.ContainsKey(table.TableName))
                {
                    DataRow row = table.Rows[0];
                    NpgsqlTypes.NpgsqlDbType[] arr = new NpgsqlTypes.NpgsqlDbType[row.ItemArray.Length];
                    for (int i = 0; i < row.ItemArray.Length; i++)
                    {
                        arr[i] = GetType(row.ItemArray[i].GetType());
                    }
                    _mappedTables[table.TableName] = arr;
                }
            }
            
            NpgsqlTypes.NpgsqlDbType[] mapped = null;
            lock (_mappedTables[table.TableName])
                mapped = _mappedTables[table.TableName];

            foreach (DataRow row in table.Rows)
            {
                importer.StartRow();
                for (int i = 0; i < row.ItemArray.Length; i++)
                {
                    if (mapped[i] == NpgsqlTypes.NpgsqlDbType.Text)
                        importer.Write(row.ItemArray[i].ToString().ToLower(), mapped[i]);
                    else
                        importer.Write(row.ItemArray[i], mapped[i]);
                }
            }

        }

        /// <summary>
        /// Pairs a table with the type schema
        /// </summary>
        private static Dictionary<string, NpgsqlTypes.NpgsqlDbType[]> _mappedTables = new Dictionary<string, NpgsqlTypes.NpgsqlDbType[]>();

        /// <summary>
        /// Map system types to db types.
        /// 
        /// TODO: Revisit some of these mappings, or allow custom overrides
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static NpgsqlTypes.NpgsqlDbType GetType(Type t)
        {
            if (t == typeof(System.Int32))
                return NpgsqlTypes.NpgsqlDbType.Integer;
            else if (t == typeof(string))
                return NpgsqlTypes.NpgsqlDbType.Text;
            else if (t == typeof(DateTime))
                return NpgsqlTypes.NpgsqlDbType.TimestampTz;
            else if (t == typeof(Guid))
                return NpgsqlTypes.NpgsqlDbType.Uuid;
            else if (t == typeof(Int64) || t == typeof(long))
                return NpgsqlTypes.NpgsqlDbType.Bigint;
            else if (t == typeof(decimal) || t == typeof(double))
                return NpgsqlTypes.NpgsqlDbType.Numeric;
            else if (t == typeof(bool))
                return NpgsqlTypes.NpgsqlDbType.Bit;
            else if (t == typeof(Single))
                return NpgsqlTypes.NpgsqlDbType.Real;
            else if (t == typeof(Int16) || t == typeof(short))
                return NpgsqlTypes.NpgsqlDbType.Smallint;

            throw new Exception("Unmapped npgsqldbtype from : " + t.ToString());
        }
    }

    /// <summary>
    /// Interface for bulk inserts using Npgsql
    /// </summary>
    public interface IPostGresCopy
    {
        void WriteData(Npgsql.NpgsqlBinaryImporter openWriter, DataTable table);
    }

    /// <summary>
    /// For bulk inserts.
    /// </summary>
    public class TableManager
    {
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="writer"></param>
        /// <param name="connString"></param>
        public TableManager(ManagedBulkWriter writer, string connString)
        {
            _writer = writer;
            _connectionString = connString;
        }

        /// <summary>
        /// Managed writer for bulk inserts. Handles translating types
        /// </summary>
        private readonly ManagedBulkWriter _writer = null;

        /// <summary>
        /// Connection string for db
        /// </summary>
        private readonly string _connectionString = null;

        /// <summary>
        /// Bulk inserts data from the supplied @table to destination db @tableName
        /// </summary>
        /// <param name="table">A populated data table</param>
        /// <param name="tableName">postgresql destination table: schema.table_name</param>
        public void BulkInsert(DataTable table, string tableName)
        {
            if (table == null || table.Rows.Count == 0)
                return;

            if (String.IsNullOrEmpty(tableName))
                throw new ArgumentException("invalid destination table name provided");

            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(_connectionString))
            {
                conn.Open();

                using (Npgsql.NpgsqlBinaryImporter importer = conn.BeginBinaryImport(String.Format("COPY {0} from STDIN (FORMAT BINARY)", tableName)))
                {
                    _writer.WriteData(importer, table);

                    importer.Complete();
                }
            }
        }

        /**
         * Data Collection
         * 
         */
        public static DataTable GetTable(string connString, string query)
        {
            DataTable table = new DataTable();
            using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connString))
            {
                conn.Open();

                using (Npgsql.NpgsqlDataAdapter adapt = new Npgsql.NpgsqlDataAdapter(query, conn))
                {
                    adapt.Fill(table);
                }
            }
            return table;
        }
    }
}
