using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using System.Data;

namespace TimeCacheNetworkServer.Query
{
    public interface IQuerier
    {
        /// <summary>
        /// Execute the query and return the results as a table
        /// </summary>
        /// <param name="query"></param>
        /// <returns></returns>
        DataTable SimpleQuery(string query);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="normalized"></param>
        /// <param name="range"></param>
        /// <returns></returns>
        DataTable CachedQuery(NormalizedQuery normalized, Caching.QueryRange range);
    }

    /// <summary>
    /// Simple database querier
    /// </summary>
    public class DatabaseQuerier : IQuerier
    {
        public DatabaseQuerier(string connString)
        {
            _connectionString = connString;
        }

        private readonly string _connectionString;

        public DataTable SimpleQuery(string query)
        {
            return Utils.Postgresql.TableManager.GetTable(_connectionString, query);
        }

        public DataTable CachedQuery(NormalizedQuery normalized, Caching.QueryRange range)
        {
            string query = normalized.QueryToExecute(range);

            return Utils.Postgresql.TableManager.GetTable(_connectionString, query);
        }
    }

    /// <summary>
    /// For testing, allow returning user defined data.
    /// </summary>
    public class TestQuerier : IQuerier
    {
        public TestQuerier()
        {
        }      

        public void SetData(DataTable table)
        {
            _table = table;
        }

        private DataTable _table = new DataTable();

        public DataTable SimpleQuery(string query)
        {
            return _table;
        }

        public DataTable CachedQuery(NormalizedQuery normalized, Caching.QueryRange range)
        {
            return _table;
        }
    }
}
