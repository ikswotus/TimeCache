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
    }
}
