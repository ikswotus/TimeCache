using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SetupTestDatabase
{
    /**
     * Helper console application that will setup a test database
     * for exploring the caching examples.
     * 
     * 1) Creates a database named 'examples'
     * 
     * 2) Creates tables:
     *  - generated_data
     *  - test_machines
     *  - test_instances
     *  - test_counters
     */
    class Program
    {
        public static void CreateSchema(string connString, string schemaName)
        {
            try
            {
                using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connString))
                {
                    conn.Open();

                    string c = string.Format("CREATE SCHEMA {0};", schemaName);
                    Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(c, conn);

                    comm.ExecuteNonQuery();


                }
                Console.WriteLine("Successfully created schema: " + schemaName);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Failed to create schemas " + schemaName + ". Exception: " + exc);
            }
        }

        public static void CreateDatabase(string connString, string dbName)
        {
            try
            {
                using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connString))
                {
                    conn.Open();

                    string c = string.Format(_createDBFormat, dbName);
                    Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(c, conn);

                    comm.ExecuteNonQuery();

                   
                }
                Console.WriteLine("Successfully created db: " + dbName);
            }
            catch(Exception exc)
            {
                Console.WriteLine("Failed to createdb " + dbName + ". Exception: " + exc);
            }
        }
        private static readonly string _createDBFormat = @"
CREATE DATABASE {0}
    WITH
    OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'English_United States.1252'
    LC_CTYPE = 'English_United States.1252'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;";

        public static void CreateTable(string connString, string tableName, string tableDefinition)
        {
            try
            {
                using (Npgsql.NpgsqlConnection conn = new Npgsql.NpgsqlConnection(connString))
                {
                    conn.Open();

                    Npgsql.NpgsqlCommand comm = new Npgsql.NpgsqlCommand(tableDefinition, conn);

                    comm.ExecuteNonQuery();


                }
                Console.WriteLine("Successfully created table: " + tableName);
            }
            catch (Exception exc)
            {
                Console.WriteLine("Failed to create table " + tableName + ". Exception: " + exc);
            }
        }

        public static readonly string _tableDefGeneratedData = @"
CREATE TABLE IF NOT EXISTS demo.generated_data
(
    metric_name text  NOT NULL,
    sample_time timestamp with time zone NOT NULL,
    current_value numeric NOT NULL
)

TABLESPACE pg_default;";


        static void Main(string[] args)
        {
            try
            {
                if(args.Length < 1)
                {
                    throw new Exception("Invalid Args: connection_string");
                }
                string connString = args[0];

                CreateDatabase(connString, "examples");

                connString += "Database=examples;";

                CreateSchema(connString, "demo");

                // Tables
                CreateTable(connString, "demo.generated_data", _tableDefGeneratedData);
            }
            catch(Exception exc)
            {
                Console.WriteLine("Failed: " + exc);
            }
            Console.WriteLine("Done");
            Console.ReadKey();
        }
    }
}
