using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    internal class SelectSystemTables
    {
        public OperationStatus Execute()
        {
            var store = Store.GetInstance();
            List<string> databases = store.GetAllDataBases();

            Console.WriteLine("Tables:");
            foreach (var db in databases)
            {
                List<string> tables = store.GetTablesInDataBase(db);
                foreach (var table in tables)
                {
                    Console.WriteLine($"Database: {db}, Table: {table}");
                }
            }

            return OperationStatus.Success;
        }
    }
}
