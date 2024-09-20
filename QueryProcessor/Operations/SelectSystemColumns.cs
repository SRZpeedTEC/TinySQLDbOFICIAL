using QueryProcessor.Parser;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    internal class SelectSystemColumns
    {
        public OperationStatus Execute()
        {
            var store = Store.GetInstance();
            List<string> databases = store.GetAllDataBasesSystemCatalog();

            Console.WriteLine("Columns:");
            foreach (var db in databases)
            {
                List<string> tables = store.GetTablesInDataBaseSystemCatalog(db);
                foreach (var table in tables)
                {
                    List<Column> columns = store.GetColumnsOfTableSystemCatalog(db, table);
                    foreach (var column in columns)
                    {
                        Console.WriteLine($"Database: {db}, Table: {table}, Column: {column.Name}, Type: {column.DataType}, Size: {column.MaxSize}");
                    }
                }
            }

            return OperationStatus.Success;
        }
    }
}
