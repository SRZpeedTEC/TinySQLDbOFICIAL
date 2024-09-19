using Entities;
using QueryProcessor.Parser;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class CreateTable
    {
        internal OperationStatus Execute(string TableName, List<Column> TableColumns)
        {
            return Store.GetInstance().CreateTable(TableName, TableColumns);
        }
    }
}
