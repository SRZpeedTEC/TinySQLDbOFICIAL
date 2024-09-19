using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class SetDataBase
    {
        internal OperationStatus Execute(string SetDataBaseName)
        {
            return Store.GetInstance().SetDataBase(SetDataBaseName);
        }
    }
}
