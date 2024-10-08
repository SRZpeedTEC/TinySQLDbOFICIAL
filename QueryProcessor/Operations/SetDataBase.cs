using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class SetDataBase
    {
        internal OperationResult Execute(string SetDataBaseName)
        {
            return Store.GetInstance().SetDataBase(SetDataBaseName);
        }
    }
}
