using QueryProcessor.Parser;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Operations
{
    public class InsertInto
    {
        internal OperationResult Execute(string TableName, List<String> Valores)
        {
            return Store.GetInstance().InsertInto(TableName, Valores);
        }
    }
     
    
}

