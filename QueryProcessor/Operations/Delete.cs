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
    internal class Delete
    {
        public OperationResult Execute(string sentence)
        {
            var store = Store.GetInstance();
            var match = Regex.Match(sentence, @"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?$", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                Console.WriteLine("Sintaxis de Delete incorrecta.");
                return new OperationResult { Status = OperationStatus.Error, Message = "Sintaxis de Delete incorrecta." };
            }

            string tableName = match.Groups[1].Value;
            string whereClause = match.Groups[2].Success ? match.Groups[2].Value : null;

            return store.DeleteFromTable(tableName, whereClause);

        }
    }
}
