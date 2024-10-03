using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;
using StoreDataManager;
using System.Text.RegularExpressions;

namespace QueryProcessor.Operations
{
    internal class Update
    {
        public OperationStatus Execute(string sentence)
        {
            var store = Store.GetInstance();
            var match = Regex.Match(sentence, @"UPDATE\s+(\w+)\s+SET\s+(\w+)\s*=\s*(.+?)\s+WHERE\s+(.+?);?$", RegexOptions.IgnoreCase);

            if (!match.Success) {
                Console.WriteLine("Sintaxis de UPDATE incorrecta.");
                return OperationStatus.Error;
            }

            string tableName = match.Groups[1].Value;
            string columnName = match.Groups[2].Value;
            string newValue = match.Groups[3].Value;
            string whereClause = match.Groups[4].Value;

        }
    }
}
