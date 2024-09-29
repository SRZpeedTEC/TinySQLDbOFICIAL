using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Entities;
using StoreDataManager;


namespace QueryProcessor.Operations
{
    public class CreateIndex
    {
        public OperationStatus Execute(string sentence)
        {
            var store = Store.GetInstance();

            var match = Regex.Match(sentence, @"CREATE\s+INDEX\s+(\w+)\s+ON\s+(\w+)\s*\(\s*(\w+)\s*\)\s+OF\s+TYPE\s+(BTREE|BST);?$", RegexOptions.IgnoreCase);

            if(!match.Success)
            {
                Console.WriteLine("Sintaxis de CREATE INDEX incorrecta.");
                return OperationStatus.Error;
            }

            string indexName = match.Groups[1].Value;
            string tableName = match.Groups[2].Value;
            string columnName = match.Groups[3].Value;
            string indexType = match.Groups[4].Value;

            return store.CreateIndex(indexName, tableName, columnName, indexType);
        }
    }
}
