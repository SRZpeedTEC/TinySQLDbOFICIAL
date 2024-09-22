using QueryProcessor.Parser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    public class Table
    {
        public string TableName {  get; set; }
        public List<Column> Columns { get; set; }


        public Table(string tableName, List<Column> columns)
        {
            TableName = tableName;
            Columns = columns;
        }
    }
}
