using QueryProcessor.Parser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Entities
{
    internal class Table
    {
        public string TableName {  get; set; }
        public List<Column> Columns { get; set; }
    }
}
