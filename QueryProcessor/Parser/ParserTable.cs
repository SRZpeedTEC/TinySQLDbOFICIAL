using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Entities;

namespace QueryProcessor.Parser
{
    internal class ParserTable
    {
        internal string GetTableName(string sentence) // Obtiene el nombre de la tabla con la referencia del parentesis
        {
            int TableNameEnd = sentence.IndexOf('(');
            string TableName = sentence.Substring(0, TableNameEnd).Trim();
            return TableName;
        }

        internal List<Column> GetColumns(string ColumnsInfo)
        {
            List<Column> Columns = new List<Column>(); // Creamos una lista de columnas

            ColumnsInfo = ColumnsInfo.Trim();
            ColumnsInfo = ColumnsInfo.Substring(1, ColumnsInfo.Length - 2);


            string[] RawColumns = ColumnsInfo.Split(',');  // ("ID", "INTEGER")
            List<string[]> RawColumnsMatrix = new List<string[]>();

            foreach (string Column in RawColumns)
            {
                string trimmedColumn = Column.Trim();
                Match match = Regex.Match(trimmedColumn, @"^(\S+)\s+(.+)$"); // Formato para la division de nombre y tipo

                if (match.Success)
                {
                    string columnName = match.Groups[1].Value.Trim();
                    string columnType = match.Groups[2].Value.Trim();

                    RawColumnsMatrix.Add(new string[] { columnName, columnType }); // ("ID", "INTEGER"), ("Nombre", "Santiago")
                }
                else
                {
                    Console.WriteLine("FORMATO INVALIDO");
                }
            }

            foreach(string[] Column in RawColumnsMatrix) // ("ID", "INTEGER")
            {
                Column NewColumn = new Column();
                NewColumn.Name = Column[0];
               
                string RawColumnDataType = Column[1];

                if (RawColumnDataType.StartsWith("INTEGER"))
                {
                    NewColumn.DataType = DataType.INTEGER;
                }
                else if (RawColumnDataType.StartsWith("VARCHAR"))
                {
                    string VarcharSize = RawColumnDataType.Substring("VARCHAR".Length).Trim();
                    string SizeStringNumbre = VarcharSize.Trim('(',')');
                    int MaxSizeNumber = int.Parse(SizeStringNumbre);
         
                    NewColumn.DataType = DataType.VARCHAR;
                    NewColumn.MaxSize = MaxSizeNumber;
                }

                else if (RawColumnDataType.StartsWith("DOUBLE"))
                {
                    NewColumn.DataType = DataType.DOUBLE;
                }

                else if (RawColumnDataType.StartsWith("DATETIME"))
                {
                    NewColumn.DataType = DataType.DATETIME;
                }

                Columns.Add(NewColumn);
             
            }


            return Columns;

        }


    }
}
