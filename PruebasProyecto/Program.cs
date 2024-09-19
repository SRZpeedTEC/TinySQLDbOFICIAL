
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using static System.Net.Mime.MediaTypeNames;
using static System.Runtime.InteropServices.JavaScript.JSType;
using Entities;

namespace TinySQLDb
{
    using QueryProcessor.Parser;
    using System;
    using System.Text;
    using System.Text.RegularExpressions;

    class Program
    {
        public static void Main()
        {

            string ColumnsInfo = " (ID INTEGER,Nombre VARCHAR(30),PrimerApellido VARCHAR(30),SegundoApellido VARCHAR(30),FechaNacimiento DATETIME)";

            static void GetColumns(string ColumnsInfo)
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

                foreach (string[] Column in RawColumnsMatrix) // ("ID", "INTEGER")
                {
                    Column NewColumn = new Column();
                    NewColumn.Name = Column[0];
                    NewColumn.DataType = DataType.DATETIME;

                    NewColumn.Name = Column[0];

                    string RawColumnDataType = Column[1];

                    if (RawColumnDataType.StartsWith("INTEGER"))
                    {
                        NewColumn.DataType = DataType.INTEGER;
                    }
                    if (RawColumnDataType.StartsWith("VARCHAR"))
                    {
                        string VarcharSize = RawColumnDataType.Substring("VARCHAR".Length).Trim();
                        string SizeStringNumbre = VarcharSize.Trim('(', ')');
                        int MaxSizeNumber = int.Parse(SizeStringNumbre);

                        NewColumn.DataType = DataType.VARCHAR;
                        NewColumn.MaxSize = MaxSizeNumber;
                    }

                    if (RawColumnDataType.StartsWith("DOUBLE"))
                    {
                        NewColumn.DataType = DataType.DOUBLE;
                    }

                    if (RawColumnDataType.StartsWith("DATETIME"))
                    {
                        NewColumn.DataType = DataType.DATETIME;
                    }

                    Columns.Add(NewColumn);

                }


                foreach (Column column in Columns)
                {

                    Console.WriteLine(column.Name);
                    Console.WriteLine(column.DataType);
                    if (column.MaxSize != null)
                    {
                        Console.WriteLine(column.MaxSize);
                    }


                }


            }

            GetColumns(ColumnsInfo);
        }
    }

}
