using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Entities;

namespace QueryProcessor.Operations
{
    internal class SelectSystemTables
    {
        public OperationResult Execute()
        {
            var store = Store.GetInstance();
            List<string> databases = store.GetAllDataBases();

            // Verificar si hay bases de datos
            if (databases == null || databases.Count == 0)
            {
                return new OperationResult { Status = OperationStatus.Success, Message = "No hay bases de datos disponibles." };
            }

            // Lista para almacenar todas las filas de la tabla
            var tableData = new List<Dictionary<string, string>>();

            foreach (var db in databases)
            {
                List<string> tables = store.GetTablesInDataBase(db);
                foreach (var table in tables)
                {
                    var row = new Dictionary<string, string>
                    {
                        { "Database", db },
                        { "Table", table }
                    };
                    tableData.Add(row);
                }
            }

            if (tableData.Count == 0)
            {
                return new OperationResult { Status = OperationStatus.Success, Message = "No se encontraron tablas en las bases de datos." };
            }

            // Construir la tabla
            string tableString = BuildTable(tableData);

            // Retornar el resultado con la tabla en el mensaje
            return new OperationResult { Status = OperationStatus.Success, Message = tableString };
        }

        private string BuildTable(List<Dictionary<string, string>> tableData)
        {
            var sb = new StringBuilder();

            // Obtener los nombres de las columnas
            var columns = new List<string> { "Database", "Table" };

            // Calcular el ancho máximo de cada columna
            var columnWidths = new Dictionary<string, int>();
            foreach (var col in columns)
            {
                int maxWidth = col.Length;
                int maxDataWidth = tableData.Any() ? tableData.Max(row => row[col].Length) : 0;
                columnWidths[col] = Math.Max(maxWidth, maxDataWidth);
            }

            // Construir la línea superior de la tabla
            sb.AppendLine("+-" + string.Join("-+-", columns.Select(col => new string('-', columnWidths[col]))) + "-+");

            // Construir la cabecera de la tabla
            sb.AppendLine("| " + string.Join(" | ", columns.Select(col => col.PadRight(columnWidths[col]))) + " |");

            // Construir la línea separadora
            sb.AppendLine("+-" + string.Join("-+-", columns.Select(col => new string('-', columnWidths[col]))) + "-+");

            // Agregar las filas de datos
            foreach (var row in tableData)
            {
                sb.AppendLine("| " + string.Join(" | ", columns.Select(col => row[col].PadRight(columnWidths[col]))) + " |");
            }

            // Construir la línea inferior de la tabla
            sb.AppendLine("+-" + string.Join("-+-", columns.Select(col => new string('-', columnWidths[col]))) + "-+");

            return sb.ToString();
        }
    }
}
