using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryProcessor.Operations
{
    internal class SelectSystemIndexes
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
                    List<Dictionary<string, string>> indexes = GetIndexesForTable(db, table);
                    if (indexes != null && indexes.Count > 0)
                    {
                        tableData.AddRange(indexes);
                    }
                }
            }

            if (tableData.Count == 0)
            {
                return new OperationResult { Status = OperationStatus.Success, Message = "No se encontraron índices en las bases de datos." };
            }

            // Construir la tabla
            string tableString = BuildTable(tableData);

            // Retornar el resultado con la tabla en el mensaje
            return new OperationResult { Status = OperationStatus.Success, Message = tableString };
        }

        private List<Dictionary<string, string>> GetIndexesForTable(string dbName, string tableName)
        {
            var store = Store.GetInstance();
            var tableIndexes = new List<Dictionary<string, string>>();

            // Abrir el archivo SystemIndexes y leer la información de los índices
            using (FileStream stream = File.Open(store.GetSystemIndexesFile(), FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string db = reader.ReadString();
                    string table = reader.ReadString();
                    string indexName = reader.ReadString();
                    string columnName = reader.ReadString();
                    string indexType = reader.ReadString();

                    // Verificar si el índice pertenece a la tabla y base de datos actual
                    if (db.Equals(dbName, StringComparison.OrdinalIgnoreCase) &&
                        table.Equals(tableName, StringComparison.OrdinalIgnoreCase))
                    {
                        var row = new Dictionary<string, string>
                        {
                            { "Database", db },
                            { "Table", table },
                            { "Index Name", indexName },
                            { "Column", columnName },
                            { "Type", indexType }
                        };
                        tableIndexes.Add(row);
                    }
                }
            }

            return tableIndexes;
        }

        private string BuildTable(List<Dictionary<string, string>> tableData)
        {
            var sb = new StringBuilder();

            // Obtener los nombres de las columnas
            var columns = new List<string> { "Database", "Table", "Index Name", "Column", "Type" };

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

