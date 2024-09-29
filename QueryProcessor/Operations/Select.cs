using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace QueryProcessor.Operations
{
    internal class Select
    {
        public OperationStatus Execute(string sentence, out object? data)
        {
            data = null;
            // Mostrar la sentencia para depuración
            Console.WriteLine($"Select.Execute - Sentencia recibida: {sentence}");

            var store = Store.GetInstance();

            // Parsear la sentencia
            var match = Regex.Match(sentence, @"SELECT\s+(\*|\w+(?:\s*,\s*\w+)*)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+?))?(?:\s+ORDER\s+BY\s+(\w+)(?:\s+(ASC|DESC))?)?;?$", RegexOptions.IgnoreCase);


            if (!match.Success)
            {
                Console.WriteLine("Sintaxis de SELECT incorrecta.");
                return OperationStatus.Error;
            }

            string columnsPart = match.Groups[1].Value;
            string tableName = match.Groups[2].Value;
            string whereClause = match.Groups[3].Success ? match.Groups[3].Value : null;
            string orderByColumn = match.Groups[4].Success ? match.Groups[4].Value : null;
            string orderByDirection = match.Groups[5].Success ? match.Groups[5].Value : "ASC"; // Por defecto ASC
                                                                                               // 

            // Obtener las columnas a seleccionar
            List<string> columnsToSelect;
            if (columnsPart.Trim() == "*")
            {
                // Seleccionar todas las columnas
                columnsToSelect = null; // Null indica todas las columnas
            }
            else
            {
                // Separar los nombres de las columnas
                columnsToSelect = columnsPart.Split(',').Select(c => c.Trim()).ToList();
            }

            return store.SelectFromTable(tableName, columnsToSelect, whereClause, orderByColumn, orderByDirection, out data);
        }

        
    }
}
