using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Entities;
using StoreDataManager;
using QueryProcessor;
using QueryProcessor.Operations;

namespace QueryProcessor.Parser
{
    internal class ParserInsert

        
    {
        public OperationStatus Parser(string sentence)
        {
            
            // Parsear la sentencia
            var match = Regex.Match(sentence, @"INSERT INTO\s+(\w+)\s*\((.+)\);?", RegexOptions.IgnoreCase);

            if (!match.Success)
            {
                Console.WriteLine("Sintaxis de INSERT INTO incorrecta.");
                return OperationStatus.Error;
            }

            string tableName = match.Groups[1].Value;
            string valuesPart = match.Groups[2].Value;

            // Separar los valores
            var values = ParseValues(valuesPart);

            return new InsertInto().Execute(tableName, values);
        }
     

        private List<string> ParseValues(string valuesPart)
        {
            // Separar los valores por comas, teniendo en cuenta comillas y espacios
            var values = new List<string>();
            var current = new StringBuilder();
            bool inQuotes = false;
            char quoteChar = '\0';

            for (int i = 0; i < valuesPart.Length; i++)
            {
                char c = valuesPart[i];

                if ((c == '\'' || c == '\"') && !inQuotes)
                {
                    inQuotes = true;
                    quoteChar = c;
                }
                else if (c == quoteChar && inQuotes)
                {
                    inQuotes = false;
                }
                else if (c == ',' && !inQuotes)
                {
                    values.Add(current.ToString().Trim());
                    current.Clear();
                    continue;
                }

                current.Append(c);
            }

            if (current.Length > 0)
            {
                values.Add(current.ToString().Trim());
            }

            return values;
        }
    }
}

    

