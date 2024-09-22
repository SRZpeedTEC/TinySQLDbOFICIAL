
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
    using System.Security.Cryptography.X509Certificates;
    using System.Text;
    using System.Text.RegularExpressions;

    class Program
    {
        public static void Main()
        {



            string Info = "INSERT INTO Estudiantes (1, 'Juan', 'Perez', '1990-01-01 01:02:00')";

            static void Parser(string sentence)
            {

                // Parsear la sentencia
                var match = Regex.Match(sentence, @"INSERT INTO\s+(\w+)\s*\((.+)\);?", RegexOptions.IgnoreCase);

                if (!match.Success)
                {
                    Console.WriteLine("Sintaxis de INSERT INTO incorrecta.");

                }

                string tableName = match.Groups[1].Value;
                string valuesPart = match.Groups[2].Value;

                // Separar los valores
                var values = ParseValues(valuesPart);

                Console.WriteLine(tableName);
                foreach (var value in values)
                {
                    Console.WriteLine(value);
                }

            }
          

            static List<string> ParseValues(string valuesPart)
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

            Parser(Info);

        }
        
    }




}


