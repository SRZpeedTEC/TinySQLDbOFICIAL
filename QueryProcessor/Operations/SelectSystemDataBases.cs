using Entities;
using StoreDataManager;
using System.Text;

namespace QueryProcessor.Operations
{
    internal class SelectSystemDataBases
    {
        public OperationResult Execute()
        {
            Store store = Store.GetInstance();
            List<string> databases = store.GetAllDataBases();

            // Verificar si hay bases de datos
            if (databases == null || databases.Count == 0)
            {
                return new OperationResult { Status = OperationStatus.Success, Message = "No hay bases de datos disponibles." };
            }

            // Construir la tabla
            var sb = new StringBuilder();

            string header = "Database Name";
            int width = header.Length;

            // Encontrar la longitud máxima de los nombres de las bases de datos
            int maxNameLength = databases.Max(db => db.Length);
            if (maxNameLength > width)
            {
                width = maxNameLength;
            }

            // Construir la cabecera de la tabla
            sb.AppendLine("+-" + new string('-', width) + "-+");
            sb.AppendLine("| " + header.PadRight(width) + " |");
            sb.AppendLine("+-" + new string('-', width) + "-+");

            // Agregar cada base de datos a la tabla
            foreach (var db in databases)
            {
                sb.AppendLine("| " + db.PadRight(width) + " |");
            }

            // Agregar el cierre de la tabla
            sb.AppendLine("+-" + new string('-', width) + "-+");

            // Convertir el StringBuilder a una cadena
            string tableString = sb.ToString();

            // Retornar el resultado con la tabla en el mensaje
            return new OperationResult { Status = OperationStatus.Success, Message = tableString };
        }
    }
}
