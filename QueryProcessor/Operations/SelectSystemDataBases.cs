using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Entities;
using StoreDataManager;

namespace QueryProcessor.Operations
{
    internal class SelectSystemDataBases
    {
        public OperationStatus Execute()
        {
            Store store = Store.GetInstance();
            List<string> databases = store.GetAllDataBasesSystemCatalog();

            // Mostrar los resultados
            Console.WriteLine("Databases:");
            foreach (var db in databases)
            {
                Console.WriteLine($"- {db}");
            }

            return OperationStatus.Success;
        }
    }
}
