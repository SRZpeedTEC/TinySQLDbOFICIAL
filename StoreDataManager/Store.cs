using Entities;
using QueryProcessor;
using QueryProcessor.Parser;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;

namespace StoreDataManager
{
    public sealed class Store
    {
        private static Store? instance = null;
        private static readonly object _lock = new object();
               
        public static Store GetInstance()
        {
            lock(_lock)
            {
                if (instance == null) 
                {
                    instance = new Store();
                }
                return instance;
            }
        }

        private const string DatabaseBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DatabaseBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";
        private string SettedDataBasePath = string.Empty;

        public Store()
        {
            this.InitializeSystemCatalog();
            
        }

        private void InitializeSystemCatalog()
        {
            // Always make sure that the system catalog and above folder
            // exist when initializing
            Directory.CreateDirectory(SystemCatalogPath);
        }


        public OperationStatus CreateDataBase(string CreateDataBaseName)
        {
            // Creates a default DB called TESTDB
            Directory.CreateDirectory($@"{DataPath}\{CreateDataBaseName}");

            Console.WriteLine("Database created successfully");            

            return OperationStatus.Success;
        }


        public OperationStatus SetDataBase(string SetDataBaseName)
        {
            string DataBasePath = $@"{DataPath}\{SetDataBaseName}";

            if (Directory.Exists(DataBasePath))
            {
                Console.WriteLine($"Setted up in {SetDataBaseName} succesfully");
                this.SettedDataBasePath = DataBasePath;
                Console.WriteLine($"Path {DataBasePath}");
                return OperationStatus.Success;

            }
            else
            {
                Console.WriteLine("Database is already created");
                return OperationStatus.Error;
            }
        }


        public OperationStatus CreateTable(string TableName, List<Column> TableColumns)
        {           

            // Creates a default Table called ESTUDIANTES
            var tablePath = $@"{SettedDataBasePath}\{TableName}";

            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))               
            using (BinaryWriter writer = new (stream))
            {

                foreach (Column column in TableColumns)
                {
                                      
                    writer.Write(column.Name);
                    writer.Write(column.DataType.ToString());
                    writer.Write(column.MaxSize.HasValue ? column.MaxSize.Value : 0);
                }
                
            }
                
                return OperationStatus.Success;
        }

        public OperationStatus Select()
        {
            // Creates a default Table called ESTUDIANTES
            var tablePath = $@"{DataPath}\TESTDB\ESTUDIANTES.Table";
            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            using (BinaryReader reader = new (stream))
            {
                // Print the values as a I know exactly the types, but this needs to be done right
                Console.WriteLine(reader.ReadInt32());
                Console.WriteLine(reader.ReadString());
                Console.WriteLine(reader.ReadString());
                return OperationStatus.Success;
            }
        }
    }
}
