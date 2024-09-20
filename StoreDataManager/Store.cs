﻿using Entities;
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
        private const string SystemColumnsFile = $@"{SystemCatalogPath}\SystemColumns.table";
        private string SettedDataBasePath = string.Empty;
        private string SettedDataBaseName = string.Empty;

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



        ///////////////////////////////////////////////// INICIO FUNCIONES BASE DE DATOS /////////////////////////////////////////////////////

        public OperationStatus CreateDataBase(string CreateDataBaseName)
        {
            // Creates a default DB called TESTDB
            if (Directory.Exists($@"{DataPath}\{CreateDataBaseName}")) 
            {
                return OperationStatus.Error;
            }

            Directory.CreateDirectory($@"{DataPath}\{CreateDataBaseName}");

            AddDataBaseToSystemDataBases(CreateDataBaseName);

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
                this.SettedDataBaseName = SetDataBaseName;
                Console.WriteLine($"Path {DataBasePath}");
                return OperationStatus.Success;

            }
            else
            {
                Console.WriteLine("Database doesn't exist created");
                return OperationStatus.Error;
            }
        }

        public void AddDataBaseToSystemDataBases(string DataBaseName)
        {
            using (FileStream stream = File.Open(SystemDatabasesFile, FileMode.OpenOrCreate))
            using(BinaryWriter writer = new (stream))
            {
                writer.Write(DataBaseName);
            }
        }

        public List<string> GetAllDataBasesSystemCatalog()
        {

            List<string> databases = new List<string>();
            

            if (!File.Exists(SystemDatabasesFile))
            {
                return databases; // Retorna una lista vacía si no existe el archivo
            }

            using (FileStream stream = new FileStream(SystemDatabasesFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string databaseName = reader.ReadString();
                    databases.Add(databaseName);
                }
            }

            return databases;
        }


        

        



        ///////////////////////////////////////////////// FINAL FUNCIONES BASE DE DATOS ////////////////////////////////////////////////////////



        ///////////////////////////////////////////////// INICIO FUNCIONES TABLAS //////////////////////////////////////////////////////////////

        public OperationStatus CreateTable(string TableName, List<Column> TableColumns)
        {

            // Creates a default Table called ESTUDIANTES
            if (string.IsNullOrEmpty(SettedDataBasePath))
            {
                Console.WriteLine("No se ha establecido una base de datos");
                return OperationStatus.Error;
            }

            string tablePath = $@"{SettedDataBasePath}\{TableName}";

            if (File.Exists(tablePath))
            {
                Console.WriteLine($"Tabla ya existente en {SettedDataBasePath}");
            }

            using (FileStream stream = File.Open(tablePath, FileMode.OpenOrCreate))
            { 
                // Archivo creado
            }
            /*
            using (BinaryWriter writer = new (stream))
            {

                foreach (Column column in TableColumns)
                {
                                      
                    writer.Write(column.Name);
                    writer.Write(column.DataType.ToString());
                    writer.Write(column.MaxSize.HasValue ? column.MaxSize.Value : 0);
                }
                
            }
            */
                
            AddTableToSystemTables(TableName);
            AddColumsToSystemColumns(TableName, TableColumns);

            Console.WriteLine($"Tabla '{TableName}' creada exitosamente en la base de datos '{SettedDataBaseName}'.");
            return OperationStatus.Success;
        }

        private void AddTableToSystemTables(string TableName)
        {
            using (FileStream stream = File.Open(SystemTablesFile, FileMode.OpenOrCreate))
            using (BinaryWriter writer = new(stream))
            {
                writer.Write(SettedDataBaseName);
                writer.Write(TableName);
            }
        }

        private void AddColumsToSystemColumns(string TableName, List<Column> Columns)
        {
            using (FileStream stream = File.Open(SystemColumnsFile, FileMode.OpenOrCreate))
            using (BinaryWriter writer = new(stream))
            {
                foreach(Column Column in Columns)
                {
                    writer.Write(SettedDataBaseName);
                    writer.Write(TableName);
                    writer.Write(Column.Name);
                    writer.Write(Column.DataType.ToString());
                    writer.Write(Column.MaxSize.HasValue ? Column.MaxSize.Value : 0);
                }
            }
        }

        public List<string> GetTablesInDataBaseSystemCatalog(string databaseName)
        {
            List<string> tables = new List<string>();
            

            if (!File.Exists(SystemTablesFile))
            {
                return tables;
            }

            using (FileStream stream = new FileStream(SystemTablesFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string dbName = reader.ReadString();
                    string tableName = reader.ReadString();

                    if (dbName == databaseName)
                    {
                        tables.Add(tableName);
                    }
                }
            }

            return tables;
        }

        public List<Column> GetColumnsOfTableSystemCatalog(string databaseName, string tableName)
        {

            List<Column> columns = new List<Column>();

            if (!File.Exists(SystemColumnsFile))
            {
                return columns;
            }

            using (FileStream stream = new FileStream(SystemColumnsFile, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(stream))
            {
                while (stream.Position < stream.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();
                    string columnName = reader.ReadString();
                    string dataTypeStr = reader.ReadString();
                    int maxSize = reader.ReadInt32();

                    if (dbName == databaseName && tblName == tableName)
                    {
                        Column column = new Column
                        {
                            Name = columnName,
                            DataType = Enum.Parse<DataType>(dataTypeStr),
                            MaxSize = maxSize > 0 ? maxSize : null
                        };
                        columns.Add(column);
                    }
                }
            }

            return columns;
        }


        ///////////////////////////////////////////////// FINAL FUNCIONES TABLAS ///////////////////////////////////////////////////////
       

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
