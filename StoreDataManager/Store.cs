using Entities;
using QueryProcessor;
using QueryProcessor.Parser;
using System.Text.Json;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.RegularExpressions;
using ApiInterface.Indexes;
using ApiInterface;


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

        private const string DataBasePath = @"C:\TinySql\";
        private const string DataPath = $@"{DataBasePath}\Data";
        private const string SystemCatalogPath = $@"{DataPath}\SystemCatalog";
        private const string SystemDatabasesFile = $@"{SystemCatalogPath}\SystemDatabases.table";
        private const string SystemTablesFile = $@"{SystemCatalogPath}\SystemTables.table";
        private const string SystemColumnsFile = $@"{SystemCatalogPath}\SystemColumns.table";
        private const string SystemIndexesFile = $@"{SystemCatalogPath}\SystemIndexes.table";



        public Dictionary<string, object> IndexTrees = new Dictionary<string, object>();
        public Dictionary<string, string> AssociatedIndexesToColumns = new Dictionary<string, string>();
        public List<string> DataBasesWithIndexes = new List<string>();
        public List<string> TablesWithIndexes = new List<string>();
        public List<string> ColumnsWithIndexes = new List<string>();

        



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

        public OperationResult CreateDataBase(string CreateDataBaseName)
        {
            // Creates a default DB called TESTDB
            if (Directory.Exists($@"{DataPath}\{CreateDataBaseName}")) 
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La base de datos '{CreateDataBaseName}' ya existe."
                };

            }

            Directory.CreateDirectory($@"{DataPath}\{CreateDataBaseName}");

            AddDataBaseToSystemDataBases(CreateDataBaseName);           

            return new OperationResult

            {
                Status = OperationStatus.Success,
                Message = $"La base de datos '{CreateDataBaseName}' ha sido creada."
            };
        }

        public OperationResult SetDataBase(string SetDataBaseName)
        {
            string DataBasePath = $@"{DataPath}\{SetDataBaseName}";

            if (Directory.Exists(DataBasePath))
            {
               
                this.SettedDataBasePath = DataBasePath;
                this.SettedDataBaseName = SetDataBaseName;
                Console.WriteLine($"Path {DataBasePath}");
                return new OperationResult                 {
                    Status = OperationStatus.Success,
                    Message = $"Base de datos '{SetDataBaseName}' ha sido establecida."
                };

            }
            else
            {               
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La base de datos '{SetDataBaseName}' no existe."
                };
            }
        }

        public void AddDataBaseToSystemDataBases(string DataBaseName)
        {
            using (FileStream stream = File.Open(SystemDatabasesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                // Posiciona el puntero al final del archivo
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(DataBaseName);
                }
            }
        }

        public List<string> GetAllDataBases()
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

        public OperationResult CreateTable(string TableName, List<Column> TableColumns)
        {
            // Validación de que haya una base de datos seteada
            if (string.IsNullOrEmpty(SettedDataBasePath))
            {               
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            string tablePath = $@"{SettedDataBasePath}\{TableName}.table";

            if (File.Exists(tablePath))
            {              
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{TableName}' ya existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            using (FileStream stream = File.Open(tablePath, FileMode.Create))           
            {
                // ARCHIVO CREADO
            }

            // Agregar la tabla a los metadatos del sistema
            AddTableToSystemTables(TableName);
            AddColumsToSystemColumns(TableName, TableColumns);

            Console.WriteLine($"Tabla '{TableName}' creada exitosamente en la base de datos '{SettedDataBaseName}'.");
            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = $"Tabla '{TableName}' creada exitosamente en la base de datos '{SettedDataBaseName}'."
            };
        }


        private void AddTableToSystemTables(string TableName)
        {
            using (FileStream stream = File.Open(SystemTablesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new BinaryWriter(stream))
                {
                    writer.Write(SettedDataBaseName);
                    writer.Write(TableName);
                }
            }
        }


        public List<string> GetTablesInDataBase(string databaseName)
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


        public OperationResult DropTable(string TableToDrop)
        {
            // Validacion de que haya una base de datos setteada
            if (string.IsNullOrEmpty(SettedDataBasePath))
            {               
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            string tablePath = $@"{SettedDataBasePath}\{TableToDrop}";

            if (File.Exists(tablePath))
            {
                
                string[] TableContent = File.ReadAllLines(tablePath);

                // Validacion de que la tabla este vacía la tabla está vacía
                if (TableContent.Length == 0)
                {
                    Console.WriteLine("La tabla está vacía, procediendo a eliminarla.");
                    File.Delete(tablePath);
                }
                else
                {                  
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = "La tabla no está vacía, no se puede eliminar."
                    };
                }
            }
            
            else
            {
                Console.WriteLine($"La tabla '{TableToDrop}' no existe.");
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{TableToDrop}' no existe."
                };
            }

            RemoveTableFromSystemTables(TableToDrop);

            RemoveColumnsFromSystemColumns(TableToDrop);

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = $"La tabla '{TableToDrop}' ha sido eliminada."
            };
        }




        private void RemoveTableFromSystemTables(string TableToDrop)
        {
            string tempPath = $@"{SystemCatalogPath}\SystemTables_Temp.table";
            using (FileStream fs = new FileStream(SystemTablesFile, FileMode.OpenOrCreate, FileAccess.Read))
            using (FileStream fsTemp = new FileStream(tempPath, FileMode.OpenOrCreate, FileAccess.Write))
            using (BinaryReader reader = new BinaryReader(fs))
            using (BinaryWriter writer = new BinaryWriter(fsTemp))
            {
                while (fs.Position < fs.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();

                    if (!(dbName == SettedDataBaseName && tblName == TableToDrop))
                    {
                        writer.Write(dbName);
                        writer.Write(tblName);
                    }
                }
            }
            File.Delete(SystemTablesFile);
            File.Move(tempPath, SystemTablesFile);
        }



        ////////////////////////////////////////// INICIO COLUMNAS /////////////////////////////////////////////////////////

        private void AddColumsToSystemColumns(string TableName, List<Column> Columns)
        {
            using (FileStream stream = File.Open(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new(stream))
                {
                    foreach (Column Column in Columns)
                    {
                        writer.Write(SettedDataBaseName);
                        writer.Write(TableName);
                        writer.Write(Column.Name);
                        writer.Write(Column.DataType.ToString());
                        writer.Write(Column.MaxSize.HasValue ? Column.MaxSize.Value : 0);
                    }
                }
            }
        }

        public List<Column> GetColumnsOfTable(string databaseName, string tableName)
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

        private void RemoveColumnsFromSystemColumns(string tableName)
        {
            string tempPath = $@"{SystemCatalogPath}\SystemColumns_Temp.table";
            using (FileStream fs = new FileStream(SystemColumnsFile, FileMode.OpenOrCreate, FileAccess.Read))
            using (FileStream fsTemp = new FileStream(tempPath, FileMode.OpenOrCreate, FileAccess.Write))
            using (BinaryReader reader = new BinaryReader(fs))
            using (BinaryWriter writer = new BinaryWriter(fsTemp))
            {
                while (fs.Position < fs.Length)
                {
                    string dbName = reader.ReadString();
                    string tblName = reader.ReadString();
                    string columnName = reader.ReadString();
                    string dataTypeStr = reader.ReadString();
                    int maxSize = reader.ReadInt32();

                    if (!(dbName == SettedDataBaseName && tblName == tableName))
                    {
                        // Escribir las entradas que no corresponden a la tabla eliminada
                        writer.Write(dbName);
                        writer.Write(tblName);
                        writer.Write(columnName);
                        writer.Write(dataTypeStr);
                        writer.Write(maxSize);
                    }
                }
            }

            // Reemplazar el archivo original por el temporal
            File.Delete(SystemColumnsFile);
            File.Move(tempPath, SystemColumnsFile);
        }


        public OperationResult InsertInto(string tableName, List<string> values)
        {
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }
            

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            // Obtener las columnas de la tabla
            List<Column> columns = GetColumnsOfTable(SettedDataBaseName, tableName);

            if (values.Count != columns.Count)
            {               
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "El número de valores proporcionados no coincide con el número de columnas."
                };
            }

            // Validar y convertir los valores
            var convertedValues = new List<object>();
            for (int i = 0; i < values.Count; i++)
            {
                string valueStr = values[i];
                Column column = columns[i];
                object convertedValue;

                try
                {
                    convertedValue = ConvertValue(valueStr, column.DataType, column.MaxSize);
                    convertedValues.Add(convertedValue);
                }
                catch (Exception ex)
                {                   
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = $"Error al convertir el valor '{valueStr}' para la columna '{column.Name}': {ex.Message}"
                    };
                }

                

                // Verificar si la columna tiene un índice
                string existingIndex = GetIndexNameIfExist(SettedDataBaseName, tableName, column.Name);
                if (existingIndex != null)
                {
                    // Obtener los datos de la columna con el índice
                    List<object> columnData = GetColumnData(SettedDataBaseName, tableName, column.Name);

                    // Verificar si el valor ya existe en la columna
                    if (columnData.Contains(convertedValue))
                    {
                        return new OperationResult 
                        { 
                            Status = OperationStatus.Error,
                            Message = $"El valor '{convertedValue}' ya existe en la columna '{column.Name}', que tiene un índice asociado. No se puede insertar un valor duplicado."
                        };
                    }
                }
            }

            // Insertar los valores en el archivo de la tabla
            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            try
            {
                using (FileStream fs = new FileStream(tablePath, FileMode.Append, FileAccess.Write))
                using (BinaryWriter writer = new BinaryWriter(fs))
                {
                    foreach (var value in convertedValues)
                    {
                        WriteValue(writer, value);
                    }
                }
               
            }
            catch (Exception ex)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"Error al insertar los valores en la tabla '{tableName}': {ex.Message}"
                };
            }

            // Regenerar los índices si la inserción fue exitosa
            try
            {
                string originalDBName = SettedDataBaseName;
                string originalDBPath = SettedDataBasePath;

                IndexGenerator indexGenerator = new IndexGenerator();
                indexGenerator.RegenerateIndexes();

                // Restaurar los valores originales de la base de datos
                this.SettedDataBaseName = originalDBName;
                this.SettedDataBasePath = originalDBPath;

              
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error al regenerar los índices: {ex.Message}");
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"Error al regenerar los índices: {ex.Message}"
                };
            }

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = "Valores insertados correctamente."
            };
        }




        ///////////////////////////////////////////////// FUNCIONES AUXILIARES DEL INSERT INTO ///////////////////////////////////////////////////////


        private object ConvertValue(string valueStr, DataType dataType, int? maxSize)
        {
            switch (dataType)
            {
                case DataType.INTEGER:
                    if (int.TryParse(valueStr, out int intValue))
                    {
                        return intValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es un entero válido.");
                    }
                case DataType.DOUBLE:
                    if (double.TryParse(valueStr, out double doubleValue))
                    {
                        return doubleValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es un double válido.");
                    }
                case DataType.VARCHAR:
                    valueStr = valueStr.Trim('\'', '\"'); // Remover comillas si las hay
                    if (valueStr.Length > maxSize)
                    {
                        throw new Exception($"El valor excede el tamaño máximo de {maxSize} caracteres.");
                    }
                    return valueStr;
                case DataType.DATETIME:
                    valueStr = valueStr.Trim('\'', '\"'); // Remover comillas si las hay
                    if (DateTime.TryParse(valueStr, out DateTime dateTimeValue))
                    {
                        return dateTimeValue;
                    }
                    else
                    {
                        throw new Exception("El valor no es una fecha válida.");
                    }
                default:
                    throw new Exception("Tipo de dato no soportado.");
            }
        }


        private void WriteValue(BinaryWriter writer, object value)
        {
            if (value is int intValue)
            {
                writer.Write(intValue);
            }
            else if (value is double doubleValue)
            {
                writer.Write(doubleValue);
            }
            else if (value is string strValue)
            {
                writer.Write(strValue);
            }
            else if (value is DateTime dateTimeValue)
            {
                writer.Write(dateTimeValue.ToBinary()); // Almacenar como ticks
            }
            else
            {
                throw new Exception("Tipo de dato no soportado para escritura.");
            }
        }



        ///////////////////////////////////////////////// FINAL FUNCIONES TABLAS ///////////////////////////////////////////////////////

        ///////////////////////////////////////////////// INICIO FUNCIONES INDICES ///////////////////////////////////////////////////////


        // esto es basicamente un add index to system indexes
        public OperationResult CreateIndex(string indexName, string tableName, string columnName, string indexType)
        {
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            // Verificar que el tipo de índice sea válido (BST o BTREE)
            if (!indexType.Equals("BST", StringComparison.OrdinalIgnoreCase) &&
                !indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"El tipo de índice '{indexType}' no es válido. Solo se permiten 'BST' o 'BTREE'."
                };
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);

            // Verificar si la columna especificada existe en la tabla
            var column = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (column == null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La columna '{columnName}' no existe en la tabla '{tableName}'."
                };
            }

            // Verificar si ya existe un índice asociado a la columna
            string existingIndex = GetIndexNameIfExist(SettedDataBaseName, tableName, columnName);
            if (existingIndex != null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"Ya existe un índice asociado a la columna '{columnName}' en la tabla '{tableName}'. Índice existente: {existingIndex}"
                };
            }

            // Obtener los datos de la columna
            List<object> columnData = GetColumnData(SettedDataBaseName, tableName, columnName);

            // Verificar si hay datos duplicados
            if (columnData.Count != columnData.Distinct().Count())
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La columna '{columnName}' contiene datos duplicados. No se puede crear el índice."
                };
            }

            // Si no hay duplicados, proceder a crear el índice
            using (FileStream stream = File.Open(SystemIndexesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End); // Mover el puntero al final para agregar nuevo índice

                using (BinaryWriter writer = new(stream))
                {
                    writer.Write(SettedDataBaseName);
                    writer.Write(tableName);
                    writer.Write(indexName);
                    writer.Write(columnName);
                    writer.Write(indexType);
                }
            }

            string originalDBName = SettedDataBaseName;
            string originalDBPath = SettedDataBasePath;

            IndexGenerator indexGenerator = new IndexGenerator();
            indexGenerator.RegenerateIndexes();

            // Restaurar los valores originales de la base de datos
            this.SettedDataBaseName = originalDBName;
            this.SettedDataBasePath = originalDBPath;

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = $"Índice '{indexName}' creado exitosamente para la columna '{columnName}' en la tabla '{tableName}'."
            };
        }




        ///////////////////////////////////////////////// FUNCIONES AUXILIARES DE LOS INDICES  ///////////////////////////////////////////////////////

        public List<object> GetColumnData(string databaseName, string tableName, string columnName)
        {

            string originalDBName = SettedDataBaseName;
            string originalDBPath = SettedDataBasePath;

            var columnData = new List<object>();

            // Verificar que la base de datos existe
            if (string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("El nombre de la base de datos no puede estar vacío.");
                return columnData; // Retorna una lista vacía si no hay nombre de base de datos
            }

            // Verificar que la tabla existe en la base de datos especificada
            List<string> tables = GetTablesInDataBase(databaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return columnData; // Retorna una lista vacía si la tabla no existe
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(databaseName, tableName);

            // Validar que la columna existe
            var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                return columnData; // Retorna una lista vacía si la columna no existe
            }

            string DataBasePath = $@"{DataPath}\{databaseName}";

            if (Directory.Exists(DataBasePath))
            {

                

                this.SettedDataBasePath = DataBasePath;
                this.SettedDataBaseName = databaseName;
                               
            }

            // Leer los registros de la tabla
            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return columnData; // Retorna una lista vacía si el archivo no existe
            }

            using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in allColumns)
                    {
                        object value = ReadValue(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    // Agregar el valor de la columna deseada a la lista
                    columnData.Add(record[targetColumn.Name]);
                }
            }

            this.SettedDataBasePath = originalDBPath;
            this.SettedDataBaseName = originalDBName;

            return columnData;
        }




        public DataType? GetColumnDataType(string databaseName, string tableName, string columnName)
        {
            // Verificar que la base de datos existe
            if (string.IsNullOrEmpty(databaseName))
            {
                Console.WriteLine("El nombre de la base de datos no puede estar vacío.");
                return null; // Retorna null si no hay nombre de base de datos
            }

            // Verificar que la tabla existe en la base de datos especificada
            List<string> tables = GetTablesInDataBase(databaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{databaseName}'.");
                return null; // Retorna null si la tabla no existe
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(databaseName, tableName);

            // Validar que la columna existe
            var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                return null; // Retorna null si la columna no existe
            }

            // Retorna el DataType de la columna
            return targetColumn.DataType;
        }

        public string GetSystemIndexesFile()
        {
            return SystemIndexesFile;
        }

        public string GetIndexNameIfExist(string databaseName, string tableName, string columnName)
        {
            // Abrir el archivo en modo de solo lectura
            if (!File.Exists(SystemIndexesFile))
            {
                // Si el archivo no existe, no puede haber ningún índice
                return null;
            }

            using (FileStream stream = File.Open(SystemIndexesFile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader reader = new(stream))
                {
                    while (stream.Position < stream.Length) // Leer hasta el final del archivo
                    {
                        // Leer los valores en el mismo orden en que fueron escritos
                        string dbName = reader.ReadString();
                        string tblName = reader.ReadString();
                        string idxName = reader.ReadString();
                        string colName = reader.ReadString();
                        string idxType = reader.ReadString(); 

                        // Comprobar si coinciden la base de datos, la tabla y la columna
                        if (dbName.Equals(databaseName, StringComparison.OrdinalIgnoreCase) &&
                            tblName.Equals(tableName, StringComparison.OrdinalIgnoreCase) &&
                            colName.Equals(columnName, StringComparison.OrdinalIgnoreCase))
                        {
                            // Si se encuentra una coincidencia, devolver el nombre del índice
                            return idxName;
                        }
                    }
                }
            }

            // Si no se encontró el índice, devolver null
            return null;
        }

        public string GetAssociatedIndex(string dataBaseName, string tableName, string columnName)
        {
            // Acceder al diccionario para obtener el nombre del índice asociado a la columna
            if (AssociatedIndexesToColumns.TryGetValue(columnName, out string indexName))
            {
                Console.WriteLine($"Índice asociado encontrado: {indexName}");
                return indexName;
            }
            else
            {
                Console.WriteLine($"No se encontró un índice asociado para la columna {columnName} en la tabla {tableName} de la base de datos {dataBaseName}.");
                return null;
            }
        }

        // Método para obtener todos los registros de un índice dado su nombre
        public List<Dictionary<string, object>> GetRecordsFromIndex(string indexName)
        {
            if (IndexTrees.TryGetValue(indexName, out object tree))
            {
                // Verificar el tipo de árbol y llamar a GetAllRecords
                switch (tree)
                {
                    case BinarySearchTree<int> bstInt:
                        return bstInt.GetAllRecords();

                    case BinarySearchTree<string> bstString:
                        return bstString.GetAllRecords();

                    case BinarySearchTree<double> bstDouble:
                        return bstDouble.GetAllRecords();

                    case BinarySearchTree<DateTime> bstDateTime:
                        return bstDateTime.GetAllRecords();

                    case BTree<int> btreeInt:
                        return btreeInt.GetAllRecords();

                    case BTree<string> btreeString:
                        return btreeString.GetAllRecords();

                    case BTree<double> btreeDouble:
                        return btreeDouble.GetAllRecords();

                    case BTree<DateTime> btreeDateTime:
                        return btreeDateTime.GetAllRecords();

                    default:
                        Console.WriteLine("Tipo de árbol no soportado.");
                        return null;
                }
            }
            else
            {
                Console.WriteLine("El índice especificado no existe.");
                return null;
            }
        }

        ///////////////////////////////////////////////// FINAL FUNCIONES INDICES ///////////////////////////////////////////////////////

        //////////////////////////////////////////////// INICIO FUNCIONES SELECT ///////////////////////////////////////////////////////

        public OperationResult SelectFromTable(string tableName, List<string> columnsToSelect, string whereClause, string orderByColumn, string orderByDirection, out object? data)
           
        {

            data = null;
            string mode = "DEFAULT";
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return new OperationResult                 
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);

            // Determinar las columnas a seleccionar
            List<Column> selectedColumns;
            if (columnsToSelect == null)
            {
                // Seleccionar todas las columnas
                selectedColumns = allColumns;
            }
            else
            {
                // Validar que las columnas existen
                selectedColumns = new List<Column>();
                foreach (var colName in columnsToSelect)
                {
                    var col = allColumns.FirstOrDefault(c => c.Name.Equals(colName, StringComparison.OrdinalIgnoreCase));
                    if (col == null)
                    {
                        return new OperationResult
                        {
                            Status = OperationStatus.Error,
                            Message = $"La columna '{colName}' no existe en la tabla '{tableName}'."
                        };
                    }
                    selectedColumns.Add(col);
                }
            }

            var records = new List<Dictionary<string, object>>();

            if (DataBasesWithIndexes.Contains(SettedDataBaseName) && TablesWithIndexes.Contains(tableName)) 
            {
                    foreach (var col in selectedColumns)
                    {
                        string? indexName = GetAssociatedIndex(SettedDataBaseName, tableName, col.Name);
                        if (indexName != null)
                        {
                            records = GetRecordsFromIndex(indexName);
                            Console.WriteLine("USANDO INDICES EN MEMORIA PARA ESTE REQUEST");
                            break;
                        }    
                    }
            }


            if (records == null || records.Count == 0)

            {
                Console.WriteLine("no hay indices asoicados");
                records = GetRecordsFromTable(tableName, allColumns);
            }


            if (records == null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se pudieron leer los registros de la tabla."
                };
            }

            if (!string.IsNullOrEmpty(whereClause))
            {
                records = FilteredRecordsWhere(records, whereClause, tableName, allColumns, mode, null, null);
                if (records == null)
                {
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = "Error al filtrar los registros."
                    };
                }
            }
         
            // Ordenar los registros si hay cláusula ORDER BY
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                records = FilteredRecordsOrderBy(records, orderByColumn, orderByDirection, tableName, allColumns);
                if (records == null)
                {
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = "Error al ordenar los registros."
                    };
                }
            }

            var filteredRecords = records.Select(record =>
            {
                var filteredRecord = new Dictionary<string, object>();
                foreach (var column in selectedColumns)
                {
                    filteredRecord[column.Name] = record[column.Name];
                }
                return filteredRecord;
            }).ToList();

            // Asignar los datos al parámetro de salida
            data = new
            {
                Columns = selectedColumns.Select(c => c.Name).ToList(),
                Rows = filteredRecords
            };

            // Mostrar los registros en formato de tabla
            PrintRecords(selectedColumns, records);

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = "Registros seleccionados correctamente."
            };
        }


        ///////////////////////////////////////////////// FUNCION UPDATE ///////////////////////////////////////////////////////

        public OperationResult UpdateFromTable(string tableName, string columnName, string newValue, string whereClause)
        {
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);
            List<Dictionary<string, object>> records = GetRecordsFromTable(tableName, allColumns);                      
            string mode = "UPDATE";
            object convertedValue;

            if (records == null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se pudieron leer los registros de la tabla."
                };
            }

            // Validar que la columna a actualizar existe
            var targetColumn = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (targetColumn == null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La columna '{columnName}' no existe en la tabla '{tableName}'."
                };
            }
         
            try
            {
                convertedValue = ConvertValue(newValue, targetColumn.DataType, targetColumn.MaxSize);
            }
            catch (Exception ex)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"Error al convertir el valor '{newValue}' para la columna '{columnName}': {ex.Message}"
                };
            }

            // Leer los registros de la tabla
            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"El archivo de la tabla '{tableName}' no existe."
                };
            }

            if (!string.IsNullOrEmpty(whereClause))
            {
                records = FilteredRecordsWhere(records, whereClause, tableName, allColumns, mode, columnName, convertedValue);
                if (records == null)
                {
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = "Error al filtrar los registros."
                    };                   
                }
            }
            else
            {
                foreach(var record in records)
                {
                    record[columnName] = convertedValue;
                }               
            }
          
            using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
            using (BinaryWriter writer = new BinaryWriter(fs))
            {
                foreach (var record in records)
                {
                    foreach (var column in allColumns)
                    {
                        object value = record[column.Name];
                        WriteValue(writer, value);
                    }
                }
            }

            string SettedDBNAME = SettedDataBaseName; //un ajuste para que la base de datos se mantenga setteada correctamente despues de actualizar los indices
            string SettedDBPATH = SettedDataBasePath;


            Console.WriteLine("Valores actualizados correctamente.");
            IndexGenerator indexGenerator = new IndexGenerator();


            indexGenerator.RegenerateIndexes();

            this.SettedDataBasePath = SettedDBPATH; //un ajuste para que la base de datos se mantenga setteada correctamente despues de actualizar los indices
            this.SettedDataBaseName = SettedDBNAME;

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = "Valores actualizados correctamente."
            };

        }

        ///////////////////////////////////////////////// FUNCION DELETE ///////////////////////////////////////////////////////
        

        public OperationResult DeleteFromTable(string tableName, string whereClause)
        {
            // Verificar que la base de datos está establecida
            string mode = "DEFAULT";

            

            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se ha establecido una base de datos."
                };
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = $"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'."
                };
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);
            List<Dictionary<string, object>> records = GetRecordsFromTable(tableName, allColumns);
            List<Dictionary<string, object>> recordsToDelete = new List<Dictionary<string, object>>();
            if (records == null)
            {
                return new OperationResult
                {
                    Status = OperationStatus.Error,
                    Message = "No se pudieron leer los registros de la tabla."
                };
            }

            if (!string.IsNullOrEmpty(whereClause))
            {
                recordsToDelete = FilteredRecordsWhere(records, whereClause, tableName, allColumns, mode, null, null);
                if (records == null)
                {
                    return new OperationResult
                    {
                        Status = OperationStatus.Error,
                        Message = "Error al filtrar los registros."
                    };
                }
            }

            records = records.Except(recordsToDelete).ToList();

            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            using (FileStream fs = new FileStream(tablePath, FileMode.Create, FileAccess.Write))
            using (BinaryWriter writer = new BinaryWriter(fs))
            {
                foreach (var record in records)
                {
                    foreach (var column in allColumns)
                    {
                        object value = record[column.Name];
                        WriteValue(writer, value);
                    }
                }
            }

            string SettedDBNAME = SettedDataBaseName; //un ajuste para que la base de datos se mantenga setteada correctamente despues de actualizar los indices
            string SettedDBPATH = SettedDataBasePath;

            IndexGenerator indexGenerator = new IndexGenerator();
            indexGenerator.RegenerateIndexes();

            this.SettedDataBasePath = SettedDBPATH; //un ajuste para que la base de datos se mantenga setteada correctamente despues de actualizar los indices
            this.SettedDataBaseName = SettedDBNAME;

            return new OperationResult
            {
                Status = OperationStatus.Success,
                Message = "Registros eliminados correctamente."
            };
        }

         

        ///////////////////////////////////////////////// FUNCIONES AUXILIARES DEL SELECT ///////////////////////////////////////////////////////

        public List<Dictionary<string, object>> GetRecordsFromTable(string tableName, List<Column> allColumns)


        {
            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in allColumns)
                    {
                        object value = ReadValue(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    records.Add(record);

                    Console.WriteLine("Registro leido:");
                    foreach (var key in record.Keys)
                    {
                        Console.WriteLine($"Columna: {key}, Valor: {record[key]}");
                    }
                }
            }

            return records;
        }

        // Esta funcion es la misma que la de arriba, pero para los indices, porque ocupo estar cambiando la base de datos
        public List<Dictionary<string, object>> GetDataFromTable(string DataBaseName, string tableName, List<Column> allColumns)

            
        {
            string DataBasePath = $@"{DataPath}\{DataBaseName}";

            if (Directory.Exists(DataBasePath))
            {

                this.SettedDataBasePath = DataBasePath;
                this.SettedDataBaseName = DataBaseName;

            }


            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            if (!File.Exists(tablePath))
            {
                Console.WriteLine($"El archivo de la tabla '{tableName}' no existe.");
                return new List<Dictionary<string, object>>();
            }

            var records = new List<Dictionary<string, object>>();

            using (FileStream fs = new FileStream(tablePath, FileMode.Open, FileAccess.Read))
            using (BinaryReader reader = new BinaryReader(fs))
            {
                while (fs.Position < fs.Length)
                {
                    var record = new Dictionary<string, object>();
                    foreach (var column in allColumns)
                    {
                        object value = ReadValue(reader, column.DataType);
                        record[column.Name] = value;
                    }
                    records.Add(record);

                    //Console.WriteLine("Registro leido:");
                    foreach (var key in record.Keys)
                    {
                        //Console.WriteLine($"Columna: {key}, Valor: {record[key]}");
                    }
                }
            }

            this.SettedDataBasePath = string.Empty;
            this.SettedDataBaseName = string.Empty;

            return records;
        }





        private List<Dictionary<string, object>> FilteredRecordsWhere(List<Dictionary<string, object>> records, string whereClause, string tableName, List<Column> allColumns, string mode, string? settedColumn,  object? updateValue)
        {
            // Parsear y evaluar la cláusula WHERE
            string whereColumn = null;
            string whereOperator = null;
            string whereValue = null;

            var whereMatch = Regex.Match(whereClause, @"(\w+)\s*(=|>|<|LIKE|NOT)\s*(.+)", RegexOptions.IgnoreCase);
            if (!whereMatch.Success)
            {
                Console.WriteLine("Sintaxis de WHERE incorrecta.");
                return records = null;
            }

            whereColumn = whereMatch.Groups[1].Value;
            whereOperator = whereMatch.Groups[2].Value.ToUpper();
            whereValue = whereMatch.Groups[3].Value.Trim('\'', '\"');

            // Validar la columna
            var whereCol = allColumns.FirstOrDefault(c => c.Name.Equals(whereColumn, StringComparison.OrdinalIgnoreCase));
            if (whereCol == null)
            {
                Console.WriteLine($"La columna '{whereColumn}' no existe en la tabla '{tableName}'.");
                return records = null;
            }

            // Aplicar el filtro
            if(mode == "DEFAULT") 
            {
                return records = records.Where(record => EvaluateWhereCondition(record, whereColumn, whereOperator, whereValue)).ToList();
            }

            else if(mode == "UPDATE")
            {
                

                foreach (var record in records)
                {                 
                    if (EvaluateWhereCondition(record, whereColumn, whereOperator, whereValue))
                    {
                        record[settedColumn] = updateValue;
                    }                 
                }

                return records;
            }

            else
            {
                return records = null;
            }
            
        }

        private List<Dictionary<string, object>> FilteredRecordsOrderBy(List<Dictionary<string, object>> records, string orderByColumn, string orderByDirection, string tableName, List<Column> allColumns)
        {
            var orderByCol = allColumns.FirstOrDefault(c => c.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
            if (orderByCol == null)
            {
                Console.WriteLine($"La columna '{orderByColumn}' no existe en la tabla '{tableName}'.");
                return null;
            }

            bool descending = orderByDirection.Equals("DESC", StringComparison.OrdinalIgnoreCase);

            // Llamar a Quicksort
            QuickSort(records, 0, records.Count - 1, orderByCol.Name, descending);

            return records;
        }




        private object ReadValue(BinaryReader reader, DataType dataType)
        {
            switch (dataType)
            {
                case DataType.INTEGER:
                    return reader.ReadInt32();
                case DataType.DOUBLE:
                    return reader.ReadDouble();
                case DataType.VARCHAR:
                    return reader.ReadString();
                case DataType.DATETIME:
                    long ticks = reader.ReadInt64();
                    return DateTime.FromBinary(ticks);
                default:
                    throw new Exception("Tipo de dato no soportado para lectura.");
            }
        }

      


        private void PrintRecords(List<Column> selectedColumns, List<Dictionary<string, object>> records)
        {
            // Crear la tabla en forma de lista de diccionarios
            var table = new List<Dictionary<string, object>>();

            foreach (var record in records)
            {
                var row = new Dictionary<string, object>();
                foreach (var column in selectedColumns)
                {
                    row[column.Name] = record[column.Name];
                }
                table.Add(row);
            }

            // Obtener los nombres de las columnas
            var columnNames = selectedColumns.Select(c => c.Name).ToList();

            // Llamar al método PrintTable
            PrintTable(table, columnNames);
        }



        private void PrintTable(List<Dictionary<string, object>> table, List<string> columns)
        {
            // Calcular el ancho máximo de cada columna
            Dictionary<string, int> columnWidths = new Dictionary<string, int>();

            // Inicializar los anchos con la longitud de los nombres de las columnas
            foreach (var col in columns)
            {
                columnWidths[col] = col.Length;
            }

            // Actualizar los anchos según los datos
            foreach (var row in table)
            {
                foreach (var col in columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    if (valueStr.Length > columnWidths[col])
                    {
                        columnWidths[col] = valueStr.Length;
                    }
                }
            }

            // Imprimir los encabezados con formato
            foreach (var col in columns)
            {
                Console.Write($"| {col.PadRight(columnWidths[col])} ");
            }
            Console.WriteLine("|");

            // Imprimir línea separadora
            Console.WriteLine(new string('-', columns.Sum(col => columnWidths[col] + 3) + 1));

            // Paso 3: Imprimir las filas con formato
            foreach (var row in table)
            {
                foreach (var col in columns)
                {
                    string valueStr = row[col]?.ToString() ?? "NULL";
                    Console.Write($"| {valueStr.PadRight(columnWidths[col])} ");
                }
                Console.WriteLine("|");
            }
        }

        // public List<string>

        private bool EvaluateWhereCondition(Dictionary<string, object> record, string columnName, string operatorStr, string valueStr)
        {
            if (!record.ContainsKey(columnName))
                return false;

            var recordValue = record[columnName];

            switch (operatorStr)
            {
                case "=":
                    return recordValue.ToString().Equals(valueStr, StringComparison.OrdinalIgnoreCase);
                case ">":
                    return CompareValues(recordValue, valueStr) > 0;
                case "<":
                    return CompareValues(recordValue, valueStr) < 0;
                case "LIKE":
                    return recordValue.ToString().Contains(valueStr, StringComparison.OrdinalIgnoreCase);
                case "NOT":
                    return !recordValue.ToString().Equals(valueStr, StringComparison.OrdinalIgnoreCase);
                default:
                    throw new Exception("Operador no soportado en WHERE.");
            }
        }

        private int CompareValues(object value1, object value2)
        {
            if (value1 == null && value2 == null)
                return 0;
            if (value1 == null)
                return -1;
            if (value2 == null)
                return 1;

            if (value1 is int int1 && value2 is int int2)
            {
                return int1.CompareTo(int2);
            }
            else if (value1 is double double1 && value2 is double double2)
            {
                return double1.CompareTo(double2);
            }
            else if (value1 is string str1 && value2 is string str2)
            {
                return string.Compare(str1, str2, StringComparison.OrdinalIgnoreCase);
            }
            else if (value1 is DateTime date1 && value2 is DateTime date2)
            {
                return date1.CompareTo(date2);
            }
            else
            {
                throw new Exception("Tipos de datos no comparables o incompatibles.");
            }
        }

        private void QuickSort(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
        {
            if (low < high)
            {
                int pivotIndex = Partition(records, low, high, columnName, descending);
                QuickSort(records, low, pivotIndex - 1, columnName, descending);
                QuickSort(records, pivotIndex + 1, high, columnName, descending);
            }
        }

        private int Partition(List<Dictionary<string, object>> records, int low, int high, string columnName, bool descending)
        {
            var pivotValue = records[high][columnName];
            int i = low - 1;

            for (int j = low; j < high; j++)
            {
                int comparisonResult = CompareValues(records[j][columnName], pivotValue);
                if (descending)
                {
                    if (comparisonResult > 0)
                    {
                        i++;
                        Swap(records, i, j);
                    }
                }
                else
                {
                    if (comparisonResult < 0)
                    {
                        i++;
                        Swap(records, i, j);
                    }
                }
            }

            Swap(records, i + 1, high);
            return i + 1;
        }

        private void Swap(List<Dictionary<string, object>> records, int index1, int index2)
        {
            var temp = records[index1];
            records[index1] = records[index2];
            records[index2] = temp;
        }

    }

}
