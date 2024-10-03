using Entities;
using QueryProcessor;
using QueryProcessor.Parser;
using System.Text.Json;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using System.Text.RegularExpressions;


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

        public OperationStatus CreateTable(string TableName, List<Column> TableColumns)
        {
            // Validación de que haya una base de datos seteada
            if (string.IsNullOrEmpty(SettedDataBasePath))
            {
                Console.WriteLine("No se ha establecido una base de datos");
                return OperationStatus.Error;
            }

            string tablePath = $@"{SettedDataBasePath}\{TableName}.table";

            if (File.Exists(tablePath))
            {
                Console.WriteLine($"Tabla ya existente en {SettedDataBasePath}");
                return OperationStatus.Error; // Cambiar a error si la tabla ya existe
            }

            using (FileStream stream = File.Open(tablePath, FileMode.Create))           
            {
                // ARCHIVO CREADO
            }

            // Agregar la tabla a los metadatos del sistema
            AddTableToSystemTables(TableName);
            AddColumsToSystemColumns(TableName, TableColumns);

            Console.WriteLine($"Tabla '{TableName}' creada exitosamente en la base de datos '{SettedDataBaseName}'.");
            return OperationStatus.Success;
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


        public OperationStatus DropTable(string TableToDrop)
        {
            // Validacion de que haya una base de datos setteada
            if (string.IsNullOrEmpty(SettedDataBasePath))
            {
                Console.WriteLine("No se ha establecido una base de datos");
                return OperationStatus.Error;
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
                    Console.WriteLine("La tabla no está vacía, no se puede eliminar.");
                    return OperationStatus.Error;
                }
            }
            
            else
            {
                Console.WriteLine($"La tabla '{TableToDrop}' no existe.");
                return OperationStatus.Error;
            }

            RemoveTableFromSystemTables(TableToDrop);

            RemoveColumnsFromSystemColumns(TableToDrop);

            return OperationStatus.Success;
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


        public OperationStatus InsertInto(string tableName, List<string> values)
        {

            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
                return OperationStatus.Error;
            }

            // Obtener las columnas de la tabla
            List<Column> columns = GetColumnsOfTable(SettedDataBaseName, tableName);

            if (values.Count != columns.Count)
            {
                Console.WriteLine("El número de valores proporcionados no coincide con el número de columnas.");
                return OperationStatus.Error;
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
                    Console.WriteLine($"Error al convertir el valor '{valueStr}' para la columna '{column.Name}': {ex.Message}");
                    return OperationStatus.Error;
                }
            }

            // Insertar los valores en el archivo de la tabla
            string tablePath = Path.Combine(SettedDataBasePath, $"{tableName}.table");

            using (FileStream fs = new FileStream(tablePath, FileMode.Append, FileAccess.Write))
            using (BinaryWriter writer = new BinaryWriter(fs))
            {
                foreach (var value in convertedValues)
                {
                    WriteValue(writer, value);
                }
            }

            Console.WriteLine("Valores insertados correctamente.");
            return OperationStatus.Success;

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
        public OperationStatus CreateIndex(string indexName, string tableName, string columnName, string indexType) 
            {
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
                return OperationStatus.Error;
            }

            // Verificar que el tipo de índice sea válido (BST o BTREE)
            if (!indexType.Equals("BST", StringComparison.OrdinalIgnoreCase) &&
                !indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
            {
                Console.WriteLine($"El tipo de índice '{indexType}' no es válido. Solo se permiten 'BST' o 'BTREE'.");
                return OperationStatus.Error;
            }

            // Obtener las columnas de la tabla
            List<Column> allColumns = GetColumnsOfTable(SettedDataBaseName, tableName);

            // Verificar si la columna especificada existe en la tabla
            var column = allColumns.FirstOrDefault(c => c.Name.Equals(columnName, StringComparison.OrdinalIgnoreCase));
            if (column == null)
            {
                Console.WriteLine($"La columna '{columnName}' no existe en la tabla '{tableName}'.");
                return OperationStatus.Error;
            }
            
  
            using (FileStream stream = File.Open(SystemIndexesFile, FileMode.OpenOrCreate, FileAccess.Write))
            {
                stream.Seek(0, SeekOrigin.End);

                using (BinaryWriter writer = new(stream))
                {

                    writer.Write(SettedDataBaseName);
                    writer.Write(tableName);
                    writer.Write(indexName);
                    writer.Write(columnName);
                    writer.Write(indexType);


                }
            }


            return OperationStatus.Success;
            }



        ///////////////////////////////////////////////// FUNCIONES AUXILIARES DE LOS INDICES  ///////////////////////////////////////////////////////

        public List<object> GetColumnData(string databaseName, string tableName, string columnName)
        {
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

            this.SettedDataBasePath = string.Empty;
            this.SettedDataBaseName = string.Empty;

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



        ///////////////////////////////////////////////// FINAL FUNCIONES INDICES ///////////////////////////////////////////////////////

        //////////////////////////////////////////////// INICIO FUNCIONES SELECT ///////////////////////////////////////////////////////

        public OperationStatus SelectFromTable(string tableName, List<string> columnsToSelect, string whereClause, string orderByColumn, string orderByDirection, out object? data)
           
        {

            data = null;
            // Verificar que la base de datos está establecida
            if (string.IsNullOrEmpty(SettedDataBaseName))
            {
                Console.WriteLine("No se ha establecido una base de datos.");
                return OperationStatus.Error;
            }

            // Verificar que la tabla existe
            List<string> tables = GetTablesInDataBase(SettedDataBaseName);
            if (!tables.Contains(tableName))
            {
                Console.WriteLine($"La tabla '{tableName}' no existe en la base de datos '{SettedDataBaseName}'.");
                return OperationStatus.Error;
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
                        Console.WriteLine($"La columna '{colName}' no existe en la tabla '{tableName}'.");
                        return OperationStatus.Error;
                    }
                    selectedColumns.Add(col);
                }
            }

            var records = GetRecordsFromTable(tableName, allColumns);
            
            if (!string.IsNullOrEmpty(whereClause))
            {
                records = FilteredRecordsWhere(records, whereClause, tableName, allColumns);
                if (records == null)
                {
                    return OperationStatus.Error;
                }
            }
         
            // Ordenar los registros si hay cláusula ORDER BY
            if (!string.IsNullOrEmpty(orderByColumn))
            {
                records = FilteredRecordsOrderBy(records, orderByColumn, orderByDirection, tableName, allColumns);
                if (records == null)
                {
                    return OperationStatus.Error;
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

            return OperationStatus.Success;
        }





        ///////////////////////////////////////////////// FUNCIONES AUXILIARES DEL SELECT ///////////////////////////////////////////////////////

        private List<Dictionary<string, object>> GetRecordsFromTable(string tableName, List<Column> allColumns)
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
        private List<Dictionary<string, object>> FilteredRecordsWhere(List<Dictionary<string, object>> records, string whereClause, string tableName, List<Column> allColumns)
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
            return records = records.Where(record => EvaluateWhereCondition(record, whereColumn, whereOperator, whereValue)).ToList();
        }

        private List<Dictionary<string, object>> FilteredRecordsOrderBy(List<Dictionary<string, object>> records, string orderByColumn, string orderByDirection, string tableName, List<Column> allColumns) 
        {
            var orderByCol = allColumns.FirstOrDefault(c => c.Name.Equals(orderByColumn, StringComparison.OrdinalIgnoreCase));
            if (orderByCol == null)
            {
                Console.WriteLine($"La columna '{orderByColumn}' no existe en la tabla '{tableName}'.");
                return records = null;
            }

            if (orderByDirection.Equals("DESC", StringComparison.OrdinalIgnoreCase))
            {
                return records = records.OrderByDescending(r => r[orderByCol.Name]).ToList();
            }
            else
            {
                return records = records.OrderBy(r => r[orderByCol.Name]).ToList();
            }
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

        private int CompareValues(object recordValue, string valueStr)
        {
            if (recordValue is int intValue && int.TryParse(valueStr, out int compareInt))
            {
                return intValue.CompareTo(compareInt);
            }
            else if (recordValue is double doubleValue && double.TryParse(valueStr, out double compareDouble))
            {
                return doubleValue.CompareTo(compareDouble);
            }
            else if (recordValue is string strValue)
            {
                return string.Compare(strValue, valueStr, StringComparison.OrdinalIgnoreCase);
            }
            else if (recordValue is DateTime dateTimeValue && DateTime.TryParse(valueStr, out DateTime compareDateTime))
            {
                return dateTimeValue.CompareTo(compareDateTime);
            }
            else
            {
                throw new Exception("Tipos de datos no comparables.");
            }
        }





        public OperationStatus Select()
        {
            
               return OperationStatus.Success;
            
        }
    }


}
