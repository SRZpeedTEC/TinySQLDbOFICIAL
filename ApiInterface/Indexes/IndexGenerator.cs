using ApiInterface.Indexes;
using QueryProcessor.Parser;
using Entities;
using StoreDataManager;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface
{
    public class IndexGenerator
    {
        // Diccionario en memoria para almacenar los árboles por índice.
        Dictionary<string, object> IndexTrees = new Dictionary<string, object>();

        public void LoadIndexesAndGenerateTrees()
        {

            string systemIndexesFile = Store.GetInstance().GetSystemIndexesFile();

            if (!File.Exists(systemIndexesFile))
            {
                Console.WriteLine($"El archivo de índices no existe: {systemIndexesFile}");
                // Aquí puedes manejar la lógica para crear un nuevo índice si es necesario
                return; // Salimos del método ya que no hay índices para cargar
            }

            // Abrimos el archivo de índices
            using (FileStream stream = File.Open(systemIndexesFile, FileMode.Open, FileAccess.Read))
            {
                using (BinaryReader reader = new(stream))
                {
                    while (stream.Position < stream.Length)
                    {
                        // Leer la metadata desde el archivo de índices
                        string DataBaseName = reader.ReadString();
                        string tableName = reader.ReadString();
                        string indexName = reader.ReadString();
                        string columnName = reader.ReadString();
                        string indexType = reader.ReadString();

                        DataType? columnDatatype = Store.GetInstance().GetColumnDataType(DataBaseName, tableName, columnName);

                        // Acceder a la columna en el disco duro y obtener los datos
                        List<object> columnData = Store.GetInstance().GetColumnData(DataBaseName, tableName, columnName);

                        // Verificar el tipo de índice y crear el árbol correspondiente
                        if (indexType.Equals("BST", StringComparison.OrdinalIgnoreCase))
                        {
                            if (columnDatatype == DataType.INTEGER)
                            {
                                var bst = new BinarySearchTree<int>();
                                foreach (int value in columnData)
                                {
                                    bst.insert(value); // Insertar valores en el BST
                                    Console.WriteLine($"Valor {value} agregado al arbol del indice: {indexName}");
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.VARCHAR)
                            {
                                var bst = new BinarySearchTree<string>();
                                foreach (string value in columnData)
                                {
                                    bst.insert(value); // Insertar valores en el
                                    Console.WriteLine($"Valor {value} agregado al arbol del indice: {indexName}");
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.DOUBLE)
                            {
                                var bst = new BinarySearchTree<double>();
                                foreach (double value in columnData)
                                {
                                    bst.insert(value); // Insertar valores en el BST
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bst;

                            }

                            else if (columnDatatype == DataType.DATETIME)
                            {
                                var bst = new BinarySearchTree<DateTime>();
                                foreach (DateTime value in columnData)
                                {
                                    bst.insert(value); // Insertar valores en el BST
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bst;

                            }


                        }
                        else if (indexType.Equals("BTREE", StringComparison.OrdinalIgnoreCase))
                        {

                            if (columnDatatype == DataType.INTEGER)
                            {
                                var bTree = new BTree<int>(3);
                                foreach (int value in columnData)
                                {
                                    bTree.Insert(value); // Insertar valores en el BTree
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.DOUBLE)
                            {
                                var bTree = new BTree<double>(3);
                                foreach (double value in columnData)
                                {
                                    bTree.Insert(value); // Insertar valores en el BTree
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.VARCHAR)
                            {
                                var bTree = new BTree<string>(3);
                                foreach (string value in columnData)
                                {
                                    bTree.Insert(value); // Insertar valores en el BTree
                                    Console.WriteLine($"Valor {value} agregado al arbol del indice: {indexName}");
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bTree;
                            }

                            else if (columnDatatype == DataType.DATETIME)
                            {
                                var bTree = new BTree<string>(3);
                                foreach (string value in columnData)
                                {
                                    bTree.Insert(value); // Insertar valores en el BTree
                                    
                                }

                                // Guardar el árbol en el diccionario en memoria
                                IndexTrees[indexName] = bTree;
                            }


                        }
                        else
                        {
                            Console.WriteLine($"Tipo de índice no válido: {indexType}");
                        }
                    }
                }
            }

            Console.WriteLine("Índices cargados y árboles generados en memoria.");
        }

        

    }
    
}
