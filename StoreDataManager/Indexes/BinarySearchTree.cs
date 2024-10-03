using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class BinarySearchTree<T> where T : IComparable<T>
    {
        TreeNode<T> root;

        public BinarySearchTree()
        {
            root = null;
        }

        // Funciones de búsqueda
        public bool search(T key)
        {
            return searchRecursive(root, key);
        }

        private bool searchRecursive(TreeNode<T> root, T key)
        {
            if (root == null || root.key.CompareTo(key) == 0)
                return root != null;

            // Si la clave actual es menor que la clave buscada
            if (root.key.CompareTo(key) < 0)
                return searchRecursive(root.right, key);

            // Si la clave actual es mayor que la clave buscada
            return searchRecursive(root.left, key);
        }

        // Funciones de inserción de elementos
        public void insert(T key)
        {
            root = insertRecursive(root, key);
        }

        private TreeNode<T> insertRecursive(TreeNode<T> root, T key)
        {
            if (root == null)
            {
                root = new TreeNode<T>(key);
                return root;
            }

            // Si la clave es menor que la clave del nodo actual, inserta en el subárbol izquierdo
            if (key.CompareTo(root.key) < 0)
                root.left = insertRecursive(root.left, key);
            // Si la clave es mayor, inserta en el subárbol derecho
            else if (key.CompareTo(root.key) > 0)
                root.right = insertRecursive(root.right, key);

            return root;
        }

        // Funciones de eliminación de elementos
        public void delete(T key)
        {
            root = deleteRecursive(root, key);
        }

        private TreeNode<T> deleteRecursive(TreeNode<T> root, T key)
        {
            if (root == null)
                return root;

            if (key.CompareTo(root.key) < 0)
                root.left = deleteRecursive(root.left, key);
            else if (key.CompareTo(root.key) > 0)
                root.right = deleteRecursive(root.right, key);
            else
            {
                // Nodo con solo un hijo o sin hijos
                if (root.left == null)
                    return root.right;
                else if (root.right == null)
                    return root.left;

                // Nodo con dos hijos: Obtener el valor mínimo en el subárbol derecho
                root.key = minValue(root.right);

                // Eliminar el nodo más pequeño del subárbol derecho
                root.right = deleteRecursive(root.right, root.key);
            }
            return root;
        }

        private T minValue(TreeNode<T> root)
        {
            T minValue = root.key;
            while (root.left != null)
            {
                minValue = root.left.key;
                root = root.left;
            }
            return minValue;
        }
    }
}
