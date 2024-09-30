﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class BTree<T> where T : IComparable<T>
    {
        private BTreeNode<T> root;  // Nodo raíz del árbol
        private int t;  // Grado mínimo (t)

        // Constructor
        public BTree(int t)
        {
            this.root = null;
            this.t = t;  // El grado mínimo t es una propiedad importante del B-tree
        }

        // Función de búsqueda
        public BTreeNode<T> Search(T key)
        {
            return root == null ? null : Search(root, key);
        }

        // Función recursiva de búsqueda
        private BTreeNode<T> Search(BTreeNode<T> node, T key)
        {
            int i = 0;
            // Encuentra la primera clave mayor o igual que key
            while (i < node.numKeys && key.CompareTo(node.keys[i]) > 0)
            {
                i++;
            }

            // Si la clave es igual a la clave encontrada, devuelve este nodo
            if (i < node.numKeys && key.CompareTo(node.keys[i]) == 0)
            {
                return node;
            }

            // Si llegamos aquí y el nodo es una hoja, la clave no está en el árbol
            if (node.isLeaf)
            {
                return null;
            }

            // Bajar al hijo adecuado
            return Search(node.children[i], key);
        }

        // Función de inserción
        public void Insert(T key)
        {
            // Si el árbol está vacío, crear la raíz
            if (root == null)
            {
                root = new BTreeNode<T>(t, true);
                root.keys[0] = key;  // Insertar la clave
                root.numKeys = 1;  // Actualizar el número de claves
            }
            else
            {
                // Si la raíz está llena, se debe dividir antes de insertar
                if (root.numKeys == 2 * t - 1)
                {
                    BTreeNode<T> newRoot = new BTreeNode<T>(t, false);
                    newRoot.children[0] = root;  // La antigua raíz es ahora el primer hijo
                    SplitChild(newRoot, 0, root);  // Dividir la antigua raíz y mover una clave
                    InsertNonFull(newRoot, key);  // Insertar en el nodo adecuado
                    root = newRoot;  // Actualizar la nueva raíz
                }
                else
                {
                    InsertNonFull(root, key);
                }
            }
        }

        // Función auxiliar para dividir un nodo hijo
        private void SplitChild(BTreeNode<T> parent, int i, BTreeNode<T> fullChild)
        {
            BTreeNode<T> newChild = new BTreeNode<T>(t, fullChild.isLeaf);
            newChild.numKeys = t - 1;  // El nuevo hijo tendrá t-1 claves

            // Copiar las claves del nodo lleno al nuevo nodo
            for (int j = 0; j < t - 1; j++)
            {
                newChild.keys[j] = fullChild.keys[j + t];
            }

            // Si no es una hoja, también copiamos los hijos
            if (!fullChild.isLeaf)
            {
                for (int j = 0; j < t; j++)
                {
                    newChild.children[j] = fullChild.children[j + t];
                }
            }

            fullChild.numKeys = t - 1;  // Actualizar el número de claves del nodo lleno

            // Mover los hijos del nodo padre para hacer espacio para el nuevo nodo
            for (int j = parent.numKeys; j >= i + 1; j--)
            {
                parent.children[j + 1] = parent.children[j];
            }

            // Colocar el nuevo nodo hijo en el padre
            parent.children[i + 1] = newChild;

            // Mover las claves del padre para hacer espacio para la clave del medio
            for (int j = parent.numKeys - 1; j >= i; j--)
            {
                parent.keys[j + 1] = parent.keys[j];
            }

            // Insertar la clave del medio del nodo lleno en el padre
            parent.keys[i] = fullChild.keys[t - 1];
            parent.numKeys++;
        }

        // Función auxiliar para insertar una clave en un nodo que no está lleno
        private void InsertNonFull(BTreeNode<T> node, T key)
        {
            int i = node.numKeys - 1;

            // Si es una hoja, insertamos la clave directamente
            if (node.isLeaf)
            {
                // Mover las claves mayores hacia adelante
                while (i >= 0 && key.CompareTo(node.keys[i]) < 0)
                {
                    node.keys[i + 1] = node.keys[i];
                    i--;
                }

                node.keys[i + 1] = key;  // Insertar la nueva clave
                node.numKeys++;  // Incrementar el número de claves
            }
            else
            {
                // Si no es una hoja, encontrar el hijo adecuado para descender
                while (i >= 0 && key.CompareTo(node.keys[i]) < 0)
                {
                    i--;
                }
                i++;

                // Si el hijo donde debemos insertar está lleno, lo dividimos
                if (node.children[i].numKeys == 2 * t - 1)
                {
                    SplitChild(node, i, node.children[i]);

                    // Después de la división, la clave del medio sube y el hijo actual puede haber cambiado
                    if (key.CompareTo(node.keys[i]) > 0)
                    {
                        i++;
                    }
                }

                InsertNonFull(node.children[i], key);
            }
        }


    }
}
