using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ApiInterface.Indexes
{
    public class TreeNode<T> where T : IComparable<T>
    {
        public T key;
        public TreeNode<T> left, right;

        public TreeNode(T item)
        {
            key = item;
            left = right = null;
        }
    }
}
