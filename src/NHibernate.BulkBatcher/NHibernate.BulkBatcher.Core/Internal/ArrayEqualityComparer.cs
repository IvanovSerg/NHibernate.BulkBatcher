using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Core.Internal
{
    /// <summary>
    /// Поэлементный сравниватель массивов объектов
    /// </summary>
    internal sealed class ArrayEqualityComparer<T> : IEqualityComparer<T[]>
    {
        /// <summary>
        /// Сравниватель по умолчанию
        /// </summary>
        public static IEqualityComparer<T[]> Default { get; } = new ArrayEqualityComparer<T>();

        private readonly IEqualityComparer<T> mElementComparer;

        public ArrayEqualityComparer() : this(EqualityComparer<T>.Default)
        {
        }

        public ArrayEqualityComparer(IEqualityComparer<T> elementComparer)
        {
            mElementComparer = elementComparer;
        }

        /// <summary>
        /// Сравнивает два массива
        /// </summary>
        public bool Equals(T[] x, T[] y)
        {
            if (x == y)
                return true;
            if (x == null || y == null)
                return false;
            if (x.Length != y.Length)
                return false;
            for (int i = 0; i < x.Length; i++)
            {
                if (!mElementComparer.Equals(x[i], y[i]))
                {
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// Формирует хэш для массива
        /// </summary>
        public int GetHashCode(T[] array)
        {
            if (array == null)
            {
                return 0;
            }
            var hash = 23;
            foreach (T item in array)
            {
                hash = hash * 31 + mElementComparer.GetHashCode(item);
            }
            return hash;
        }
    }
}
