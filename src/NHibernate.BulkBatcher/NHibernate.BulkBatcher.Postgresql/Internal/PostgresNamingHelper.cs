using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace NHibernate.BulkBatcher.PostgreSql.Internal
{
    /// <summary>
    /// Хелпер по наименованиям в PostgreSql
    /// </summary>
    public static class PostgreNamingHelper
    {
        /// <summary>
        /// Возвращает безопасное для использования строковое значение
        /// </summary>
        public static string EscapeString(string str)
        {
            return string.IsNullOrEmpty(str) ? string.Empty : $"{str.Replace("'", "''")}";
        }

        /// <summary>
        /// Возвращает безопасный для использования путь к объекту для использования в PostgreSql
        /// </summary>
        public static string Escape(params string[] path)
        {
            return string.Join(".", path.Select(EscapeName));
        }

        /// <summary>
        /// Возвращает безопасное имя объекта для использования в PostgreSql
        /// </summary>
        private static string EscapeName(string name)
        {
            return string.IsNullOrEmpty(name) ? string.Empty : $"\"{name.Replace("\"", "\"\"")}\"";
        }
    }
}
