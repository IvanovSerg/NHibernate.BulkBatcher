using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using NHibernate.SqlCommand.Parser;

namespace NHibernate.BulkBatcher.Core.EntityInfoExtractors
{
    /// <summary>
    /// Хелпер для экстракторов
    /// </summary>
    internal static class ExtractorHelper
    {
        /// <summary>
        /// Убирает кавычки из значения
        /// </summary>
        public static string UnquoteValue(this SqlToken token)
        {
            switch (token.TokenType)
            {
                case SqlTokenType.DelimitedText when token.Value.StartsWith("\""):
                    return token.Value.Substring(1, token.Value.Length - 2).Replace("\"\"", "\"");
                case SqlTokenType.DelimitedText when token.Value.StartsWith("'"):
                    return token.Value.Substring(1, token.Value.Length - 2).Replace("''", "'");
                case SqlTokenType.DelimitedText when token.Value.StartsWith("["):
                    return token.Value.Substring(1, token.Value.Length - 2).Replace("]]", "]");
                case SqlTokenType.Text:
                    return token.Value;
                default:
                    return null;
            }
        }
        /// <summary>
        /// Вытаскивает значения параметров из команды и формирует словарь столбцов и значений
        /// </summary>
        public static IDictionary<string, object> CreateValues(string[] columns, string[] parameters, DbCommand command)
        {
            var result = new Dictionary<string, object>();
            for (var i = 0; i < columns.Length; i++)
            {
                var column = columns[i];
                var parameter = parameters[i];
                var value = command.Parameters[parameter].Value;
                result.Add(column, value);
            }

            return result;
        }
    }
}
