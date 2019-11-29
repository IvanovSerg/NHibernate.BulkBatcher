using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Text;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.SqlCommand;
using NHibernate.SqlCommand.Parser;

namespace NHibernate.BulkBatcher.Core.EntityInfoExtractors
{
    /// <summary>
    /// Экстрактор для команды Insert
    /// </summary>
    public class EntityInfoInsertExtractor : IEntityInfoExtractor
    {
        /// <inheritdoc />
        public EntityInfo Extract(DbCommand command, SqlString sqlString)
        {
            if (!sqlString.StartsWithCaseInsensitive("INSERT INTO"))
                return null;
            
            var parser = new SqlTokenizer(new SqlString(command.CommandText));
            
            using (var enumerator = parser.GetEnumerator())
            {
                ReadToTablePath(enumerator);
                var tablePath = ReadTablePath(enumerator);
                if (tablePath == null)
                    return null;
                var columns = ReadColumns(enumerator);
                if (columns == null)
                    return null;
                ReadToValues(enumerator);
                var parameters = ReadParameters(enumerator);
                if (parameters == null)
                    return null;

                if (columns.Length != parameters.Length)
                    return null;

                return new EntityInfo()
                {
                    State = EntityState.Added,
                    TablePath = tablePath,
                    Values = ExtractorHelper.CreateValues(columns, parameters, command)
                };
            }
        }


        /// <summary>
        /// Читаем до места где начинается путь к таблице
        /// </summary>
        private void ReadToTablePath(IEnumerator<SqlToken> enumerator)
        {
            var insert = false;
            var into = false;
            while (!(insert && into) && enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value == "INSERT":
                        insert = true;
                        break;
                    case SqlTokenType.Text when token.Value == "INTO":
                        into = true;
                        break;
                }
            };
        }

        /// <summary>
        /// Читаем путь таблицы
        /// </summary>
        private string[] ReadTablePath(IEnumerator<SqlToken> enumerator)
        {
            var tablePathTokens = new List<SqlToken>();
            
            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.BracketOpen) != 0)
                    break;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value != ".":
                    case SqlTokenType.DelimitedText:
                        tablePathTokens.Add(token);
                        break;
                }
            }

            return tablePathTokens.Any() ? tablePathTokens.Select(x=>x.UnquoteValue()).ToArray() : null;
        }

        /// <summary>
        /// Читаем названия столбцов
        /// </summary>
        private string[] ReadColumns(IEnumerator<SqlToken> enumerator)
        {
            var columns = new List<SqlToken>();

            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.BracketClose) != 0)
                    break;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text:
                    case SqlTokenType.DelimitedText:
                        columns.Add(token);
                        break;
                }
            }

            return columns.Any() ? columns.Select(x => x.UnquoteValue()).ToArray() : null;
        }

        /// <summary>
        /// Читаем до параметров
        /// </summary>
        private void ReadToValues(IEnumerator<SqlToken> enumerator)
        {
            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.BracketOpen) != 0)
                    break;

            }
        }

        /// <summary>
        /// Читаем названия параметров
        /// </summary>
        private string[] ReadParameters(IEnumerator<SqlToken> enumerator)
        {
            var parameters = new List<string>();

            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.BracketClose) != 0)
                    break;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value.StartsWith(":"):
                        parameters.Add(token.Value);
                        break;
                }
            }

            return parameters.Any() ? parameters.ToArray() : null;
        }
    }
}
