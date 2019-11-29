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
    /// Экстрактор для команды Update
    /// </summary>
    public class EntityInfoUpdateExtractor : IEntityInfoExtractor
    {
        /// <inheritdoc />
        public EntityInfo Extract(DbCommand command, SqlString sqlString)
        {
            if (!sqlString.StartsWithCaseInsensitive("UPDATE"))
                return null;

            var parser = new SqlTokenizer(new SqlString(command.CommandText));

            using (var enumerator = parser.GetEnumerator())
            {
                ReadToTablePath(enumerator);
                var tablePath = ReadTablePath(enumerator);
                if (tablePath == null)
                    return null;
                var set = ReadSet(enumerator);
                if (set == null)
                    return null;
                var where = ReadWhere(enumerator);
                if (where == null)
                    return null;
                
                return new EntityInfo()
                {
                    State = EntityState.Modified,
                    TablePath = tablePath,
                    Values = ExtractorHelper.CreateValues(set.Item1.Concat(where.Item1).ToArray(), set.Item2.Concat(where.Item2).ToArray(), command)
                };
            }
        }

        /// <summary>
        /// Читаем до места где начинается путь к таблице
        /// </summary>
        private void ReadToTablePath(IEnumerator<SqlToken> enumerator)
        {
            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.Text) != 0 && token.Value == "UPDATE")
                    break;
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

                if ((token.TokenType & SqlTokenType.Text) != 0 && token.Value == "SET")
                    break;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value != ".":
                    case SqlTokenType.DelimitedText:
                        tablePathTokens.Add(token);
                        break;
                }
            }

            return tablePathTokens.Any() ? tablePathTokens.Select(x => x.UnquoteValue()).ToArray() : null;
        }

        /// <summary>
        /// Читаем названия столбцов и параметров из SET
        /// </summary>
        private Tuple<string[],string[]> ReadSet(IEnumerator<SqlToken> enumerator)
        {
            var columns = new List<SqlToken>();
            var parameters = new List<SqlToken>();

            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;

                if ((token.TokenType & SqlTokenType.Text) != 0 && token.Value == "WHERE")
                    break;

                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value == "=":
                    case SqlTokenType.Text when token.Value == ",":
                        break;
                    case SqlTokenType.Text when token.Value.StartsWith(":"):
                        parameters.Add(token);
                        break;
                    case SqlTokenType.Text:
                    case SqlTokenType.DelimitedText:
                        columns.Add(token);
                        break;
                }
            }

            if (!columns.Any() || !parameters.Any() || columns.Count != parameters.Count)
                return null;

            return new Tuple<string[], string[]>(columns.Select(x => x.UnquoteValue()).ToArray(), parameters.Select(x => x.UnquoteValue()).ToArray());
        }

        /// <summary>
        /// Читаем названия столбцов и параметров из WHERE
        /// </summary>
        private Tuple<string[], string[]> ReadWhere(IEnumerator<SqlToken> enumerator)
        {
            var columns = new List<SqlToken>();
            var parameters = new List<SqlToken>();

            while (enumerator.MoveNext())
            {
                var token = enumerator.Current;
                if (token == null)
                    continue;
                
                switch (token.TokenType)
                {
                    case SqlTokenType.Text when token.Value == "AND":
                    case SqlTokenType.Text when token.Value == "=":
                        break;
                    case SqlTokenType.Text when token.Value.StartsWith(":"):
                        parameters.Add(token);
                        break;
                    case SqlTokenType.Text:
                    case SqlTokenType.DelimitedText:
                        columns.Add(token);
                        break;
                }
            }

            if (!columns.Any() || !parameters.Any() || columns.Count != parameters.Count)
                return null;

            return new Tuple<string[], string[]>(columns.Select(x => x.UnquoteValue()).ToArray(), parameters.Select(x => x.UnquoteValue()).ToArray());
        }
    }
}
