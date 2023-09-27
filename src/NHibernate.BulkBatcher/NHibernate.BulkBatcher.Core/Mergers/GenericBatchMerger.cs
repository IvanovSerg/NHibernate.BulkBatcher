using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Общий мерджер для пакетной обработки сущностей
    /// </summary>
    public class GenericBatchMerger : IBulkMerger
    {
        /// <summary>
        /// Размер пакета
        /// </summary>
        protected int BatchSize { get; }

        /// <summary>
        /// Разделитель для команд
        /// </summary>
        protected string CommandDelimiter { get; }

        public GenericBatchMerger(int batchSize, string commandDelimiter = "; ")
        {
            CommandDelimiter = commandDelimiter;
            BatchSize = batchSize;
        }

        /// <inheritdoc />
        public int Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction, bool isGeometryPresent, Action<IDbCommand> logAction = null)
        {
            var count = 0;
            foreach (var page in Paged(entities, BatchSize))
            {
                using (var command = CreateCommand(page, driver, connection, transaction))
                {
                    logAction?.Invoke(command);
                    count += command.ExecuteNonQuery();
                }
            }

            return count;
        }

        /// <inheritdoc />
        public async Task<int> MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction,
            bool isGeometryPresent,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var count = 0;
            foreach (var page in Paged(entities, BatchSize))
            {
                var command = CreateCommand(page, driver, connection, transaction);
                if (command == null)
                    continue;

                using (command)
                {
                    logAction?.Invoke(command);
                    count += await command.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            return count;
        }

        /// <summary>
        /// Формирует страницы из последовательности
        /// </summary>
        private IEnumerable<IList<T>> Paged<T>(IEnumerable<T> entities, int batchSize)
        {
            var page = new List<T>();
            foreach (var entity in entities)
            {
                page.Add(entity);

                if (page.Count >= batchSize)
                {
                    yield return page;
                    page = new List<T>();
                }
            }

            if (page.Any())
                yield return page;
        }

        /// <summary>
        /// Создает команду
        /// </summary>
        private DbCommand CreateCommand(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection, IDbTransaction transaction)
        {
            var commandType = CommandType.Text;
            var sql = new SqlStringBuilder();
            var sqlTypes = new List<SqlType>();
            var parameters = new List<CommandParameterInfo>();

            var count = 0;
            foreach (var entity in entities)
            {
                var commandInfo = entity.CommandInfo;
                if (commandInfo == null)
                    continue;

                if (count > 0)
                {
                    sql.Add(CommandDelimiter);
                }
                else
                {
                    commandType = commandInfo.Type;
                }

                sql.Add(commandInfo.Sql);
                sqlTypes.AddRange(commandInfo.ParameterTypes);
                parameters.AddRange(commandInfo.Parameters);

                count++;
            }

            if (count == 0)
                return null;

            var command = driver.GenerateCommand(commandType, sql.ToSqlString(), sqlTypes.ToArray());
            for (var i = 0; i < parameters.Count; i++)
            {
                var parameter = parameters[i];
                var cmdParam = command.Parameters[i];
                cmdParam.Value = parameter.Value;
                cmdParam.Direction = parameter.Direction;
                cmdParam.Precision = parameter.Precision;
                cmdParam.Scale = parameter.Scale;
                cmdParam.Size = parameter.Size;
            }

            command.Connection = (DbConnection)connection;
            command.Transaction = transaction as DbTransaction;

            return command;
        }
    }
}