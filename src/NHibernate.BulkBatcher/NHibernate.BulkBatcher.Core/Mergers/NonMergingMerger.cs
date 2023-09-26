using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Мерджер, который просто выполняет команды над сервером
    /// </summary>
    public class NonMergingMerger : IBulkMerger
    {
        /// <inheritdoc />
        public int Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction, bool isGeometryPresent, Action<IDbCommand> logAction = null)
        {
            var count = 0;
            foreach (var entity in entities)
            {
                var commandInfo = entity.CommandInfo;
                if (commandInfo == null)
                    continue;

                var command = CreateCommand(driver, connection, transaction, commandInfo);

                logAction?.Invoke(command);
                count += command.ExecuteNonQuery();
            }

            return count;
        }

        /// <inheritdoc />
        public async Task<int> MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var count = 0;
            foreach (var entity in entities)
            {
                var commandInfo = entity.CommandInfo;
                if (commandInfo == null)
                    continue;

                var command = CreateCommand(driver, connection, transaction, commandInfo);

                logAction?.Invoke(command);
                count += await command.ExecuteNonQueryAsync(cancellationToken);
            }

            return count;
        }

        /// <summary>
        /// Создает команду для исполнения
        /// </summary>
        private DbCommand CreateCommand(IDriver driver, IDbConnection connection, IDbTransaction transaction, CommandInfo commandInfo)
        {
            var command = driver.GenerateCommand(commandInfo.Type, commandInfo.Sql, commandInfo.ParameterTypes);
            for (var i = 0; i < commandInfo.Parameters.Count; i++)
            {
                var parameter = commandInfo.Parameters[i];
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