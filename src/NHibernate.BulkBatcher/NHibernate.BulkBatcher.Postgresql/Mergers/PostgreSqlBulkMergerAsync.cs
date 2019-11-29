using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Model;
using Npgsql;

namespace NHibernate.BulkBatcher.PostgreSql.Mergers
{
    public partial class PostgreSqlBulkMerger
    {
        /// <inheritdoc />
        protected override async Task<DataTable> GetSchemaTableFromDatabaseAsync(string[] tablePath, NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken cancellationToken, Action<IDbCommand> logAction)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                //Получение основной информации по схеме
                cmd.CommandText = GetSchemaTableSql(tablePath);
                logAction?.Invoke(cmd);
                DataTable schemaTable;
                using (var reader = await cmd.ExecuteReaderAsync(CommandBehavior.KeyInfo | CommandBehavior.SchemaOnly, cancellationToken))
                {
                    schemaTable = reader.GetSchemaTable();
                }
                //Проверка наличия первичного ключа в таблице
                AssertHasPrimaryKey(schemaTable, tablePath);

                //Получение информации о индексе
                cmd.CommandText = GetTableIndexInfoSql(tablePath);
                logAction?.Invoke(cmd);

                short[] pkOrders = null;
                short[] pkOptions = null;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    if (reader.Read())
                    {
                        pkOrders = reader.GetFieldValue<short[]>(0);
                        pkOptions = reader.GetFieldValue<short[]>(1);
                    }
                }
                AppendIndexOptions(schemaTable, pkOrders, pkOptions);

                return schemaTable;
            }
        }

        /// <inheritdoc />
        protected override async Task<string[]> CreateTempTableAsync(DataTable schema, NpgsqlConnection connection, NpgsqlTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction)
        {
            var tempTableName = $"tmp{Guid.NewGuid():N}";

            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;
                cmd.CommandText = GetCreateTempTableSql(schema, tempTableName);
                logAction?.Invoke(cmd);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }

            return new[] { tempTableName };
        }

        /// <inheritdoc />
        protected override async Task<int> CopyDataAsync(string[] tempTablePath, IEnumerable<EntityInfo> entities,
            DataTable schema, NpgsqlConnection connection, NpgsqlTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction)
        {
            return await Task.Run(() => CopyData(tempTablePath, entities, schema, connection, transaction, logAction),
                cancellationToken);
        }

        /// <inheritdoc />
        protected override async Task<int> MergeTablesAsync(string[] tempTablePath, string[] tablePath, DataTable schema,
            int expectedCount, NpgsqlConnection connection,
            NpgsqlTransaction transaction, CancellationToken cancellationToken, Action<IDbCommand> logAction)
        {
            var map = CreateMap(schema);

            var count = 0;
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                if (AvoidConcurrencyErrors)
                {
                    if (map.Any(x => !x.IsKey))
                    {
                        cmd.CommandText = GetTryUpdateSql(tempTablePath, tablePath, map);
                        logAction?.Invoke(cmd);
                        count += await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }

                    cmd.CommandText = GetTryInsertSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += await cmd.ExecuteNonQueryAsync(cancellationToken);

                    cmd.CommandText = GetDeleteSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
                else
                {
                    cmd.CommandText = GetInsertSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += await cmd.ExecuteNonQueryAsync(cancellationToken);

                    if (map.Any(x => !x.IsKey))
                    {
                        cmd.CommandText = GetUpdateSql(tempTablePath, tablePath, map);
                        logAction?.Invoke(cmd);
                        count += await cmd.ExecuteNonQueryAsync(cancellationToken);
                    }

                    cmd.CommandText = GetDeleteSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += await cmd.ExecuteNonQueryAsync(cancellationToken);
                }
            }

            return AvoidConcurrencyErrors ? expectedCount : count;
        }

        /// <inheritdoc />
        protected override async Task DropTempTableAsync(string[] tempTablePath, NpgsqlConnection connection, NpgsqlTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                cmd.CommandText = GetDropTableSql(tempTablePath);
                logAction?.Invoke(cmd);
                await cmd.ExecuteNonQueryAsync(cancellationToken);
            }
        }
    }
}
