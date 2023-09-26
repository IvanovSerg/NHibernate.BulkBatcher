using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Internal;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Базовый класс для имплементации <see cref="IBulkMerger"/> (асинхронные методы)
    /// </summary>
    public abstract partial class AbstractBulkMerger<TConnection, TTransaction>
    {
        public async Task<int> MergeAsync(IEnumerable<EntityInfo> entities, TConnection connection, TTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var count = 0;
            //Разделяем сущности по типам и производим обработку потипно
            foreach (var entityType in entities.GroupBy(x => x.TablePath,
                         new ArrayEqualityComparer<string>(StringComparer.InvariantCultureIgnoreCase)))
            {
                //Получаем схему таблицы
                var schema = await GetSchemaTableAsync(entityType.Key, connection, transaction, cancellationToken, logAction);

                //Создание временной таблицы
                var tempTablePath = await CreateTempTableAsync(schema, connection, transaction, cancellationToken, logAction);

                //Копирование данных во временную таблицу
                var expectedCount = await CopyDataAsync(tempTablePath, entityType, schema, connection, transaction, cancellationToken, logAction);

                //Мердж временной таблицы с реальной таблицей
                count += await MergeTablesAsync(tempTablePath, entityType.Key, schema, expectedCount, connection, transaction, cancellationToken, logAction);

                //Дроп временной таблицы
                await DropTempTableAsync(tempTablePath, connection, transaction, cancellationToken, logAction);
            }

            return count;
        }

        /// <summary>
        /// Получает схему таблицы
        /// </summary>
        protected virtual async Task<DataTable> GetSchemaTableAsync(string[] tablePath, TConnection connection, TTransaction transaction, CancellationToken cancellationToken,
            Action<IDbCommand> logAction)
        {
            if (mSchemaCache.TryGetValue(tablePath, out var schema))
                return schema;

            schema = await GetSchemaTableFromDatabaseAsync(tablePath, connection, transaction, cancellationToken, logAction);
            mSchemaCache[tablePath] = schema;
            return schema;
        }

        /// <summary>
        /// Получает схему таблицы из БД
        /// </summary>
        protected abstract Task<DataTable> GetSchemaTableFromDatabaseAsync(string[] tablePath, TConnection connection, TTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction);

        /// <summary>
        /// Создает временную таблицу и возвращает к ней путь
        /// </summary>
        protected abstract Task<string[]> CreateTempTableAsync(DataTable schema, TConnection connection, TTransaction transaction, CancellationToken cancellationToken,
            Action<IDbCommand> logAction);

        /// <summary>
        /// Производит копирование данных во временную таблицу
        /// </summary>
        protected abstract Task<int> CopyDataAsync(string[] tempTablePath, IEnumerable<EntityInfo> entities,
            DataTable schema, TConnection connection, TTransaction transaction, CancellationToken cancellationToken,
            Action<IDbCommand> logAction);

        /// <summary>
        /// Производит мердж из временной таблицы в реальную таблицу
        /// </summary>
        protected abstract Task<int> MergeTablesAsync(string[] tempTablePath, string[] tablePath, DataTable schema,
            int expectedCount, TConnection connection, TTransaction transaction, CancellationToken cancellationToken,
            Action<IDbCommand> logAction);

        /// <summary>
        /// Дропает временную таблицу
        /// </summary>
        protected abstract Task DropTempTableAsync(string[] tempTablePath, TConnection connection, TTransaction transaction, CancellationToken cancellationToken,
            Action<IDbCommand> logAction);

        /// <inheritdoc />
        Task<int> IBulkMerger.MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var typedConnection = connection is TConnection cn ? cn : default(TConnection);
            var typedTransaction = transaction is TTransaction tr ? tr : default(TTransaction);
            return MergeAsync(entities, typedConnection, typedTransaction, cancellationToken, logAction);
        }
    }
}