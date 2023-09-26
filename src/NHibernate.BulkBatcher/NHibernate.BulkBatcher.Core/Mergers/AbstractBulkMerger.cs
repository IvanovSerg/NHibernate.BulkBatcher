using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using NHibernate.BulkBatcher.Core.Internal;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Базовый класс для имплементации <see cref="IBulkMerger"/>
    /// </summary>
    public abstract partial class AbstractBulkMerger<TConnection, TTransaction> : IBulkMerger
        where TConnection : IDbConnection
        where TTransaction : IDbTransaction
    {
        /// <summary>
        /// Должен избегать ошибок конкуренции
        /// </summary>
        public virtual bool AvoidConcurrencyErrors { get; set; }

        /// <summary>
        /// Комперер для имен таблиц
        /// </summary>
        private readonly ArrayEqualityComparer<string> mInvariantComparer = new ArrayEqualityComparer<string>(StringComparer.InvariantCultureIgnoreCase);

        /// <summary>
        /// Кэши схем
        /// </summary>
        private readonly IDictionary<string[], DataTable> mSchemaCache;

        protected AbstractBulkMerger()
        {
            mSchemaCache = new Dictionary<string[], DataTable>(mInvariantComparer);
        }

        public int Merge(IEnumerable<EntityInfo> entities, TConnection connection, TTransaction transaction,
            Action<IDbCommand> logAction = null)
        {
            var count = 0;

            //Разделяем сущности по типам и производим обработку потипно
            foreach (var entityType in entities.GroupBy(x => x.TablePath,
                         new ArrayEqualityComparer<string>(StringComparer.InvariantCultureIgnoreCase)))
            {
                //Получаем схему таблицы
                var schema = GetSchemaTable(entityType.Key, connection, transaction, logAction);

                //Создание временной таблицы
                var tempTablePath = CreateTempTable(schema, connection, transaction, logAction);

                //Копирование данных во временную таблицу
                var expectedCount = CopyData(tempTablePath, entityType, schema, connection, transaction, logAction);

                //Мердж временной таблицы с реальной таблицей
                count += MergeTables(tempTablePath, entityType.Key, schema, expectedCount, connection, transaction, logAction);

                //Дроп временной таблицы
                DropTempTable(tempTablePath, connection, transaction, logAction);
            }

            return count;
        }

        /// <summary>
        /// Получает схему таблицы
        /// </summary>
        protected virtual DataTable GetSchemaTable(string[] tablePath, TConnection connection, TTransaction transaction, Action<IDbCommand> logAction)
        {
            if (mSchemaCache.TryGetValue(tablePath, out var schema))
                return schema;

            schema = GetSchemaTableFromDatabase(tablePath, connection, transaction, logAction);
            mSchemaCache[tablePath] = schema;
            return schema;
        }

        /// <summary>
        /// Получает схему таблицы из БД
        /// </summary>
        protected abstract DataTable GetSchemaTableFromDatabase(string[] tablePath, TConnection connection, TTransaction transaction, Action<IDbCommand> logAction);

        /// <summary>
        /// Создает временную таблицу и возвращает к ней путь
        /// </summary>
        protected abstract string[] CreateTempTable(DataTable schema, TConnection connection, TTransaction transaction, Action<IDbCommand> logAction);

        /// <summary>
        /// Производит копирование данных во временную таблицу
        /// </summary>
        protected abstract int CopyData(string[] tempTablePath, IEnumerable<EntityInfo> entities, DataTable schema,
            TConnection connection, TTransaction transaction, Action<IDbCommand> logAction);

        /// <summary>
        /// Производит мердж из временной таблицы в реальную таблицу
        /// </summary>
        protected abstract int MergeTables(string[] tempTablePath, string[] tablePath, DataTable schema,
            int expectedCount, TConnection connection, TTransaction transaction, Action<IDbCommand> logAction);

        /// <summary>
        /// Дропает временную таблицу
        /// </summary>
        protected abstract void DropTempTable(string[] tempTablePath, TConnection connection, TTransaction transaction, Action<IDbCommand> logAction);

        /// <inheritdoc />
        int IBulkMerger.Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction, bool isGeometryPresent, Action<IDbCommand> logAction = null)
        {
            var typedConnection = connection is TConnection cn ? cn : default(TConnection);
            var typedTransaction = transaction is TTransaction tr ? tr : default(TTransaction);
            return Merge(entities, typedConnection, typedTransaction, logAction);
        }
    }
}