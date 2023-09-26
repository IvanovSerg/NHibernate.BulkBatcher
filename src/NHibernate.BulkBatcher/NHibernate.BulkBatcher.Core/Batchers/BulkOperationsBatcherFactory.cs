using System.Collections.Generic;
using NHibernate.AdoNet;
using NHibernate.BulkBatcher.Core.EntityInfoExtractors;
using NHibernate.BulkBatcher.Core.Mergers;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Engine;

namespace NHibernate.BulkBatcher.Core.Batchers
{
    /// <summary>
    /// Фабрика для <see cref="BulkOperationsBatcher"/>
    /// </summary>
    public class BulkOperationsBatcherFactory : IBatcherFactory
    {
        /// <summary>
        /// Размер пакета для Batch Merge
        /// </summary>
        public int BatchMergeBatchSize { get; set; } = 50;

        /// <summary>
        /// Минимальное количество сущностей для которых применять Batch Merge
        /// </summary>
        public int MinimumBatchMergeEntitiesCount { get; set; } = 2;

        /// <summary>
        /// Минимальное количество сущностей для которых применять Bulk Merge
        /// </summary>
        public int MinimumBulkMergeEntitiesCount { get; set; } = 1000;

        /// <inheritdoc />
        public IBatcher CreateBatcher(ConnectionManager connectionManager, IInterceptor interceptor)
        {
            return new BulkOperationsBatcher(connectionManager, interceptor, CreateExtractors(), CreateMerger());
        }

        /// <summary>
        /// Создает экстракторы
        /// </summary>
        protected virtual ICollection<IEntityInfoExtractor> CreateExtractors()
        {
            return new List<IEntityInfoExtractor>()
            {
                new EntityInfoInsertExtractor(),
                new EntityInfoUpdateExtractor(),
                new EntityInfoDeleteExtractor()
            };
        }

        /// <summary>
        /// Создает мерджер
        /// </summary>
        protected virtual IBulkMerger CreateMerger()
        {
            return new SmartMerger(CreateSmartMergerConfiguration());
        }

        /// <summary>
        /// Создает конфигурацию для смарт мерджера
        /// </summary>
        protected virtual IList<SmartMergerConfiguration> CreateSmartMergerConfiguration()
        {
            var config = new List<SmartMergerConfiguration>();

            var simpleMergerConfig = CreateSimpleMergerConfiguration();
            if (simpleMergerConfig != null)
                config.Add(simpleMergerConfig);

            var batchMergerConfig = CreateBatchMergerConfiguration();
            if (batchMergerConfig != null)
                config.Add(batchMergerConfig);

            var bulkMergerConfig = CreateBulkMergerConfiguration();
            if (bulkMergerConfig != null)
                config.Add(bulkMergerConfig);

            return config;
        }

        /// <summary>
        /// Создает конфигурацию для смарт мерджера для простого выполнения команд
        /// </summary>
        protected virtual SmartMergerConfiguration CreateSimpleMergerConfiguration()
        {
            return new SmartMergerConfiguration(new NonMergingMerger(), 0);
        }

        /// <summary>
        /// Создает конфигурацию для смарт мерджера для пакетного выполнения команд
        /// </summary>
        protected virtual SmartMergerConfiguration CreateBatchMergerConfiguration()
        {
            return new SmartMergerConfiguration(new GenericBatchMerger(BatchMergeBatchSize), MinimumBatchMergeEntitiesCount);
        }

        /// <summary>
        /// Создает конфигурацию для смарт мерджера для Bulk Merge
        /// </summary>
        protected virtual SmartMergerConfiguration CreateBulkMergerConfiguration()
        {
            return null;
        }
    }
}