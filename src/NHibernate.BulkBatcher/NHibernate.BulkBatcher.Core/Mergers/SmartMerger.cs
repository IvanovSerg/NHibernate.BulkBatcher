using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Умный мерджер
    /// </summary>
    public class SmartMerger : IBulkMerger
    {
        private readonly IList<SmartMergerConfiguration> _mConfigurations;

        public SmartMerger(IEnumerable<SmartMergerConfiguration> configurations)
        {
            _mConfigurations = configurations.OrderByDescending(x => x.MinBatchSize).ToList();
        }

        /// <inheritdoc />
        public int Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction, bool isGeometryPresent, Action<IDbCommand> logAction = null)
        {
            var entitiesList = entities as ICollection<EntityInfo> ?? entities.ToList();

            var merger = GetMerger(isGeometryPresent
                ?
                //TODO: Пока так для фикса геометрии
                100000
                : entitiesList.Count);

            if (merger == null)
                throw new InvalidOperationException($"Can't find appropriate merger for {entitiesList.Count}");

            return merger.Merge(entitiesList, driver, connection, transaction, isGeometryPresent, logAction);
        }

        /// <inheritdoc />
        public async Task<int> MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var entitiesList = entities as ICollection<EntityInfo> ?? entities.ToList();

            var merger = GetMerger(entitiesList.Count);
            if (merger == null)
                throw new InvalidOperationException($"Can't find appropriate merger for {entitiesList.Count}");

            return await merger.MergeAsync(entitiesList, driver, connection, transaction, cancellationToken, logAction);
        }

        /// <summary>
        /// Возвращает подходящий мерджер для указанного количества элементов
        /// </summary>
        private IBulkMerger GetMerger(int count)
        {
            foreach (var cfg in _mConfigurations)
            {
                if (cfg.MinBatchSize <= count)
                    return cfg.Merger;
            }

            return null;
        }
    }
}