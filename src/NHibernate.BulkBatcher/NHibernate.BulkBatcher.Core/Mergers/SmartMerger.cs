using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
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
        private readonly IList<SmartMergerConfiguration> mConfigurations;

        public SmartMerger(IEnumerable<SmartMergerConfiguration> configurations)
        {
            mConfigurations = configurations.OrderByDescending(x => x.MinBatchSize).ToList();
        }

        /// <inheritdoc />
        public int Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction, Action<IDbCommand> logAction = null)
        {
            var entitiesList = (entities is ICollection<EntityInfo> collection) ? collection : entities.ToList();

            var merger = GetMerger(entitiesList.Count);
            if (merger == null)
                throw new InvalidOperationException($"Can't find appropriate merger for {entitiesList.Count}");

            return merger.Merge(entitiesList, driver, connection, transaction, logAction);
        }

        /// <inheritdoc />
        public async Task<int> MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection,
            IDbTransaction transaction,
            CancellationToken cancellationToken, Action<IDbCommand> logAction = null)
        {
            var entitiesList = (entities is ICollection<EntityInfo> collection) ? collection : entities.ToList();

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
            foreach (var cfg in mConfigurations)
            {
                if (cfg.MinBatchSize <= count)
                    return cfg.Merger;
            }

            return null;
        }

    }
}
