using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.Driver;

namespace NHibernate.BulkBatcher.Core.Mergers
{
    /// <summary>
    /// Интерфейс мерджера, который производит запись сущностей в БД используя Bulk операции
    /// </summary>
    public interface IBulkMerger
    {
        /// <summary>
        /// Производит запись сущностей в БД используя Bulk операции
        /// </summary>
        int Merge(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection, IDbTransaction transaction, Action<IDbCommand> logAction = null);

        /// <summary>
        /// Производит запись сущностей в БД используя Bulk операции
        /// </summary>
        Task<int> MergeAsync(IEnumerable<EntityInfo> entities, IDriver driver, IDbConnection connection, IDbTransaction transaction, CancellationToken cancellationToken, Action<IDbCommand> logAction = null);
    }
}
