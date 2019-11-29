using System;
using System.Collections.Generic;
using System.Text;
using NHibernate.AdoNet;
using NHibernate.BulkBatcher.Core.Batchers;
using NHibernate.BulkBatcher.Core.EntityInfoExtractors;
using NHibernate.BulkBatcher.Core.Mergers;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.BulkBatcher.PostgreSql.Mergers;
using NHibernate.Engine;

namespace NHibernate.BulkBatcher.PostgreSql.Batchers
{
    /// <summary>
    /// Батчер использующий Bulk операции для PostgreSql
    /// </summary>
    public class PostgreSqlBulkBatcherFactory : BulkOperationsBatcherFactory
    {
        /// <inheritdoc />
        protected override SmartMergerConfiguration CreateBulkMergerConfiguration()
        {
            return new SmartMergerConfiguration(new PostgreSqlBulkMerger(), MinimumBulkMergeEntitiesCount);
        }
    }
}
