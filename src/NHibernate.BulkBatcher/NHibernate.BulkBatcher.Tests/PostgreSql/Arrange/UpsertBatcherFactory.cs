using System;
using System.Collections.Generic;
using System.Text;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.BulkBatcher.PostgreSql.Batchers;
using NHibernate.BulkBatcher.PostgreSql.Mergers;

namespace NHibernate.BulkBatcher.Tests.PostgreSql.Arrange
{
    public class UpsertBatcherFactory : PostgreSqlBulkBatcherFactory 
    {
        /// <inheritdoc />
        protected override SmartMergerConfiguration CreateBulkMergerConfiguration()
        {
            return new SmartMergerConfiguration(new PostgreSqlBulkMerger() { AvoidConcurrencyErrors = true }, MinimumBulkMergeEntitiesCount);
        }
    }
}
