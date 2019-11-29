using System;
using System.Collections.Generic;
using System.Text;
using Xunit;

namespace NHibernate.BulkBatcher.Tests.PostgreSql.Arrange
{
    [CollectionDefinition("PostgreSqlTestsArrange")]
    public class PostgresSqlTestsArrangeCollection : ICollectionFixture<PostgreSqlTestsArrange>
    {
    }
}
