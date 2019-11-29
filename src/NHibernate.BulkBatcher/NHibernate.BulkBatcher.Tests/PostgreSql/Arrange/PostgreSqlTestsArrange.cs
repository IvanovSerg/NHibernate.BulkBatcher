using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using Microsoft.VisualStudio.TestPlatform.TestHost;
using NHibernate.BulkBatcher.PostgreSql.Batchers;
using NHibernate.BulkBatcher.Tests.Model;
using NHibernate.Cfg;
using NHibernate.Dialect;
using NHibernate.Driver;
using NHibernate.Tool.hbm2ddl;

namespace NHibernate.BulkBatcher.Tests.PostgreSql.Arrange
{
    public class PostgreSqlTestsArrange
    {
        public PostgreSqlTestsArrange()
        {
            //Конфигурирование модели
            var configuration = GetConfiguration();

            //Создание таблиц в бд
            var schemaUpdate = new SchemaExport(configuration);
            schemaUpdate.Execute(true, true, false);

            SessionFactory = configuration.BuildSessionFactory();

            var upsertConfiguration = GetUpsertConfiguration();
            UpsertSessionFactory = upsertConfiguration.BuildSessionFactory();
        }

        public ISessionFactory SessionFactory { get; }

        public ISessionFactory UpsertSessionFactory { get; }

        public ICollection<TestEntity> GenerateTestData(int count)
        {
            var result = new List<TestEntity>();
            for (var i = 0; i < count; i++)
            {
                var testRelatedEntity = new TestRelatedEntity()
                {
                    Id = Guid.NewGuid(),
                    Value = Guid.NewGuid().ToString("N"),
                };
                var testChildEntity = new TestChildEntity()
                {
                    Value = Guid.NewGuid().ToString("N"),
                    RelatedEntity = testRelatedEntity,
                };
                var testEntity = new TestEntity()
                {
                    Id = Guid.NewGuid(),
                    Value = Guid.NewGuid().ToString("N"),
                    ChildEntities = new List<TestChildEntity> { testChildEntity},
                    RelatedEntities = new List<TestRelatedEntity> {testRelatedEntity}
                };
                result.Add(testEntity);
            }

            return result;
        }

        private Configuration GetConfiguration()
        {
            var cfg = new Configuration();
            cfg.DataBaseIntegration(x =>
            {
                x.ConnectionString = "Host=localhost;Port=5432;Database=Test;Username=postgres;Password=postgres";
                x.Driver<NpgsqlDriver>();
                x.Dialect<PostgreSQL83Dialect>();
                x.BatchSize = 10000;
                x.Batcher<PostgreSqlBulkBatcherFactory>();
                x.LogSqlInConsole = true;
            });
            cfg.AddInputStream(GetStream("NHibernate.BulkBatcher.Tests.Assets.nhibernate-mapping.xml"));
            return cfg;
        }

        private Configuration GetUpsertConfiguration()
        {
            var cfg = new Configuration();
            cfg.DataBaseIntegration(x =>
            {
                x.ConnectionString = "Host=localhost;Port=5432;Database=Test;Username=postgres;Password=postgres";
                x.Driver<NpgsqlDriver>();
                x.Dialect<PostgreSQL83Dialect>();
                x.BatchSize = 10000;
                x.Batcher<UpsertBatcherFactory>();
                x.LogSqlInConsole = true;
            });
            cfg.AddInputStream(GetStream("NHibernate.BulkBatcher.Tests.Assets.nhibernate-mapping.xml"));
            return cfg;
        }

        private Stream GetStream(string resourcePath)
        {
            var assembly = Assembly.GetAssembly(typeof(TestEntity));
            var stream = assembly.GetManifestResourceStream(resourcePath);
            return stream;
        }

    }
}
