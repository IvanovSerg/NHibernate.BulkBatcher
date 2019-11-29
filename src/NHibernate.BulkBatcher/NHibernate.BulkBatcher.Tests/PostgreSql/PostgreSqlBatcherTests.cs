using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Tests.PostgreSql.Arrange;
using NHibernate.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace NHibernate.BulkBatcher.Tests.PostgreSql
{
    [Collection("PostgreSqlTestsArrange")]
    public class PostgreSqlBatcherTests
    {
        private readonly PostgreSqlTestsArrange mArrange;
        private readonly ITestOutputHelper mHelper;

        public PostgreSqlBatcherTests(PostgreSqlTestsArrange arrange, ITestOutputHelper helper)
        {
            mArrange = arrange;
            mHelper = helper;
        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public void DmlTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            var testDeleteData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                //act
                foreach (var entity in testDeleteData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testDeleteData)
                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Удалено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }
        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task DmlAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            var testDeleteData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                //act
                foreach (var entity in testDeleteData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testDeleteData)
                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Удалено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public void ConflictInsertTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                Assert.Throws<GenericADOException>(()=> session.Flush());
            }
        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task ConflictInsertAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                await Assert.ThrowsAsync<GenericADOException>(() => session.FlushAsync());
            }

        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public void ConflictUpdateTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Update(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Update(relatedEntity);
                    }
                }

                Assert.Throws<StaleStateException>(() => session.Flush());
            }
        }

        [Theory]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task ConflictUpdateAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Update(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Update(relatedEntity);
                    }
                }

                await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
            }

        }

        [Theory(Skip = "No exception")]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public void ConflictDeleteTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                Assert.Throws<StaleStateException>(() => session.Flush());
            }
        }

        [Theory(Skip= "No exception")]
        [InlineData(9)]
        [InlineData(100)]
        [InlineData(1000)]
        public async Task ConflictDeleteAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)

                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                await Assert.ThrowsAsync<StaleStateException>(() => session.FlushAsync());
            }

        }

        [Theory]
        [InlineData(1000)]
        public void UpsertTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count/2);
            var testUpsertData = mArrange.GenerateTestData(count/2);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testUpsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            foreach (var testEntity in testUpsertData)
            {
                testEntity.Value = "AAAAA";
            }
            using (var session = mArrange.UpsertSessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData.Concat(testUpsertData))
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }
        }

        [Theory]
        [InlineData(1000)]
        public async Task UpsertAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count / 2);
            var testUpsertData = mArrange.GenerateTestData(count / 2);
            using (var session = mArrange.SessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testUpsertData)
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }

            foreach (var testEntity in testUpsertData)
            {
                testEntity.Value = "AAAAA";
            }
            using (var session = mArrange.UpsertSessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData.Concat(testUpsertData))
                {
                    session.Save(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Save(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }
        }

        [Theory]
        [InlineData(1000)]
        public void TryDeleteTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.UpsertSessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                session.Flush();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }
        }

        [Theory]
        [InlineData(1000)]
        public async Task TryDeleteAsyncTest(int count)
        {
            var testInsertData = mArrange.GenerateTestData(count);
            using (var session = mArrange.UpsertSessionFactory.OpenSession())
            using (var transaction = session.BeginTransaction())
            {
                foreach (var entity in testInsertData)
                {
                    session.Delete(entity);
                    foreach (var relatedEntity in entity.RelatedEntities)
                    {
                        session.Delete(relatedEntity);
                    }
                }

                var sw = Stopwatch.StartNew();
                await session.FlushAsync();
                sw.Stop();
                mHelper.WriteLine($"Вставлено {count} записей за {sw.ElapsedMilliseconds} мс.");
                transaction.Commit();
            }
        }
    }
}
