using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet;
using NHibernate.BulkBatcher.Core.Model;
using NpgsqlTypes;

namespace NHibernate.BulkBatcher.Core.Batchers
{
    /// <summary>
    /// Батчер использующий Bulk операции (асинхронные методы)
    /// </summary>
    public partial class BulkOperationsBatcher
    {
        /// <inheritdoc />
        protected override async Task DoExecuteBatchAsync(DbCommand ps, CancellationToken cancellationToken)
        {
            try
            {
                if (mCurrentEntites.Any())
                {
                    Prepare(ps);
                    
                    var isGeometryPresent = ps.Parameters.OfType<Npgsql.NpgsqlParameter>().Any(x => x.NpgsqlDbType == NpgsqlDbType.Geometry);
#if DEBUG
                    Debug.WriteLine($"Начата обработка {mCurrentEntites.Count} записей.");
                    var sw = Stopwatch.StartNew();
#endif
                    var result = await mBulkMerger.MergeAsync(mCurrentEntites, Driver, ps.Connection, ps.Transaction, isGeometryPresent, cancellationToken);
#if DEBUG
                    sw.Stop();
                    Debug.WriteLine($"Обработка {mCurrentEntites.Count} записей завершена. Прошло {sw.ElapsedMilliseconds} мс.");
#endif
                    Expectations.VerifyOutcomeBatched(mCurrentEntites.Count, result, ps);
                }
            }
            finally
            {
                mCurrentEntites.Clear();
            }
        }

        /// <inheritdoc />
        public override async Task AddToBatchAsync(IExpectation expectation, CancellationToken cancellationToken)
        {
            var entityInfo = ExtractEntityInfo();

            var cmd = CurrentCommand;
            Driver.AdjustCommand(cmd);

            if (entityInfo == null)
            {
                //Выполняем текущий батч
                await ExecuteBatchWithTimingAsync(CurrentCommand, cancellationToken);

                //Исполняем команду
                LogCommand(cmd);
                var rowCount = await ExecuteNonQueryAsync(cmd, cancellationToken);
                expectation.VerifyOutcomeNonBatched(rowCount, cmd);
                return;
            }
            
            mCurrentEntites.Add(entityInfo);

            if (mCurrentEntites.Count >= BatchSize)
            {
                await ExecuteBatchWithTimingAsync(CurrentCommand, cancellationToken);
            }
        }

    }
}
