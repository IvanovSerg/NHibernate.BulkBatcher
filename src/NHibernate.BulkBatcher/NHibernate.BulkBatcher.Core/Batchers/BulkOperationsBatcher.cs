using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.AdoNet;
using NHibernate.BulkBatcher.Core.EntityInfoExtractors;
using NHibernate.BulkBatcher.Core.Mergers;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernate.BulkBatcher.Core.Batchers
{
    /// <summary>
    /// Батчер использующий Bulk операции
    /// </summary>
    public partial class BulkOperationsBatcher : AbstractBatcher
    {
        private readonly ICollection<IEntityInfoExtractor> mExtractors;
        private readonly IBulkMerger mBulkMerger;
        private readonly List<EntityInfo> mCurrentEntites;

        /// <inheritdoc />
        protected override int CountOfStatementsInCurrentBatch => mCurrentEntites.Count;

        /// <inheritdoc />
        public override int BatchSize { get; set; }

        /// <inheritdoc />
        public BulkOperationsBatcher(
            ConnectionManager connectionManager, 
            IInterceptor interceptor, 
            ICollection<IEntityInfoExtractor> extractors, 
            IBulkMerger bulkMerger) : base(connectionManager, interceptor)
        {
            BatchSize = Factory.Settings.AdoBatchSize;
            mExtractors = extractors;
            mBulkMerger = bulkMerger;
            mCurrentEntites = new List<EntityInfo>(BatchSize);
        }

        /// <inheritdoc />
        protected override void DoExecuteBatch(DbCommand ps)
        {
            try
            {
                if (mCurrentEntites.Any())
                {
                    Prepare(ps);

#if DEBUG
                    Debug.WriteLine($"Начата обработка {mCurrentEntites.Count} записей.");
                    var sw = Stopwatch.StartNew();
#endif
                    var result = mBulkMerger.Merge(mCurrentEntites, Driver, ps.Connection, ps.Transaction, x => LogCommand((DbCommand)x));
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
        public override void AddToBatch(IExpectation expectation)
        {
            var entityInfo = ExtractEntityInfo();

            var cmd = CurrentCommand;

            if (entityInfo == null)
            {
                //Выполняем текущий батч
                ExecuteBatchWithTiming(CurrentCommand);

                //Исполняем команду
                Driver.AdjustCommand(cmd);
                LogCommand(cmd);
                var rowCount = ExecuteNonQuery(cmd);
                expectation.VerifyOutcomeNonBatched(rowCount, cmd);
                return;
            }
            
            mCurrentEntites.Add(entityInfo);

            if (mCurrentEntites.Count >= BatchSize)
            {
                ExecuteBatchWithTiming(CurrentCommand);
            }
        }
        
        /// <summary>
        /// Пытается извлечь <see cref="EntityInfo"/> из текущего состояния
        /// </summary>
        private EntityInfo ExtractEntityInfo()
        {
            var entityInfo = mExtractors
                .Select(extractor => extractor.Extract(CurrentCommand, CurrentCommandSql))
                .FirstOrDefault(extracted => extracted != null);
            if (entityInfo != null)
            {
                var commandInfo = new CommandInfo()
                {
                    Type = CurrentCommand.CommandType,
                    ParameterTypes = CurrentCommandParameterTypes,
                    Sql = CurrentCommandSql,
                    Parameters = CurrentCommand.Parameters.OfType<IDbDataParameter>().Select(CreateParameterInfo).ToList()
                };
                entityInfo.CommandInfo = commandInfo;
            }

            return entityInfo;
        }

        /// <summary>
        /// Создает информацию о параметре
        /// </summary>
        private CommandParameterInfo CreateParameterInfo(IDbDataParameter parameter)
        {
            return new CommandParameterInfo
            {
                Direction = parameter.Direction,
                Precision = parameter.Precision,
                Scale = parameter.Scale,
                Size = parameter.Size,
                Value = parameter.Value
            };
        }
    }
}
