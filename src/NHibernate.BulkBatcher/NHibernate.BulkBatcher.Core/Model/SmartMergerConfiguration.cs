using NHibernate.BulkBatcher.Core.Mergers;

namespace NHibernate.BulkBatcher.Core.Model
{
    /// <summary>
    /// Конфигурация умного мерджера
    /// </summary>
    public class SmartMergerConfiguration
    {
        public SmartMergerConfiguration(IBulkMerger merger, int minBatchSize)
        {
            Merger = merger;
            MinBatchSize = minBatchSize;
        }

        /// <summary>
        /// Минимальный размер пакета
        /// </summary>
        public int MinBatchSize { get; }

        /// <summary>
        /// Мерджер который нужно использовать
        /// </summary>
        public IBulkMerger Merger { get; }
    }
}