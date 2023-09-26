using System.Collections.Generic;

namespace NHibernate.BulkBatcher.Core.Model
{
    /// <summary>
    /// Информация о сущности
    /// </summary>
    public class EntityInfo
    {
        /// <summary>
        /// Путь к таблице
        /// </summary>
        public string[] TablePath { get; set; }

        /// <summary>
        /// Состояние
        /// </summary>
        public EntityState State { get; set; }

        /// <summary>
        /// Значения
        /// </summary>
        public IDictionary<string, object> Values { get; set; }

        /// <summary>
        /// Новый ключ (при операции обновления)
        /// </summary>
        public IDictionary<string, object> UpdatedKey { get; set; }

        /// <summary>
        /// Информация о команде из которой была создана эта сущность
        /// </summary>
        public CommandInfo CommandInfo { get; set; }
    }
}