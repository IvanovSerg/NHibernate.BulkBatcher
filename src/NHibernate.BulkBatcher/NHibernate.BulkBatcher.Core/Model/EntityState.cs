using System;
using System.Collections.Generic;
using System.Text;

namespace NHibernate.BulkBatcher.Core.Model
{
    /// <summary>
    /// Состояние сущности
    /// </summary>
    public enum EntityState
    {
        /// <summary>
        /// Не изменена
        /// </summary>
        Unchanged,
        /// <summary>
        /// Добавлена
        /// </summary>
        Added,
        /// <summary>
        /// Модифицирована
        /// </summary>
        Modified,
        /// <summary>
        /// Удалена
        /// </summary>
        Deleted
    }
}
