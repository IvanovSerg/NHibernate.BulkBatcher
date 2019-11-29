using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace NHibernate.BulkBatcher.Core.Model
{
    /// <summary>
    /// Информация о параметре <see cref="CommandInfo"/>
    /// </summary>
    public class CommandParameterInfo
    {
        /// <summary>
        /// Направление
        /// </summary>
        public ParameterDirection Direction { get; set; }

        /// <summary>
        /// Точность
        /// </summary>
        public byte Precision { get; set; }

        /// <summary>
        /// Порядок
        /// </summary>
        public byte Scale { get; set; }

        /// <summary>
        /// Длина
        /// </summary>
        public int Size { get; set; }

        /// <summary>
        /// Значение
        /// </summary>
        public object Value { get; set; }
    }
}
