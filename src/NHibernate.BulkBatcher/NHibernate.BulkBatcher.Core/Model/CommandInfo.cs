using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

namespace NHibernate.BulkBatcher.Core.Model
{
    /// <summary>
    /// Информация о команде из которой было собрано <see cref="EntityInfo"/>
    /// </summary>
    public class CommandInfo
    {
        /// <summary>
        /// Тип команды из которой была создана сущность
        /// </summary>
        public CommandType Type { get; set; }

        /// <summary>
        /// Sql команды из которой была создана сущность
        /// </summary>
        public SqlString Sql { get; set; }

        /// <summary>
        /// Типы параметров команды
        /// </summary>
        public SqlType[] ParameterTypes { get; set; }

        /// <summary>
        /// Параметры команды из которой была создана сущность
        /// </summary>
        public IList<CommandParameterInfo> Parameters { get; set; }
    }
}
