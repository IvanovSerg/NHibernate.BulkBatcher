using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using Antlr.Runtime.Tree;
using NHibernate.Driver;
using NHibernate.SqlCommand;
using NHibernate.SqlTypes;

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
        /// Информация о команде из которой была создана эта сущность
        /// </summary>
        public CommandInfo CommandInfo { get; set; }


    }
}
