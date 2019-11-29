using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Text;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.SqlCommand;

namespace NHibernate.BulkBatcher.Core.EntityInfoExtractors
{
    /// <summary>
    /// Интерфейс извлекателя <see cref="EntityInfo"/> из <see cref="DbCommand"/> и <see cref="SqlString"/>
    /// </summary>
    public interface IEntityInfoExtractor
    {
        /// <summary>
        /// Пытается сделать <see cref="EntityInfo"/> на основе <see cref="DbCommand"/> и <see cref="SqlString"/> 
        /// </summary>
        EntityInfo Extract(DbCommand command, SqlString sqlString);
    }
}
