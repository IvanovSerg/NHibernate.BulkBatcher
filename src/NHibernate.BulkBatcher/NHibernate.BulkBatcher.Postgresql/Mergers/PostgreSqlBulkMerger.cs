using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NHibernate.BulkBatcher.Core.Mergers;
using NHibernate.BulkBatcher.Core.Model;
using NHibernate.BulkBatcher.PostgreSql.Internal;
using Npgsql;

namespace NHibernate.BulkBatcher.PostgreSql.Mergers
{
    public partial class PostgreSqlBulkMerger : AbstractBulkMerger<NpgsqlConnection, NpgsqlTransaction>
    {
        /// <summary>
        /// Мэппинг столбца для запроса
        /// </summary>
        private class ColumnMap
        {
            public ColumnMap(string tempColumn, string realColumn, bool isKey)
            {
                TempColumn = tempColumn;
                TempSpecifiedColumn = $"{tempColumn}Specified";
                RealColumn = realColumn;

                EscapedTempColumn = PostgreNamingHelper.Escape(TempColumn);
                EscapedTempSpecifiedColumn = PostgreNamingHelper.Escape(TempSpecifiedColumn);
                EscapedRealColumn = PostgreNamingHelper.Escape(RealColumn);

                IsKey = isKey;
            }

            /// <summary>
            /// Имя столбца во временной таблице
            /// </summary>
            public string EscapedTempColumn { get; }

            /// <summary>
            /// Имя столбца спецификации во временной таблице
            /// </summary>
            public string EscapedTempSpecifiedColumn { get; }

            /// <summary>
            /// Имя столбца во реальной таблице
            /// </summary>
            public string EscapedRealColumn { get; }

            /// <summary>
            /// Имя столбца во временной таблице
            /// </summary>
            public string TempColumn { get; }

            /// <summary>
            /// Имя столбца спецификации во временной таблице
            /// </summary>
            public string TempSpecifiedColumn { get; }

            /// <summary>
            /// Имя столбца во реальной таблице
            /// </summary>
            public string RealColumn { get; }

            /// <summary>
            /// Признак что поле ключевое
            /// </summary>
            public bool IsKey { get; }
        }

        /// <summary>
        /// Название столбца для состояния строки
        /// </summary>
        private const string cRowStateColumnName = "$RowState";

        /// <summary>
        /// Тип столбца для состояния строки
        /// </summary>
        private const string cRowStateColumnType = "character varying(1)";

        /// <inheritdoc />
        protected override DataTable GetSchemaTableFromDatabase(string[] tablePath, NpgsqlConnection connection, NpgsqlTransaction transaction, Action<IDbCommand> logAction)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                //Получение основной информации по схеме
                cmd.CommandText = GetSchemaTableSql(tablePath);
                logAction?.Invoke(cmd);
                DataTable schemaTable;
                using (var reader = cmd.ExecuteReader(CommandBehavior.KeyInfo | CommandBehavior.SchemaOnly))
                {
                    schemaTable = reader.GetSchemaTable();
                }
                //Проверка наличия первичного ключа в таблице
                AssertHasPrimaryKey(schemaTable, tablePath);

                //Получение информации о индексе
                cmd.CommandText = GetTableIndexInfoSql(tablePath);
                logAction?.Invoke(cmd);

                short[] pkOrders = null;
                short[] pkOptions = null;
                using (var reader = cmd.ExecuteReader(CommandBehavior.SingleRow))
                {
                    if (reader.Read())
                    {
                        pkOrders = reader.GetFieldValue<short[]>(0);
                        pkOptions = reader.GetFieldValue<short[]>(1);
                    }
                }
                AppendIndexOptions(schemaTable, pkOrders, pkOptions);

                return schemaTable;
            }
        }
        
        /// <inheritdoc />
        protected override string[] CreateTempTable(DataTable schema, NpgsqlConnection connection, NpgsqlTransaction transaction,
            Action<IDbCommand> logAction)
        {
            var tempTableName = $"tmp{Guid.NewGuid():N}";

            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;
                cmd.CommandText = GetCreateTempTableSql(schema, tempTableName);
                logAction?.Invoke(cmd);
                cmd.ExecuteNonQuery();
            }

            return new[] { tempTableName };
        }


        /// <inheritdoc />
        protected override int CopyData(string[] tempTablePath, IEnumerable<EntityInfo> entities, DataTable schema,
            NpgsqlConnection connection, NpgsqlTransaction transaction, Action<IDbCommand> logAction)
        {
            //Формируем команду импорта
            var columnNames = schema.Rows.OfType<DataRow>().Select(x => (string) x["ColumnName"]).ToList();
            var copyCommand = GetImportDataSql(tempTablePath, schema);
            using (var writer = connection.BeginBinaryImport(copyCommand))
            {
                var count = 0;
                foreach (var entity in entities)
                {
                    //Получение состояния сущности
                    var state = GetEntityState(entity);
                    if (state == null)
                        continue;

                    writer.StartRow();

                    //Запись состояния сущности
                    writer.Write(state);

                    foreach (var column in columnNames)
                    {
                        var result = entity.Values.TryGetValue(column, out var value);
                        
                        //Запись значения столбца
                        if (!result || value == null || value == DBNull.Value)
                            writer.WriteNull();
                        else
                            writer.Write(value);

                        //Запись о том что столбец передан
                        writer.Write(result);
                    }

                    count++;
                }
                writer.Complete();
                return count;
            }
        }
        
        /// <inheritdoc />
        protected override int MergeTables(string[] tempTablePath, string[] tablePath, DataTable schema,
            int expectedCount, NpgsqlConnection connection, NpgsqlTransaction transaction, Action<IDbCommand> logAction)
        {
            var map = CreateMap(schema);

            var count = 0;
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                if (AvoidConcurrencyErrors)
                {
                    if (map.Any(x => !x.IsKey))
                    {
                        cmd.CommandText = GetTryUpdateSql(tempTablePath, tablePath, map);
                        logAction?.Invoke(cmd);
                        count += cmd.ExecuteNonQuery();
                    }

                    cmd.CommandText = GetTryInsertSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += cmd.ExecuteNonQuery();

                    cmd.CommandText = GetDeleteSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += cmd.ExecuteNonQuery();
                }
                else
                {
                    cmd.CommandText = GetInsertSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += cmd.ExecuteNonQuery();

                    if (map.Any(x => !x.IsKey))
                    {
                        cmd.CommandText = GetUpdateSql(tempTablePath, tablePath, map);
                        logAction?.Invoke(cmd);
                        count += cmd.ExecuteNonQuery();
                    }

                    cmd.CommandText = GetDeleteSql(tempTablePath, tablePath, map);
                    logAction?.Invoke(cmd);
                    count += cmd.ExecuteNonQuery();
                }
            }

            return AvoidConcurrencyErrors ? expectedCount : count;
        }
        
        /// <inheritdoc />
        protected override void DropTempTable(string[] tempTablePath, NpgsqlConnection connection,
            NpgsqlTransaction transaction, Action<IDbCommand> logAction)
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.Transaction = transaction;

                cmd.CommandText = GetDropTableSql(tempTablePath);
                logAction?.Invoke(cmd);
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Проверяет наличие первичного ключа в схеме
        /// </summary>
        private void AssertHasPrimaryKey(DataTable schema, string[] tablePath)
        {
            if (schema.Rows.OfType<DataRow>().All(x=>!(bool)x["IsKey"]))
                throw new InvalidOperationException($"Table {PostgreNamingHelper.Escape(tablePath)} does not have primary key.");
        }

        /// <summary>
        /// Возвращает обозначение состояния сущности в бд
        /// </summary>
        private string GetEntityState(EntityInfo entity)
        {
            switch (entity.State)
            {
                case EntityState.Added:
                    return "I";
                case EntityState.Modified:
                    return "U";
                case EntityState.Deleted:
                    return "D";
            }

            return null;
        }

        /// <summary>
        /// Формирует Sql для команды получения схемы таблицы
        /// </summary>
        private string GetSchemaTableSql(string[] tablePath)
        {
            return $"SELECT * FROM {PostgreNamingHelper.Escape(tablePath)} LIMIT 0";
        }

        /// <summary>
        /// Формирует Sql для команды получения индекса первичного ключа таблицы
        /// </summary>
        private string GetTableIndexInfoSql(string[] tablePath)
        {
            return $"SELECT indkey, indoption as k " +
                   $"FROM pg_index i " +
                   $"WHERE i.indrelid = \'{PostgreNamingHelper.EscapeString(PostgreNamingHelper.Escape(tablePath))}\'::regclass " +
                   $"AND i.indisprimary = true ";
        }

        /// <summary>
        /// Добавляет информацию об индексе для схемы
        /// </summary>
        private void AppendIndexOptions(DataTable schemaTable, short[] primaryKeyOrders, short[] primaryKeyOptions)
        {
            schemaTable.Columns.Add("PrimaryKeyIndexOrdinal", typeof(int));
            schemaTable.Columns.Add("PrimaryKeyIndexOrderBy", typeof(string));
            schemaTable.Columns.Add("PrimaryKeyIndexNulls", typeof(string));

            var orders = (primaryKeyOrders ?? Enumerable.Empty<short>())
                .Select((o, i) => new {Index = i, Ordinal = o}).ToDictionary(x => (int)x.Ordinal, x => x.Index);

            var options = (primaryKeyOptions ?? Enumerable.Empty<short>())
                .Select((o, i) => new { Index = i, Options = o }).ToDictionary(x => x.Index, x => x.Options);

            foreach (DataRow row in schemaTable.Rows)
            {
                if ((bool)row["IsKey"])
                {
                    row["PrimaryKeyIndexOrdinal"] = 10000;
                    row["PrimaryKeyIndexOrderBy"] = "ASC";
                    row["PrimaryKeyIndexNulls"] = "NULLS LAST";

                    if (orders.TryGetValue((int) row["ColumnOrdinal"], out var order))
                    {
                        row["PrimaryKeyIndexOrdinal"] = order;
                        if (options.TryGetValue(order, out var flags))
                        {
                            if ((flags & 1) == 1)
                            {
                                row["PrimaryKeyIndexOrderBy"] = "DESC";
                            }
                            if ((flags & 2) == 2)
                            {
                                row["PrimaryKeyIndexNulls"] = "NULLS FIRST";
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Формирует Sql для команды создания временной таблицы
        /// </summary>
        private string GetCreateTempTableSql(DataTable schema, string tempTableName)
        {
            var sb = new StringBuilder();

            //Создание таблицы
            sb.Append($"CREATE TEMP TABLE {PostgreNamingHelper.Escape(tempTableName)} (");
            sb.Append($"{PostgreNamingHelper.Escape(cRowStateColumnName)} {cRowStateColumnType}");
            
            foreach (DataRow columnInfo in schema.Rows)
            {
                var columnName = $"col{(int) columnInfo["ColumnOrdinal"]}";
                sb.Append($", {PostgreNamingHelper.Escape(columnName)} " +
                          $"{(string)columnInfo["DataTypeName"]}");
                sb.Append($", {PostgreNamingHelper.Escape($"{columnName}Specified")} bool ");
            }
            sb.Append("); ");

            //Создание индекса
            sb.Append($"CREATE INDEX {PostgreNamingHelper.Escape($"IX_{tempTableName}")} ");
            sb.Append($"ON {PostgreNamingHelper.Escape(tempTableName)} (");
            var indexInfos = new List<string> {$"{PostgreNamingHelper.Escape(cRowStateColumnName)} ASC NULLS LAST"};
            indexInfos.AddRange(
                schema.Rows.OfType<DataRow>()
                    .Where(x => (bool) x["IsKey"])
                    .OrderBy(x => (int) x["PrimaryKeyIndexOrdinal"])
                    .Select(x => $"{PostgreNamingHelper.Escape($"col{(int) x["ColumnOrdinal"]}")} " +
                                 $"{(string) x["PrimaryKeyIndexOrderBy"]} " +
                                 $"{(string) x["PrimaryKeyIndexNulls"]}"));
            sb.Append(string.Join(", ", indexInfos));
            sb.Append("); ");

            return sb.ToString();
        }
        
        /// <summary>
        /// Формирует Sql для команды удаления временной таблицы
        /// </summary>
        private string GetDropTableSql(string[] tempTablePath)
        {
            return $"DROP TABLE IF EXISTS {PostgreNamingHelper.Escape(tempTablePath)}";
        }

        /// <summary>
        /// Формирует Sql для команды импорта данных во временную таблицу
        /// </summary>
        private string GetImportDataSql(string[] tempTablePath, DataTable schema)
        {
            var columns = new List<string> {cRowStateColumnName};
            foreach (DataRow row in schema.Rows)
            {
                columns.Add($"col{(int)row["ColumnOrdinal"]}");
                columns.Add($"col{(int)row["ColumnOrdinal"]}Specified");
            }

            return
                $"COPY {PostgreNamingHelper.Escape(tempTablePath)}" +
                $"({string.Join(", ", columns.Select(x=>PostgreNamingHelper.Escape(x)))}) " +
                $"FROM STDIN BINARY;";
        }

        /// <summary>
        /// Создает мэппинг для столбцов
        /// </summary>
        private IList<ColumnMap> CreateMap(DataTable schema)
        {
            return schema.Rows.Cast<DataRow>()
                .Select(x => new ColumnMap($"col{(int)x["ColumnOrdinal"]}", (string) x["ColumnName"], (bool) x["IsKey"])).ToList();
        }

        /// <summary>
        /// Формирует Sql для попытки вставки данных
        /// </summary>
        private string GetTryInsertSql(string[] tempTablePath, string[] tablePath, IList<ColumnMap> map)
        {
            return $"INSERT INTO {PostgreNamingHelper.Escape(tablePath)} " +
                   $"({string.Join(", ", map.Select(x => x.EscapedRealColumn))}) " +
                   $"SELECT " +
                   $"{string.Join(", ", map.Select(x => x.EscapedTempColumn))} " +
                   $"FROM {PostgreNamingHelper.Escape(tempTablePath)} AS tmp "+
                   $"WHERE tmp.{PostgreNamingHelper.Escape(cRowStateColumnName)} IN ('I','U') " +
                   $"ON CONFLICT DO NOTHING";
        }

        /// <summary>
        /// Формирует Sql для попытки обновления данных
        /// </summary>
        private string GetTryUpdateSql(string[] tempTablePath, string[] tablePath, IList<ColumnMap> map)
        {
            return $"UPDATE {PostgreNamingHelper.Escape(tablePath)} AS rl " +
                   $"SET {string.Join(", ", map.Where(x => !x.IsKey).Select(x => $"{x.EscapedRealColumn} = CASE WHEN tmp.{x.EscapedTempSpecifiedColumn} THEN tmp.{x.EscapedTempColumn} ELSE rl.{x.EscapedRealColumn} END"))} " +
                   $"FROM {PostgreNamingHelper.Escape(tempTablePath)} AS tmp " +
                   $"WHERE tmp.{PostgreNamingHelper.Escape(cRowStateColumnName)} IN ('I','U') " +
                   $"AND {string.Join(" AND ", map.Where(x => x.IsKey).Select(x => $"rl.{x.EscapedRealColumn} = tmp.{x.EscapedTempColumn}"))} ";
        }

        /// <summary>
        /// Формирует Sql для вставки данных
        /// </summary>
        private string GetInsertSql(string[] tempTablePath, string[] tablePath, IList<ColumnMap> map)
        {
            return $"INSERT INTO {PostgreNamingHelper.Escape(tablePath)} " +
                   $"({string.Join(", ", map.Select(x => x.EscapedRealColumn))}) " +
                   $"SELECT " +
                   $"{string.Join(", ", map.Select(x => x.EscapedTempColumn))} " +
                   $"FROM {PostgreNamingHelper.Escape(tempTablePath)} AS tmp " +
                   $"WHERE tmp.{PostgreNamingHelper.Escape(cRowStateColumnName)} IN ('I') ";
        }

        /// <summary>
        /// Формирует Sql для обновления данных
        /// </summary>
        private string GetUpdateSql(string[] tempTablePath, string[] tablePath, IList<ColumnMap> map)
        {
            return $"UPDATE {PostgreNamingHelper.Escape(tablePath)} AS rl " +
                   $"SET {string.Join(", ", map.Where(x => !x.IsKey).Select(x => $"{x.EscapedRealColumn} = CASE WHEN tmp.{x.EscapedTempSpecifiedColumn} THEN tmp.{x.EscapedTempColumn} ELSE rl.{x.EscapedRealColumn} END"))} " +
                   $"FROM {PostgreNamingHelper.Escape(tempTablePath)} AS tmp " +
                   $"WHERE tmp.{PostgreNamingHelper.Escape(cRowStateColumnName)} IN ('U') " +
                   $"AND {string.Join(" AND ", map.Where(x => x.IsKey).Select(x => $"rl.{x.EscapedRealColumn} = tmp.{x.EscapedTempColumn}"))} ";
        }

        /// <summary>
        /// Формирует Sql для удаления данных
        /// </summary>
        private string GetDeleteSql(string[] tempTablePath, string[] tablePath, IList<ColumnMap> map)
        {
            return $"DELETE FROM {PostgreNamingHelper.Escape(tablePath)} AS rl " +
                   $"USING {PostgreNamingHelper.Escape(tempTablePath)} AS tmp " +
                   $"WHERE tmp.{PostgreNamingHelper.Escape(cRowStateColumnName)} IN ('D') " +
                   $"AND {string.Join(" AND ", map.Where(x => x.IsKey).Select(x => $"rl.{x.EscapedRealColumn} = tmp.{x.EscapedTempColumn}"))} ";
        }
    }
}
