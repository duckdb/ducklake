#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "common/ducklake_types.hpp"

namespace duckdb {

static bool HasFourDigitDatePrefix(const string &value) {
	return value.size() >= 10 && StringUtil::CharacterIsDigit(value[0]) && StringUtil::CharacterIsDigit(value[1]) &&
	       StringUtil::CharacterIsDigit(value[2]) && StringUtil::CharacterIsDigit(value[3]) && value[4] == '-' &&
	       StringUtil::CharacterIsDigit(value[5]) && StringUtil::CharacterIsDigit(value[6]) && value[7] == '-' &&
	       StringUtil::CharacterIsDigit(value[8]) && StringUtil::CharacterIsDigit(value[9]);
}

static string WithPostgresBinaryCollation(const string &expression) {
	return "(" + expression + " COLLATE \"C\")";
}

static bool IsPostgresTemporalStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_TZ:
		return true;
	default:
		return false;
	}
}

static string GetPostgresStatsType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BOOLEAN";
	case LogicalTypeId::TINYINT:
	case LogicalTypeId::SMALLINT:
		return "SMALLINT";
	case LogicalTypeId::INTEGER:
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INTEGER";
	case LogicalTypeId::BIGINT:
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "NUMERIC";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::DATE:
		return "DATE";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
		return "TIMESTAMP";
	case LogicalTypeId::TIMESTAMP_TZ:
		return "TIMESTAMPTZ";
	case LogicalTypeId::DECIMAL:
		return type.ToString();
	default:
		return string();
	}
}

static bool CanCastPostgresStatsForValueComparison(const LogicalType &type) {
	return type.IsNumeric() || type.id() == LogicalTypeId::BOOLEAN || IsPostgresTemporalStatsType(type);
}

static bool CanCastPostgresTemporalValue(const Value &value, const LogicalType &type) {
	auto string_value = value.ToString();
	if (!HasFourDigitDatePrefix(string_value)) {
		return false;
	}
	return type.id() != LogicalTypeId::DATE || string_value.size() == 10;
}

string PostgresMetadataManager::CastValueToTarget(const Value &value, const LogicalType &type) {
	if (value.IsNull() || value.ToString().find('\0') != string::npos || type.id() == LogicalTypeId::BLOB) {
		return string();
	}
	if (RequiresValueComparison(type) && (!CanCastPostgresStatsForValueComparison(type) || !ValueIsFinite(value))) {
		return string();
	}
	if (!RequiresValueComparison(type) && type.id() != LogicalTypeId::VARCHAR) {
		return string();
	}
	if (IsPostgresTemporalStatsType(type) && !CanCastPostgresTemporalValue(value, type)) {
		return string();
	}
	if (type.IsNumeric()) {
		return value.ToString();
	}
	auto literal = SQLString::ToString(value.ToString());
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(literal);
	}
	if (IsPostgresTemporalStatsType(type)) {
		return literal + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::BOOLEAN) {
		return literal + "::BOOLEAN";
	}
	return string();
}

static string PostgresSafeTemporalStatsCast(const string &stats, const LogicalType &type) {
	string regex;
	if (type.id() == LogicalTypeId::DATE) {
		regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])$'";
	} else if (type.id() == LogicalTypeId::TIMESTAMP_TZ) {
		regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01]) "
		        "([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?"
		        "(Z|[+-](0[0-9]|1[0-5])(:[0-5][0-9])?)$'";
	} else {
		regex = "'^[0-9]{4}-(0[1-9]|1[0-2])-([0][1-9]|[12][0-9]|3[01])"
		        "( ([01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9](\\.[0-9]{1,6})?)?$'";
	}

	auto year = StringUtil::Format("substring(%s FROM 1 FOR 4)::INTEGER", stats);
	auto month = StringUtil::Format("substring(%s FROM 6 FOR 2)::INTEGER", stats);
	auto day = StringUtil::Format("substring(%s FROM 9 FOR 2)::INTEGER", stats);
	auto max_day = StringUtil::Format(
	    "(CASE WHEN %s = 2 THEN CASE WHEN mod(%s, 4) = 0 AND (mod(%s, 100) <> 0 OR mod(%s, 400) = 0) "
	    "THEN 29 ELSE 28 END WHEN %s IN (4, 6, 9, 11) THEN 30 ELSE 31 END)",
	    month, year, year, year, month);
	auto valid_date = StringUtil::Format("%s > 0 AND %s <= %s", year, day, max_day);
	return StringUtil::Format("(CASE WHEN %s ~ %s THEN CASE WHEN %s THEN %s::%s END END)", stats, regex, valid_date,
	                          stats, GetPostgresStatsType(type));
}

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type,
                                                  StatsCastType cast_type) {
	if (IsPostgresTemporalStatsType(type)) {
		auto cast = PostgresSafeTemporalStatsCast(stats, type);
		if (cast_type == StatsCastType::ORDERING) {
			return cast;
		}
		return BoundOrInfinity(cast, GetPostgresStatsType(type), cast_type);
	}
	if (CanCastPostgresStatsForValueComparison(type)) {
		return stats + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(stats);
	}
	return string();
}

PostgresMetadataManager::PostgresMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool PostgresMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	// Unnamed composite types are not supported.
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	// Postgres timestamp/date ranges are narrower than DuckDB's
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
	// Postgres bytea input format differs from DuckDB's blob text format
	case LogicalTypeId::BLOB:
	// Postgres cannot store null bytes in VARCHAR/TEXT columns
	case LogicalTypeId::VARCHAR:
	case LogicalTypeId::VARIANT:
	// If we knew that the Postgres installation has PostGIS installed, we could support GEOMETRY in the future.
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
	case LogicalTypeId::SQLNULL:
		return "INTEGER";
	case LogicalTypeId::UINTEGER:
		return "BIGINT";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::BLOB:
	case LogicalTypeId::VARCHAR:
		return "BYTEA";
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::DATE:
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "VARCHAR";
	default:
		return column_type.ToString();
	}
}

bool PostgresMetadataManager::InlinedDeletionTableExists(const string &table_name) {
	auto &catalog = transaction.GetCatalog();
	auto remote_query = StringUtil::Format(
	    "SELECT 1 FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename = %s LIMIT 1",
	    SQLString::ToString(catalog.MetadataSchemaName().GetIdentifierName()), SQLString::ToString(table_name));
	auto query =
	    StringUtil::Format("SELECT 1 FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s, use_transaction = true)",
	                       SQLString::ToString(remote_query));
	auto result = DuckLakeMetadataManager::Query(query);
	result->ThrowIfError("Failed to probe for DuckLake inlined-deletion table: ");
	return result->Fetch() != nullptr;
}

void PostgresMetadataManager::MigrateInlinedDataTypes() {
	// the user columns of every inlined table with their types at the schema version of that table
	auto columns = DuckLakeMetadataManager::Query(R"(
WITH inlined AS (
	SELECT idt.table_id, idt.table_name, COALESCE(sv.begin_snapshot, (
		SELECT MIN(t.begin_snapshot) FROM {METADATA_CATALOG}.ducklake_table t WHERE t.table_id = idt.table_id
	)) AS snapshot_id
	FROM {METADATA_CATALOG}.ducklake_inlined_data_tables idt
	LEFT JOIN {METADATA_CATALOG}.ducklake_schema_versions sv
		ON sv.table_id = idt.table_id AND sv.schema_version = idt.schema_version
)
SELECT inlined.table_name, col.column_name, col.column_type
FROM inlined
JOIN {METADATA_CATALOG}.ducklake_column col ON col.table_id = inlined.table_id AND col.parent_column IS NULL
WHERE col.begin_snapshot <= inlined.snapshot_id AND (col.end_snapshot IS NULL OR col.end_snapshot > inlined.snapshot_id)
)");
	columns->ThrowIfError("Failed to read the columns of inlined-data tables while migrating: ");
	map<string, case_insensitive_map_t<string>> inlined_tables;
	for (auto &row : *columns) {
		inlined_tables[row.GetValue<string>(0)][row.GetValue<string>(1)] = row.GetValue<string>(2);
	}
	for (auto &inlined_table : inlined_tables) {
		auto &table_name = inlined_table.first;
		auto probe = DuckLakeMetadataManager::Query(
		    StringUtil::Format("SELECT * FROM {METADATA_CATALOG}.%s LIMIT 0", SQLIdentifier(table_name)));
		if (probe->HasError() || probe->GetNames().size() < 3) {
			continue;
		}
		// the metadata columns are always the first three columns, user columns follow
		auto &names = probe->GetNames();
		DuckLakeInlinedColNames col_names(false);
		col_names.row_id = names[0].GetIdentifierName();
		col_names.begin_snapshot = names[1].GetIdentifierName();
		col_names.end_snapshot = names[2].GetIdentifierName();
		vector<string> select_list;
		for (idx_t i = 0; i < 3; i++) {
			select_list.push_back(SQLIdentifier::ToString(names[i].GetIdentifierName()));
		}
		string column_defs;
		bool rewrite = false;
		for (idx_t i = 3; i < names.size(); i++) {
			auto name = names[i].GetIdentifierName();
			auto column_type = inlined_table.second.find(name);
			if (column_type == inlined_table.second.end()) {
				rewrite = false;
				break;
			}
			DuckLakeColumnInfo column;
			column.type = column_type->second;
			auto storage_type_name = GetColumnType(column);
			auto storage_type = UnboundType::TryParseAndDefaultBind(storage_type_name);
			auto type = DuckLakeTypes::FromString(column.type);
			auto native_type = type.HasAlias() ? LogicalType(type.id()) : type;
			// DuckLake 0.3 stored values with the native type of their DuckLake type, other columns are kept
			auto &stored_type = probe->GetTypes()[i];
			bool convert = stored_type != storage_type && stored_type == native_type;
			auto column_name = SQLIdentifier::ToString(name);
			column_defs += StringUtil::Format("%s%s %s", column_defs.empty() ? "" : ", ", column_name,
			                                  convert ? storage_type_name : stored_type.ToString());
			select_list.push_back(convert ? DuckLakeUtil::InlinedStorageExpression(*this, column_name, type)
			                              : column_name);
			rewrite = rewrite || convert;
		}
		if (!rewrite) {
			continue;
		}
		auto migrated_name = table_name + "_migrated";
		auto migrate_query = InlinedTableDdlSql(migrated_name, column_defs, col_names);
		migrate_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s SELECT %s FROM {METADATA_CATALOG}.%s;",
		                                    SQLIdentifier(migrated_name), StringUtil::Join(select_list, ", "),
		                                    SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("DROP TABLE {METADATA_CATALOG}.%s;", SQLIdentifier(table_name));
		migrate_query += StringUtil::Format("ALTER TABLE {METADATA_CATALOG}.%s RENAME TO %s;",
		                                    SQLIdentifier(migrated_name), SQLIdentifier(table_name));
		auto result = DuckLakeMetadataManager::Execute(migrate_query);
		result->ThrowIfError(
		    StringUtil::Format("Failed to migrate the column types of inlined-data table \"%s\": ", table_name));
	}
}

unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	SubstituteTransactionPlaceholders(snapshot, query);
	auto &ducklake_catalog = transaction.GetCatalog();
	SubstituteCatalogPlaceholders(query, SQLQuotedIdentifier::ToString(ducklake_catalog.MetadataSchemaName()));
	auto catalog_literal = SQLString::ToString(ducklake_catalog.MetadataDatabaseName());
	auto result = transaction.GetConnection().Query(
	    StringUtil::Format("CALL postgres_execute(%s, %s, prepare=FALSE)", catalog_literal, SQLString(query)));
	return std::move(result);
}

void PostgresMetadataManager::ClearCache() {
	auto result = transaction.ExecuteRaw("CALL pg_clear_cache();");
	result->ThrowIfError("Failed to clear the PostgreSQL metadata cache: ");
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id,
		 (SELECT MAX(value) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_metadata WHERE key = ''version'')
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

string PostgresMetadataManager::GenerateFileListQuery(DuckLakeTableEntry &table, const FilterPushdownInfo *filter_info,
                                                      const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
                                                      const vector<idx_t> &runtime_filter_stats_columns,
                                                      FileListType file_list_type, const string &) {
	auto remote_query = DuckLakeMetadataManager::GenerateFileListQuery(
	    table, filter_info, dynamic_filters, runtime_filter_stats_columns, file_list_type, "{METADATA_SCHEMA_ESCAPED}");

	return StringUtil::Format("SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s)",
	                          SQLString(remote_query));
}

} // namespace duckdb
