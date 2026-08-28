#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_table_entry.hpp"

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

static string PostgresCastValueToTarget(const Value &value, const LogicalType &type) {
	if (value.IsNull() || value.ToString().find('\0') != string::npos || type.id() == LogicalTypeId::BLOB) {
		return string();
	}
	if (RequiresValueComparison(type) &&
	    (!CanCastPostgresStatsForValueComparison(type) ||
	     ((value.type().id() == LogicalTypeId::FLOAT || value.type().id() == LogicalTypeId::DOUBLE) &&
	      !Value::IsFinite(value.GetValue<double>())))) {
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
	auto literal = DuckLakeUtil::SQLLiteralToString(value.ToString());
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

static string PostgresCastStatsToTarget(const string &stats, const LogicalType &type) {
	if (IsPostgresTemporalStatsType(type)) {
		return PostgresSafeTemporalStatsCast(stats, type);
	}
	if (CanCastPostgresStatsForValueComparison(type)) {
		return stats + "::" + GetPostgresStatsType(type);
	}
	if (type.id() == LogicalTypeId::VARCHAR) {
		return WithPostgresBinaryCollation(stats);
	}
	return string();
}

static string GeneratePostgresNativeFileColumnStatsCTEBody(const CTERequirement &requirement, TableIndex table_id) {
	string select_list = "data_file_id";
	for (const auto &stat : requirement.referenced_stats) {
		select_list += ", " + stat;
	}
	return StringUtil::Format("  SELECT %s\n"
	                          "  FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
	                          "  WHERE column_id = %d AND table_id = %d\n",
	                          select_list, requirement.column_field_index, table_id.index);
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

bool PostgresMetadataManager::SupportsInlining(const LogicalType &type) {
	if (type.id() == LogicalTypeId::VARIANT) {
		return false;
	}
	return DuckLakeMetadataManager::SupportsInlining(type);
}

string PostgresMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::DOUBLE:
		return "DOUBLE PRECISION";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
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

unique_ptr<QueryResult> PostgresMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                              string command) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName().GetIdentifierName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);

	auto result = connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
	return std::move(result);
}
unique_ptr<QueryResult> PostgresMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, "postgres_execute");
}

unique_ptr<QueryResult> PostgresMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return DuckLakeMetadataManager::Query(snapshot, query);
}

string PostgresMetadataManager::GetLatestSnapshotQuery() const {
	return R"(
	SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
		'SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
		 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot WHERE snapshot_id = (
		     SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
		 );')
	)";
}

string PostgresMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) {
	string select_list = "data_file_id";
	for (const auto &stat : req.referenced_stats) {
		select_list += ", " + stat;
	}
	return StringUtil::Format("  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
	                          "    'SELECT %s\n"
	                          "     FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_column_stats\n"
	                          "     WHERE column_id = %d AND table_id = %d')\n",
	                          select_list, req.column_field_index, table_id.index);
}

string PostgresMetadataManager::GenerateFileListQuery(DuckLakeTableEntry &table, const FilterPushdownInfo *filter_info,
                                                      const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
                                                      const string &, const ColumnStatsFilterSQL *,
                                                      const FileColumnStatsCTEBodyGenerator &,
                                                      const FileListStatsCastGenerator &) {
	ColumnStatsFilterSQL filter_sql;
	filter_sql.cast_value = PostgresCastValueToTarget;
	filter_sql.cast_stats = [](const string &stats, const LogicalType &type, bool is_min) {
		auto cast = PostgresCastStatsToTarget(stats, type);
		if (cast.empty() || !IsPostgresTemporalStatsType(type)) {
			return cast;
		}
		return StringUtil::Format("COALESCE(%s, '%s'::%s)", cast, is_min ? "-infinity" : "infinity",
		                          GetPostgresStatsType(type));
	};
	auto remote_query = DuckLakeMetadataManager::GenerateFileListQuery(
	    table, filter_info, dynamic_filters, "{METADATA_SCHEMA_ESCAPED}", &filter_sql,
	    GeneratePostgresNativeFileColumnStatsCTEBody, PostgresCastStatsToTarget);

	return StringUtil::Format("SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, %s)",
	                          SQLString(remote_query));
}

// We need a specialized function here to do a reinterpret for postgres from BLOB to VARCHAR
shared_ptr<DuckLakeInlinedData> PostgresMetadataManager::TransformInlinedData(QueryResult &result,
                                                                              const vector<LogicalType> &expected_types,
                                                                              const string &inlined_table_name) {
	CheckInlinedDataReadError(result, inlined_table_name);
	bool needs_reinterpret = false;
	if (!expected_types.empty()) {
		auto &result_types = result.GetTypes();
		if (result_types.size() < expected_types.size()) {
			throw InvalidInputException(
			    "Failed to read inlined data from DuckLake: expected %llu columns but read %llu", expected_types.size(),
			    result_types.size());
		}
		for (idx_t i = 0; i < expected_types.size(); i++) {
			if (result_types[i] != expected_types[i]) {
				D_ASSERT(result_types[i].id() == LogicalTypeId::BLOB &&
				         expected_types[i].id() == LogicalTypeId::VARCHAR);
				needs_reinterpret = true;
			}
		}
	}
	if (!needs_reinterpret) {
		return DuckLakeMetadataManager::TransformInlinedData(result, expected_types, inlined_table_name);
	}

	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, expected_types);
	DataChunk reinterpret_chunk;
	reinterpret_chunk.Initialize(*context, expected_types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		for (idx_t i = 0; i < expected_types.size(); i++) {
			reinterpret_chunk.data[i].Reinterpret(chunk->data[i]);
		}
		// Use SetChildCardinality (not SetCardinality): on current duckdb SetCardinality only updates the
		// chunk count, while ColumnDataCollection::Append reads each vector via ToUnifiedFormat(), which
		// relies on the vector's own size. SetChildCardinality also FlatVector::SetSize()s every vector, so
		// the reinterpreted (BLOB->VARCHAR) vectors are sized to the row count and the rows are appended.
		reinterpret_chunk.SetChildCardinality(chunk->size());
		data->Append(reinterpret_chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

} // namespace duckdb
