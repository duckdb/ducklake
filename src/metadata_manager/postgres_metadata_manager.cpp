#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/main/database.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"
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

string PostgresMetadataManager::CastValueToTarget(const Value &value, const LogicalType &type) {
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

string PostgresMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type,
                                                  StatsCastType cast_type) {
	if (IsPostgresTemporalStatsType(type)) {
		auto cast = PostgresSafeTemporalStatsCast(stats, type);
		if (cast_type == StatsCastType::ORDERING) {
			return cast;
		}
		// Unknown bounds must not exclude a file that can satisfy the data filter.
		return StringUtil::Format("COALESCE(%s, '%s'::%s)", cast,
		                          cast_type == StatsCastType::MIN ? "-infinity" : "infinity",
		                          GetPostgresStatsType(type));
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

//! postgres_execute prepares statements; prepared statements reject batches
static vector<string> SplitBatchStatements(const string &sql) {
	vector<string> statements;
	string current;
	bool in_squote = false;
	bool in_dquote = false;
	for (idx_t i = 0; i < sql.size(); i++) {
		auto c = sql[i];
		if (in_squote) {
			current += c;
			if (c == '\'') {
				if (i + 1 < sql.size() && sql[i + 1] == '\'') {
					current += sql[++i];
				} else {
					in_squote = false;
				}
			}
		} else if (in_dquote) {
			current += c;
			if (c == '"') {
				if (i + 1 < sql.size() && sql[i + 1] == '"') {
					current += sql[++i];
				} else {
					in_dquote = false;
				}
			}
		} else if (c == '\'') {
			in_squote = true;
			current += c;
		} else if (c == '"') {
			in_dquote = true;
			current += c;
		} else if (c == ';') {
			StringUtil::Trim(current);
			if (!current.empty()) {
				statements.push_back(std::move(current));
			}
			current = string();
		} else {
			current += c;
		}
	}
	StringUtil::Trim(current);
	if (!current.empty()) {
		statements.push_back(std::move(current));
	}
	return statements;
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

	auto statements = SplitBatchStatements(query);
	if (statements.size() <= 1) {
		auto result =
		    connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(query)));
		return std::move(result);
	}
	unique_ptr<QueryResult> result;
	for (auto &statement : statements) {
		result =
		    connection.Query(StringUtil::Format("CALL %s(%s, %s)", command, catalog_literal, SQLString(statement)));
		if (result->HasError()) {
			return std::move(result);
		}
	}
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

string PostgresMetadataManager::GetSnapshotAndStatsAndChangesQuery(bool include_exactness) {
	auto inner_query =
	    StringUtil::Replace(DuckLakeMetadataManager::BaseSnapshotAndStatsAndChangesQuery(include_exactness),
	                        "{METADATA_CATALOG}", "{METADATA_SCHEMA_ESCAPED}");
	inner_query = StringUtil::Replace(inner_query, "'", "''");
	return "SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, '" + inner_query + "')";
}

idx_t PostgresMetadataManager::FetchScalarSequenceValue(const string &seq_name) {
	DuckLakeSnapshot dummy {0, 0, 0, 0};
	string query = "SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, "
	               "'SELECT nextval(''{METADATA_SCHEMA_ESCAPED}." +
	               seq_name + "'')')";
	auto result = Query(dummy, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to allocate next value from " + seq_name + ": ");
	}
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InternalException("ducklake: %s returned no value from nextval()", seq_name);
	}
	auto v = chunk->data[0].GetValue(0).GetValue<int64_t>();
	if (v < 0) {
		throw InternalException("ducklake: %s returned negative value: %lld", seq_name, (long long)v);
	}
	return static_cast<idx_t>(v);
}

idx_t PostgresMetadataManager::AllocateNextSnapshotId(idx_t /*current_snapshot_id*/) {
	return FetchScalarSequenceValue("ducklake_snapshot_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextCatalogId(idx_t /*current_next_catalog_id*/) {
	return FetchScalarSequenceValue("ducklake_catalog_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextFileId(idx_t /*current_next_file_id*/) {
	return FetchScalarSequenceValue("ducklake_file_id_seq");
}

idx_t PostgresMetadataManager::AllocateNextSchemaVersion(idx_t /*current_schema_version*/) {
	return FetchScalarSequenceValue("ducklake_schema_version_seq");
}

idx_t PostgresMetadataManager::EnsureCatalogClassid() {
	if (catalog_classid.IsValid()) {
		return catalog_classid.GetIndex();
	}
	DuckLakeSnapshot dummy {};
	string probe = "SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL}, "
	               "'SELECT hashtext(''{METADATA_SCHEMA_ESCAPED}'')::int4')";
	auto probe_result = Query(dummy, probe);
	if (probe_result->HasError()) {
		probe_result->GetErrorObject().Throw("concurrent: failed to compute DuckLake advisory lock classid: ");
	}
	auto chunk = probe_result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw InternalException("ducklake: hashtext probe returned no row");
	}
	catalog_classid = static_cast<idx_t>(static_cast<uint32_t>(chunk->data[0].GetValue(0).GetValue<int32_t>()));
	return catalog_classid.GetIndex();
}

void PostgresMetadataManager::AcquireCommitLock(const TransactionChangeInformation &changes) {
	if (!RequiresCommitLock(changes)) {
		return;
	}
	DuckLakeSnapshot dummy {};
	auto classid = EnsureCatalogClassid();

	string set_timeout = "SET LOCAL lock_timeout = '30s'";
	auto timeout_res = Execute(dummy, set_timeout);
	if (timeout_res->HasError()) {
		timeout_res->GetErrorObject().Throw("concurrent: failed to set lock_timeout: ");
	}

	string query = "SELECT pg_advisory_xact_lock(" + std::to_string(static_cast<int32_t>(classid)) + ", " +
	               std::to_string(DUCKLAKE_COMMIT_ADVISORY_SUBKEY) + ")";
	auto result = Execute(dummy, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("concurrent: DuckLake commit serialization lock failed: ");
	}
}

bool PostgresMetadataManager::BootstrapObjectsPresent() {
	DuckLakeSnapshot dummy {};
	string probe = R"(
SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},
'SELECT COUNT(*)::BIGINT FROM pg_catalog.pg_class c
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname = ''{METADATA_SCHEMA_ESCAPED}''
  AND c.relname IN (
    ''ducklake_snapshot_id_seq'',''ducklake_catalog_id_seq'',
    ''ducklake_file_id_seq'',''ducklake_schema_version_seq'',
    ''ducklake_schema_name_active_uidx'',''ducklake_table_name_active_uidx'',
    ''ducklake_view_name_active_uidx'',''ducklake_delete_file_active_uidx''
  )')
)";
	auto res = Query(dummy, probe);
	if (res->HasError()) {
		return false;
	}
	auto chunk = res->Fetch();
	if (!chunk || chunk->size() == 0) {
		return false;
	}
	auto v = chunk->data[0].GetValue(0).GetValue<int64_t>();
	return v == 8;
}

void PostgresMetadataManager::EnsureIdSequences() {
	DuckLakeSnapshot dummy {0, 0, 0, 0};

	if (BootstrapObjectsPresent()) {
		return;
	}

	auto classid = EnsureCatalogClassid();
	string acquire = "SELECT pg_advisory_lock(" + std::to_string(static_cast<int32_t>(classid)) + ", " +
	                 std::to_string(DUCKLAKE_BOOTSTRAP_ADVISORY_SUBKEY) + ")";
	auto acq_res = Execute(dummy, acquire);
	if (acq_res->HasError()) {
		acq_res->GetErrorObject().Throw("concurrent: DuckLake bootstrap serialization lock failed: ");
	}
	string release = "SELECT pg_advisory_unlock(" + std::to_string(static_cast<int32_t>(classid)) + ", " +
	                 std::to_string(DUCKLAKE_BOOTSTRAP_ADVISORY_SUBKEY) + ")";
	auto release_lock = [&]() {
		auto r = Execute(dummy, release);
		(void)r;
	};

	if (BootstrapObjectsPresent()) {
		release_lock();
		return;
	}

	auto run = [&](string query) {
		auto result = Execute(dummy, query);
		if (result->HasError()) {
			release_lock();
			result->GetErrorObject().Throw("Failed to ensure DuckLake id sequences: ");
		}
	};

	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq MINVALUE 0 START WITH 0 CACHE 1");
	run("CREATE SEQUENCE IF NOT EXISTS {METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq CACHE 1");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot_id_seq),
    GREATEST(1, COALESCE((SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 1) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_catalog_id_seq),
    GREATEST(1, COALESCE((SELECT MAX(next_catalog_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(next_catalog_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_file_id_seq),
    GREATEST(0, COALESCE((SELECT MAX(next_file_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(next_file_id) - 1 FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0) >= 1
))");
	run(R"(SELECT setval(
  '{METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq',
  GREATEST(
    (SELECT last_value FROM {METADATA_SCHEMA_ESCAPED}.ducklake_schema_version_seq),
    GREATEST(1, COALESCE((SELECT MAX(schema_version) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 0))
  ),
  COALESCE((SELECT MAX(schema_version) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot), 1) >= 1
))");

	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_schema_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_schema (schema_name) "
	    "WHERE end_snapshot IS NULL");
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_table_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_table (schema_id, table_name) "
	    "WHERE end_snapshot IS NULL");
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_view_name_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_view (schema_id, view_name) "
	    "WHERE end_snapshot IS NULL");
	run("CREATE UNIQUE INDEX IF NOT EXISTS ducklake_delete_file_active_uidx "
	    "ON {METADATA_SCHEMA_ESCAPED}.ducklake_delete_file (data_file_id) "
	    "WHERE end_snapshot IS NULL");

	release_lock();
}

string PostgresMetadataManager::GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id) {
	auto native_query = GeneratePostgresNativeFileColumnStatsCTEBody(req, table_id);
	return StringUtil::Format("  SELECT * FROM postgres_query({METADATA_CATALOG_NAME_LITERAL},\n"
	                          "    %s)\n",
	                          SQLString(native_query));
}

string PostgresMetadataManager::GenerateFileListQuery(DuckLakeTableEntry &table, const FilterPushdownInfo *filter_info,
                                                      const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
                                                      const vector<idx_t> &runtime_filter_stats_columns,
                                                      FileListType file_list_type, const string &,
                                                      const FileColumnStatsCTEBodyGenerator &) {
	auto remote_query = DuckLakeMetadataManager::GenerateFileListQuery(
	    table, filter_info, dynamic_filters, runtime_filter_stats_columns, file_list_type, "{METADATA_SCHEMA_ESCAPED}",
	    GeneratePostgresNativeFileColumnStatsCTEBody);

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
