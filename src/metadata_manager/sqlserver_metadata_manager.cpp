#include "metadata_manager/sqlserver_metadata_manager.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

SQLServerMetadataManager::SQLServerMetadataManager(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

bool SQLServerMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::STRUCT:
	case LogicalTypeId::MAP:
	case LogicalTypeId::LIST:
	case LogicalTypeId::UBIGINT:
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
	case LogicalTypeId::VARIANT:
	case LogicalTypeId::GEOMETRY:
		return false;
	default:
		return true;
	}
}

string SQLServerMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	switch (column_type.id()) {
	case LogicalTypeId::BOOLEAN:
		return "BIT";
	case LogicalTypeId::UUID:
		return "UNIQUEIDENTIFIER";
	case LogicalTypeId::VARCHAR:
		return "NVARCHAR(MAX)";
	case LogicalTypeId::BLOB:
		return "VARBINARY(MAX)";
	case LogicalTypeId::FLOAT:
		return "REAL";
	case LogicalTypeId::DOUBLE:
		return "FLOAT";
	case LogicalTypeId::TINYINT:
		return "SMALLINT";
	case LogicalTypeId::UTINYINT:
	case LogicalTypeId::USMALLINT:
		return "INT";
	case LogicalTypeId::UINTEGER:
	case LogicalTypeId::UBIGINT:
		return "BIGINT";
	case LogicalTypeId::HUGEINT:
	case LogicalTypeId::UHUGEINT:
		return "NVARCHAR(128)";
	case LogicalTypeId::TIMESTAMP_TZ:
	case LogicalTypeId::TIMESTAMP_TZ_NS:
		return "DATETIMEOFFSET";
	case LogicalTypeId::TIMESTAMP:
	case LogicalTypeId::TIMESTAMP_SEC:
	case LogicalTypeId::TIMESTAMP_MS:
	case LogicalTypeId::TIMESTAMP_NS:
		return "DATETIME2";
	default:
		return column_type.ToString();
	}
}

string SQLServerMetadataManager::CastColumnToTarget(const string &column, const LogicalType &type) {
	return "CAST(" + column + " AS " + GetColumnTypeInternal(type) + ")";
}

string SQLServerMetadataManager::GetCreateTableStatements() {
	return R"(
CREATE TABLE {METADATA_CATALOG}.ducklake_metadata([key] NVARCHAR(4000) NOT NULL, value NVARCHAR(MAX) NOT NULL, scope NVARCHAR(4000), scope_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time DATETIMEOFFSET, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made NVARCHAR(MAX), author NVARCHAR(MAX), commit_message NVARCHAR(MAX), commit_extra_info NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UNIQUEIDENTIFIER, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name NVARCHAR(4000), path NVARCHAR(MAX), path_is_relative BIT);
CREATE TABLE {METADATA_CATALOG}.ducklake_table(table_id BIGINT, table_uuid UNIQUEIDENTIFIER, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name NVARCHAR(4000), path NVARCHAR(MAX), path_is_relative BIT);
CREATE TABLE {METADATA_CATALOG}.ducklake_view(view_id BIGINT, view_uuid UNIQUEIDENTIFIER, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name NVARCHAR(4000), dialect NVARCHAR(4000), sql NVARCHAR(MAX), column_aliases NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, [key] NVARCHAR(4000), value NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, [key] NVARCHAR(4000), value NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path NVARCHAR(MAX), path_is_relative BIT, file_format NVARCHAR(4000), record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key NVARCHAR(MAX), mapping_id BIGINT, partial_max BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value NVARCHAR(MAX), max_value NVARCHAR(MAX), contains_nan BIT, extra_stats NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path NVARCHAR(MAX), shredded_type NVARCHAR(4000), column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value NVARCHAR(MAX), max_value NVARCHAR(MAX), contains_nan BIT, extra_stats NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path NVARCHAR(MAX), path_is_relative BIT, format NVARCHAR(4000), delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key NVARCHAR(MAX), partial_max BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name NVARCHAR(4000), column_type NVARCHAR(4000), initial_default NVARCHAR(MAX), default_value NVARCHAR(MAX), nulls_allowed BIT, parent_column BIGINT, default_value_type NVARCHAR(4000), default_value_dialect NVARCHAR(4000));
CREATE TABLE {METADATA_CATALOG}.ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BIT, contains_nan BIT, min_value NVARCHAR(MAX), max_value NVARCHAR(MAX), extra_stats NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform NVARCHAR(4000));
CREATE TABLE {METADATA_CATALOG}.ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value NVARCHAR(MAX));
CREATE TABLE {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path NVARCHAR(MAX), path_is_relative BIT, schedule_start DATETIMEOFFSET);
CREATE TABLE {METADATA_CATALOG}.ducklake_inlined_data_tables(table_id BIGINT, table_name NVARCHAR(4000), schema_version BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type NVARCHAR(4000));
CREATE TABLE {METADATA_CATALOG}.ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name NVARCHAR(4000), target_field_id BIGINT, parent_column BIGINT, is_partition BIT);
CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name NVARCHAR(4000), begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect NVARCHAR(4000), sql NVARCHAR(MAX), type NVARCHAR(4000));
CREATE TABLE {METADATA_CATALOG}.ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT,column_id BIGINT, parameter_name NVARCHAR(4000), parameter_type NVARCHAR(4000), default_value NVARCHAR(MAX), default_value_type NVARCHAR(4000));
CREATE TABLE {METADATA_CATALOG}.ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression NVARCHAR(MAX), dialect NVARCHAR(4000), sort_direction NVARCHAR(4000), null_order NVARCHAR(4000));
)";
}

static string BuildSQLServerDynamicDDL(const string &ddl) {
	string result;
	auto statements = StringUtil::Split(ddl, "\n");
	for (auto &statement : statements) {
		StringUtil::Trim(statement);
		if (statement.empty()) {
			continue;
		}
		result += "EXEC('" + StringUtil::Replace(statement, "'", "''") + "');\n";
	}
	return result;
}

void SQLServerMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	auto snapshot = DuckLakeSnapshot();
	string create_schema_query = "IF SCHEMA_ID({METADATA_SCHEMA_NAME_LITERAL}) IS NULL EXEC('CREATE SCHEMA {METADATA_CATALOG}');";
	auto schema_result = Execute(snapshot, create_schema_query);
	if (schema_result->HasError()) {
		schema_result->GetErrorObject().Throw("Failed to initialize DuckLake: ");
	}

	string initialize_ddl = BuildSQLServerDynamicDDL(GetCreateTableStatements());
	auto ddl_result = Execute(snapshot, initialize_ddl);
	if (ddl_result->HasError()) {
		ddl_result->GetErrorObject().Throw("Failed to initialize DuckLake: ");
	}

	string initialize_query;

	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	string data_path = StorePath(base_data_path);
	string encryption_str = encryption == DuckLakeEncryption::ENCRYPTED ? "true" : "false";
	string initial_schema_uuid = transaction.GenerateUUID();
	initialize_query += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES (0, SYSDATETIMEOFFSET(), 0, 1, 0);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"',  NULL, NULL, NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_metadata ([key], value) VALUES ('version', '%s'), ('created_by', 'DuckDB %s'), ('data_path', %s), ('encrypted', '%s');
INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES (0, CAST('%s' AS UNIQUEIDENTIFIER), 0, NULL, 'main', 'main/', 1);
	)",
	                                       GetVersionString(), DuckDB::SourceID(), SQLString(data_path), encryption_str,
	                                       initial_schema_uuid);
	auto result = Execute(snapshot, initialize_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to initialize DuckLake: ");
	}

	RefreshMetadataCache();
}

void SQLServerMetadataManager::RefreshMetadataCache() {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto refresh_result =
	    transaction.GetConnection().Query(StringUtil::Format("SELECT mssql_refresh_cache(%s)", catalog_literal));
	if (refresh_result->HasError()) {
		refresh_result->GetErrorObject().Throw("Failed to refresh SQL Server metadata cache for DuckLake: ");
	}
	while (true) {
		auto chunk = refresh_result->Fetch();
		if (!chunk || chunk->size() == 0) {
			break;
		}
	}
}

void SQLServerMetadataManager::ExpandPlaceholders(DuckLakeSnapshot snapshot, string &query) {
	auto &commit_info = transaction.GetCommitInfo();

	query = StringUtil::Replace(query, "{SNAPSHOT_ID}", to_string(snapshot.snapshot_id));
	query = StringUtil::Replace(query, "{SCHEMA_VERSION}", to_string(snapshot.schema_version));
	query = StringUtil::Replace(query, "{NEXT_CATALOG_ID}", to_string(snapshot.next_catalog_id));
	query = StringUtil::Replace(query, "{NEXT_FILE_ID}", to_string(snapshot.next_file_id));
	query = StringUtil::Replace(query, "{AUTHOR}", commit_info.author.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_MESSAGE}", commit_info.commit_message.ToSQLString());
	query = StringUtil::Replace(query, "{COMMIT_EXTRA_INFO}", commit_info.commit_extra_info.ToSQLString());

	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataDatabaseName());
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	auto schema_identifier = DuckLakeUtil::SQLIdentifierToString(ducklake_catalog.MetadataSchemaName());
	auto schema_identifier_escaped = StringUtil::Replace(schema_identifier, "'", "''");
	auto schema_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataSchemaName());
	auto metadata_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataPath());
	auto data_path = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.DataPath());

	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_LITERAL}", catalog_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG_NAME_IDENTIFIER}", catalog_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_NAME_LITERAL}", schema_literal);
	query = StringUtil::Replace(query, "{METADATA_CATALOG}", schema_identifier);
	query = StringUtil::Replace(query, "{METADATA_SCHEMA_ESCAPED}", schema_identifier_escaped);
	query = StringUtil::Replace(query, "{METADATA_PATH}", metadata_path);
	query = StringUtil::Replace(query, "{DATA_PATH}", data_path);
}

unique_ptr<QueryResult> SQLServerMetadataManager::ExecuteQuery(DuckLakeSnapshot snapshot, string &query,
                                                               bool returns_rows) {
	ExpandPlaceholders(snapshot, query);

	auto &connection = transaction.GetConnection();
	auto &ducklake_catalog = transaction.GetCatalog();
	auto catalog_literal = DuckLakeUtil::SQLLiteralToString(ducklake_catalog.MetadataDatabaseName());
	if (returns_rows) {
		return connection.Query(StringUtil::Format("SELECT * FROM mssql_scan(%s, %s)", catalog_literal, SQLString(query)));
	}
	return connection.Query(StringUtil::Format("SELECT mssql_exec(%s, %s)", catalog_literal, SQLString(query)));
}

unique_ptr<QueryResult> SQLServerMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	auto result = ExecuteQuery(snapshot, query, false);
	if (!result->HasError()) {
		while (true) {
			auto chunk = result->Fetch();
			if (!chunk || chunk->size() == 0) {
				break;
			}
		}
	}
	return result;
}

unique_ptr<QueryResult> SQLServerMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return ExecuteQuery(snapshot, query, true);
}

string SQLServerMetadataManager::InsertSnapshot() {
	return R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES ({SNAPSHOT_ID}, SYSDATETIMEOFFSET(), {SCHEMA_VERSION}, {NEXT_CATALOG_ID}, {NEXT_FILE_ID});)";
}

string SQLServerMetadataManager::GetLatestSnapshotQuery() const {
	string query = R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot);
)";
	return StringUtil::Format("SELECT * FROM mssql_scan({METADATA_CATALOG_NAME_LITERAL}, %s)", SQLString(query));
}

static unique_ptr<DuckLakeSnapshot> TryGetSQLServerSnapshot(QueryResult &result) {
	unique_ptr<DuckLakeSnapshot> snapshot;
	for (auto &row : result) {
		if (snapshot) {
			throw InvalidInputException("Corrupt DuckLake - multiple snapshots returned from database");
		}
		auto snapshot_id = row.GetValue<idx_t>(0);
		auto schema_version = row.GetValue<idx_t>(1);
		auto next_catalog_id = row.GetValue<idx_t>(2);
		auto next_file_id = row.GetValue<idx_t>(3);
		snapshot = make_uniq<DuckLakeSnapshot>(snapshot_id, schema_version, next_catalog_id, next_file_id);
	}
	return snapshot;
}

unique_ptr<DuckLakeSnapshot> SQLServerMetadataManager::GetSnapshot() {
	RefreshMetadataCache();
	auto result = transaction.Query(GetLatestSnapshotQuery());
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to query most recent snapshot for DuckLake: ");
	}
	auto snapshot = TryGetSQLServerSnapshot(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found in DuckLake");
	}
	return snapshot;
}

unique_ptr<DuckLakeSnapshot> SQLServerMetadataManager::GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound) {
	RefreshMetadataCache();
	auto &unit = at_clause.Unit();
	auto &val = at_clause.GetValue();
	unique_ptr<QueryResult> result;
	if (StringUtil::CIEquals(unit, "version")) {
		string query = StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
WHERE snapshot_id = %llu;)",
		                                  val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>());
		result = transaction.Query(
		    StringUtil::Format("SELECT * FROM mssql_scan({METADATA_CATALOG_NAME_LITERAL}, %s)", SQLString(query)));
	} else if (StringUtil::CIEquals(unit, "timestamp")) {
		const string timestamp_order = bound == SnapshotBound::LOWER_BOUND ? "ASC" : "DESC";
		const string timestamp_condition = bound == SnapshotBound::LOWER_BOUND ? ">" : "<";
		string query = StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_SCHEMA_ESCAPED}.ducklake_snapshot
WHERE snapshot_time %s= CAST(%s AS DATETIMEOFFSET)
ORDER BY snapshot_time %s
OFFSET 0 ROWS FETCH NEXT 1 ROWS ONLY;)",
		                                  timestamp_condition, val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString(),
		                                  timestamp_order);
		result = transaction.Query(
		    StringUtil::Format("SELECT * FROM mssql_scan({METADATA_CATALOG_NAME_LITERAL}, %s)", SQLString(query)));
	} else {
		throw InvalidInputException("Unsupported AT clause unit - %s", unit);
	}
	if (result->HasError()) {
		result->GetErrorObject().Throw(StringUtil::Format(
		    "Failed to query snapshot at %s %s for DuckLake: ", StringUtil::Lower(unit), val.ToString()));
	}
	auto snapshot = TryGetSQLServerSnapshot(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found at %s %s", StringUtil::Lower(unit), val.ToString());
	}
	return snapshot;
}

} // namespace duckdb
