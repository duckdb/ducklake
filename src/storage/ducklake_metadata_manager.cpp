#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/planner/tableref/bound_at_clause.hpp"
#include "duckdb/common/types/blob.hpp"
#include "storage/ducklake_catalog.hpp"
#include "common/ducklake_types.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "duckdb.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/null_filter.hpp"
#include "duckdb/planner/filter/optional_filter.hpp"
#include "duckdb/planner/filter/dynamic_filter.hpp"
#include "duckdb/planner/filter/in_filter.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/expression/cast_expression.hpp"

namespace duckdb {

DuckLakeMetadataManager::DuckLakeMetadataManager(DuckLakeTransaction &transaction) : transaction(transaction) {
}

DuckLakeMetadataManager::~DuckLakeMetadataManager() {
}
optional_ptr<AttachedDatabase> GetDatabase(ClientContext &context, const string &name);

unordered_map<string /* name */, DuckLakeMetadataManager::create_t> DuckLakeMetadataManager::metadata_managers = {
    {"postgres", PostgresMetadataManager::Create},
    {"postgres_scanner", PostgresMetadataManager::Create},
    {"sqlite", SQLiteMetadataManager::Create},
    {"sqlite_scanner", SQLiteMetadataManager::Create}};

mutex DuckLakeMetadataManager::metadata_managers_lock;

void DuckLakeMetadataManager::Register(const string &name, DuckLakeMetadataManager::create_t create) {
	lock_guard<mutex> lock(metadata_managers_lock);
	if (metadata_managers.find(name) != metadata_managers.end()) {
		throw InternalException("Metadata manager with name \"%s\" already exists!", name);
	}
	metadata_managers[name] = create;
}

unique_ptr<DuckLakeMetadataManager> DuckLakeMetadataManager::Create(DuckLakeTransaction &transaction) {
	lock_guard<mutex> lock(metadata_managers_lock);
	auto &catalog = transaction.GetCatalog();
	auto catalog_type = catalog.MetadataType();
	auto metadata_manager_iter = metadata_managers.find(catalog_type);
	if (metadata_manager_iter != metadata_managers.end()) {
		auto create = metadata_manager_iter->second;
		return create(transaction);
	}
	return make_uniq<DuckLakeMetadataManager>(transaction);
}

DuckLakeMetadataManager &DuckLakeMetadataManager::Get(DuckLakeTransaction &transaction) {
	return transaction.GetMetadataManager();
}

bool DuckLakeMetadataManager::TypeIsNativelySupported(const LogicalType &type) {
	return true;
}

FileSystem &DuckLakeMetadataManager::GetFileSystem() {
	return FileSystem::GetFileSystem(transaction.GetCatalog().GetDatabase());
}

string DuckLakeMetadataManager::ListAggregation(const vector<pair<string, string>> &fields) const {
	// DuckDB syntax: LIST({'key1': val1, 'key2': val2, ...})
	string fields_part;
	for (auto const &entry : fields) {
		if (!fields_part.empty()) {
			fields_part += ", ";
		}
		fields_part += "'" + entry.first + "': " + entry.second;
	}
	return "LIST({" + fields_part + "})";
}

void DuckLakeMetadataManager::InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) {
	string initialize_query;
	if (has_explicit_schema) {
		// if the schema is user provided create it
		initialize_query += "CREATE SCHEMA IF NOT EXISTS {METADATA_CATALOG};\n";
	}
	// we default to false unless explicitly specified otherwise
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	string data_path = StorePath(base_data_path);
	string encryption_str = encryption == DuckLakeEncryption::ENCRYPTED ? "true" : "false";
	initialize_query += StringUtil::Format(R"(
CREATE TABLE {METADATA_CATALOG}.ducklake_metadata(key VARCHAR NOT NULL, value VARCHAR NOT NULL, scope VARCHAR, scope_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(snapshot_id BIGINT PRIMARY KEY, snapshot_time TIMESTAMPTZ, schema_version BIGINT, next_catalog_id BIGINT, next_file_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(snapshot_id BIGINT PRIMARY KEY, changes_made VARCHAR, author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, end_snapshot BIGINT, schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_tag(object_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_column_tag(table_id BIGINT, column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR,  mapping_id BIGINT, partial_max BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_column(column_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, table_id BIGINT, column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_table_stats(table_id BIGINT, record_count BIGINT, next_row_id BIGINT, file_size_bytes BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(partition_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_partition_column(partition_id BIGINT, table_id BIGINT, partition_key_index BIGINT, column_id BIGINT, transform VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_file_partition_value(data_file_id BIGINT, table_id BIGINT, partition_key_index BIGINT, partition_value VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion(data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, schedule_start TIMESTAMPTZ);
CREATE TABLE {METADATA_CATALOG}.ducklake_inlined_data_tables(table_id BIGINT, table_name VARCHAR, schema_version BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT, is_partition BOOLEAN);
CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, table_id BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, sql VARCHAR, type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT,column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {METADATA_CATALOG}.ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES (0, NOW(), 0, 1, 0);
INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES (0, 'created_schema:"main"',  NULL, NULL, NULL);
INSERT INTO {METADATA_CATALOG}.ducklake_metadata (key, value) VALUES ('version', '0.4-dev1'), ('created_by', 'DuckDB %s'), ('data_path', %s), ('encrypted', '%s');
INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES (0, UUID(), 0, NULL, 'main', 'main/', true);
	)",
	                                       DuckDB::SourceID(), SQLString(data_path), encryption_str);
	auto result = transaction.Query(initialize_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to initialize DuckLake:");
	}
}

void DuckLakeMetadataManager::MigrateV01() {
	string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_schema ADD COLUMN path VARCHAR DEFAULT '';
ALTER TABLE {METADATA_CATALOG}.ducklake_schema ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;
ALTER TABLE {METADATA_CATALOG}.ducklake_table ADD COLUMN path VARCHAR DEFAULT '';
ALTER TABLE {METADATA_CATALOG}.ducklake_table ADD COLUMN path_is_relative BOOLEAN DEFAULT TRUE;
ALTER TABLE {METADATA_CATALOG}.ducklake_metadata ADD COLUMN scope VARCHAR;
ALTER TABLE {METADATA_CATALOG}.ducklake_metadata ADD COLUMN scope_id BIGINT;
ALTER TABLE {METADATA_CATALOG}.ducklake_data_file ADD COLUMN mapping_id BIGINT;
CREATE TABLE {METADATA_CATALOG}.ducklake_column_mapping(mapping_id BIGINT, table_id BIGINT, type VARCHAR);
CREATE TABLE {METADATA_CATALOG}.ducklake_name_mapping(mapping_id BIGINT, column_id BIGINT, source_name VARCHAR, target_field_id BIGINT, parent_column BIGINT);
UPDATE {METADATA_CATALOG}.ducklake_partition_column SET column_id = (SELECT LIST(column_id ORDER BY column_order) FROM {METADATA_CATALOG}.ducklake_column WHERE table_id = ducklake_partition_column.table_id AND parent_column IS NULL AND end_snapshot IS NULL)[ducklake_partition_column.column_id + 1];
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.2' WHERE key = 'version';
	)";
	auto result = transaction.Query(migrate_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to migrate DuckLake from v0.1 to v0.2:");
	}
}

void DuckLakeMetadataManager::ExecuteMigration(string migrate_query, bool allow_failures) {
	if (allow_failures) {
		migrate_query = StringUtil::Replace(migrate_query, "{IF_NOT_EXISTS}", "IF NOT EXISTS");
		migrate_query = StringUtil::Replace(migrate_query, "{IF_EXISTS}", "IF EXISTS");
		migrate_query =
		    StringUtil::Replace(migrate_query, "{WHERE_EMPTY}",
		                        "WHERE NOT EXISTS (SELECT 1 FROM {METADATA_CATALOG}.ducklake_schema_versions);");
	} else {
		// All our place-holders are empty, so if any of these exist it will fail
		migrate_query = StringUtil::Replace(migrate_query, "{IF_NOT_EXISTS}", "");
		migrate_query = StringUtil::Replace(migrate_query, "{IF_EXISTS}", "");
		migrate_query = StringUtil::Replace(migrate_query, "{WHERE_EMPTY}", "");
	}
	auto result = transaction.Query(migrate_query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to migrate DuckLake from v0.2 to v0.3:");
	}
}

void DuckLakeMetadataManager::MigrateV02(bool allow_failures) {
	string migrate_query = R"(
ALTER TABLE {METADATA_CATALOG}.ducklake_name_mapping ADD COLUMN {IF_NOT_EXISTS} is_partition BOOLEAN DEFAULT false;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN {IF_NOT_EXISTS} author VARCHAR DEFAULT NULL;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN {IF_NOT_EXISTS} commit_message VARCHAR DEFAULT NULL;
ALTER TABLE {METADATA_CATALOG}.ducklake_snapshot_changes ADD COLUMN {IF_NOT_EXISTS} commit_extra_info VARCHAR DEFAULT NULL;
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.3' WHERE key = 'version';
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT);
INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions SELECT * FROM (SELECT MIN(snapshot_id), schema_version FROM {METADATA_CATALOG}.ducklake_snapshot GROUP BY schema_version ORDER BY schema_version) t {WHERE_EMPTY};
ALTER TABLE {IF_EXISTS} {METADATA_CATALOG}.ducklake_file_column_statistics RENAME TO ducklake_file_column_stats;
ALTER TABLE {METADATA_CATALOG}.ducklake_file_column_stats ADD COLUMN {IF_NOT_EXISTS} extra_stats VARCHAR DEFAULT NULL;
ALTER TABLE {METADATA_CATALOG}.ducklake_table_column_stats ADD COLUMN {IF_NOT_EXISTS} extra_stats VARCHAR DEFAULT NULL;
	)";
	ExecuteMigration(migrate_query, allow_failures);
}

void DuckLakeMetadataManager::MigrateV03(bool allow_failures) {
	string migrate_query = R"(
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_macro_impl(macro_id BIGINT, impl_id BIGINT, dialect VARCHAR, sql VARCHAR, type VARCHAR);
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_macro_parameters(macro_id BIGINT, impl_id BIGINT,column_id BIGINT, parameter_name VARCHAR, parameter_type VARCHAR, default_value VARCHAR, default_value_type VARCHAR);
ALTER TABLE {METADATA_CATALOG}.ducklake_column ADD COLUMN {IF_NOT_EXISTS} default_value_type VARCHAR DEFAULT 'literal';
ALTER TABLE {METADATA_CATALOG}.ducklake_column ADD COLUMN {IF_NOT_EXISTS} default_value_dialect VARCHAR DEFAULT NULL;
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT);
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_sort_expression(sort_id BIGINT, table_id BIGINT, sort_key_index BIGINT, expression VARCHAR, dialect VARCHAR, sort_direction VARCHAR, null_order VARCHAR);
CREATE TABLE {IF_NOT_EXISTS} {METADATA_CATALOG}.ducklake_file_variant_stats(data_file_id BIGINT, table_id BIGINT, column_id BIGINT, variant_path VARCHAR, shredded_type VARCHAR, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);
ALTER TABLE {METADATA_CATALOG}.ducklake_schema_versions ADD COLUMN {IF_NOT_EXISTS} table_id BIGINT;
ALTER TABLE {METADATA_CATALOG}.ducklake_data_file ADD COLUMN {IF_NOT_EXISTS} partial_max BIGINT;
ALTER TABLE {METADATA_CATALOG}.ducklake_delete_file ADD COLUMN {IF_NOT_EXISTS} partial_max BIGINT;
CREATE TEMP TABLE {IF_NOT_EXISTS} __ducklake_partial_max_migration AS
SELECT data_file_id, TRY_CAST(regexp_extract(partial_file_info, 'partial_max:(\d+)', 1) AS BIGINT) AS partial_max
FROM {METADATA_CATALOG}.ducklake_data_file
WHERE partial_file_info IS NOT NULL AND partial_file_info LIKE '%partial_max:%';
ALTER TABLE {METADATA_CATALOG}.ducklake_data_file DROP COLUMN {IF_EXISTS} partial_file_info;
UPDATE {METADATA_CATALOG}.ducklake_data_file AS df
SET partial_max = m.partial_max
FROM __ducklake_partial_max_migration m
WHERE df.data_file_id = m.data_file_id;
DROP TABLE IF EXISTS __ducklake_partial_max_migration;
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value = '0.4-dev1' WHERE key = 'version';
	)";
	ExecuteMigration(migrate_query, allow_failures);

	auto migrate_schema_versions = transaction.Query(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions (table_id, begin_snapshot, schema_version)
SELECT t.table_id, t.begin_snapshot, sv.schema_version
FROM {METADATA_CATALOG}.ducklake_schema_versions sv
JOIN {METADATA_CATALOG}.ducklake_table t
  ON sv.begin_snapshot BETWEEN t.begin_snapshot
                           AND COALESCE(t.end_snapshot, sv.begin_snapshot);
)");
	if (migrate_schema_versions->HasError()) {
		if (!allow_failures) {
			migrate_schema_versions->GetErrorObject().Throw(
			    "Failed to migrate schema_versions to per-table tracking: ");
		}
	}
	auto delete_global_entries = transaction.Query(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_schema_versions WHERE table_id IS NULL;
)");
	if (delete_global_entries->HasError()) {
		if (!allow_failures) {
			delete_global_entries->GetErrorObject().Throw("Failed to clean up global schema_versions entries: ");
		}
	}
}

DuckLakeMetadata DuckLakeMetadataManager::LoadDuckLake() {
	auto result = transaction.Query(R"(
SELECT key, value, scope, scope_id FROM {METADATA_CATALOG}.ducklake_metadata
)");
	if (result->HasError()) {
		// we might be loading from a v0.1 database - if so we don't have scope yet
		result = transaction.Query(R"(
SELECT key, value FROM {METADATA_CATALOG}.ducklake_metadata
)");
		if (result->HasError()) {
			auto &error_obj = result->GetErrorObject();
			error_obj.Throw("Failed to load existing DuckLake: ");
		}
	}
	DuckLakeMetadata metadata;
	for (auto &row : *result) {
		DuckLakeTag tag;
		tag.key = row.GetValue<string>(0);
		tag.value = row.GetValue<string>(1);
		if (result->ColumnCount() == 2 || row.IsNull(2)) {
			// scope is NULL: global tag
			// global tag
			metadata.tags.push_back(std::move(tag));
			continue;
		}
		auto scope = row.GetValue<string>(2);
		if (scope == "schema") {
			// schema-level setting
			DuckLakeSchemaSetting schema_setting;
			schema_setting.schema_id = SchemaIndex(row.GetValue<idx_t>(3));
			schema_setting.tag = std::move(tag);
			metadata.schema_settings.push_back(std::move(schema_setting));
			continue;
		}
		if (scope == "table") {
			// table-level setting
			DuckLakeTableSetting table_setting;
			table_setting.table_id = TableIndex(row.GetValue<idx_t>(3));
			table_setting.tag = std::move(tag);
			metadata.table_settings.push_back(std::move(table_setting));
			continue;
		}
		throw InvalidInputException("Unsupported setting scope %s - only schema/table are supported", scope);
	}
	return metadata;
}

static bool AddChildColumn(vector<DuckLakeColumnInfo> &columns, FieldIndex parent_id, DuckLakeColumnInfo &column_info) {
	for (auto &col : columns) {
		if (col.id == parent_id) {
			col.children.push_back(std::move(column_info));
			return true;
		}
		if (AddChildColumn(col.children, parent_id, column_info)) {
			return true;
		}
	}
	return false;
}

vector<DuckLakeTag> DuckLakeMetadataManager::LoadTags(const Value &tag_map) const {
	vector<DuckLakeTag> result;
	for (auto &tag : ListValue::GetChildren(tag_map)) {
		auto &struct_children = StructValue::GetChildren(tag);
		if (struct_children[1].IsNull()) {
			continue;
		}
		DuckLakeTag tag_info;
		tag_info.key = struct_children[0].ToString();
		tag_info.value = struct_children[1].ToString();
		result.push_back(std::move(tag_info));
	}
	return result;
}

vector<DuckLakeInlinedTableInfo> DuckLakeMetadataManager::LoadInlinedDataTables(const Value &list) const {
	vector<DuckLakeInlinedTableInfo> result;
	for (auto &val : ListValue::GetChildren(list)) {
		auto &struct_children = StructValue::GetChildren(val);
		DuckLakeInlinedTableInfo inlined_data_table;
		inlined_data_table.table_name = StringValue::Get(struct_children[0]);
		inlined_data_table.schema_version = struct_children[1].GetValue<idx_t>();
		result.push_back(std::move(inlined_data_table));
	}
	return result;
}

vector<DuckLakeMacroImplementation> DuckLakeMetadataManager::LoadMacroImplementations(const Value &list) const {
	vector<DuckLakeMacroImplementation> result;
	for (auto &val : ListValue::GetChildren(list)) {
		auto &struct_children = StructValue::GetChildren(val);
		DuckLakeMacroImplementation impl_info;
		impl_info.dialect = StringValue::Get(struct_children[0]);
		impl_info.sql = StringValue::Get(struct_children[1]);
		impl_info.type = StringValue::Get(struct_children[2]);
		auto param_list = struct_children[3].GetValue<Value>();
		if (!param_list.IsNull()) {
			for (auto &param_value : ListValue::GetChildren(param_list)) {
				auto &param_struct_children = StructValue::GetChildren(param_value);
				DuckLakeMacroParameters param;
				param.parameter_name = StringValue::Get(param_struct_children[0]);
				param.parameter_type = StringValue::Get(param_struct_children[1]);
				param.default_value = StringValue::Get(param_struct_children[2]);
				param.default_value_type = StringValue::Get(param_struct_children[3]);
				impl_info.parameters.push_back(std::move(param));
			}
		}

		result.push_back(std::move(impl_info));
	}
	return result;
}

idx_t DuckLakeMetadataManager::GetBeginSnapshotForTable(TableIndex table_id) {
	string query = R"(
SELECT begin_snapshot
FROM {METADATA_CATALOG}.ducklake_table
WHERE table_id = {TABLE_ID})";
	query = StringUtil::Replace(query, "{TABLE_ID}", to_string(table_id.index)).c_str();
	auto result = transaction.Query(query);
	for (auto &row : *result) {
		return row.GetValue<idx_t>(0);
	}
	throw InternalException("Table %llu does not exist", table_id.index);
}

idx_t DuckLakeMetadataManager::GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot) {
	// Compute sum(record_count) - sum(delete_count) - inlined_deletions in a single query
	// Delete files are only counted if their corresponding data file is still visible
	// This handles TRUNCATE correctly: when a data file's end_snapshot is set,
	// its associated delete files are no longer counted

	// Check if inlined deletion table exists
	auto inlined_table_name = GetInlinedDeletionTableName(table_id, snapshot);

	string inlined_deletion_subquery = "0";
	if (!inlined_table_name.empty()) {
		inlined_deletion_subquery = StringUtil::Format(R"(
COALESCE((SELECT COUNT(*) FROM {METADATA_CATALOG}.%s del
          JOIN {METADATA_CATALOG}.ducklake_data_file data ON del.file_id = data.data_file_id
          WHERE del.begin_snapshot <= {SNAPSHOT_ID}
            AND data.table_id = {TABLE_ID}
            AND {SNAPSHOT_ID} >= data.begin_snapshot
            AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)), 0))",
		                                               inlined_table_name);
	}

	string query = StringUtil::Format(R"(
SELECT
  COALESCE((SELECT SUM(record_count) FROM {METADATA_CATALOG}.ducklake_data_file
            WHERE table_id = {TABLE_ID}
              AND {SNAPSHOT_ID} >= begin_snapshot
              AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)), 0)
  -
  COALESCE((SELECT SUM(del.delete_count) FROM {METADATA_CATALOG}.ducklake_delete_file del
            JOIN {METADATA_CATALOG}.ducklake_data_file data ON del.data_file_id = data.data_file_id
            WHERE del.table_id = {TABLE_ID}
              AND {SNAPSHOT_ID} >= del.begin_snapshot
              AND ({SNAPSHOT_ID} < del.end_snapshot OR del.end_snapshot IS NULL)
              AND {SNAPSHOT_ID} >= data.begin_snapshot
              AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)), 0)
  -
  %s)",
	                                  inlined_deletion_subquery);
	query = StringUtil::Replace(query, "{TABLE_ID}", to_string(table_id.index));
	auto result = transaction.Query(snapshot, query);
	for (auto &row : *result) {
		return row.GetValue<idx_t>(0);
	}
	return 0;
}

idx_t DuckLakeMetadataManager::GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot) {
	string query = StringUtil::Format(R"(
SELECT COUNT(*)
FROM {METADATA_CATALOG}.%s
WHERE {SNAPSHOT_ID} >= begin_snapshot
  AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL))",
	                                  inlined_table_name);
	auto result = transaction.Query(snapshot, query);
	for (auto &row : *result) {
		return row.GetValue<idx_t>(0);
	}
	return 0;
}

DuckLakeCatalogInfo DuckLakeMetadataManager::GetCatalogForSnapshot(DuckLakeSnapshot snapshot) {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto &base_data_path = ducklake_catalog.DataPath();
	DuckLakeCatalogInfo catalog;
	// load the schema information
	auto result = transaction.Query(snapshot, R"(
SELECT schema_id, schema_uuid::VARCHAR, schema_name, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get schema information from DuckLake: ");
	}
	map<SchemaIndex, idx_t> schema_map;
	for (auto &row : *result) {
		DuckLakeSchemaInfo schema;
		schema.id = SchemaIndex(row.GetValue<uint64_t>(0));
		schema.uuid = row.GetValue<string>(1);
		schema.name = row.GetValue<string>(2);
		if (row.IsNull(3)) {
			// no path provided - fallback to base data path
			schema.path = base_data_path;
		} else {
			// path is provided - load it
			DuckLakePath path;
			path.path = row.GetValue<string>(3);
			path.path_is_relative = row.GetValue<bool>(4);

			schema.path = FromRelativePath(path);
		}
		schema_map[schema.id] = catalog.schemas.size();
		catalog.schemas.push_back(std::move(schema));
	}

	static const vector<pair<string, string>> TAG_FIELDS = {
	    {"key", "key"},
	    {"value", "value"},
	};
	static const vector<pair<string, string>> INLINED_DATA_TABLES_FIELDS = {
	    {"name", "table_name"},
	    {"schema_version", "schema_version"},
	};

	// load the table information
	result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT schema_id, tbl.table_id, table_uuid::VARCHAR, table_name,
	(
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=table_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag,
	(
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_inlined_data_tables inlined_data_tables
		WHERE inlined_data_tables.table_id = tbl.table_id
	) AS inlined_data_tables,
	path, path_is_relative,
	col.column_id, column_name, column_type, initial_default, default_value, nulls_allowed, parent_column,
	(
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_column_tag col_tag
		WHERE col_tag.table_id=tbl.table_id AND col_tag.column_id=col.column_id AND
		      {SNAPSHOT_ID} >= col_tag.begin_snapshot AND ({SNAPSHOT_ID} < col_tag.end_snapshot OR col_tag.end_snapshot IS NULL)
	) AS column_tags, default_value_type
FROM {METADATA_CATALOG}.ducklake_table tbl
LEFT JOIN {METADATA_CATALOG}.ducklake_column col USING (table_id)
WHERE {SNAPSHOT_ID} >= tbl.begin_snapshot AND ({SNAPSHOT_ID} < tbl.end_snapshot OR tbl.end_snapshot IS NULL)
  AND (({SNAPSHOT_ID} >= col.begin_snapshot AND ({SNAPSHOT_ID} < col.end_snapshot OR col.end_snapshot IS NULL)) OR column_id IS NULL)
ORDER BY table_id, parent_column NULLS FIRST, column_order
)",
	                                                        ListAggregation(TAG_FIELDS),
	                                                        ListAggregation(INLINED_DATA_TABLES_FIELDS),
	                                                        ListAggregation(TAG_FIELDS)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table information from DuckLake: ");
	}
	const idx_t COLUMN_INDEX_START = 8;
	auto &tables = catalog.tables;
	for (auto &row : *result) {
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		// check if this column belongs to the current table or not
		if (tables.empty() || tables.back().id != table_id) {
			// new table
			DuckLakeTableInfo table_info;
			table_info.id = table_id;
			table_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(0));
			table_info.uuid = row.GetValue<string>(2);
			table_info.name = row.GetValue<string>(3);
			if (!row.IsNull(4)) {
				auto tags = row.GetValue<Value>(4);
				table_info.tags = LoadTags(tags);
			}
			if (!row.IsNull(5)) {
				auto inlined_data_tables = row.GetValue<Value>(5);
				table_info.inlined_data_tables = LoadInlinedDataTables(inlined_data_tables);
			}
			// find the schema
			auto schema_entry = schema_map.find(table_info.schema_id);
			if (schema_entry == schema_map.end()) {
				throw InvalidInputException(
				    "Failed to load DuckLake - table with id %d references schema id %d that does not exist",
				    table_info.id.index, table_info.schema_id.index);
			}
			auto &schema = catalog.schemas[schema_entry->second];
			if (row.IsNull(6)) {
				// no path provided - fallback to schema path
				table_info.path = schema.path;
			} else {
				// path is provided - load it
				DuckLakePath path;
				path.path = row.GetValue<string>(6);
				path.path_is_relative = row.GetValue<bool>(7);

				table_info.path = FromRelativePath(path, schema.path);
			}
			tables.push_back(std::move(table_info));
		}
		auto &table_entry = tables.back();
		if (row.GetValue<Value>(COLUMN_INDEX_START).IsNull()) {
			throw InvalidInputException("Failed to load DuckLake - Table entry \"%s\" does not have any columns",
			                            table_entry.name);
		}
		DuckLakeColumnInfo column_info;
		column_info.id = FieldIndex(row.GetValue<uint64_t>(COLUMN_INDEX_START));
		column_info.name = row.GetValue<string>(COLUMN_INDEX_START + 1);
		column_info.type = row.GetValue<string>(COLUMN_INDEX_START + 2);
		if (!row.IsNull(COLUMN_INDEX_START + 3)) {
			column_info.initial_default = Value(row.GetValue<string>(COLUMN_INDEX_START + 3));
		}
		if (!row.IsNull(COLUMN_INDEX_START + 4)) {
			auto value = row.GetValue<string>(COLUMN_INDEX_START + 4);
			if (value == "NULL") {
				column_info.default_value = Value();
			} else {
				column_info.default_value = Value(value);
			}
		}
		if (!row.IsNull(COLUMN_INDEX_START + 8)) {
			column_info.default_value_type = row.GetValue<string>(COLUMN_INDEX_START + 8);
		}
		column_info.nulls_allowed = row.GetValue<bool>(COLUMN_INDEX_START + 5);
		if (!row.IsNull(COLUMN_INDEX_START + 7)) {
			auto tags = row.GetValue<Value>(COLUMN_INDEX_START + 7);
			column_info.tags = LoadTags(tags);
		}

		if (row.IsNull(COLUMN_INDEX_START + 6)) {
			// base column - add the column to this table
			table_entry.columns.push_back(std::move(column_info));
		} else {
			auto parent_id = FieldIndex(row.GetValue<idx_t>(COLUMN_INDEX_START + 6));
			if (!AddChildColumn(table_entry.columns, parent_id, column_info)) {
				throw InvalidInputException("Failed to load DuckLake - Could not find parent column for column %s",
				                            column_info.name);
			}
		}
	}
	// load view information
	result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT view_id, view_uuid, schema_id, view_name, dialect, sql, column_aliases,
	(
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_tag tag
		WHERE object_id=view_id AND
		      {SNAPSHOT_ID} >= tag.begin_snapshot AND ({SNAPSHOT_ID} < tag.end_snapshot OR tag.end_snapshot IS NULL)
	) AS tag
FROM {METADATA_CATALOG}.ducklake_view view
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < view.end_snapshot OR view.end_snapshot IS NULL)
)",
	                                                        ListAggregation(TAG_FIELDS)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &views = catalog.views;
	for (auto &row : *result) {
		DuckLakeViewInfo view_info;
		view_info.id = TableIndex(row.GetValue<uint64_t>(0));
		view_info.uuid = row.GetValue<string>(1);
		view_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(2));
		view_info.name = row.GetValue<string>(3);
		view_info.dialect = row.GetValue<string>(4);
		view_info.sql = row.GetValue<string>(5);
		view_info.column_aliases = DuckLakeUtil::ParseQuotedList(row.GetValue<string>(6));
		if (!row.IsNull(7)) {
			auto tags = row.GetValue<Value>(7);
			view_info.tags = LoadTags(tags);
		}
		views.push_back(std::move(view_info));
	}

	static const vector<pair<string, string>> MACRO_PARAM_FIELDS = {{"parameter_name", "parameter_name"},
	                                                                {"parameter_type", "parameter_type"},
	                                                                {"default_value", "default_value"},
	                                                                {"default_value_type", "default_value_type"}};
	auto macro_param_query = StringUtil::Format(R"(
		(
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_macro_parameters
		WHERE ducklake_macro_impl.macro_id = ducklake_macro_parameters.macro_id
		AND ducklake_macro_impl.impl_id = ducklake_macro_parameters.impl_id
		)
	)",
	                                            ListAggregation(MACRO_PARAM_FIELDS));
	const vector<pair<string, string>> MACRO_IMPL_FIELDS = {
	    {"dialect", "dialect"}, {"sql", "sql"}, {"type", "type"}, {"params", macro_param_query}};

	// load macro information
	result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT schema_id, ducklake_macro.macro_id, macro_name, (
		SELECT %s
		FROM {METADATA_CATALOG}.ducklake_macro_impl
		WHERE ducklake_macro.macro_id = ducklake_macro_impl.macro_id
	) AS impl
FROM {METADATA_CATALOG}.ducklake_macro
WHERE  {SNAPSHOT_ID} >= ducklake_macro.begin_snapshot AND ({SNAPSHOT_ID} < ducklake_macro.end_snapshot OR ducklake_macro.end_snapshot IS NULL)
)",
	                                                        ListAggregation(MACRO_IMPL_FIELDS)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get macro information from DuckLake: ");
	}
	auto &macros = catalog.macros;
	for (auto &row : *result) {
		DuckLakeMacroInfo macro_info;
		macro_info.schema_id = SchemaIndex(row.GetValue<uint64_t>(0));
		macro_info.macro_id = MacroIndex(row.GetValue<uint64_t>(1));
		macro_info.macro_name = row.GetValue<string>(2);
		auto macro_implementations = row.GetValue<Value>(3);
		macro_info.implementations = LoadMacroImplementations(macro_implementations);
		macros.push_back(std::move(macro_info));
	}

	// load partition information
	result = transaction.Query(snapshot, R"(
SELECT partition_id, part.table_id, partition_key_index, column_id, transform
FROM {METADATA_CATALOG}.ducklake_partition_info part
JOIN {METADATA_CATALOG}.ducklake_partition_column part_col USING (partition_id)
WHERE {SNAPSHOT_ID} >= part.begin_snapshot AND ({SNAPSHOT_ID} < part.end_snapshot OR part.end_snapshot IS NULL)
ORDER BY part.table_id, partition_id, partition_key_index
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get partition information from DuckLake: ");
	}
	auto &partitions = catalog.partitions;
	for (auto &row : *result) {
		auto partition_id = row.GetValue<uint64_t>(0);
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		if (partitions.empty() || partitions.back().table_id != table_id) {
			DuckLakePartitionInfo partition_info;
			partition_info.id = partition_id;
			partition_info.table_id = table_id;
			partitions.push_back(std::move(partition_info));
		}
		auto &partition_entry = partitions.back();

		DuckLakePartitionFieldInfo partition_field;
		partition_field.partition_key_index = row.GetValue<uint64_t>(2);
		partition_field.field_id = FieldIndex(row.GetValue<uint64_t>(3));
		partition_field.transform = row.GetValue<string>(4);
		partition_entry.fields.push_back(std::move(partition_field));
	}

	// load sort information
	result = transaction.Query(snapshot, R"(
SELECT sort.sort_id, sort.table_id, sort_expr.sort_key_index, sort_expr.expression, sort_expr.dialect, sort_expr.sort_direction, sort_expr.null_order
FROM {METADATA_CATALOG}.ducklake_sort_info sort
JOIN {METADATA_CATALOG}.ducklake_sort_expression sort_expr USING (sort_id)
WHERE {SNAPSHOT_ID} >= sort.begin_snapshot AND ({SNAPSHOT_ID} < sort.end_snapshot OR sort.end_snapshot IS NULL)
ORDER BY sort.table_id, sort.sort_id, sort_expr.sort_key_index
)");
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get sort information from DuckLake: ");
	}
	auto &sorts = catalog.sorts;
	for (auto &row : *result) {
		auto sort_id = row.GetValue<uint64_t>(0);
		auto table_id = TableIndex(row.GetValue<uint64_t>(1));

		if (sorts.empty() || sorts.back().table_id != table_id) {
			DuckLakeSortInfo sort_info;
			sort_info.id = sort_id;
			sort_info.table_id = table_id;
			sorts.push_back(std::move(sort_info));
		}
		auto &sort_entry = sorts.back();

		DuckLakeSortFieldInfo sort_field;
		sort_field.sort_key_index = row.GetValue<uint64_t>(2);
		sort_field.expression = row.GetValue<string>(3);
		sort_field.dialect = row.GetValue<string>(4);

		auto sort_direction_str = row.GetValue<string>(5);
		sort_field.sort_direction =
		    (StringUtil::CIEquals(sort_direction_str, "DESC") ? OrderType::DESCENDING : OrderType::ASCENDING);

		auto null_order_str = row.GetValue<string>(6);
		sort_field.null_order = (StringUtil::CIEquals(null_order_str, "NULLS_FIRST") ? OrderByNullType::NULLS_FIRST
		                                                                             : OrderByNullType::NULLS_LAST);
		sort_entry.fields.push_back(std::move(sort_field));
	}

	return catalog;
}

template <class ROW>
void TransformGlobalStatsRow(const ROW &row, vector<DuckLakeGlobalStatsInfo> &global_stats, idx_t from_column = 0) {
	auto table_id = TableIndex(row.template GetValue<uint64_t>(0 + from_column));

	if (global_stats.empty() || global_stats.back().table_id != table_id) {
		DuckLakeGlobalStatsInfo new_entry;
		new_entry.table_id = table_id;
		new_entry.initialized = true;
		new_entry.record_count = row.template GetValue<uint64_t>(2 + from_column);
		new_entry.next_row_id = row.template GetValue<uint64_t>(3 + from_column);
		new_entry.table_size_bytes = row.template GetValue<uint64_t>(4 + from_column);
		global_stats.push_back(std::move(new_entry));
	}

	auto &stats_entry = global_stats.back();

	DuckLakeGlobalColumnStatsInfo column_stats;
	column_stats.column_id = FieldIndex(row.template GetValue<uint64_t>(1 + from_column));

	const idx_t COLUMN_STATS_START = 5 + from_column;

	if (row.IsNull(COLUMN_STATS_START)) {
		column_stats.has_contains_null = false;
	} else {
		column_stats.has_contains_null = true;
		column_stats.contains_null = row.template GetValue<bool>(COLUMN_STATS_START);
	}

	if (row.IsNull(COLUMN_STATS_START + 1)) {
		column_stats.has_contains_nan = false;
	} else {
		column_stats.has_contains_nan = true;
		column_stats.contains_nan = row.template GetValue<bool>(COLUMN_STATS_START + 1);
	}

	if (row.IsNull(COLUMN_STATS_START + 2)) {
		column_stats.has_min = false;
	} else {
		column_stats.has_min = true;
		column_stats.min_val = row.template GetValue<string>(COLUMN_STATS_START + 2);
	}

	if (row.IsNull(COLUMN_STATS_START + 3)) {
		column_stats.has_max = false;
	} else {
		column_stats.has_max = true;
		column_stats.max_val = row.template GetValue<string>(COLUMN_STATS_START + 3);
	}

	if (row.IsNull(COLUMN_STATS_START + 4)) {
		column_stats.has_extra_stats = false;
	} else {
		column_stats.has_extra_stats = true;
		column_stats.extra_stats = row.template GetValue<string>(COLUMN_STATS_START + 4);
	}

	stats_entry.column_stats.push_back(std::move(column_stats));
}

vector<DuckLakeGlobalStatsInfo> TransformGlobalStats(QueryResult &result) {
	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to get global stats information from DuckLake: ");
	}

	vector<DuckLakeGlobalStatsInfo> global_stats;

	for (auto &row : result) {
		TransformGlobalStatsRow(row, global_stats);
	}

	return global_stats;
}

vector<DuckLakeGlobalStatsInfo> DuckLakeMetadataManager::GetGlobalTableStats(DuckLakeSnapshot snapshot) {
	// query the most recent stats
	auto result = transaction.Query(snapshot, R"(
SELECT table_id, column_id, record_count, next_row_id, file_size_bytes, contains_null, contains_nan, min_value, max_value, extra_stats
FROM {METADATA_CATALOG}.ducklake_table_stats
LEFT JOIN {METADATA_CATALOG}.ducklake_table_column_stats USING (table_id)
WHERE record_count IS NOT NULL AND file_size_bytes IS NOT NULL
ORDER BY table_id;
)");
	return TransformGlobalStats(*result);
}

string DuckLakeMetadataManager::GetFileSelectList(const string &prefix) {
	auto result = StringUtil::Replace(
	    "{PREFIX}.path, {PREFIX}.path_is_relative, {PREFIX}.file_size_bytes, {PREFIX}.footer_size", "{PREFIX}", prefix);
	if (IsEncrypted()) {
		result += ", " + prefix + ".encryption_key";
	}
	return result;
}

template <class T>
DuckLakeFileData DuckLakeMetadataManager::ReadDataFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx,
                                                       bool is_encrypted) {
	DuckLakeFileData data;
	if (row.IsNull(col_idx)) {
		// file is not there
		col_idx += 4;
		if (is_encrypted) {
			col_idx++;
		}
		return data;
	}
	DuckLakePath path;
	path.path = row.template GetValue<string>(col_idx++);
	path.path_is_relative = row.template GetValue<bool>(col_idx++);

	data.path = FromRelativePath(path, table.DataPath());
	data.file_size_bytes = row.template GetValue<idx_t>(col_idx++);
	if (!row.IsNull(col_idx)) {
		data.footer_size = row.template GetValue<idx_t>(col_idx);
	}
	col_idx++;
	if (is_encrypted) {
		if (row.IsNull(col_idx)) {
			throw InvalidInputException("Database is encrypted, but file %s does not have an encryption key",
			                            data.path);
		}
		data.encryption_key = Blob::FromBase64(row.template GetValue<string>(col_idx++));
	}
	return data;
}

static void SetSnapshotFilter(const DuckLakeSnapshot &snapshot, idx_t max_partial_file_snapshot,
                              DuckLakeFileListEntry &file_entry) {
	if (max_partial_file_snapshot <= snapshot.snapshot_id) {
		// all snapshot ids are included for this snapshot - skip filtering
		return;
	}
	file_entry.snapshot_filter_max = snapshot.snapshot_id;
}

string DuckLakeMetadataManager::GenerateFilterFromTableFilter(const TableFilter &filter, const LogicalType &type,
                                                              unordered_set<string> &referenced_stats) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		switch (type.id()) {
		case LogicalTypeId::BLOB:
			return string();
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return GenerateConstantFilterDouble(constant_filter, type, referenced_stats);
		default:
			return GenerateConstantFilter(constant_filter, type, referenced_stats);
		}
	}
	case TableFilterType::IS_NULL:
		referenced_stats.insert("null_count");
		return "null_count > 0";
	case TableFilterType::IS_NOT_NULL:
		referenced_stats.insert("value_count");
		return "value_count > 0";
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or_filter = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child_filter : conjunction_or_filter.child_filters) {
			if (!result.empty()) {
				result += " OR ";
			}
			string child_str = GenerateFilterFromTableFilter(*child_filter, type, referenced_stats);
			if (child_str.empty()) {
				return string();
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child_filter : conjunction_and_filter.child_filters) {
			string child_str = GenerateFilterFromTableFilter(*child_filter, type, referenced_stats);
			if (child_str.empty()) {
				continue;
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return GenerateFilterFromTableFilter(*optional_filter.child_filter, type, referenced_stats);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string result;
		for (auto &value : in_filter.values) {
			if (!result.empty()) {
				result += " OR ";
			}
			auto temporary_constant_filter = ConstantFilter(ExpressionType::COMPARE_EQUAL, value);
			auto next_filter = GenerateFilterFromTableFilter(temporary_constant_filter, type, referenced_stats);
			if (next_filter.empty()) {
				return string();
			}
			result += "(" + next_filter + ")";
		}
		return result;
	}
	default:
		return string();
	}
}

bool DuckLakeMetadataManager::ValueIsFinite(const Value &val) {
	if (val.type().id() != LogicalTypeId::FLOAT && val.type().id() != LogicalTypeId::DOUBLE) {
		return true;
	}
	double constant_val = val.GetValue<double>();
	return Value::IsFinite(constant_val);
}

string DuckLakeMetadataManager::CastValueToTarget(const Value &val, const LogicalType &type) {
	if (type.IsNumeric() && ValueIsFinite(val)) {
		// for (finite) numerics we directly emit the number
		return val.ToString();
	}
	// convert to a string
	return DuckLakeUtil::SQLLiteralToString(val.ToString());
}

string DuckLakeMetadataManager::CastStatsToTarget(const string &stats, const LogicalType &type) {
	// we only need to cast numerics
	if (type.IsNumeric()) {
		return "TRY_CAST(" + stats + " AS " + type.ToString() + ")";
	}
	return stats;
}

string DuckLakeMetadataManager::GenerateConstantFilter(const ConstantFilter &constant_filter, const LogicalType &type,
                                                       unordered_set<string> &referenced_stats) {
	auto constant_str = CastValueToTarget(constant_filter.constant, type);
	auto min_value = CastStatsToTarget("min_value", type);
	auto max_value = CastStatsToTarget("max_value", type);
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// x = constant
		// this can only be true if "constant BETWEEN min AND max"
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s BETWEEN %s AND %s", constant_str, min_value, max_value);
	case ExpressionType::COMPARE_NOTEQUAL:
		// x <> constant
		// this can only be false if "constant = min AND constant = max" (i.e. min = max = constant)
		referenced_stats.insert("min_value");
		referenced_stats.insert("max_value");
		return StringUtil::Format("NOT (%s = %s AND %s = %s)", min_value, constant_str, max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		// x >= constant
		// this can only be true if "max >= C"
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s >= %s", max_value, constant_str);
	case ExpressionType::COMPARE_GREATERTHAN:
		// x > constant
		// this can only be true if "max > C"
		referenced_stats.insert("max_value");
		return StringUtil::Format("%s > %s", max_value, constant_str);
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		// x <= constant
		// this can only be true if "min <= C"
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s <= %s", min_value, constant_str);
	case ExpressionType::COMPARE_LESSTHAN:
		// x < constant
		// this can only be true if "min < C"
		referenced_stats.insert("min_value");
		return StringUtil::Format("%s < %s", min_value, constant_str);
	default:
		// unsupported
		return string();
	}
}

string DuckLakeMetadataManager::GenerateConstantFilterDouble(const ConstantFilter &constant_filter,
                                                             const LogicalType &type,
                                                             unordered_set<string> &referenced_stats) {
	double constant_val = constant_filter.constant.GetValue<double>();
	bool constant_is_nan = Value::IsNan(constant_val);
	switch (constant_filter.comparison_type) {
	case ExpressionType::COMPARE_EQUAL:
		// x = constant
		if (constant_is_nan) {
			// x = NAN - check for `contains_nan`
			referenced_stats.insert("contains_nan");
			return "contains_nan";
		}
		// else check as if this is a numeric
		return GenerateConstantFilter(constant_filter, type, referenced_stats);
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
	case ExpressionType::COMPARE_GREATERTHAN: {
		if (constant_is_nan) {
			// skip these filters if the constant is nan
			// note that > and >= we can actually handle since nan is the biggest value
			// (>= is equal to =, > is always false)
			return string();
		}
		// generate the numeric filter
		string filter = GenerateConstantFilter(constant_filter, type, referenced_stats);
		if (filter.empty()) {
			return string();
		}
		// since NaN is bigger than anything - we also need to check for contains_nan
		referenced_stats.insert("contains_nan");
		return filter + " OR contains_nan";
	}
	case ExpressionType::COMPARE_NOTEQUAL:
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
	case ExpressionType::COMPARE_LESSTHAN:
		if (constant_is_nan) {
			// skip these filters if the constant is nan
			return string();
		}
		// these are equivalent to the numeric filter
		return GenerateConstantFilter(constant_filter, type, referenced_stats);
	default:
		// unsupported
		return string();
	}
}

string DuckLakeMetadataManager::GenerateFilterPushdown(const TableFilter &filter,
                                                       unordered_set<string> &referenced_stats) {
	switch (filter.filter_type) {
	case TableFilterType::CONSTANT_COMPARISON: {
		auto &constant_filter = filter.Cast<ConstantFilter>();
		auto &type = constant_filter.constant.type();
		switch (type.id()) {
		case LogicalTypeId::BLOB:
			return string();
		case LogicalTypeId::FLOAT:
		case LogicalTypeId::DOUBLE:
			return GenerateConstantFilterDouble(constant_filter, type, referenced_stats);
		default:
			return GenerateConstantFilter(constant_filter, type, referenced_stats);
		}
	}
	case TableFilterType::IS_NULL:
		// IS NULL can only be true if the file has any NULL values
		referenced_stats.insert("null_count");
		return "null_count > 0";
	case TableFilterType::IS_NOT_NULL:
		// IS NOT NULL can only be true if the file has any valid values
		referenced_stats.insert("value_count");
		return "value_count > 0";
	case TableFilterType::CONJUNCTION_OR: {
		auto &conjunction_or_filter = filter.Cast<ConjunctionOrFilter>();
		string result;
		for (auto &child_filter : conjunction_or_filter.child_filters) {
			if (!result.empty()) {
				result += " OR ";
			}
			string child_str = GenerateFilterPushdown(*child_filter, referenced_stats);
			if (child_str.empty()) {
				return string();
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::CONJUNCTION_AND: {
		auto &conjunction_and_filter = filter.Cast<ConjunctionAndFilter>();
		string result;
		for (auto &child_filter : conjunction_and_filter.child_filters) {
			string child_str = GenerateFilterPushdown(*child_filter, referenced_stats);
			if (child_str.empty()) {
				continue; // skip this child, we can still use other children
			}
			if (!result.empty()) {
				result += " AND ";
			}
			result += "(" + child_str + ")";
		}
		return result;
	}
	case TableFilterType::OPTIONAL_FILTER: {
		auto &optional_filter = filter.Cast<OptionalFilter>();
		return GenerateFilterPushdown(*optional_filter.child_filter, referenced_stats);
	}
	case TableFilterType::IN_FILTER: {
		auto &in_filter = filter.Cast<InFilter>();
		string result;
		for (auto &value : in_filter.values) {
			if (!result.empty()) {
				result += " OR ";
			}
			auto temporary_constant_filter = ConstantFilter(ExpressionType::COMPARE_EQUAL, value);
			auto next_filter = GenerateFilterPushdown(temporary_constant_filter, referenced_stats);
			if (next_filter.empty()) {
				return string();
			}
			result += "(" + next_filter + ")";
		}
		return result;
	}
	default:
		// unsupported filter
		return string();
	}
}

FilterSQLResult DuckLakeMetadataManager::ConvertFilterPushdownToSQL(const FilterPushdownInfo &filter_info) {
	FilterSQLResult result;
	string conditions;

	for (const auto &entry : filter_info.column_filters) {
		const auto &column_filter = entry.second;

		unordered_set<string> referenced_stats;
		auto filter_condition = GenerateFilterPushdown(*column_filter.table_filter, referenced_stats);

		if (filter_condition.empty()) {
			continue;
		}

		string cte_name = StringUtil::Format("col_%d_stats", column_filter.column_field_index);

		string null_checks;
		for (const auto &stat : referenced_stats) {
			null_checks += stat + " IS NULL OR ";
		}

		if (!conditions.empty()) {
			conditions += " AND ";
		}
		conditions += StringUtil::Format("data.data_file_id IN (SELECT data_file_id FROM %s WHERE %s(%s))", cte_name,
		                                 null_checks.c_str(), filter_condition.c_str());

		CTERequirement req(column_filter.column_field_index, referenced_stats);
		result.required_ctes.emplace(column_filter.column_field_index, std::move(req));
	}

	result.where_conditions = conditions;
	return result;
}

string
DuckLakeMetadataManager::GenerateCTESectionFromRequirements(const unordered_map<idx_t, CTERequirement> &requirements,
                                                            TableIndex table_id) {
	if (requirements.empty()) {
		return "";
	}

	string cte_section = "WITH ";
	bool first_cte = true;

	for (const auto &entry : requirements) {
		const auto &req = entry.second;

		if (!first_cte) {
			cte_section += ",\n";
		}
		first_cte = false;

		string select_list = "data_file_id";
		for (const auto &stat : req.referenced_stats) {
			select_list += ", " + stat;
		}

		string materialized_hint = (req.reference_count > 1) ? " AS MATERIALIZED" : " AS NOT MATERIALIZED";

		cte_section += StringUtil::Format("col_%d_stats%s (\n", req.column_field_index, materialized_hint.c_str());
		cte_section += StringUtil::Format("  SELECT %s\n", select_list.c_str());
		cte_section += "  FROM {METADATA_CATALOG}.ducklake_file_column_stats\n";
		cte_section +=
		    StringUtil::Format("  WHERE column_id = %d AND table_id = %d\n", req.column_field_index, table_id.index);
		cte_section += ")";
	}

	return cte_section + "\n";
}

FilterPushdownQueryComponents
DuckLakeMetadataManager::GenerateFilterPushdownComponents(const FilterPushdownInfo &filter_info, TableIndex table_id) {
	FilterPushdownQueryComponents result;

	if (filter_info.column_filters.empty()) {
		return result;
	}

	auto filter_result = ConvertFilterPushdownToSQL(filter_info);
	result.cte_section = GenerateCTESectionFromRequirements(filter_result.required_ctes, table_id);
	result.where_clause = filter_result.where_conditions;

	return result;
}

struct DynamicFilterColumn {
	idx_t column_field_index;
	ExpressionType comparison_type;
	LogicalType column_type;
};

vector<DuckLakeFileListEntry> DuckLakeMetadataManager::GetFilesForTable(DuckLakeTableEntry &table,
                                                                        DuckLakeSnapshot snapshot,
                                                                        const FilterPushdownInfo *filter_info) {
	auto table_id = table.GetTableId();

	// If we have Top-N dynamic filter pushdown, include file-level min/max stats for pruning and ordering
	vector<DynamicFilterColumn> dynamic_filter_columns;
	if (filter_info) {
		for (auto &entry : filter_info->column_filters) {
			auto &col_filter = entry.second;
			auto *dynamic = DuckLakeUtil::GetOptionalDynamicFilter(*col_filter.table_filter);
			if (dynamic) {
				ExpressionType comparison_type;
				{
					lock_guard<mutex> l(dynamic->filter_data->lock);
					comparison_type = dynamic->filter_data->filter->comparison_type;
				}
				dynamic_filter_columns.push_back(
				    {col_filter.column_field_index, comparison_type, col_filter.column_type});
			}
		}
	}

	string stats_select_list;
	string stats_join_list;
	string order_by_clause;
	for (idx_t i = 0; i < dynamic_filter_columns.size(); i++) {
		auto &dfc = dynamic_filter_columns[i];
		auto alias = StringUtil::Format("stats_%d", NumericCast<int64_t>(i));
		stats_select_list += StringUtil::Format(", %s.min_value, %s.max_value", alias.c_str(), alias.c_str());
		stats_join_list += StringUtil::Format(
		    "\nLEFT JOIN {METADATA_CATALOG}.ducklake_file_column_stats %s ON %s.data_file_id = data.data_file_id AND "
		    "%s.table_id = data.table_id AND %s.column_id = %d",
		    alias.c_str(), alias.c_str(), alias.c_str(), alias.c_str(), NumericCast<int64_t>(dfc.column_field_index));

		// Generate ORDER BY clause to optimize Top-N queries - order files by their min/max stats
		// so we find satisfying rows early and can skip remaining files via dynamic filter pruning.
		// We only order by the first dynamic filter column: Top-N typically has a single ordering column,
		// and multiple columns would have conflicting requirements (e.g., ORDER BY a DESC, b ASC).
		if (order_by_clause.empty()) {
			const bool seeking_high_values = dfc.comparison_type == ExpressionType::COMPARE_GREATERTHAN ||
			                                 dfc.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
			const bool seeking_low_values = dfc.comparison_type == ExpressionType::COMPARE_LESSTHAN ||
			                                dfc.comparison_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
			if (seeking_high_values) {
				// For DESC Top-N (seeking high values), order by max_value DESC so files with highest values come first
				auto cast_expr = CastStatsToTarget(alias + ".max_value", dfc.column_type);
				order_by_clause = StringUtil::Format("\nORDER BY %s DESC NULLS LAST", cast_expr);
			} else if (seeking_low_values) {
				// For ASC Top-N (seeking low values), order by min_value ASC so files with lowest values come first
				auto cast_expr = CastStatsToTarget(alias + ".min_value", dfc.column_type);
				order_by_clause = StringUtil::Format("\nORDER BY %s ASC NULLS LAST", cast_expr);
			}
		}
	}

	string select_list = "data.data_file_id, " + GetFileSelectList("data") +
	                     ", data.row_id_start, data.begin_snapshot, data.partial_max, data.mapping_id, " +
	                     GetFileSelectList("del") + stats_select_list;

	string query;
	string where_clause;

	// Generate CTE section and WHERE clause if we have filter pushdown info
	if (filter_info && !filter_info->column_filters.empty()) {
		auto components = GenerateFilterPushdownComponents(*filter_info, table_id);
		query = components.cte_section;
		where_clause = components.where_clause;
	}

	// Add base query
	query += StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.ducklake_data_file data
%s
LEFT JOIN (
    SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d  AND {SNAPSHOT_ID} >= begin_snapshot
          AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
    ) del ON del.data_file_id = data.data_file_id
WHERE data.table_id=%d AND {SNAPSHOT_ID} >= data.begin_snapshot AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
		)",
	                            select_list, stats_join_list, table_id.index, table_id.index);

	// Add WHERE clause from filters if it was generated
	if (!where_clause.empty()) {
		query += "\nAND " + where_clause;
	}
	// Add ORDER BY clause for Top-N optimization if generated
	query += order_by_clause;
	auto result = transaction.Query(snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get data file list from DuckLake: ");
	}

	// Query inlined file deletions for this table
	auto inlined_deletions = ReadInlinedFileDeletions(table_id, snapshot);

	vector<DuckLakeFileListEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListEntry file_entry;
		idx_t col_idx = 0;
		file_entry.file_id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		file_entry.snapshot_id = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			auto partial_max = row.GetValue<idx_t>(col_idx);
			SetSnapshotFilter(snapshot, partial_max, file_entry);
		}
		col_idx++;
		if (!row.IsNull(col_idx)) {
			file_entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		for (auto &dfc : dynamic_filter_columns) {
			string min_val;
			string max_val;
			if (!row.IsNull(col_idx)) {
				min_val = row.GetValue<string>(col_idx);
			}
			col_idx++;
			if (!row.IsNull(col_idx)) {
				max_val = row.GetValue<string>(col_idx);
			}
			col_idx++;
			file_entry.column_min_max.emplace(dfc.column_field_index,
			                                  make_pair(std::move(min_val), std::move(max_val)));
		}

		// Populate inlined file deletions for this file
		auto del_entry = inlined_deletions.find(file_entry.file_id.index);
		if (del_entry != inlined_deletions.end()) {
			file_entry.inlined_file_deletions = std::move(del_entry->second);
		}

		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeFileListEntry> DuckLakeMetadataManager::GetTableInsertions(DuckLakeTableEntry &table,
                                                                          DuckLakeSnapshot start_snapshot,
                                                                          DuckLakeSnapshot end_snapshot) {
	auto table_id = table.GetTableId();
	string select_list = GetFileSelectList("data") +
	                     ", data.row_id_start, data.begin_snapshot, data.partial_max, data.mapping_id, " +
	                     GetFileSelectList("del");
	// Files either match the exact snapshot range
	// Or they have partial_max set, which means they are a file with many snapshot ids, and might contain
	// the snapshot we need
	auto query =
	    StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.ducklake_data_file data, (
	SELECT NULL path, NULL path_is_relative, NULL file_size_bytes, NULL footer_size, NULL encryption_key
) del
WHERE data.table_id=%d AND data.begin_snapshot <= {SNAPSHOT_ID} AND (
	(data.begin_snapshot >= %d) OR
	(data.partial_max IS NOT NULL AND data.partial_max >= %d)
);
		)",
	                       select_list, table_id.index, start_snapshot.snapshot_id, start_snapshot.snapshot_id);

	auto result = transaction.Query(end_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table insertion file list from DuckLake: ");
	}
	vector<DuckLakeFileListEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListEntry file_entry;
		idx_t col_idx = 0;
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		auto begin_snapshot = row.GetValue<idx_t>(col_idx++);
		file_entry.snapshot_id = begin_snapshot;
		if (!row.IsNull(col_idx)) {
			auto partial_max = row.GetValue<idx_t>(col_idx);
			// Set upper bound filter if partial_max > end_snapshot
			SetSnapshotFilter(end_snapshot, partial_max, file_entry);
			// Set lower bound filter if begin_snapshot < start_snapshot
			// This means the file contains rows from before start_snapshot that we need to filter out
			if (begin_snapshot < start_snapshot.snapshot_id) {
				file_entry.snapshot_filter_min = start_snapshot.snapshot_id;
			}
		}
		col_idx++;
		if (!row.IsNull(col_idx)) {
			file_entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeDeleteScanEntry> DuckLakeMetadataManager::GetTableDeletions(DuckLakeTableEntry &table,
                                                                           DuckLakeSnapshot start_snapshot,
                                                                           DuckLakeSnapshot end_snapshot) {
	auto table_id = table.GetTableId();
	string select_list = "data.data_file_id, " + GetFileSelectList("data") +
	                     ", data.row_id_start, data.record_count, data.mapping_id, " +
	                     GetFileSelectList("current_delete") + ", " + GetFileSelectList("previous_delete");

	// Check if we have an inlined deletion table for this table (usually cached, no DB hit)
	auto inlined_table_name = GetInlinedDeletionTableName(table_id, end_snapshot);
	bool has_inlined_table = !inlined_table_name.empty();

	// Build the query with optional CTE for inlined deletions
	// Deletes come in three flavors:
	// 1. Deletes stored in the ducklake_delete_file table (partial deletes)
	// 2. Data files being deleted entirely through setting end_snapshot (full file deletes)
	// 3. Inlined file deletions stored in the metadata database
	// For all deletes, we need to obtain any PREVIOUS deletes as well to exclude rows already deleted
	string query;

	// Add CTE for aggregated inlined deletions if the table exists
	if (has_inlined_table) {
		query = StringUtil::Format(R"(
WITH inlined_dels AS (
	SELECT file_id,
	       LIST(STRUCT_PACK(row_id := row_id, snapshot_id := begin_snapshot)) as deletions,
	       MIN(begin_snapshot) as min_snapshot
	FROM {METADATA_CATALOG}.%s
	WHERE begin_snapshot >= %d AND begin_snapshot <= {SNAPSHOT_ID}
	GROUP BY file_id
),
main_results AS (
)",
		                           inlined_table_name, start_snapshot.snapshot_id);
	} else {
		query = "WITH main_results AS (\n";
	}

	// Main query: partial deletes from delete_file table and full file deletes
	query += StringUtil::Format(R"(
SELECT %s, current_delete.begin_snapshot FROM (
	SELECT data_file_id, begin_snapshot, path, path_is_relative, file_size_bytes, footer_size, encryption_key
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot <= {SNAPSHOT_ID}
) AS current_delete
LEFT JOIN (
	SELECT data_file_id, MAX_BY(COLUMNS(['path', 'path_is_relative', 'file_size_bytes', 'footer_size', 'encryption_key']), begin_snapshot) AS '\0'
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot < %d
	GROUP BY data_file_id
) AS previous_delete
USING (data_file_id)
JOIN (
	FROM {METADATA_CATALOG}.ducklake_data_file data
	WHERE table_id = %d
) AS data
USING (data_file_id)

UNION ALL

SELECT %s, data.end_snapshot FROM (
	FROM {METADATA_CATALOG}.ducklake_data_file
	WHERE table_id = %d AND end_snapshot >= %d AND end_snapshot <= {SNAPSHOT_ID}
) AS data
LEFT JOIN (
	SELECT data_file_id, MAX_BY(COLUMNS(['path', 'path_is_relative', 'file_size_bytes', 'footer_size', 'encryption_key']), begin_snapshot) AS '\0'
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE table_id = %d AND begin_snapshot < data.end_snapshot
	GROUP BY data_file_id
) AS previous_delete
USING (data_file_id), (
	SELECT NULL path, NULL path_is_relative, NULL file_size_bytes, NULL footer_size, NULL encryption_key
) current_delete
)",
	                            select_list, table_id.index, table_id.index, start_snapshot.snapshot_id, table_id.index,
	                            select_list, table_id.index, start_snapshot.snapshot_id, table_id.index);

	if (has_inlined_table) {
		string null_file_cols = "NULL path, NULL path_is_relative, NULL file_size_bytes, NULL footer_size";
		if (IsEncrypted()) {
			null_file_cols += ", NULL encryption_key";
		}
		query += StringUtil::Format(R"(
UNION ALL

SELECT data.data_file_id, %s, data.row_id_start, data.record_count, data.mapping_id,
       %s,
       %s,
       inlined_dels.min_snapshot
FROM {METADATA_CATALOG}.ducklake_data_file data
JOIN inlined_dels ON data.data_file_id = inlined_dels.file_id
WHERE data.table_id = %d
  AND data.data_file_id NOT IN (
      SELECT data_file_id FROM {METADATA_CATALOG}.ducklake_delete_file
      WHERE table_id = %d AND begin_snapshot <= {SNAPSHOT_ID}
  )
  AND (data.end_snapshot IS NULL OR data.end_snapshot < %d OR data.end_snapshot > {SNAPSHOT_ID})
)",
		                            GetFileSelectList("data"), null_file_cols, null_file_cols, table_id.index,
		                            table_id.index, start_snapshot.snapshot_id);
	}

	// Close the main_results CTE and do the final SELECT with LEFT JOIN on inlined_dels
	if (has_inlined_table) {
		query += R"(
)
SELECT main_results.*, inlined_dels.deletions
FROM main_results
LEFT JOIN inlined_dels ON main_results.data_file_id = inlined_dels.file_id
)";
	} else {
		query += R"(
)
SELECT main_results.*, NULL as deletions
FROM main_results
)";
	}

	auto result = transaction.Query(end_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get table deletion file list from DuckLake: ");
	}

	// Build entries from the unified query result
	vector<DuckLakeDeleteScanEntry> files;
	for (auto &row : *result) {
		DuckLakeDeleteScanEntry entry;
		idx_t col_idx = 0;
		auto file_id = row.GetValue<idx_t>(col_idx++);
		entry.file_id = DataFileIndex(file_id);
		entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		entry.row_count = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			entry.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		entry.previous_delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		entry.snapshot_id = row.GetValue<idx_t>(col_idx++);
		// store the snapshot range for filtering embedded snapshot IDs
		entry.start_snapshot = start_snapshot.snapshot_id;
		entry.end_snapshot = end_snapshot.snapshot_id;

		// Parse inlined file deletions from the LIST column (last column)
		if (!row.IsNull(col_idx)) {
			auto deletions_list = row.GetValue<Value>(col_idx);
			auto &list_children = ListValue::GetChildren(deletions_list);
			for (auto &child : list_children) {
				auto &struct_children = StructValue::GetChildren(child);
				auto row_id = struct_children[0].GetValue<idx_t>();
				auto snapshot_id = struct_children[1].GetValue<idx_t>();
				entry.inlined_file_deletions[row_id] = snapshot_id;
			}
		}

		files.push_back(std::move(entry));
	}

	return files;
}

vector<DuckLakeFileListExtendedEntry>
DuckLakeMetadataManager::GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
                                                  const FilterPushdownInfo *filter_info) {
	auto table_id = table.GetTableId();
	string select_list =
	    GetFileSelectList("data") + ", data.row_id_start, " + GetFileSelectList("del") + ", del.begin_snapshot";

	string query;
	string where_clause;

	// Generate CTE section and WHERE clause if we have filter pushdown info
	if (filter_info && !filter_info->column_filters.empty()) {
		auto components = GenerateFilterPushdownComponents(*filter_info, table_id);
		query = components.cte_section;
		where_clause = components.where_clause;
	}

	// Add base query
	query += StringUtil::Format(R"(
SELECT data.data_file_id, del.delete_file_id, data.record_count, %s
FROM {METADATA_CATALOG}.ducklake_data_file data
LEFT JOIN (
	SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d  AND {SNAPSHOT_ID} >= begin_snapshot
          AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
    ) del USING (data_file_id)
WHERE data.table_id=%d AND {SNAPSHOT_ID} >= data.begin_snapshot AND ({SNAPSHOT_ID} < data.end_snapshot OR data.end_snapshot IS NULL)
		)",
	                            select_list, table_id.index, table_id.index);

	// Add WHERE clause from filters if it was generated
	if (!where_clause.empty()) {
		query += "\nAND " + where_clause;
	}

	auto result = transaction.Query(snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get extended data file list from DuckLake: ");
	}
	vector<DuckLakeFileListExtendedEntry> files;
	for (auto &row : *result) {
		DuckLakeFileListExtendedEntry file_entry;
		file_entry.file_id = DataFileIndex(row.GetValue<idx_t>(0));
		if (!row.IsNull(1)) {
			file_entry.delete_file_id = DataFileIndex(row.GetValue<idx_t>(1));
		}
		file_entry.row_count = row.GetValue<idx_t>(2);
		idx_t col_idx = 3;
		file_entry.file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		file_entry.delete_file = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (!row.IsNull(col_idx)) {
			file_entry.delete_file_begin_snapshot = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		files.push_back(std::move(file_entry));
	}
	return files;
}

vector<DuckLakeCompactionFileEntry> DuckLakeMetadataManager::GetFilesForCompaction(DuckLakeTableEntry &table,
                                                                                   CompactionType type,
                                                                                   double deletion_threshold,
                                                                                   DuckLakeSnapshot snapshot,
                                                                                   DuckLakeFileSizeOptions options) {
	auto table_id = table.GetTableId();
	// Determine the effective max file size threshold for filtering
	idx_t effective_max_file_size =
	    options.max_file_size.IsValid() ? options.max_file_size.GetIndex() : options.target_file_size;
	string data_select_list = "data.data_file_id, data.record_count, data.row_id_start, data.begin_snapshot, "
	                          "data.end_snapshot, data.mapping_id, sr.schema_version , data.partial_max, "
	                          "data.partition_id, partition_info.keys, " +
	                          GetFileSelectList("data");
	string delete_select_list = "del.data_file_id,del.delete_file_id, del.delete_count, del.begin_snapshot, "
	                            "del.end_snapshot, del.partial_max, " +
	                            GetFileSelectList("del");
	string select_list = data_select_list + ", " + delete_select_list;
	string deletion_threshold_clause;
	if (type == CompactionType::REWRITE_DELETES) {
		deletion_threshold_clause = StringUtil::Format(
		    " AND del.delete_count/data.record_count >= %f and data.end_snapshot is null", deletion_threshold);
	}
	// Add file size filtering for MERGE_ADJACENT_TABLES compaction
	string file_size_filter_clause;
	if (type == CompactionType::MERGE_ADJACENT_TABLES) {
		if (options.min_file_size.IsValid()) {
			file_size_filter_clause +=
			    StringUtil::Format(" AND data.file_size_bytes >= %llu", options.min_file_size.GetIndex());
		}
		file_size_filter_clause += StringUtil::Format(" AND data.file_size_bytes < %llu", effective_max_file_size);
	}
	auto query = StringUtil::Format(R"(
WITH snapshot_ranges AS (
  SELECT
    begin_snapshot,
    COALESCE(
      LEAD(begin_snapshot) OVER (ORDER BY begin_snapshot),
      9223372036854775807
    ) AS end_snapshot,
	schema_version
	FROM {METADATA_CATALOG}.ducklake_schema_versions
	WHERE table_id=%d
	ORDER BY begin_snapshot
)
SELECT %s
FROM {METADATA_CATALOG}.ducklake_data_file data
LEFT JOIN snapshot_ranges sr
  ON data.begin_snapshot >= sr.begin_snapshot AND data.begin_snapshot < sr.end_snapshot
LEFT JOIN (
	SELECT *
    FROM {METADATA_CATALOG}.ducklake_delete_file
    WHERE table_id=%d
) del USING (data_file_id)
LEFT JOIN (
   SELECT data_file_id, ARRAY_AGG(partition_value ORDER BY partition_key_index) keys
   FROM {METADATA_CATALOG}.ducklake_file_partition_value
   GROUP BY data_file_id
) partition_info USING (data_file_id)
WHERE data.table_id=%d %s%s
ORDER BY data.begin_snapshot, data.row_id_start, data.data_file_id, del.begin_snapshot
		)",
	                                table_id.index, select_list, table_id.index, table_id.index,
	                                deletion_threshold_clause, file_size_filter_clause);
	auto result = transaction.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get compaction file list from DuckLake: ");
	}
	vector<DuckLakeCompactionFileEntry> files;
	for (auto &row : *result) {
		idx_t col_idx = 0;
		DuckLakeCompactionFileEntry new_entry;
		// parse the data file
		new_entry.file.id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		new_entry.file.row_count = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			new_entry.file.row_id_start = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		new_entry.file.begin_snapshot = row.GetValue<idx_t>(col_idx++);
		new_entry.file.end_snapshot = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		if (!row.IsNull(col_idx)) {
			new_entry.file.mapping_id = MappingIndex(row.GetValue<idx_t>(col_idx));
		}
		col_idx++;
		new_entry.schema_version = row.GetValue<idx_t>(col_idx++);
		if (!row.IsNull(col_idx)) {
			new_entry.max_partial_file_snapshot = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		new_entry.file.partition_id = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		if (!row.IsNull(col_idx)) {
			auto list_val = row.GetValue<Value>(col_idx);
			for (auto &entry : ListValue::GetChildren(list_val)) {
				new_entry.file.partition_values.push_back(StringValue::Get(entry));
			}
		}
		col_idx++;
		new_entry.file.data = ReadDataFile(table, row, col_idx, IsEncrypted());
		if (files.empty() || files.back().file.id != new_entry.file.id) {
			// new file - push it into the file list
			files.push_back(std::move(new_entry));
		}
		auto &file_entry = files.back();
		// parse the delete file (if any)
		if (row.IsNull(col_idx)) {
			// no delete file
			continue;
		}
		DuckLakeCompactionDeleteFileData delete_file;
		delete_file.id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		delete_file.delete_file_id = DataFileIndex(row.GetValue<idx_t>(col_idx++));
		delete_file.row_count = row.GetValue<idx_t>(col_idx++);
		delete_file.begin_snapshot = row.GetValue<idx_t>(col_idx++);
		delete_file.end_snapshot = row.IsNull(col_idx) ? optional_idx() : row.GetValue<idx_t>(col_idx);
		col_idx++;
		if (!row.IsNull(col_idx)) {
			delete_file.max_snapshot = row.GetValue<idx_t>(col_idx);
		}
		col_idx++;
		delete_file.data = ReadDataFile(table, row, col_idx, IsEncrypted());
		file_entry.delete_files.push_back(std::move(delete_file));
	}

	// Check for inlined deletions and mark affected files
	// Gather file IDs first, then query only for existence
	vector<idx_t> file_ids;
	file_ids.reserve(files.size());
	for (auto &file : files) {
		file_ids.push_back(file.file.id.index);
	}
	auto files_with_deletions = GetFileIdsWithInlinedDeletions(table_id, snapshot, file_ids);
	for (auto &file : files) {
		if (files_with_deletions.count(file.file.id.index)) {
			file.has_inlined_deletions = true;
		}
	}

	return files;
}

template <class T>
string GenerateIDList(const set<T> &dropped_entries) {
	string dropped_id_list;
	for (auto &dropped_id : dropped_entries) {
		if (!dropped_id_list.empty()) {
			dropped_id_list += ", ";
		}
		dropped_id_list += to_string(dropped_id.index);
	}
	return dropped_id_list;
}

template <class T>
string DuckLakeMetadataManager::FlushDrop(const string &metadata_table_name, const string &id_name,
                                          const set<T> &dropped_entries) {
	if (dropped_entries.empty()) {
		return {};
	}
	auto dropped_id_list = GenerateIDList(dropped_entries);

	return StringUtil::Format(
	    R"(UPDATE {METADATA_CATALOG}.%s SET end_snapshot = {SNAPSHOT_ID} WHERE end_snapshot IS NULL AND %s IN (%s);)",
	    metadata_table_name, id_name, dropped_id_list);
}

string DuckLakeMetadataManager::DropSchemas(const set<SchemaIndex> &ids) {
	return FlushDrop("ducklake_schema", "schema_id", ids);
}

string DuckLakeMetadataManager::DropTables(const set<TableIndex> &ids, bool renamed) {
	string batch_query = FlushDrop("ducklake_table", "table_id", ids);
	if (renamed == false) {
		batch_query += FlushDrop("ducklake_partition_info", "table_id", ids);
		batch_query += FlushDrop("ducklake_column", "table_id", ids);
		batch_query += FlushDrop("ducklake_column_tag", "table_id", ids);
		batch_query += FlushDrop("ducklake_data_file", "table_id", ids);
		batch_query += FlushDrop("ducklake_delete_file", "table_id", ids);
		batch_query += FlushDrop("ducklake_tag", "object_id", ids);
		batch_query += FlushDrop("ducklake_sort_info", "table_id", ids);
	}
	return batch_query;
}

string DuckLakeMetadataManager::DropViews(const set<TableIndex> &ids) {
	return FlushDrop("ducklake_view", "view_id", ids);
}

unique_ptr<QueryResult> DuckLakeMetadataManager::Execute(DuckLakeSnapshot snapshot, string &query) {
	return transaction.Query(snapshot, query);
}

unique_ptr<QueryResult> DuckLakeMetadataManager::Query(DuckLakeSnapshot snapshot, string &query) {
	return transaction.Query(snapshot, query);
}

string DuckLakeMetadataManager::DropMacros(const set<MacroIndex> &ids) {
	return FlushDrop("ducklake_macro", "macro_id", ids);
}
string DuckLakeMetadataManager::WriteNewSchemas(const vector<DuckLakeSchemaInfo> &new_schemas) {
	if (new_schemas.empty()) {
		throw InternalException("No schemas to create - should be handled elsewhere");
	}
	string schema_insert_sql;
	for (auto &new_schema : new_schemas) {
		if (!schema_insert_sql.empty()) {
			schema_insert_sql += ",";
		}
		auto schema_id = new_schema.id.index;
		auto path = GetRelativePath(new_schema.path);
		schema_insert_sql += StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %s, %s, %s)", schema_id,
		                                        new_schema.uuid, SQLString(new_schema.name), SQLString(path.path),
		                                        path.path_is_relative ? "true" : "false");
	}
	return "INSERT INTO {METADATA_CATALOG}.ducklake_schema VALUES " + schema_insert_sql + ";";
}

string GetExpressionType(ParsedExpression &expression) {
	switch (expression.type) {
	case ExpressionType::OPERATOR_CAST: {
		auto &cast_expression = expression.Cast<CastExpression>();
		if (cast_expression.child->type == ExpressionType::VALUE_CONSTANT) {
			return "literal";
		}
		return "expression";
	}
	case ExpressionType::VALUE_CONSTANT:
		return "literal";
	default:
		return "expression";
	}
}

static void ColumnToSQLRecursive(const DuckLakeColumnInfo &column, TableIndex table_id, optional_idx parent,
                                 string &result) {
	if (!result.empty()) {
		result += ",";
	}
	string parent_idx = parent.IsValid() ? to_string(parent.GetIndex()) : "NULL";

	string initial_default_val =
	    !column.initial_default.IsNull() ? KeywordHelper::WriteQuoted(column.initial_default.ToString(), '\'') : "NULL";

	string default_val = "'NULL'";
	string default_val_system = "'duckdb'";
	string default_val_type = "'" + column.default_value_type + "'";

	if (!column.default_value.IsNull()) {
		auto value = column.default_value.GetValue<string>();
		if (column.default_value_type == "literal") {
			default_val = KeywordHelper::WriteQuoted(value, '\'');
		} else if (column.default_value_type == "expression") {
			if (value.empty()) {
				default_val = "''";
			} else {
				auto sql_expr = Parser::ParseExpressionList(column.default_value.GetValue<string>());
				if (sql_expr.size() != 1) {
					throw InternalException("Expected a single expression");
				}
				default_val = KeywordHelper::WriteQuoted(sql_expr[0]->ToString(), '\'');
			}
		} else {
			throw InvalidInputException("Expression type %s not implemented for default value",
			                            column.default_value_type);
		}
	}

	auto column_id = column.id.index;
	auto column_order = column_id;

	result += StringUtil::Format("(%d, {SNAPSHOT_ID}, NULL, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s)", column_id,
	                             table_id.index, column_order, SQLString(column.name), SQLString(column.type),
	                             initial_default_val, default_val, column.nulls_allowed ? "true" : "false", parent_idx,
	                             default_val_type, default_val_system);
	for (auto &child : column.children) {
		ColumnToSQLRecursive(child, table_id, column_id, result);
	}
}

string DuckLakeMetadataManager::GetColumnTypeInternal(const LogicalType &column_type) {
	return column_type.ToString();
}

string DuckLakeMetadataManager::GetColumnType(const DuckLakeColumnInfo &col) {
	auto column_type = DuckLakeTypes::FromString(col.type);
	if (!TypeIsNativelySupported(column_type)) {
		return "VARCHAR";
	}
	switch (column_type.id()) {
	case LogicalTypeId::STRUCT: {
		string result;
		for (auto &child : col.children) {
			if (!result.empty()) {
				result += ", ";
			}
			result += StringUtil::Format("%s %s", SQLIdentifier(child.name), GetColumnType(child));
		}
		return "STRUCT(" + result + ")";
	}
	case LogicalTypeId::LIST: {
		return GetColumnType(col.children[0]) + "[]";
	}
	case LogicalTypeId::MAP: {
		return StringUtil::Format("MAP(%s, %s)", GetColumnType(col.children[0]), GetColumnType(col.children[1]));
	}
	default:
		if (!col.children.empty()) {
			// This is a nested structure that we currently do not support.
			throw NotImplementedException("Unsupported nested type %s in DuckLakeMetadataManager::GetColumnType",
			                              col.type);
		}
		return GetColumnTypeInternal(column_type);
	}
}

string DuckLakeMetadataManager::GetInlinedTableQuery(const DuckLakeTableInfo &table, const string &table_name) {
	string columns;

	for (auto &col : table.columns) {
		if (!columns.empty()) {
			columns += ", ";
		}
		columns += StringUtil::Format("%s %s", SQLIdentifier(col.name), GetColumnType(col));
	}
	return StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.%s(row_id BIGINT, begin_snapshot BIGINT, "
	                          "end_snapshot BIGINT, %s);",
	                          SQLIdentifier(table_name), columns);
}

string DuckLakeMetadataManager::WriteNewTables(DuckLakeSnapshot commit_snapshot,
                                               const vector<DuckLakeTableInfo> &new_tables,
                                               vector<DuckLakeSchemaInfo> &new_schemas_result) {
	if (new_tables.empty()) {
		return {};
	}

	string column_insert_sql;
	string table_insert_sql;

	for (auto &table : new_tables) {
		if (!table_insert_sql.empty()) {
			table_insert_sql += ", ";
		}
		auto schema_id = table.schema_id.index;
		auto path = GetRelativePath(table.schema_id, table.path, new_schemas_result);
		table_insert_sql +=
		    StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %d, %s, %s, %s)", table.id.index, table.uuid, schema_id,
		                       SQLString(table.name), SQLString(path.path), path.path_is_relative ? "true" : "false");
		for (auto &column : table.columns) {
			ColumnToSQLRecursive(column, table.id, optional_idx(), column_insert_sql);
		}
	}
	string batch_query;
	// Batch table and column inserts into a single multi-statement query
	if (!table_insert_sql.empty()) {
		batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_table VALUES " + table_insert_sql + ";";
	}
	if (!column_insert_sql.empty()) {
		batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_column VALUES " + column_insert_sql + ";";
	}

	// write new data-inlining tables (if data-inlining is enabled)
	batch_query += WriteNewInlinedTables(commit_snapshot, new_tables);

	return batch_query;
}

static string GetInlinedTableName(const DuckLakeTableInfo &table, const DuckLakeSnapshot &snapshot) {
	return StringUtil::Format("ducklake_inlined_data_%d_%d", table.id.index, snapshot.schema_version);
}

string DuckLakeMetadataManager::GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
                                                       string &inlined_tables, string &inlined_table_queries) {
	if (!inlined_tables.empty()) {
		inlined_tables += ", ";
	}
	auto schema_version = commit_snapshot.schema_version;
	string inlined_table_name = GetInlinedTableName(table, commit_snapshot);
	inlined_tables += StringUtil::Format("(%d, %s, %d)", table.id.index, SQLString(inlined_table_name), schema_version);
	if (!inlined_table_queries.empty()) {
		inlined_table_queries += "\n";
	}
	inlined_table_queries += GetInlinedTableQuery(table, inlined_table_name);
	return inlined_table_name;
}

string DuckLakeMetadataManager::WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot,
                                                      const vector<DuckLakeTableInfo> &new_tables) {
	auto &catalog = transaction.GetCatalog();
	string inlined_tables;
	string inlined_table_queries;
	for (auto &table : new_tables) {
		if (catalog.DataInliningRowLimit(table.schema_id, table.id) == 0 || table.id.IsTransactionLocal()) {
			// not inlining for this table or inlining is for a table on this transaction, hence handled there - skip it
			continue;
		}
		GetInlinedTableQueries(commit_snapshot, table, inlined_tables, inlined_table_queries);
	}
	if (inlined_tables.empty()) {
		return {};
	}
	string batch_query;
	// Batch both INSERT queries into a single multi-statement query to reduce round-trips
	batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables VALUES " + inlined_tables + ";";
	batch_query += inlined_table_queries;
	return batch_query;
}

string DuckLakeMetadataManager::WriteNewMacros(const vector<DuckLakeMacroInfo> &new_macros) {
	string batch_query;
	for (auto &macro : new_macros) {
		// Insert in the macro table
		batch_query = StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_macro values(%llu,%llu,'%s',{SNAPSHOT_ID}, NULL);
)",
		                                 macro.schema_id.index, macro.macro_id.index, macro.macro_name);
		// Insert in the implementation table
		for (idx_t impl_id = 0; impl_id < macro.implementations.size(); ++impl_id) {
			auto &impl = macro.implementations[impl_id];
			batch_query += StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_macro_impl values(%llu,%llu,'%s','%s','%s');
)",
			                                  macro.macro_id.index, impl_id, impl.dialect, impl.sql, impl.type);

			for (idx_t param_id = 0; param_id < impl.parameters.size(); ++param_id) {
				// Insert in the parameter table
				auto &param = impl.parameters[param_id];
				batch_query +=
				    StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_macro_parameters values(%llu,%llu,%llu,'%s','%s','%s', '%s');
)",
				                       macro.macro_id.index, impl_id, param_id, param.parameter_name,
				                       param.parameter_type, param.default_value.ToString(), param.default_value_type);
			}
		}
	}
	return batch_query;
}

string DuckLakeMetadataManager::WriteDroppedColumns(const vector<DuckLakeDroppedColumn> &dropped_columns) {
	if (dropped_columns.empty()) {
		return {};
	}
	string dropped_cols;
	for (auto &dropped_col : dropped_columns) {
		if (!dropped_cols.empty()) {
			dropped_cols += ", ";
		}
		dropped_cols += StringUtil::Format("(%d, %d)", dropped_col.table_id.index, dropped_col.field_id.index);
	}
	// overwrite the snapshot for the old columns
	return StringUtil::Format(R"(
WITH dropped_cols(tid, cid) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_column
SET end_snapshot = {SNAPSHOT_ID}
FROM dropped_cols
WHERE table_id=tid AND column_id=cid AND end_snapshot IS NULL
;)",
	                          dropped_cols);
}

string DuckLakeMetadataManager::WriteNewColumns(const vector<DuckLakeNewColumn> &new_columns) {
	if (new_columns.empty()) {
		return {};
	}
	string column_insert_sql;
	for (auto &new_col : new_columns) {
		ColumnToSQLRecursive(new_col.column_info, new_col.table_id, new_col.parent_idx, column_insert_sql);
	}

	// insert column entries
	return "INSERT INTO {METADATA_CATALOG}.ducklake_column VALUES " + column_insert_sql + ";";
}

string DuckLakeMetadataManager::WriteNewViews(const vector<DuckLakeViewInfo> &new_views) {
	string view_insert_sql;
	for (auto &view : new_views) {
		if (!view_insert_sql.empty()) {
			view_insert_sql += ", ";
		}
		auto schema_id = view.schema_id.index;
		view_insert_sql +=
		    StringUtil::Format("(%d, '%s', {SNAPSHOT_ID}, NULL, %d, %s, %s, %s, %s)", view.id.index, view.uuid,
		                       schema_id, SQLString(view.name), SQLString(view.dialect), SQLString(view.sql),
		                       SQLString(DuckLakeUtil::ToQuotedList(view.column_aliases)));
	}
	if (!view_insert_sql.empty()) {
		// insert table entries
		return "INSERT INTO {METADATA_CATALOG}.ducklake_view VALUES " + view_insert_sql + ";";
	}
	return {};
}

string DuckLakeMetadataManager::WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
                                                    const vector<DuckLakeInlinedDataInfo> &new_data,
                                                    const vector<DuckLakeTableInfo> &new_tables,
                                                    const vector<DuckLakeTableInfo> &new_inlined_data_tables_result) {
	string batch_query;
	if (new_data.empty()) {
		return batch_query;
	}

	auto context_ptr = transaction.context.lock();
	auto &context = *context_ptr;
	for (auto &entry : new_data) {
		string inlined_table_name;
		for (auto &inlined_table : new_inlined_data_tables_result) {
			if (inlined_table.id == entry.table_id) {
				inlined_table_name = GetInlinedTableName(inlined_table, commit_snapshot);
			}
		}
		if (inlined_table_name.empty()) {
			// get the latest table to insert into
			auto it = insert_inlined_table_name_cache.find(entry.table_id.index);
			if (it != insert_inlined_table_name_cache.end()) {
				inlined_table_name = it->second;
			}
		}
		if (inlined_table_name.empty()) {
			auto query = StringUtil::Format(R"(
SELECT table_name
FROM {METADATA_CATALOG}.ducklake_inlined_data_tables
WHERE table_id = %d AND schema_version=(
    SELECT MAX(schema_version)
    FROM {METADATA_CATALOG}.ducklake_inlined_data_tables
    WHERE table_id=%d
);)",
			                                entry.table_id.index, entry.table_id.index);
			auto result = transaction.Query(commit_snapshot, query);
			for (auto &row : *result) {
				inlined_table_name = row.GetValue<string>(0);
				insert_inlined_table_name_cache[entry.table_id.index] = inlined_table_name;
			}
		}

		DuckLakeTableInfo table_info;
		if (inlined_table_name.empty()) {
			// no inlined table yet - create a new one
			// first fetch the table info
			auto current_snapshot = transaction.GetSnapshot();
			auto table_entry = transaction.GetCatalog().GetEntryById(transaction, current_snapshot, entry.table_id);
			if (table_entry) {
				auto &table = table_entry->Cast<DuckLakeTableEntry>();
				table_info = table.GetTableInfo();
				table_info.columns = table.GetTableColumns();
			} else {
				// We try from our added tables
				bool found = false;
				for (auto &new_table : new_tables) {
					if (new_table.id == entry.table_id) {
						table_info = new_table;
						found = true;
					}
				}
				if (!found) {
					throw InternalException("Writing inlined data for a table that cannot be found in the catalog");
				}
			}
			// write the new inlined table
			string inlined_tables;
			string inlined_table_queries;
			commit_snapshot.schema_version++;
			inlined_table_name =
			    GetInlinedTableQueries(commit_snapshot, table_info, inlined_tables, inlined_table_queries);
			batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_inlined_data_tables VALUES " + inlined_tables + ";";
			batch_query += inlined_table_queries;
		}

		// append the data
		// FIXME: we can do a much faster append than this
		string values;
		idx_t row_id = entry.row_id_start;
		for (auto &chunk : entry.data->data->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				if (!values.empty()) {
					values += ", ";
				}
				values += "(";
				values += to_string(row_id);
				values += ", {SNAPSHOT_ID}, NULL";
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					values += ", ";
					values += DuckLakeUtil::ValueToSQL(*this, context, chunk.GetValue(c, r));
				}
				values += ")";
				row_id++;
			}
		}
		string append_query = StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s VALUES %s;",
		                                         SQLIdentifier(inlined_table_name), values);
		batch_query += append_query;
	}
	return batch_query;
}

string DuckLakeMetadataManager::WriteNewInlinedDeletes(const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes) {
	string batch_queries;
	if (new_deletes.empty()) {
		return batch_queries;
	}
	for (auto &entry : new_deletes) {
		// get a list of all deleted row-ids for this table
		string row_id_list;
		for (auto &deleted_id : entry.deleted_row_ids) {
			if (!row_id_list.empty()) {
				row_id_list += ", ";
			}
			row_id_list += StringUtil::Format("(%d)", deleted_id);
		}
		// overwrite the snapshot for the old tags
		batch_queries += StringUtil::Format(R"(
WITH deleted_row_list(deleted_row_id) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.%s
SET end_snapshot = {SNAPSHOT_ID}
FROM deleted_row_list
WHERE row_id=deleted_row_id;
)",
		                                    row_id_list, entry.table_name);
	}
	return batch_queries;
}

string DuckLakeMetadataManager::WriteNewInlinedFileDeletes(DuckLakeSnapshot &commit_snapshot,
                                                           const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes) {
	string batch_queries;
	if (new_deletes.empty()) {
		return batch_queries;
	}
	for (auto &entry : new_deletes) {
		// Get or create the inlined deletion table (handles caching internally)
		auto table_name = GetInlinedDeletionTableName(entry.table_id, commit_snapshot, true);

		// Build the values for the deletions
		string values;
		for (auto &file_entry : entry.file_deletions.file_deletes) {
			auto file_id = file_entry.first;
			for (auto &row_id : file_entry.second) {
				if (!values.empty()) {
					values += ", ";
				}
				values += StringUtil::Format("(%d, %d, {SNAPSHOT_ID})", file_id, row_id);
			}
		}
		batch_queries += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s VALUES %s;\n", table_name, values);
	}
	return batch_queries;
}

map<idx_t, set<idx_t>> DuckLakeMetadataManager::ReadInlinedFileDeletions(TableIndex table_id,
                                                                         DuckLakeSnapshot snapshot) {
	map<idx_t, set<idx_t>> result;
	auto inlined_table_name = GetInlinedDeletionTableName(table_id, snapshot);
	if (inlined_table_name.empty()) {
		return result;
	}
	auto query = StringUtil::Format("SELECT file_id, row_id FROM {METADATA_CATALOG}.%s WHERE begin_snapshot <= "
	                                "{SNAPSHOT_ID}",
	                                inlined_table_name);
	auto query_result = transaction.Query(snapshot, query);
	if (!query_result->HasError()) {
		for (auto &row : *query_result) {
			auto file_id = row.GetValue<idx_t>(0);
			auto row_id = row.GetValue<idx_t>(1);
			result[file_id].insert(row_id);
		}
	}
	return result;
}

// FIXME: We should probably cache this..
unordered_set<idx_t> DuckLakeMetadataManager::GetFileIdsWithInlinedDeletions(TableIndex table_id,
                                                                             DuckLakeSnapshot snapshot,
                                                                             const vector<idx_t> &file_ids) {
	unordered_set<idx_t> result;
	if (file_ids.empty()) {
		return result;
	}
	auto inlined_table_name = GetInlinedDeletionTableName(table_id, snapshot);
	if (inlined_table_name.empty()) {
		return result;
	}
	// Build the IN clause with file IDs
	string file_id_list;
	for (auto &file_id : file_ids) {
		if (!file_id_list.empty()) {
			file_id_list += ", ";
		}
		file_id_list += to_string(file_id);
	}
	auto query = StringUtil::Format("SELECT DISTINCT file_id FROM {METADATA_CATALOG}.%s WHERE file_id IN (%s) AND "
	                                "begin_snapshot <= {SNAPSHOT_ID}",
	                                inlined_table_name, file_id_list);
	auto query_result = transaction.Query(snapshot, query);
	if (!query_result->HasError()) {
		for (auto &row : *query_result) {
			result.insert(row.GetValue<idx_t>(0));
		}
	}
	return result;
}

map<idx_t, unordered_map<idx_t, idx_t>>
DuckLakeMetadataManager::ReadInlinedFileDeletionsForRange(TableIndex table_id, DuckLakeSnapshot start_snapshot,
                                                          DuckLakeSnapshot end_snapshot) {
	map<idx_t, unordered_map<idx_t, idx_t>> result;
	auto inlined_table_name = GetInlinedDeletionTableName(table_id, end_snapshot);
	if (inlined_table_name.empty()) {
		return result;
	}
	auto query = StringUtil::Format("SELECT file_id, row_id, begin_snapshot FROM {METADATA_CATALOG}.%s "
	                                "WHERE begin_snapshot >= %d AND begin_snapshot <= {SNAPSHOT_ID}",
	                                inlined_table_name, start_snapshot.snapshot_id);
	auto query_result = transaction.Query(end_snapshot, query);
	if (!query_result->HasError()) {
		for (auto &row : *query_result) {
			auto file_id = row.GetValue<idx_t>(0);
			auto row_id = row.GetValue<idx_t>(1);
			auto snapshot_id = row.GetValue<idx_t>(2);
			result[file_id][row_id] = snapshot_id;
		}
	}
	return result;
}

string DuckLakeMetadataManager::GetInlinedDeletionTableName(TableIndex table_id, DuckLakeSnapshot snapshot,
                                                            bool create_if_not_exists) {
	// The table name is always deterministic
	string table_name = StringUtil::Format("ducklake_inlined_delete_%d", table_id.index);

	// Check the cache first (tracks whether table exists)
	if (delete_inlined_table_cache.find(table_id.index) != delete_inlined_table_cache.end()) {
		return table_name;
	}

	if (create_if_not_exists) {
		// Create the table if it doesn't exist
		auto create_query = StringUtil::Format(
		    "CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.%s(file_id BIGINT, row_id BIGINT, begin_snapshot BIGINT);",
		    table_name);
		auto create_result = transaction.Query(snapshot, create_query);
		if (create_result->HasError()) {
			create_result->GetErrorObject().Throw("Failed to create inlined deletion table: ");
		}
		delete_inlined_table_cache.insert(table_id.index);
		return table_name;
	}

	// Check if the table exists by querying duckdb_tables()
	auto query = StringUtil::Format(
	    "SELECT COUNT(*) FROM duckdb_tables() WHERE database_name={METADATA_CATALOG_NAME_LITERAL} AND "
	    "schema_name={METADATA_SCHEMA_NAME_LITERAL} AND table_name = '%s'",
	    table_name);
	auto result = transaction.Query(snapshot, query);
	if (!result->HasError()) {
		auto chunk = result->Fetch();
		if (chunk && chunk->size() > 0 && chunk->GetValue(0, 0).GetValue<idx_t>() > 0) {
			delete_inlined_table_cache.insert(table_id.index);
			return table_name;
		}
	}
	return string();
}

shared_ptr<DuckLakeInlinedData> DuckLakeMetadataManager::TransformInlinedData(QueryResult &result) {
	if (result.HasError()) {
		result.GetErrorObject().Throw("Failed to read inlined data from DuckLake: ");
	}

	auto context = transaction.context.lock();
	auto data = make_uniq<ColumnDataCollection>(*context, result.types);
	while (true) {
		auto chunk = result.Fetch();
		if (!chunk) {
			break;
		}
		data->Append(*chunk);
	}
	auto inlined_data = make_shared_ptr<DuckLakeInlinedData>();
	inlined_data->data = std::move(data);
	return inlined_data;
}

static string GetProjection(const vector<string> &columns_to_read) {
	string result;
	for (auto &entry : columns_to_read) {
		if (!result.empty()) {
			result += ", ";
		}
		result += entry;
	}
	return result;
}

shared_ptr<DuckLakeInlinedData> DuckLakeMetadataManager::ReadInlinedData(DuckLakeSnapshot snapshot,
                                                                         const string &inlined_table_name,
                                                                         const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL);)",
	                                                             projection, inlined_table_name));
	return TransformInlinedData(*result);
}

shared_ptr<DuckLakeInlinedData>
DuckLakeMetadataManager::ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
                                                   const string &inlined_table_name,
                                                   const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result =
	    transaction.Query(end_snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE inlined_data.begin_snapshot >= %d AND inlined_data.begin_snapshot <= {SNAPSHOT_ID};)",
	                                                       projection, inlined_table_name, start_snapshot.snapshot_id));
	return TransformInlinedData(*result);
}

shared_ptr<DuckLakeInlinedData>
DuckLakeMetadataManager::ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
                                                  const string &inlined_table_name,
                                                  const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result =
	    transaction.Query(end_snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE inlined_data.end_snapshot >= %d AND inlined_data.end_snapshot <= {SNAPSHOT_ID};)",
	                                                       projection, inlined_table_name, start_snapshot.snapshot_id));
	return TransformInlinedData(*result);
}

shared_ptr<DuckLakeInlinedData>
DuckLakeMetadataManager::ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot, const string &inlined_table_name,
                                                    const vector<string> &columns_to_read) {
	auto projection = GetProjection(columns_to_read);
	auto result = transaction.Query(snapshot, StringUtil::Format(R"(
SELECT %s
FROM {METADATA_CATALOG}.%s inlined_data
WHERE {SNAPSHOT_ID} >= begin_snapshot
ORDER BY row_id;)",
	                                                             projection, inlined_table_name));
	return TransformInlinedData(*result);
}

string DuckLakeMetadataManager::GetPathForSchema(SchemaIndex schema_id,
                                                 vector<DuckLakeSchemaInfo> &new_schemas_result) {
	for (auto &schema : new_schemas_result) {
		if (schema_id == schema.id) {
			DuckLakePath path;
			path.path = schema.path;
			path.path_is_relative = false;
			return FromRelativePath(path);
		}
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema
WHERE schema_id = %d;)",
	                                                   schema_id.index));
	for (auto &row : *result) {
		DuckLakePath path;
		path.path = row.GetValue<string>(0);
		path.path_is_relative = row.GetValue<bool>(1);
		return FromRelativePath(path);
	}
	throw InvalidInputException("Failed to get path for schema with id %d - schema not found in metadata catalog",
	                            schema_id.index);
}

bool DuckLakeMetadataManager::IsColumnCreatedWithTable(const string &table_name, const string &column_name) {
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT TRUE
FROM {METADATA_CATALOG}.ducklake_table t
INNER JOIN {METADATA_CATALOG}.ducklake_column c
  ON c.table_id = t.table_id
 WHERE c.column_name = '%s' AND
 t.table_name = '%s' AND c.begin_snapshot = t.begin_snapshot AND c.end_snapshot IS NULL;
)",
	                                                   column_name, table_name));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get schema information from DuckLake: ");
	}
	// We are only interested if this returns any rows or not
	return result->Fetch() != nullptr;
}

string DuckLakeMetadataManager::GetPathForTable(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
                                                const vector<DuckLakeSchemaInfo> &new_schemas_result) {
	for (const auto &new_table : new_tables) {
		if (new_table.id == table_id) {
			// This is a table not yet in the catalog
			auto result = transaction.Query(StringUtil::Format(R"(
SELECT s.path, s.path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema s
WHERE schema_id = %d;)",
			                                                   new_table.schema_id.index));
			for (auto &row : *result) {
				DuckLakePath schema_path;
				schema_path.path = row.GetValue<string>(0);
				schema_path.path_is_relative = row.GetValue<bool>(1);
				auto resolved_schema_path = FromRelativePath(schema_path);

				DuckLakePath table_path;
				table_path.path = new_table.path;
				table_path.path_is_relative = false;
				return FromRelativePath(table_path, resolved_schema_path);
			}
			for (auto &schema : new_schemas_result) {
				if (schema.id == new_table.schema_id) {
					DuckLakePath schema_path;
					schema_path.path = schema.path;
					schema_path.path_is_relative = false;
					auto resolved_schema_path = FromRelativePath(schema_path);

					DuckLakePath table_path;
					table_path.path = new_table.path;
					table_path.path_is_relative = false;
					return FromRelativePath(table_path, resolved_schema_path);
				}
			}
		}
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT s.path, s.path_is_relative, t.path, t.path_is_relative
FROM {METADATA_CATALOG}.ducklake_schema s
JOIN {METADATA_CATALOG}.ducklake_table t
USING (schema_id)
WHERE table_id = %d;)",
	                                                   table_id.index));
	for (auto &row : *result) {
		DuckLakePath schema_path;
		schema_path.path = row.GetValue<string>(0);
		schema_path.path_is_relative = row.GetValue<bool>(1);
		auto resolved_schema_path = FromRelativePath(schema_path);

		DuckLakePath table_path;
		table_path.path = row.GetValue<string>(2);
		table_path.path_is_relative = row.GetValue<bool>(3);
		return FromRelativePath(table_path, resolved_schema_path);
	}

	throw InvalidInputException("Failed to get path for table with id %d - table not found in metadata catalog",
	                            table_id.index);
}

string DuckLakeMetadataManager::GetPath(SchemaIndex schema_id, vector<DuckLakeSchemaInfo> &new_schemas_result) {
	lock_guard<mutex> guard(paths_lock);
	// get the path from the list of cached paths
	auto entry = schema_paths.find(schema_id);
	if (entry != schema_paths.end()) {
		return entry->second;
	}
	// get the path from the current snapshot if possible
	// otherwise fetch it from the metadata catalog
	auto &catalog = transaction.GetCatalog();
	auto schema = catalog.GetEntryById(transaction, transaction.GetSnapshot(), schema_id);
	string path;
	if (schema) {
		path = schema->Cast<DuckLakeSchemaEntry>().DataPath();
	} else {
		path = GetPathForSchema(schema_id, new_schemas_result);
	}
	schema_paths.emplace(schema_id, path);
	return path;
}

string DuckLakeMetadataManager::GetPath(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
                                        const vector<DuckLakeSchemaInfo> &new_schemas_result) {
	lock_guard<mutex> guard(paths_lock);
	// get the path from the list of cached paths
	auto entry = table_paths.find(table_id);
	if (entry != table_paths.end()) {
		return entry->second;
	}
	// get the path from the current snapshot if possible
	auto &catalog = transaction.GetCatalog();
	auto table = catalog.GetEntryById(transaction, transaction.GetSnapshot(), table_id);
	string path;
	if (table) {
		path = table->Cast<DuckLakeTableEntry>().DataPath();
	} else {
		path = GetPathForTable(table_id, new_tables, new_schemas_result);
	}
	table_paths.emplace(table_id, path);
	return path;
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(const string &path) {
	auto &data_path = transaction.GetCatalog().DataPath();
	return GetRelativePath(path, data_path);
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(SchemaIndex schema_id, const string &path,
                                                      vector<DuckLakeSchemaInfo> &new_schemas_result) {
	return GetRelativePath(path, GetPath(schema_id, new_schemas_result));
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(TableIndex table_id, const string &path,
                                                      const vector<DuckLakeTableInfo> &new_tables,
                                                      vector<DuckLakeSchemaInfo> &new_schemas_result) {
	return GetRelativePath(path, GetPath(table_id, new_tables, new_schemas_result));
}

DuckLakePath DuckLakeMetadataManager::GetRelativePath(const string &path, const string &data_path) {
	DuckLakePath result;
	if (StringUtil::StartsWith(path, data_path)) {
		result.path = path.substr(data_path.size());
		result.path_is_relative = true;
	} else {
		result.path = path;
		result.path_is_relative = false;
	}
	result.path = StorePath(std::move(result.path));
	return result;
}

string DuckLakeMetadataManager::StorePath(string path) {
	auto &fs = GetFileSystem();
	auto separator = fs.PathSeparator(path);
	if (separator == "/") {
		return path;
	}
	return StringUtil::Replace(path, separator, "/");
}

string DuckLakeMetadataManager::LoadPath(string path) {
	auto &fs = GetFileSystem();
	auto separator = fs.PathSeparator(path);
	if (separator == "/") {
		return path;
	}
	return StringUtil::Replace(path, "/", separator);
}

string DuckLakeMetadataManager::FromRelativePath(const DuckLakePath &path, const string &base_path) {
	if (!path.path_is_relative) {
		return LoadPath(path.path);
	}
	return LoadPath(base_path + path.path);
}

string DuckLakeMetadataManager::FromRelativePath(const DuckLakePath &path) {
	return FromRelativePath(path, transaction.GetCatalog().DataPath());
}

string DuckLakeMetadataManager::FromRelativePath(TableIndex table_id, const DuckLakePath &path) {
	return FromRelativePath(path, GetPath(table_id, {}, {}));
}

string DuckLakeMetadataManager::WriteNewDataFiles(const vector<DuckLakeFileInfo> &new_files,
                                                  const vector<DuckLakeTableInfo> &new_tables,
                                                  vector<DuckLakeSchemaInfo> &new_schemas_result) {
	string batch_query;
	if (new_files.empty()) {
		return batch_query;
	}
	string data_file_insert_query;
	string column_stats_insert_query;
	string variant_stats_insert_query;
	string partition_insert_query;

	for (auto &file : new_files) {
		if (!data_file_insert_query.empty()) {
			data_file_insert_query += ",";
		}
		auto row_id = file.row_id_start.IsValid() ? to_string(file.row_id_start.GetIndex()) : "NULL";
		auto partition_id = file.partition_id.IsValid() ? to_string(file.partition_id.GetIndex()) : "NULL";
		auto begin_snapshot =
		    file.begin_snapshot.IsValid() ? to_string(file.begin_snapshot.GetIndex()) : "{SNAPSHOT_ID}";
		auto data_file_index = file.id.index;
		auto table_id = file.table_id.index;
		auto encryption_key =
		    file.encryption_key.empty() ? "NULL" : "'" + Blob::ToBase64(string_t(file.encryption_key)) + "'";
		string partial_max =
		    file.max_partial_file_snapshot.IsValid() ? to_string(file.max_partial_file_snapshot.GetIndex()) : "NULL";
		string footer_size = file.footer_size.IsValid() ? to_string(file.footer_size.GetIndex()) : "NULL";
		string mapping = file.mapping_id.IsValid() ? to_string(file.mapping_id.index) : "NULL";
		auto path = GetRelativePath(file.table_id, file.file_name, new_tables, new_schemas_result);
		data_file_insert_query += StringUtil::Format(
		    "(%d, %d, %s, NULL, NULL, %s, %s, 'parquet', %d, %d, %s, %s, %s, %s, %s, %s)", data_file_index, table_id,
		    begin_snapshot, SQLString(path.path), path.path_is_relative ? "true" : "false", file.row_count,
		    file.file_size_bytes, footer_size, row_id, partition_id, encryption_key, mapping, partial_max);
		for (auto &column_stats : file.column_stats) {
			if (!column_stats_insert_query.empty()) {
				column_stats_insert_query += ",";
			}
			auto column_id = column_stats.column_id.index;
			column_stats_insert_query += StringUtil::Format(
			    "(%d, %d, %d, %s, %s, %s, %s, %s, %s, %s)", data_file_index, table_id, column_id,
			    column_stats.column_size_bytes, column_stats.value_count, column_stats.null_count, column_stats.min_val,
			    column_stats.max_val, column_stats.contains_nan, column_stats.extra_stats);
			for (auto &variant_stats : column_stats.variant_stats) {
				if (!variant_stats_insert_query.empty()) {
					variant_stats_insert_query += ",";
				}
				auto &field_stats = variant_stats.field_stats;
				variant_stats_insert_query += StringUtil::Format(
				    "(%d, %d, %d, %s, %s, %s, %s, %s, %s, %s, %s, %s)", data_file_index, table_id, column_id,
				    SQLString(variant_stats.field_name), SQLString(variant_stats.shredded_type),
				    field_stats.column_size_bytes, field_stats.value_count, field_stats.null_count, field_stats.min_val,
				    field_stats.max_val, field_stats.contains_nan, field_stats.extra_stats);
			}
		}
		if (file.partition_id.IsValid() == file.partition_values.empty()) {
			throw InternalException("File should either not be partitioned, or have partition values");
		}
		for (auto &part_val : file.partition_values) {
			if (!partition_insert_query.empty()) {
				partition_insert_query += ",";
			}
			partition_insert_query +=
			    StringUtil::Format("(%d, %d, %d, %s)", data_file_index, table_id, part_val.partition_column_idx,
			                       SQLString(part_val.partition_value));
		}
	}
	if (data_file_insert_query.empty()) {
		throw InternalException("No files found!?");
	}

	// insert the data files
	batch_query +=
	    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_data_file VALUES %s;", data_file_insert_query);

	// insert the column stats
	batch_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_file_column_stats VALUES %s;",
	                                  column_stats_insert_query);

	if (!partition_insert_query.empty()) {
		// insert the partition values
		batch_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_file_partition_value VALUES %s;",
		                                  partition_insert_query);
	}
	if (!variant_stats_insert_query.empty()) {
		batch_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_file_variant_stats VALUES %s;",
		                                  variant_stats_insert_query);
	}
	return batch_query;
}

string DuckLakeMetadataManager::DropDataFiles(const set<DataFileIndex> &dropped_files) {
	return FlushDrop("ducklake_data_file", "data_file_id", dropped_files);
}

string DuckLakeMetadataManager::DropDeleteFiles(const set<DataFileIndex> &dropped_files) {
	return FlushDrop("ducklake_delete_file", "data_file_id", dropped_files);
}

string
DuckLakeMetadataManager::DeleteOverwrittenDeleteFiles(const vector<DuckLakeOverwrittenDeleteFile> &overwritten_files) {
	if (overwritten_files.empty()) {
		return {};
	}
	string deleted_file_ids;
	string scheduled_deletions;
	for (auto &file : overwritten_files) {
		if (!deleted_file_ids.empty()) {
			deleted_file_ids += ", ";
		}
		deleted_file_ids += to_string(file.delete_file_id.index);

		if (!scheduled_deletions.empty()) {
			scheduled_deletions += ", ";
		}
		auto path = GetRelativePath(file.path);
		scheduled_deletions += StringUtil::Format("(%d, %s, %s, NOW())", file.delete_file_id.index,
		                                          SQLString(path.path), path.path_is_relative ? "true" : "false");
	}

	string batch_query;
	// delete the old delete file metadata records
	batch_query += StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE delete_file_id IN (%s);
)",
	                                  deleted_file_ids);
	// schedule the old files for disk deletion
	batch_query +=
	    "INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion VALUES " + scheduled_deletions + ";";
	return batch_query;
}

string DuckLakeMetadataManager::WriteNewDeleteFiles(const vector<DuckLakeDeleteFileInfo> &new_files,
                                                    const vector<DuckLakeTableInfo> &new_tables,
                                                    vector<DuckLakeSchemaInfo> &new_schemas_result) {
	if (new_files.empty()) {
		return {};
	}
	string delete_file_insert_query;
	for (auto &file : new_files) {
		if (!delete_file_insert_query.empty()) {
			delete_file_insert_query += ",";
		}
		auto delete_file_index = file.id.index;
		auto table_id = file.table_id.index;
		auto data_file_index = file.data_file_id.index;
		auto encryption_key =
		    file.encryption_key.empty() ? "NULL" : "'" + Blob::ToBase64(string_t(file.encryption_key)) + "'";
		auto path = GetRelativePath(file.table_id, file.path, new_tables, new_schemas_result);
		// Use explicit begin_snapshot if set (for flush operations), otherwise use commit snapshot
		string begin_snapshot_str =
		    file.begin_snapshot.IsValid() ? std::to_string(file.begin_snapshot.GetIndex()) : "{SNAPSHOT_ID}";
		string partial_max = file.max_snapshot.IsValid() ? to_string(file.max_snapshot.GetIndex()) : "NULL";
		delete_file_insert_query += StringUtil::Format(
		    "(%d, %d, %s, NULL,  %d, %s, %s, 'parquet', %d, %d, %d, %s, %s)", delete_file_index, table_id,
		    begin_snapshot_str, data_file_index, SQLString(path.path), path.path_is_relative ? "true" : "false",
		    file.delete_count, file.file_size_bytes, file.footer_size, encryption_key, partial_max);
	}

	// insert the data files
	return StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_delete_file VALUES %s;",
	                          delete_file_insert_query);
}

vector<DuckLakeColumnMappingInfo> DuckLakeMetadataManager::GetColumnMappings(optional_idx start_from) {
	string filter;
	if (start_from.IsValid()) {
		filter = "WHERE mapping_id >= " + to_string(start_from.GetIndex());
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT mapping_id, table_id, type, column_id, source_name, target_field_id, parent_column, is_partition
FROM {METADATA_CATALOG}.ducklake_column_mapping
JOIN {METADATA_CATALOG}.ducklake_name_mapping USING (mapping_id)
%s
ORDER BY mapping_id, parent_column NULLS FIRST
)",
	                                                   filter));
	vector<DuckLakeColumnMappingInfo> column_maps;
	for (auto &row : *result) {
		MappingIndex mapping_id(row.GetValue<idx_t>(0));
		if (column_maps.empty() || column_maps.back().mapping_id != mapping_id) {
			DuckLakeColumnMappingInfo mapping_info;
			mapping_info.mapping_id = mapping_id;
			mapping_info.table_id = TableIndex(row.GetValue<idx_t>(1));
			mapping_info.map_type = row.GetValue<string>(2);
			column_maps.push_back(std::move(mapping_info));
		}
		auto &mapping_info = column_maps.back();
		DuckLakeNameMapColumnInfo name_map_column;
		name_map_column.column_id = row.GetValue<idx_t>(3);
		name_map_column.source_name = row.GetValue<string>(4);
		name_map_column.target_field_id = FieldIndex(row.GetValue<idx_t>(5));
		if (!row.IsNull(6)) {
			name_map_column.parent_column = row.GetValue<idx_t>(6);
		}
		name_map_column.hive_partition = row.GetValue<bool>(7);
		mapping_info.map_columns.push_back(std::move(name_map_column));
	}
	return column_maps;
}

string DuckLakeMetadataManager::WriteNewColumnMappings(const vector<DuckLakeColumnMappingInfo> &new_column_mappings) {
	string column_mapping_insert_query;
	string name_map_insert_query;
	for (auto &column_mapping : new_column_mappings) {
		if (!column_mapping_insert_query.empty()) {
			column_mapping_insert_query += ", ";
		}
		column_mapping_insert_query +=
		    StringUtil::Format("(%d, %d, %s)", column_mapping.mapping_id.index, column_mapping.table_id.index,
		                       SQLString(column_mapping.map_type));
		for (auto &name_map_column : column_mapping.map_columns) {
			if (!name_map_insert_query.empty()) {
				name_map_insert_query += ", ";
			}
			string parent_column =
			    name_map_column.parent_column.IsValid() ? to_string(name_map_column.parent_column.GetIndex()) : "NULL";
			string is_partition = name_map_column.hive_partition ? "true" : "false";
			name_map_insert_query +=
			    StringUtil::Format("(%d, %d, %s, %d, %s, %s)", column_mapping.mapping_id.index,
			                       name_map_column.column_id, SQLString(name_map_column.source_name),
			                       name_map_column.target_field_id.index, parent_column, is_partition);
		}
	}
	string batch_query;
	batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_column_mapping VALUES " + column_mapping_insert_query + ";";
	batch_query += "INSERT INTO {METADATA_CATALOG}.ducklake_name_mapping VALUES " + name_map_insert_query + ";";
	return batch_query;
}

string DuckLakeMetadataManager::InsertSnapshot() {
	return R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot VALUES ({SNAPSHOT_ID}, NOW(), {SCHEMA_VERSION}, {NEXT_CATALOG_ID}, {NEXT_FILE_ID});)";
}

static string SQLStringOrNull(const string &str) {
	if (str.empty()) {
		return "NULL";
	}
	return KeywordHelper::WriteQuoted(str, '\'');
}

string DuckLakeMetadataManager::WriteSnapshotChanges(const SnapshotChangeInfo &change_info,
                                                     const DuckLakeSnapshotCommit &commit_info) {
	// insert the snapshot changes
	return StringUtil::Format(
	    R"(INSERT INTO {METADATA_CATALOG}.ducklake_snapshot_changes VALUES ({SNAPSHOT_ID}, %s, %s, %s, %s);)",
	    SQLStringOrNull(change_info.changes_made), commit_info.author.ToSQLString(),
	    commit_info.commit_message.ToSQLString(), commit_info.commit_extra_info.ToSQLString());
}

SnapshotChangeInfo DuckLakeMetadataManager::GetSnapshotAndStatsAndChanges(DuckLakeSnapshot start_snapshot,
                                                                          SnapshotAndStats &current_snapshot) {
	// get all changes made to the system after the snapshot was started
	string query = R"(
SELECT
    snapshot_id,
    schema_version,
    next_catalog_id,
    next_file_id,
    COALESCE((
            SELECT STRING_AGG(changes_made, '')
            FROM {METADATA_CATALOG}.ducklake_snapshot_changes c
            WHERE c.snapshot_id > {SNAPSHOT_ID}
            ),'') AS changes,
    NULL AS table_id,
    NULL AS column_id,
    NULL AS record_count,
    NULL AS next_row_id,
    NULL AS file_size_bytes,
    NULL AS contains_null,
    NULL AS contains_nan,
    NULL AS min_value,
    NULL AS max_value,
    NULL AS extra_stats
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id = (
        SELECT MAX(snapshot_id)
        FROM {METADATA_CATALOG}.ducklake_snapshot)
UNION ALL
SELECT
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    table_id,
    column_id,
    record_count,
    next_row_id,
    file_size_bytes,
    contains_null,
    contains_nan,
    min_value,
    max_value,
    extra_stats
FROM {METADATA_CATALOG}.ducklake_table_stats
LEFT JOIN {METADATA_CATALOG}.ducklake_table_column_stats
    USING (table_id)
WHERE record_count IS NOT NULL
    AND file_size_bytes IS NOT NULL
ORDER BY table_id NULLS FIRST;
	)";
	auto result = Query(start_snapshot, query);
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to commit DuckLake transaction - failed to get snapshot and snapshot "
		                               "changes for conflict resolution:");
	}
	// parse changes made by other transactions
	SnapshotChangeInfo change_info;

	bool first_row = true;
	for (auto &row : *result) {
		if (first_row) {
			current_snapshot.snapshot.snapshot_id = row.GetValue<idx_t>(0);
			current_snapshot.snapshot.schema_version = row.GetValue<idx_t>(1);
			current_snapshot.snapshot.next_catalog_id = row.GetValue<idx_t>(2);
			current_snapshot.snapshot.next_file_id = row.GetValue<idx_t>(3);
			change_info.changes_made = row.GetValue<string>(4);
		} else {
			TransformGlobalStatsRow(row, current_snapshot.stats, 5);
		}
		first_row = false;
	}
	return change_info;
}

SnapshotDeletedFromFiles
DuckLakeMetadataManager::GetFilesDeletedOrDroppedAfterSnapshot(const DuckLakeSnapshot &start_snapshot) const {
	// get all changes made to the system after the snapshot was started
	auto result = transaction.Query(start_snapshot, R"(
	SELECT data_file_id
	FROM {METADATA_CATALOG}.ducklake_delete_file
	WHERE begin_snapshot > {SNAPSHOT_ID}
	UNION ALL
	SELECT data_file_id
	FROM {METADATA_CATALOG}.ducklake_data_file
	WHERE end_snapshot IS NOT NULL AND end_snapshot > {SNAPSHOT_ID}
	)");
	if (result->HasError()) {
		result->GetErrorObject().Throw(
		    "Failed to commit DuckLake transaction - failed to get files with deletions for conflict resolution:");
	}
	// parse changes made by other transactions
	SnapshotDeletedFromFiles change_info;
	for (auto &row : *result) {
		change_info.deleted_from_files.insert(DataFileIndex(row.GetValue<idx_t>(0)));
	}
	return change_info;
}

static unique_ptr<DuckLakeSnapshot> TryGetSnapshotInternal(QueryResult &result) {
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

string DuckLakeMetadataManager::GetLatestSnapshotQuery() const {
	return R"(SELECT snapshot_id, schema_version, next_catalog_id, next_file_id FROM {METADATA_CATALOG}.ducklake_snapshot WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM {METADATA_CATALOG}.ducklake_snapshot);)";
}

unique_ptr<DuckLakeSnapshot> DuckLakeMetadataManager::GetSnapshot() {
	auto result = transaction.Query(GetLatestSnapshotQuery());
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to query most recent snapshot for DuckLake: ");
	}
	auto snapshot = TryGetSnapshotInternal(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found in DuckLake");
	}
	return snapshot;
}

unique_ptr<DuckLakeSnapshot> DuckLakeMetadataManager::GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound) {
	auto &unit = at_clause.Unit();
	auto &val = at_clause.GetValue();
	unique_ptr<QueryResult> result;
	const string timestamp_aggregate = bound == SnapshotBound::LOWER_BOUND ? "MIN" : "MAX";
	const string timestamp_condition = bound == SnapshotBound::LOWER_BOUND ? ">" : "<";
	if (StringUtil::CIEquals(unit, "version")) {
		result = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE snapshot_id = %llu;)",
		                                              val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>()));
	} else if (StringUtil::CIEquals(unit, "timestamp")) {
		result = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
FROM {METADATA_CATALOG}.ducklake_snapshot
WHERE snapshot_id = (
	SELECT %s_BY(snapshot_id, snapshot_time)
	FROM {METADATA_CATALOG}.ducklake_snapshot
	WHERE snapshot_time %s= %s);)",
		                                              timestamp_aggregate, timestamp_condition,
		                                              val.DefaultCastAs(LogicalType::VARCHAR).ToSQLString()));
	} else {
		throw InvalidInputException("Unsupported AT clause unit - %s", unit);
	}
	if (result->HasError()) {
		result->GetErrorObject().Throw(StringUtil::Format(
		    "Failed to query snapshot at %s %s for DuckLake: ", StringUtil::Lower(unit), val.ToString()));
	}
	auto snapshot = TryGetSnapshotInternal(*result);
	if (!snapshot) {
		throw InvalidInputException("No snapshot found at %s %s", StringUtil::Lower(unit), val.ToString());
	}
	return snapshot;
}

static unordered_map<idx_t, DuckLakePartitionInfo>
GetNewPartitions(const vector<DuckLakePartitionInfo> &old_partitions,
                 const vector<DuckLakePartitionInfo> &new_partitions) {
	unordered_map<idx_t, DuckLakePartitionInfo> new_partition_map;

	for (auto &partition : new_partitions) {
		new_partition_map[partition.table_id.index] = partition;
	}

	unordered_set<idx_t> old_partition_set;
	for (auto &partition : old_partitions) {
		old_partition_set.insert(partition.table_id.index);
		if (new_partition_map.find(partition.table_id.index) != new_partition_map.end()) {
			if (new_partition_map[partition.table_id.index] == partition) {
				// If a new partition already exists in an old partition, it's a nop, we can remove it
				new_partition_map.erase(partition.table_id.index);
			}
		}
	}

	vector<idx_t> partition_ids_to_erase;
	for (auto &partition : new_partitions) {
		if (old_partition_set.find(partition.table_id.index) == old_partition_set.end() && partition.fields.empty()) {
			// If a map does not exist on the old partition and the partition has no fields, this is an reset over
			// and empty partition definition, hence also a nop
			partition_ids_to_erase.push_back(partition.table_id.index);
		}
	}
	for (auto &id : partition_ids_to_erase) {
		new_partition_map.erase(id);
	}
	return new_partition_map;
}

string DuckLakeMetadataManager::WriteNewPartitionKeys(DuckLakeSnapshot commit_snapshot,
                                                      const vector<DuckLakePartitionInfo> &new_partitions) {
	if (new_partitions.empty()) {
		return {};
	}
	auto catalog = GetCatalogForSnapshot(commit_snapshot);

	string old_partition_table_ids;
	string new_partition_values;
	string insert_partition_cols;

	auto new_partition_map = GetNewPartitions(catalog.partitions, new_partitions);
	if (new_partition_map.empty()) {
		return {};
	}
	for (auto &new_partition : new_partition_map) {
		// set old partition data as no longer valid
		if (!old_partition_table_ids.empty()) {
			old_partition_table_ids += ", ";
		}
		old_partition_table_ids += to_string(new_partition.second.table_id.index);
		if (!new_partition.second.id.IsValid()) {
			// dropping partition data - we don't need to do anything
			return {};
		}
		auto partition_id = new_partition.second.id.GetIndex();
		if (!new_partition_values.empty()) {
			new_partition_values += ", ";
		}
		new_partition_values +=
		    StringUtil::Format(R"((%d, %d, {SNAPSHOT_ID}, NULL))", partition_id, new_partition.second.table_id.index);
		for (auto &field : new_partition.second.fields) {
			if (!insert_partition_cols.empty()) {
				insert_partition_cols += ", ";
			}
			insert_partition_cols +=
			    StringUtil::Format("(%d, %d, %d, %d, %s)", partition_id, new_partition.second.table_id.index,
			                       field.partition_key_index, field.field_id.index, SQLString(field.transform));
		}
	}

	// update old partition information for any tables that have been altered
	auto update_partition_query = StringUtil::Format(R"(
UPDATE {METADATA_CATALOG}.ducklake_partition_info
SET end_snapshot = {SNAPSHOT_ID}
WHERE table_id IN (%s) AND end_snapshot IS NULL
;)",
	                                                 old_partition_table_ids);
	string batch_query = update_partition_query;

	if (!new_partition_values.empty()) {
		new_partition_values =
		    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_info VALUES " + new_partition_values + ";";
		batch_query += new_partition_values;
	}
	if (!insert_partition_cols.empty()) {
		insert_partition_cols =
		    "INSERT INTO {METADATA_CATALOG}.ducklake_partition_column VALUES " + insert_partition_cols + ";";
		batch_query += insert_partition_cols;
	}
	return batch_query;
}

void CheckTableSortEqual(const vector<DuckLakeSortInfo> &old_sorts,
                         unordered_map<idx_t, DuckLakeSortInfo> &new_sort_map) {
	for (auto &sort : old_sorts) {
		if (new_sort_map.find(sort.table_id.index) != new_sort_map.end()) {
			if (new_sort_map[sort.table_id.index] == sort) {
				// If a new sort already exists in an old sort, it's a nop, we can remove it
				new_sort_map.erase(sort.table_id.index);
			}
		}
	}
}

void CheckTableSortReset(const unordered_set<idx_t> &old_sort_set, const vector<DuckLakeSortInfo> &new_sorts,
                         unordered_map<idx_t, DuckLakeSortInfo> &new_sort_map) {
	vector<idx_t> sort_ids_to_erase;
	for (auto &sort : new_sorts) {
		if (old_sort_set.find(sort.table_id.index) == old_sort_set.end() && sort.fields.empty()) {
			// If a map does not exist on the old sort and the sort has no fields, this is an reset over
			// an empty sort definition, hence also a nop
			sort_ids_to_erase.push_back(sort.table_id.index);
		}
	}
	for (auto &id : sort_ids_to_erase) {
		new_sort_map.erase(id);
	}
}

static unordered_map<idx_t, DuckLakeSortInfo> GetNewSorts(const vector<DuckLakeSortInfo> &old_sorts,
                                                          const vector<DuckLakeSortInfo> &new_sorts) {
	unordered_map<idx_t, DuckLakeSortInfo> new_sort_map;
	for (auto &sort : new_sorts) {
		new_sort_map[sort.table_id.index] = sort;
	}
	unordered_set<idx_t> old_sort_set;
	for (auto &sort : old_sorts) {
		old_sort_set.insert(sort.table_id.index);
	}
	CheckTableSortEqual(old_sorts, new_sort_map);
	CheckTableSortReset(old_sort_set, new_sorts, new_sort_map);

	return new_sort_map;
}

string DuckLakeMetadataManager::WriteNewSortKeys(DuckLakeSnapshot commit_snapshot,
                                                 const vector<DuckLakeSortInfo> &new_sorts) {
	if (new_sorts.empty()) {
		return {};
	}
	auto catalog = GetCatalogForSnapshot(commit_snapshot);

	string old_sort_table_ids;
	string new_sort_values;
	string new_sort_expressions;

	// Do not update if they are the same
	auto new_sort_map = GetNewSorts(catalog.sorts, new_sorts);
	if (new_sort_map.empty()) {
		return {};
	}
	for (auto &new_sort : new_sort_map) {
		// set old partition data as no longer valid
		if (!old_sort_table_ids.empty()) {
			old_sort_table_ids += ", ";
		}
		old_sort_table_ids += to_string(new_sort.second.table_id.index);

		if (!new_sort.second.id.IsValid()) {
			// dropping sort data - skip adding new values but continue to set end_snapshot on old sort
			continue;
		}
		auto sort_id = new_sort.second.id.GetIndex();

		if (!new_sort_values.empty()) {
			new_sort_values += ", ";
		}
		new_sort_values +=
		    StringUtil::Format(R"((%d, %d, {SNAPSHOT_ID}, NULL))", sort_id, new_sort.second.table_id.index);

		for (auto &field : new_sort.second.fields) {
			if (!new_sort_expressions.empty()) {
				new_sort_expressions += ", ";
			}
			string sort_direction = (field.sort_direction == OrderType::DESCENDING ? "DESC" : "ASC");
			string null_order = (field.null_order == OrderByNullType::NULLS_FIRST ? "NULLS_FIRST" : "NULLS_LAST");
			new_sort_expressions +=
			    StringUtil::Format("(%d, %d, %d, %s, %s, %s, %s)", sort_id, new_sort.second.table_id.index,
			                       field.sort_key_index, SQLString(field.expression), SQLString(field.dialect),
			                       SQLString(sort_direction), SQLString(null_order));
		}
	}
	// update old sort information for any tables that have been altered
	auto update_sort_query = StringUtil::Format(R"(
UPDATE {METADATA_CATALOG}.ducklake_sort_info
SET end_snapshot = {SNAPSHOT_ID}
WHERE table_id IN (%s) AND end_snapshot IS NULL
;)",
	                                            old_sort_table_ids);
	string batch_query = update_sort_query;
	if (!new_sort_values.empty()) {
		new_sort_values = "INSERT INTO {METADATA_CATALOG}.ducklake_sort_info VALUES " + new_sort_values + ";";
		batch_query += new_sort_values;
	}
	if (!new_sort_expressions.empty()) {
		new_sort_expressions =
		    "INSERT INTO {METADATA_CATALOG}.ducklake_sort_expression VALUES " + new_sort_expressions + ";";
		batch_query += new_sort_expressions;
	}

	return batch_query;
}

string DuckLakeMetadataManager::WriteNewTags(const vector<DuckLakeTagInfo> &new_tags) {
	if (new_tags.empty()) {
		return {};
	}
	// update old tags (if there were any)
	// get a list of all tags
	string tags_list;
	for (auto &tag : new_tags) {
		if (!tags_list.empty()) {
			tags_list += ", ";
		}
		tags_list += StringUtil::Format("(%d, %s)", tag.id, SQLString(tag.key));
	}

	// overwrite the snapshot for the old tags
	string batch_query = StringUtil::Format(R"(
WITH overwritten_tags(tid, key) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_tag
SET end_snapshot = {SNAPSHOT_ID}
FROM overwritten_tags
WHERE object_id=tid
;)",
	                                        tags_list);

	// now insert the new tags
	string new_tag_query;
	for (auto &tag : new_tags) {
		if (!new_tag_query.empty()) {
			new_tag_query += ", ";
		}
		new_tag_query += StringUtil::Format("(%d, {SNAPSHOT_ID}, NULL, %s, %s)", tag.id, SQLString(tag.key),
		                                    tag.value.ToSQLString());
	}

	new_tag_query = "INSERT INTO {METADATA_CATALOG}.ducklake_tag VALUES " + new_tag_query + ";";
	batch_query += new_tag_query;
	return batch_query;
}

string DuckLakeMetadataManager::WriteNewColumnTags(const vector<DuckLakeColumnTagInfo> &new_tags) {
	if (new_tags.empty()) {
		return {};
	}
	// update old tags (if there were any)
	// get a list of all tags
	string tags_list;
	for (auto &tag : new_tags) {
		if (!tags_list.empty()) {
			tags_list += ", ";
		}
		tags_list += StringUtil::Format("(%d, %d, %s)", tag.table_id.index, tag.field_index.index, SQLString(tag.key));
	}

	// overwrite the snapshot for the old tags
	string batch_query = StringUtil::Format(R"(
WITH overwritten_tags(tid, cid, key) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_column_tag
SET end_snapshot = {SNAPSHOT_ID}
FROM overwritten_tags
WHERE table_id=tid AND column_id=cid
;)",
	                                        tags_list);

	// now insert the new tags
	string new_tag_query;
	for (auto &tag : new_tags) {
		if (!new_tag_query.empty()) {
			new_tag_query += ", ";
		}
		new_tag_query += StringUtil::Format("(%d, %d, {SNAPSHOT_ID}, NULL, %s, %s)", tag.table_id.index,
		                                    tag.field_index.index, SQLString(tag.key), tag.value.ToSQLString());
	}

	new_tag_query = "INSERT INTO {METADATA_CATALOG}.ducklake_column_tag VALUES " + new_tag_query + ";";
	batch_query += new_tag_query;
	return batch_query;
}

string DuckLakeMetadataManager::UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats) {
	string column_stats_values;
	for (auto &col_stats : stats.column_stats) {
		if (!column_stats_values.empty()) {
			column_stats_values += ",";
		}
		string contains_null;
		if (col_stats.has_contains_null) {
			contains_null = col_stats.contains_null ? "true" : "false";
		} else {
			contains_null = "NULL";
		}
		string contains_nan;
		if (col_stats.has_contains_nan) {
			contains_nan = col_stats.contains_nan ? "true" : "false";
		} else {
			contains_nan = "NULL";
		}
		string min_val = col_stats.has_min ? DuckLakeUtil::StatsToString(col_stats.min_val) : "NULL";
		string max_val = col_stats.has_max ? DuckLakeUtil::StatsToString(col_stats.max_val) : "NULL";
		string extra_stats_val = col_stats.has_extra_stats ? col_stats.extra_stats : "NULL";

		column_stats_values +=
		    StringUtil::Format("(%d, %d, %s, %s, %s, %s, %s)", stats.table_id.index, col_stats.column_id.index,
		                       contains_null, contains_nan, min_val, max_val, extra_stats_val);
	}
	string batch_query;

	if (!stats.initialized) {
		// stats have not been initialized yet - insert them
		batch_query +=
		    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_stats VALUES (%d, %d, %d, %d);",
		                       stats.table_id.index, stats.record_count, stats.next_row_id, stats.table_size_bytes);
		batch_query += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_table_column_stats VALUES %s;",
		                                  column_stats_values);
	} else {
		// stats have been initialized - update them
		batch_query += StringUtil::Format(
		    "UPDATE {METADATA_CATALOG}.ducklake_table_stats SET record_count=%d, file_size_bytes=%d, "
		    "next_row_id=%d WHERE table_id=%d;",
		    stats.record_count, stats.table_size_bytes, stats.next_row_id, stats.table_id.index);
		batch_query += StringUtil::Format(R"(
WITH new_values(tid, cid, new_contains_null, new_contains_nan, new_min, new_max, new_extra_stats) AS (
VALUES %s
)
UPDATE {METADATA_CATALOG}.ducklake_table_column_stats
SET contains_null=new_contains_null::boolean, contains_nan=new_contains_nan::boolean, min_value=new_min, max_value=new_max, extra_stats=new_extra_stats
FROM new_values
WHERE table_id=tid AND column_id=cid;
)",
		                                  column_stats_values);
	}
	return batch_query;
}

template <class T>
static timestamp_tz_t GetTimestampTZFromRow(ClientContext &context, const T &row, idx_t col_idx) {
	auto val = row.GetChunk().GetValue(col_idx, row.GetRowInChunk());
	return val.CastAs(context, LogicalType::TIMESTAMP_TZ).template GetValue<timestamp_tz_t>();
}

vector<DuckLakeSnapshotInfo> DuckLakeMetadataManager::GetAllSnapshots(const string &filter) {
	auto res = transaction.Query(StringUtil::Format(R"(
SELECT snapshot_id, snapshot_time, schema_version, changes_made, author, commit_message, commit_extra_info
FROM {METADATA_CATALOG}.ducklake_snapshot
LEFT JOIN {METADATA_CATALOG}.ducklake_snapshot_changes USING (snapshot_id)
%s %s
ORDER BY snapshot_id
)",
	                                                filter.empty() ? "" : "WHERE", filter));
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to get snapshot information from DuckLake: ");
	}
	auto context = transaction.context.lock();
	vector<DuckLakeSnapshotInfo> snapshots;

	for (auto &row : *res) {
		DuckLakeSnapshotInfo snapshot_info;
		snapshot_info.id = row.GetValue<idx_t>(0);
		snapshot_info.time = GetTimestampTZFromRow(*context, row, 1);
		snapshot_info.schema_version = row.GetValue<idx_t>(2);
		snapshot_info.change_info.changes_made = row.IsNull(3) ? string() : row.GetValue<string>(3);
		snapshot_info.author = row.GetChunk().GetValue(4, row.GetRowInChunk());
		snapshot_info.commit_message = row.GetChunk().GetValue(5, row.GetRowInChunk());
		snapshot_info.commit_extra_info = row.GetChunk().GetValue(6, row.GetRowInChunk());
		snapshots.push_back(std::move(snapshot_info));
	}
	return snapshots;
}

vector<DuckLakeFileForCleanup> DuckLakeMetadataManager::GetOldFilesForCleanup(const string &filter) {
	auto query = R"(
SELECT data_file_id, path, path_is_relative, schedule_start
FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
)" + filter;
	auto res = transaction.Query(query);
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to get files scheduled for deletion from DuckLake: ");
	}
	auto context = transaction.context.lock();
	vector<DuckLakeFileForCleanup> result;
	for (auto &row : *res) {
		DuckLakeFileForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		DuckLakePath path;
		path.path = row.GetValue<string>(1);
		path.path_is_relative = row.GetValue<bool>(2);
		info.path = FromRelativePath(path);
		info.time = GetTimestampTZFromRow(*context, row, 3);
		result.push_back(std::move(info));
	}
	return result;
}
vector<DuckLakeFileForCleanup> DuckLakeMetadataManager::GetOrphanFilesForCleanup(const string &filter,
                                                                                 const string &separator) {
	auto query = R"(SELECT filename
FROM read_blob({DATA_PATH} || '**')
WHERE filename NOT IN (
SELECT REPLACE(
           CASE
               WHEN NOT file_relative THEN file_path
               ELSE CASE
                        WHEN NOT table_relative THEN table_path || file_path
                        ELSE CASE
                                 WHEN NOT schema_relative THEN schema_path || table_path || file_path
                                 ELSE {DATA_PATH} || schema_path || table_path || file_path
                             END
                   END
           END,
           '/',
           '{SEPARATOR}'
       ) AS full_path
FROM
  (SELECT s.path AS schema_path, t.path AS table_path, file_path, s.path_is_relative AS schema_relative, t.path_is_relative AS table_relative, file_relative FROM (
    SELECT f.path AS file_path, f.path_is_relative AS file_relative, table_id
    FROM {METADATA_CATALOG}.ducklake_data_file f
    UNION ALL
    SELECT f.path AS file_path, f.path_is_relative AS file_relative, table_id
    FROM {METADATA_CATALOG}.ducklake_delete_file f
  ) AS f
   JOIN {METADATA_CATALOG}.ducklake_table t ON f.table_id = t.table_id
   JOIN {METADATA_CATALOG}.ducklake_schema s ON t.schema_id = s.schema_id) AS r
UNION ALL
SELECT REPLACE(
    CASE
        WHEN NOT f.path_is_relative THEN f.path
        ELSE {DATA_PATH} || f.path
    END ,
           '/',
           '{SEPARATOR}'
) AS full_path
FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion f
)
)" + filter;
	query = StringUtil::Replace(query, "{SEPARATOR}", separator);
	auto res = transaction.Query(query);
	if (res->HasError()) {
		res->GetErrorObject().Throw("Failed to get files scheduled for deletion from DuckLake: ");
	}
	auto context = transaction.context.lock();
	vector<DuckLakeFileForCleanup> result;
	for (auto &row : *res) {
		DuckLakeFileForCleanup info;
		info.path = row.GetValue<string>(0);
		result.push_back(std::move(info));
	}
	return result;
}

vector<DuckLakeFileForCleanup> DuckLakeMetadataManager::GetFilesForCleanup(const string &filter, CleanupType type,
                                                                           const string &separator) {
	switch (type) {
	case CleanupType::OLD_FILES:
		return GetOldFilesForCleanup(filter);
	case CleanupType::ORPHANED_FILES:
		return GetOrphanFilesForCleanup(filter, separator);
	default:
		throw InternalException("CleanupType in DuckLakeMetadataManager::GetFilesForCleanup is not valid");
	}
}

void DuckLakeMetadataManager::RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) {
	string deleted_file_ids;
	for (auto &file : cleaned_up_files) {
		if (!deleted_file_ids.empty()) {
			deleted_file_ids += ", ";
		}
		deleted_file_ids += to_string(file.id.index);
	}
	auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
WHERE data_file_id IN (%s);
)",
	                                                   deleted_file_ids));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete scheduled cleanup files in DuckLake: ");
	}
}

idx_t DuckLakeMetadataManager::GetNextColumnId(TableIndex table_id) {
	auto result = transaction.Query(StringUtil::Format(R"(
	SELECT MAX(column_id)
	FROM {METADATA_CATALOG}.ducklake_column
	WHERE table_id=%d
)",
	                                                   table_id.index));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to get next column id in DuckLake: ");
	}
	for (auto &row : *result) {
		if (row.IsNull(0)) {
			break;
		}
		return row.GetValue<idx_t>(00) + 1;
	}
	throw InternalException("Invalid result for GetNextColumnId");
}

string DuckLakeMetadataManager::WriteMergeAdjacent(const vector<DuckLakeCompactedFileInfo> &compactions) {
	if (compactions.empty()) {
		return {};
	}
	string deleted_file_ids;
	string scheduled_deletions;
	for (auto &compaction : compactions) {
		D_ASSERT(!compaction.path.empty());
		// add a data file id to list of files to delete
		if (!deleted_file_ids.empty()) {
			deleted_file_ids += ", ";
		}
		deleted_file_ids += to_string(compaction.source_id.index);

		// schedule the file for deletion
		if (!scheduled_deletions.empty()) {
			scheduled_deletions += ", ";
		}
		auto path = GetRelativePath(compaction.path);
		scheduled_deletions += StringUtil::Format("(%d, %s, %s, NOW())", compaction.source_id.index,
		                                          SQLString(path.path), path.path_is_relative ? "true" : "false");
	}
	// for each file that has been compacted - delete it from the list of data files entirely
	// including all other info (stats, delete files, partition values, etc)
	vector<string> tables_to_delete_from {"ducklake_data_file", "ducklake_file_column_stats", "ducklake_delete_file",
	                                      "ducklake_file_partition_value"};
	string batch_query;
	for (auto &delete_from_tbl : tables_to_delete_from) {
		batch_query += StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE data_file_id IN (%s);
)",
		                                  delete_from_tbl, deleted_file_ids);
	}
	// add the files we cleared to the deletion schedule
	batch_query +=
	    "INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion VALUES " + scheduled_deletions + ";";
	return batch_query;
}
string DuckLakeMetadataManager::WriteDeleteRewrites(const vector<DuckLakeCompactedFileInfo> &compactions) {
	if (compactions.empty()) {
		return {};
	}
	unordered_map<idx_t, idx_t> table_idx_last_snapshot;
	for (idx_t i = compactions.size(); i > 0; i--) {
		auto &compaction = compactions[i - 1];
		if (table_idx_last_snapshot.find(compaction.table_index.index) == table_idx_last_snapshot.end()) {
			table_idx_last_snapshot[compaction.table_index.index] = compaction.delete_file_start_snapshot.GetIndex();
		}
	}

	string batch_query;
	for (idx_t i = 0; i < compactions.size(); ++i) {
		auto &compaction = compactions[i];
		D_ASSERT(!compaction.path.empty());
		if (!compaction.delete_file_end_snapshot.IsValid()) {
			batch_query += StringUtil::Format(R"(
			UPDATE {METADATA_CATALOG}.ducklake_delete_file SET end_snapshot = %llu
			WHERE delete_file_id = %llu;
			)",
			                                  table_idx_last_snapshot[compaction.table_index.index],
			                                  compaction.delete_file_id.index);
		}
		// We must update the data file table
		batch_query +=
		    StringUtil::Format(R"(
		UPDATE {METADATA_CATALOG}.ducklake_data_file SET end_snapshot = %llu
		WHERE data_file_id = %llu;
		)",
		                       table_idx_last_snapshot[compaction.table_index.index], compaction.source_id.index);
		// update the snapshot of our newly added file
		batch_query +=
		    StringUtil::Format(R"(
			UPDATE {METADATA_CATALOG}.ducklake_data_file SET begin_snapshot = %llu
			WHERE data_file_id = %llu;
			)",
		                       table_idx_last_snapshot[compaction.table_index.index], compaction.new_id.index);
	}
	return batch_query;
}

string DuckLakeMetadataManager::WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions,
                                                 CompactionType type) {
	switch (type) {
	case CompactionType::MERGE_ADJACENT_TABLES:
		return WriteMergeAdjacent(compactions);
	case CompactionType::REWRITE_DELETES:
		return WriteDeleteRewrites(compactions);
	default:
		throw InternalException("DuckLakeMetadataManager::WriteCompactions: CompactionType is not accepted");
	}
}

void DuckLakeMetadataManager::DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) {
	unique_ptr<QueryResult> result;
	// first delete the actual snapshots
	string snapshot_ids;
	for (auto &snapshot : snapshots) {
		if (!snapshot_ids.empty()) {
			snapshot_ids += ", ";
		}
		snapshot_ids += to_string(snapshot.id);
	}
	vector<string> tables_to_delete_from {"ducklake_snapshot", "ducklake_snapshot_changes"};
	for (auto &delete_tbl : tables_to_delete_from) {
		result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE snapshot_id IN (%s);
)",
		                                              delete_tbl, snapshot_ids));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete snapshots in DuckLake: ");
		}
	}
	// get a list of tables that are no longer required after these deletions
	result = transaction.Query(R"(
SELECT table_id
FROM {METADATA_CATALOG}.ducklake_table t
WHERE end_snapshot IS NOT NULL AND NOT EXISTS (
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
)
AND NOT EXISTS (
    SELECT 1
    FROM {METADATA_CATALOG}.ducklake_table t2
    WHERE t2.table_id = t.table_id
      AND (t2.end_snapshot IS NULL OR  EXISTS (SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE  snapshot_id >= begin_snapshot AND snapshot_id < t2.end_snapshot))
  );)");

	vector<TableIndex> cleanup_tables;
	for (auto &row : *result) {
		cleanup_tables.push_back(TableIndex(row.GetValue<idx_t>(0)));
	}
	string deleted_table_ids;
	for (auto &table_id : cleanup_tables) {
		if (!deleted_table_ids.empty()) {
			deleted_table_ids += ", ";
		}
		deleted_table_ids += to_string(table_id.index);
	}

	// get a list of files that are no longer required after these deletions
	string table_id_filter;
	if (!deleted_table_ids.empty()) {
		table_id_filter = StringUtil::Format("table_id IN (%s) OR", deleted_table_ids);
	}

	result = transaction.Query(StringUtil::Format(R"(
SELECT data_file_id, table_id, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_data_file
WHERE %s (end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
));)",
	                                              table_id_filter));
	vector<DuckLakeFileForCleanup> cleanup_files;
	for (auto &row : *result) {
		DuckLakeFileForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		TableIndex table_id(row.GetValue<idx_t>(1));
		DuckLakePath path;
		path.path = row.GetValue<string>(2);
		path.path_is_relative = row.GetValue<bool>(3);
		info.path = FromRelativePath(table_id, path);

		cleanup_files.push_back(std::move(info));
	}
	string deleted_file_ids;
	if (!cleanup_files.empty()) {
		string files_scheduled_for_cleanup;
		for (auto &file : cleanup_files) {
			if (!deleted_file_ids.empty()) {
				deleted_file_ids += ", ";
			}
			deleted_file_ids += to_string(file.id.index);

			if (!files_scheduled_for_cleanup.empty()) {
				files_scheduled_for_cleanup += ", ";
			}
			auto path = GetRelativePath(file.path);
			files_scheduled_for_cleanup += StringUtil::Format(
			    "(%d, %s, %s, NOW())", file.id.index, SQLString(path.path), path.path_is_relative ? "true" : "false");
		}

		// delete the data files
		tables_to_delete_from = {"ducklake_data_file", "ducklake_file_column_stats"};
		for (auto &delete_tbl : tables_to_delete_from) {
			result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE data_file_id IN (%s);
)",
			                                              delete_tbl, deleted_file_ids));
			if (result->HasError()) {
				result->GetErrorObject().Throw("Failed to delete old data file information in DuckLake: ");
			}
		}
		// insert the to-be-cleaned-up files
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
VALUES %s;
)",
		                                              files_scheduled_for_cleanup));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to schedule files for clean-up in DuckLake: ");
		}
	}

	// get a list of delete files that are no longer required after these deletions
	string file_id_filter;
	if (!deleted_file_ids.empty()) {
		file_id_filter = StringUtil::Format("data_file_id IN (%s) OR", deleted_file_ids);
	}

	result = transaction.Query(StringUtil::Format(R"(
SELECT delete_file_id, table_id, path, path_is_relative
FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE %s %s (end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
));)",
	                                              table_id_filter, file_id_filter));
	vector<DuckLakeFileForCleanup> cleanup_deletes;
	for (auto &row : *result) {
		DuckLakeFileForCleanup info;
		info.id = DataFileIndex(row.GetValue<idx_t>(0));
		TableIndex table_id(row.GetValue<idx_t>(1));

		DuckLakePath path;
		path.path = row.GetValue<string>(2);
		path.path_is_relative = row.GetValue<bool>(3);
		info.path = FromRelativePath(table_id, path);

		cleanup_deletes.push_back(std::move(info));
	}
	if (!cleanup_deletes.empty()) {
		string deleted_delete_ids;
		string files_scheduled_for_cleanup;
		for (auto &file : cleanup_deletes) {
			if (!deleted_delete_ids.empty()) {
				deleted_delete_ids += ", ";
			}
			deleted_delete_ids += to_string(file.id.index);

			if (!files_scheduled_for_cleanup.empty()) {
				files_scheduled_for_cleanup += ", ";
			}
			auto path = GetRelativePath(file.path);
			files_scheduled_for_cleanup += StringUtil::Format(
			    "(%d, %s, %s, NOW())", file.id.index, SQLString(path.path), path.path_is_relative ? "true" : "false");
		}
		// delete the delete files
		result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.ducklake_delete_file
WHERE delete_file_id IN (%s);
)",
		                                              deleted_delete_ids));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete old delete file information in DuckLake: ");
		}
		// insert the to-be-cleaned-up files
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_files_scheduled_for_deletion
VALUES %s;
)",
		                                              files_scheduled_for_cleanup));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to schedule files for clean-up in DuckLake: ");
		}
	}

	// delete based on table id -> ducklake_table_stats, ducklake_table_column_stats, ducklake_partition_info
	if (!deleted_table_ids.empty()) {
		tables_to_delete_from = {"ducklake_table",          "ducklake_table_stats",      "ducklake_table_column_stats",
		                         "ducklake_partition_info", "ducklake_partition_column", "ducklake_column",
		                         "ducklake_column_tag",     "ducklake_sort_info",        "ducklake_sort_expression"};
		for (auto &delete_tbl : tables_to_delete_from) {
			auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE table_id IN (%s);)",
			                                                   delete_tbl, deleted_table_ids));
			if (result->HasError()) {
				result->GetErrorObject().Throw("Failed to delete from " + delete_tbl + " in DuckLake: ");
			}
		}
	}

	// delete any views, schemas, etc that are no longer referenced
	tables_to_delete_from = {"ducklake_schema", "ducklake_view", "ducklake_tag"};
	for (auto &delete_tbl : tables_to_delete_from) {
		auto result = transaction.Query(StringUtil::Format(R"(
DELETE FROM {METADATA_CATALOG}.%s
WHERE end_snapshot IS NOT NULL AND NOT EXISTS(
    SELECT snapshot_id
    FROM {METADATA_CATALOG}.ducklake_snapshot
    WHERE snapshot_id >= begin_snapshot AND snapshot_id < end_snapshot
);)",
		                                                   delete_tbl));
		if (result->HasError()) {
			result->GetErrorObject().Throw("Failed to delete from " + delete_tbl + " in DuckLake: ");
		}
	}
}

void DuckLakeMetadataManager::DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table) {
	auto result = transaction.Query(StringUtil::Format(R"(
		DELETE FROM {METADATA_CATALOG}.%s
)",
	                                                   SQLIdentifier(inlined_table.table_name)));
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to delete inlined data in DuckLake from table " +
		                               inlined_table.table_name + ": ");
	}
}

string DuckLakeMetadataManager::InsertNewSchema(const DuckLakeSnapshot &snapshot, const set<TableIndex> &table_ids) {
	if (table_ids.empty()) {
		return {};
	}
	string result;
	for (auto &table_id : table_ids) {
		result += StringUtil::Format(R"(INSERT INTO {METADATA_CATALOG}.ducklake_schema_versions VALUES (%d,%d,%d);)",
		                             snapshot.snapshot_id, snapshot.schema_version, table_id.index);
	}
	return result;
}

vector<DuckLakeTableSizeInfo> DuckLakeMetadataManager::GetTableSizes(DuckLakeSnapshot snapshot) {
	vector<DuckLakeTableSizeInfo> table_sizes;
	auto result = transaction.Query(snapshot, R"(
SELECT schema_id, table_id, table_name, table_uuid, data_file_info.file_count, data_file_info.total_file_size, delete_file_info.file_count, delete_file_info.total_file_size
FROM {METADATA_CATALOG}.ducklake_table tbl, LATERAL (
	SELECT COUNT(*) file_count, SUM(file_size_bytes) total_file_size
	FROM {METADATA_CATALOG}.ducklake_data_file df
	WHERE df.table_id = tbl.table_id AND {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
) data_file_info, LATERAL (
	SELECT COUNT(*) file_count, SUM(file_size_bytes) total_file_size
	FROM {METADATA_CATALOG}.ducklake_delete_file df
	WHERE df.table_id = tbl.table_id AND {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
) delete_file_info
WHERE {SNAPSHOT_ID} >= begin_snapshot AND ({SNAPSHOT_ID} < end_snapshot OR end_snapshot IS NULL)
)");
	for (auto &row : *result) {
		DuckLakeTableSizeInfo table_size;
		table_size.schema_id = SchemaIndex(row.GetValue<idx_t>(0));
		table_size.table_id = TableIndex(row.GetValue<idx_t>(1));
		table_size.table_name = row.GetValue<string>(2);
		table_size.table_uuid = row.GetValue<string>(3);
		if (!row.IsNull(4)) {
			table_size.file_count = row.GetValue<idx_t>(4);
		}
		if (!row.IsNull(5)) {
			table_size.file_size_bytes = row.GetValue<idx_t>(5);
		}
		if (!row.IsNull(6)) {
			table_size.delete_file_count = row.GetValue<idx_t>(6);
		}
		if (!row.IsNull(7)) {
			table_size.delete_file_size_bytes = row.GetValue<idx_t>(7);
		}
		table_sizes.push_back(std::move(table_size));
	}
	return table_sizes;
}

void DuckLakeMetadataManager::SetConfigOption(const DuckLakeConfigOption &option) {
	// check if the option already exists
	auto &option_key = option.option.key;
	auto &option_value = option.option.value;
	string scope;
	string scope_id;
	string scope_filter;
	if (option.table_id.IsValid()) {
		scope = "'table'";
		scope_id = to_string(option.table_id.index);
		scope_filter = StringUtil::Format("scope = 'table' AND scope_id = %d", option.table_id.index);
	} else if (option.schema_id.IsValid()) {
		scope = "'schema'";
		scope_id = to_string(option.schema_id.index);
		scope_filter = StringUtil::Format("scope = 'schema' AND scope_id = %d", option.schema_id.index);
	} else {
		scope = "NULL";
		scope_id = "NULL";
		scope_filter = "scope IS NULL";
	}
	auto result = transaction.Query(StringUtil::Format(R"(
SELECT COUNT(*)
FROM {METADATA_CATALOG}.ducklake_metadata
WHERE key = %s AND %s
)",
	                                                   SQLString(option_key), scope_filter));

	auto count = result->Fetch()->GetValue(0, 0).GetValue<idx_t>();
	if (count == 0) {
		// option does not yet exist - insert the value
		result = transaction.Query(StringUtil::Format(R"(
INSERT INTO {METADATA_CATALOG}.ducklake_metadata VALUES (%s, %s, %s, %s)
)",
		                                              SQLString(option_key), SQLString(option_value), scope, scope_id));
	} else {
		// option already exists - update it
		result = transaction.Query(StringUtil::Format(R"(
UPDATE {METADATA_CATALOG}.ducklake_metadata SET value=%s WHERE key=%s AND %s
)",
		                                              SQLString(option_value), SQLString(option_key), scope_filter));
	}
	if (result->HasError()) {
		result->GetErrorObject().Throw("Failed to insert config option in DuckLake: ");
	}
}

bool DuckLakeMetadataManager::IsEncrypted() const {
	return transaction.GetCatalog().Encryption() == DuckLakeEncryption::ENCRYPTED;
}

} // namespace duckdb
