#include "metadata_manager/ducklake_metadata_manager_v2_0.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_version.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetCreateTableStatements() {
	auto result = DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements();
	result += DuckLakeMetadataManager::GetSnapshotTagTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetBranchTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetSnapshotRemapTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetFileVisibilityTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetDeleteFileVisibilityTableStatement() + "\n";
	for (auto &kind : DuckLakeMetadataManager::RowVisibilityKinds()) {
		result += DuckLakeMetadataManager::GetRowVisibilityTableStatement(kind.kind, kind.key_columns) + "\n";
	}
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSnapshotTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(" +
	       DuckLakeMetadataManager::BranchedSnapshotTableColumns() + ");";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSnapshotChangesTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot_changes(" +
	       DuckLakeMetadataManager::BranchedSnapshotChangesTableColumns() + ");";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSchemaVersionsTableStatement() {
	// branch_id is the branch whose snapshot begin_snapshot is
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_schema_versions(begin_snapshot BIGINT, schema_version BIGINT, "
	       "table_id BIGINT, branch_id BIGINT);";
}

// the version rows keep their birth and end in the companion tables, branch_id comes last like a healed catalog
template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSchemaTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, "
	       "begin_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, parent_schema_id "
	       "BIGINT, branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetTableTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_table(table_id BIGINT, table_uuid UUID, begin_snapshot BIGINT, "
	       "schema_id BIGINT, table_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetViewTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_view(view_id BIGINT, view_uuid UUID, begin_snapshot BIGINT, "
	       "schema_id BIGINT, view_name VARCHAR, dialect VARCHAR, sql VARCHAR, column_aliases VARCHAR, branch_id "
	       "BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetColumnTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_column(column_id BIGINT, begin_snapshot BIGINT, table_id BIGINT, "
	       "column_order BIGINT, column_name VARCHAR, column_type VARCHAR, initial_default VARCHAR, default_value "
	       "VARCHAR, nulls_allowed BOOLEAN, parent_column BIGINT, default_value_type VARCHAR, default_value_dialect "
	       "VARCHAR, branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetPartitionInfoTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_partition_info(partition_id BIGINT, table_id BIGINT, "
	       "begin_snapshot BIGINT, branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetMacroTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_macro(schema_id BIGINT, macro_id BIGINT, macro_name VARCHAR, "
	       "begin_snapshot BIGINT, branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSortInfoTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_sort_info(sort_id BIGINT, table_id BIGINT, begin_snapshot BIGINT, "
	       "branch_id BIGINT DEFAULT 0);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetDataFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
	       "file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, record_count BIGINT, "
	       "file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, partition_id BIGINT, encryption_key "
	       "VARCHAR, mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetDeleteFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
	       "data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, format VARCHAR, delete_count BIGINT, "
	       "file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, partial_max BIGINT, row_group_count "
	       "BIGINT);";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetInitialDataStatements() {
	return DuckLakeMetadataManager::GetMainBranchRowStatement() + "\n";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V2_0_DEV1;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV2_0<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV2_0<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV2_0<QuackMetadataManager>;

} // namespace duckdb
