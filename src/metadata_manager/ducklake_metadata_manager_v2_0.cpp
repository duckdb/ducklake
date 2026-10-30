#include "metadata_manager/ducklake_metadata_manager_v2_0.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
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
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetSnapshotTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_snapshot(" + DuckLakeMetadataManager::GetSnapshotTableColumns() +
	       ", branch_id BIGINT DEFAULT 0);";
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
template class DuckLakeMetadataManagerV2_0<SQLiteMetadataManager>;
template class DuckLakeMetadataManagerV2_0<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV2_0<QuackMetadataManager>;

} // namespace duckdb
