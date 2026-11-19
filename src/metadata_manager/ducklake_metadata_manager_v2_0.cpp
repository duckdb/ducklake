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
	result += DuckLakeMetadataManager::GetMainBranchRowStatement() + "\n";
	result += DuckLakeMetadataManager::GetFileVisibilityTableStatement() + "\n";
	result += DuckLakeMetadataManager::GetDeleteFileVisibilityTableStatement() + "\n";
	// a fresh catalog has the healed layout, so it carries the marker of a completed heal
	result += DuckLakeMetadataManager::HealMarkerSql();
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetDataFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(" +
	       DuckLakeMetadataManager::DataFileTableColumns(false, true) + ");";
}

template <typename Base>
string DuckLakeMetadataManagerV2_0<Base>::GetDeleteFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(" +
	       DuckLakeMetadataManager::DeleteFileTableColumns(false, true) + ");";
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
string DuckLakeMetadataManagerV2_0<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V2_0_DEV3;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV2_0<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV2_0<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV2_0<QuackMetadataManager>;

} // namespace duckdb
