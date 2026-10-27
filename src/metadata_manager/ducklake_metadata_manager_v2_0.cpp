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
	return result;
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
