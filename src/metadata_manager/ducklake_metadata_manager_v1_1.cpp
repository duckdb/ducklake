#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "common/ducklake_version.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements() {
	string result = Base::GetCreateTableStatements();
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V1_1_DEV_1;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV1_1<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV1_1<SQLiteMetadataManager>;
template class DuckLakeMetadataManagerV1_1<PostgresMetadataManager>;

} // namespace duckdb
