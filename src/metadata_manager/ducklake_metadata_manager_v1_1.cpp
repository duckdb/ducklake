#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"

namespace duckdb {

DuckLakeMetadataManagerV1_1::DuckLakeMetadataManagerV1_1(DuckLakeTransaction &transaction)
    : DuckLakeMetadataManager(transaction) {
}

string DuckLakeMetadataManagerV1_1::GetCreateTableStatements() {
	string result = DuckLakeMetadataManager::GetCreateTableStatements();
	return result;
}

string DuckLakeMetadataManagerV1_1::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V1_1_DEV_1;
	return DuckLakeVersionToString(VERSION);
}

} // namespace duckdb
