#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/sqlite_metadata_manager.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

static constexpr const char *DATA_FILE_TABLE_V1_0 =
    "CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
    "begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
    "file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, "
    "partition_id BIGINT, encryption_key VARCHAR,  mapping_id BIGINT, partial_max BIGINT);";

static constexpr const char *DATA_FILE_TABLE_V1_1 =
    "CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(data_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
    "begin_snapshot BIGINT, end_snapshot BIGINT, file_order BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
    "file_format VARCHAR, record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, row_id_start BIGINT, "
    "partition_id BIGINT, encryption_key VARCHAR,  mapping_id BIGINT, partial_max BIGINT, row_group_count BIGINT);";

static constexpr const char *DELETE_FILE_TABLE_V1_0 =
    "CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
    "begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
    "format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
    "partial_max BIGINT);";

static constexpr const char *DELETE_FILE_TABLE_V1_1 =
    "CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(delete_file_id BIGINT PRIMARY KEY, table_id BIGINT, "
    "begin_snapshot BIGINT, end_snapshot BIGINT, data_file_id BIGINT, path VARCHAR, path_is_relative BOOLEAN, "
    "format VARCHAR, delete_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, encryption_key VARCHAR, "
    "partial_max BIGINT, row_group_count BIGINT);";

static string ReplaceCreateTableStatement(const string &statements, const string &old_statement,
                                          const string &new_statement) {
	if (statements.find(old_statement) == string::npos) {
		throw InternalException(
		    "DuckLakeMetadataManagerV1_1::GetCreateTableStatements - statement to replace was not found");
	}
	return StringUtil::Replace(statements, old_statement, new_statement);
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements() {
	string result = Base::GetCreateTableStatements();
	result = ReplaceCreateTableStatement(result, DATA_FILE_TABLE_V1_0, DATA_FILE_TABLE_V1_1);
	result = ReplaceCreateTableStatement(result, DELETE_FILE_TABLE_V1_0, DELETE_FILE_TABLE_V1_1);
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
template class DuckLakeMetadataManagerV1_1<QuackMetadataManager>;

} // namespace duckdb
