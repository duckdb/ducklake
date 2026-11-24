#include "metadata_manager/ducklake_metadata_manager_v1_1.hpp"
#include "metadata_manager/postgres_metadata_manager.hpp"
#include "metadata_manager/quack_metadata_manager.hpp"
#include "common/ducklake_version.hpp"

namespace duckdb {

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetSchemaTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_schema(schema_id BIGINT PRIMARY KEY, schema_uuid UUID, "
	       "begin_snapshot BIGINT, end_snapshot BIGINT, schema_name VARCHAR, path VARCHAR, path_is_relative BOOLEAN, "
	       "parent_schema_id BIGINT);";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetDataFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_data_file(" +
	       DuckLakeMetadataManager::DataFileTableColumns(true, true) + ");";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetDeleteFileTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_delete_file(" +
	       DuckLakeMetadataManager::DeleteFileTableColumns(true, true) + ");";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetFileColumnStatsTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_file_column_stats(data_file_id BIGINT, table_id BIGINT, "
	       "column_id BIGINT, column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, min_value VARCHAR, "
	       "max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR, min_is_exact BOOLEAN, max_is_exact "
	       "BOOLEAN);";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetTableColumnStatsTableStatement() {
	return "CREATE TABLE {METADATA_CATALOG}.ducklake_table_column_stats(table_id BIGINT, column_id BIGINT, "
	       "contains_null BOOLEAN, contains_nan BOOLEAN, min_value VARCHAR, max_value VARCHAR, extra_stats VARCHAR, "
	       "min_is_exact BOOLEAN, max_is_exact BOOLEAN);";
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetCreateTableStatements() {
	auto result = Base::GetCreateTableStatements();
	result += "CREATE TABLE {METADATA_CATALOG}.ducklake_view_column_tag(view_id BIGINT, column_name VARCHAR, "
	          "begin_snapshot BIGINT, end_snapshot BIGINT, key VARCHAR, value VARCHAR);\n";
	return result;
}

template <typename Base>
string DuckLakeMetadataManagerV1_1<Base>::GetVersionString() {
	constexpr auto VERSION = DuckLakeVersion::V1_1;
	return DuckLakeVersionToString(VERSION);
}

// explicit instantiations for all backends
template class DuckLakeMetadataManagerV1_1<DuckLakeMetadataManager>;
template class DuckLakeMetadataManagerV1_1<PostgresMetadataManager>;
template class DuckLakeMetadataManagerV1_1<QuackMetadataManager>;

} // namespace duckdb
