#include "storage/ducklake_change_info.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

template <>
string ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>::Serialize(const string &suffix) const {
	string sql;
	sql += StringUtil::Format("CREATE TEMP TABLE IF NOT EXISTS ducklake_staged_change_touch_%s("
	                          "kind VARCHAR, entity_id BIGINT);",
	                          suffix);
	for (auto &table_id : data) {
		sql += StringUtil::Format("INSERT INTO ducklake_staged_change_touch_%s VALUES ('inserted_into', %llu);", suffix,
		                          static_cast<unsigned long long>(table_id.index));
	}
	return sql;
}

template <>
string ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>::Drop(const string &suffix) const {
	return StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_change_touch_%s;", suffix);
}

template <>
string ChangeInfo<DuckLakeSnapshotCommit, ChangeKind::COMMIT_HEADER>::Serialize(const string &suffix) const {
	string sql;
	sql += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_commit_%s("
	                          "commit_author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);",
	                          suffix);
	sql += StringUtil::Format("INSERT INTO ducklake_staged_commit_%s VALUES (%s, %s, %s);", suffix,
	                          data.author.ToSQLString(), data.commit_message.ToSQLString(),
	                          data.commit_extra_info.ToSQLString());
	return sql;
}

template <>
string ChangeInfo<DuckLakeSnapshotCommit, ChangeKind::COMMIT_HEADER>::Drop(const string &suffix) const {
	return StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_commit_%s;", suffix);
}

template <>
string ChangeInfo<LocalTableChanges, ChangeKind::DATA_FILES>::Serialize(const string &suffix) const {
	string sql;
	sql += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_data_file_%s("
	                          "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
	                          "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
	                          "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
	                          "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
	                          "mapping_id BIGINT, partial_max BIGINT);",
	                          suffix);
	sql += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_data_file_column_stats_%s("
	                          "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, "
	                          "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, "
	                          "min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);",
	                          suffix);

	idx_t local_file_id = 0;
	for (auto &entry : data.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		idx_t file_order = 0;
		for (auto &file : table_changes.new_data_files) {
			sql += StringUtil::Format(
			    "INSERT INTO ducklake_staged_data_file_%s VALUES "
			    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s);",
			    suffix, static_cast<unsigned long long>(local_file_id),
			    static_cast<unsigned long long>(table_id.index), static_cast<unsigned long long>(file_order),
			    SQLString(file.file_name), static_cast<unsigned long long>(file.row_count),
			    static_cast<unsigned long long>(file.file_size_bytes), OptionalIdxOrNull(file.footer_size),
			    OptionalIdxOrNull(file.flush_row_id_start), OptionalIdxOrNull(file.partition_id),
			    EncryptionKeyLiteral(file.encryption_key), MappingIdOrNull(file.mapping_id),
			    OptionalIdxOrNull(file.max_partial_file_snapshot));
			for (auto &stat : file.column_stats) {
				auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
				sql += StringUtil::Format("INSERT INTO ducklake_staged_data_file_column_stats_%s VALUES "
				                          "(%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
				                          suffix, static_cast<unsigned long long>(local_file_id),
				                          static_cast<unsigned long long>(table_id.index),
				                          static_cast<unsigned long long>(info.column_id.index),
				                          info.column_size_bytes, info.value_count, info.null_count, info.min_val,
				                          info.max_val, info.contains_nan, info.extra_stats);
			}
			local_file_id++;
			file_order++;
		}
	}
	return sql;
}

template <>
string ChangeInfo<LocalTableChanges, ChangeKind::DATA_FILES>::Drop(const string &suffix) const {
	string sql;
	sql += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_%s;", suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_column_stats_%s;", suffix);
	return sql;
}

} // namespace duckdb
