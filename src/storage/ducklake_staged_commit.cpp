#include "storage/ducklake_staged_commit.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/string_util.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

DuckLakeStagedCommit::DuckLakeStagedCommit(string commit_uuid)
    : commit_uuid(std::move(commit_uuid)), identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

static string OptionalIdxOrNull(const optional_idx &v) {
	return v.IsValid() ? std::to_string(v.GetIndex()) : "NULL";
}

static string MappingIdOrNull(const MappingIndex &m) {
	return m.IsValid() ? std::to_string(m.index) : "NULL";
}

static string EncryptionKeyLiteral(const string &key) {
	if (key.empty()) {
		return "NULL";
	}
	return "'" + Blob::ToBase64(string_t(key)) + "'";
}

static string EmitStagedCommitHeader(const DuckLakeSnapshotCommit &h, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_commit_%s("
	                          "commit_author VARCHAR, commit_message VARCHAR, commit_extra_info VARCHAR);",
	                          sfx);
	sql +=
	    StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_commit_%s VALUES (%s, %s, %s);", sfx,
	                       h.author.ToSQLString(), h.commit_message.ToSQLString(), h.commit_extra_info.ToSQLString());
	return sql;
}

static string EmitStagedChangeTouch(const set<TableIndex> &tables_inserted_into, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_change_touch_%s("
	                          "kind VARCHAR, entity_id BIGINT);",
	                          sfx);
	for (auto &table_id : tables_inserted_into) {
		sql += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_change_touch_%s VALUES ('inserted_into', %llu);", sfx,
		    static_cast<unsigned long long>(table_id.index));
	}
	return sql;
}

static string EmitStagedDataFiles(const LocalTableChanges &local_changes, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_data_file_%s("
	                          "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
	                          "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
	                          "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
	                          "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
	                          "mapping_id BIGINT, partial_max BIGINT);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s("
	                          "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, "
	                          "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, "
	                          "min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);",
	                          sfx);

	idx_t local_file_id = 0;
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		idx_t file_order = 0;
		for (auto &file : table_changes.new_data_files) {
			sql += StringUtil::Format(
			    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_%s VALUES "
			    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s);",
			    sfx, static_cast<unsigned long long>(local_file_id), static_cast<unsigned long long>(table_id.index),
			    static_cast<unsigned long long>(file_order), SQLString(file.file_name),
			    static_cast<unsigned long long>(file.row_count), static_cast<unsigned long long>(file.file_size_bytes),
			    OptionalIdxOrNull(file.footer_size), OptionalIdxOrNull(file.flush_row_id_start),
			    OptionalIdxOrNull(file.partition_id), EncryptionKeyLiteral(file.encryption_key),
			    MappingIdOrNull(file.mapping_id), OptionalIdxOrNull(file.max_partial_file_snapshot));
			for (auto &stat : file.column_stats) {
				auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s "
				                          "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
				                          sfx, static_cast<unsigned long long>(local_file_id),
				                          static_cast<unsigned long long>(table_id.index),
				                          static_cast<unsigned long long>(info.column_id.index), info.column_size_bytes,
				                          info.value_count, info.null_count, info.min_val, info.max_val,
				                          info.contains_nan, info.extra_stats);
			}
			local_file_id++;
			file_order++;
		}
	}
	return sql;
}

string DuckLakeStagedCommit::Build(DuckLakeTransaction &transaction,
                                   const TransactionChangeInformation &transaction_changes,
                                   DuckLakeSnapshot transaction_snapshot) const {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_name = ducklake_catalog.MetadataSchemaName();
	string batch;
	batch += EmitStagedCommitHeader(transaction.GetCommitInfo(), identifier_suffix);
	batch += EmitStagedChangeTouch(transaction_changes.tables_inserted_into, identifier_suffix);
	batch += EmitStagedDataFiles(transaction.GetLocalChanges(), identifier_suffix);
	batch += StringUtil::Format(
	    "SELECT * FROM ducklake_commit(%s, %s, %lld);", DuckLakeUtil::SQLLiteralToString(identifier_suffix),
	    DuckLakeUtil::SQLLiteralToString(schema_name), static_cast<long long>(transaction_snapshot.schema_version));
	return batch;
}

} // namespace duckdb
