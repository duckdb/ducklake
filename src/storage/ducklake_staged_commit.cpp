#include "storage/ducklake_staged_commit.hpp"

#include "common/ducklake_data_file.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {


DuckLakeStagedCommit::DuckLakeStagedCommit(DuckLakeMetadataManager &manager, string commit_uuid)
    : manager(manager), commit_uuid(std::move(commit_uuid)),
      identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

void DuckLakeStagedCommit::Write(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
                                 const TransactionChangeInformation &transaction_changes) {
	auto &commit_info = transaction.GetCommitInfo();
	string batch;

	batch += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_commit_%s("
	                            "transaction_snapshot_id BIGINT, transaction_schema_version BIGINT, "
	                            "commit_author VARCHAR, commit_message VARCHAR);",
	                            identifier_suffix);
	batch += StringUtil::Format("INSERT INTO ducklake_staged_commit_%s VALUES (%llu, %llu, %s, %s);", identifier_suffix,
	                            static_cast<unsigned long long>(transaction_snapshot.snapshot_id),
	                            static_cast<unsigned long long>(transaction_snapshot.schema_version),
	                            commit_info.author.ToSQLString(), commit_info.commit_message.ToSQLString());

	batch += transaction_changes.tables_inserted_into.Serialize(identifier_suffix);

	batch += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_data_file_%s("
	                            "data_file_id BIGINT, table_id BIGINT, file_order BIGINT, "
	                            "path VARCHAR, path_is_relative BOOLEAN, file_format VARCHAR, "
	                            "record_count BIGINT, file_size_bytes BIGINT, footer_size BIGINT, "
	                            "row_id_start BIGINT, partition_id BIGINT, encryption_key VARCHAR, "
	                            "mapping_id BIGINT, partial_max BIGINT);",
	                            identifier_suffix);
	batch += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_data_file_column_stats_%s("
	                            "data_file_id BIGINT, table_id BIGINT, column_id BIGINT, "
	                            "column_size_bytes BIGINT, value_count BIGINT, null_count BIGINT, "
	                            "min_value VARCHAR, max_value VARCHAR, contains_nan BOOLEAN, extra_stats VARCHAR);",
	                            identifier_suffix);

	idx_t local_file_id = 0;
	auto &local_changes = transaction.GetLocalChanges();
	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		idx_t file_order = 0;
		// for (auto &file : table_changes.new_data_files) {
		// 	batch += StringUtil::Format(
		// 	    "INSERT INTO ducklake_staged_data_file_%s VALUES "
		// 	    "(%llu, %llu, %llu, %s, false, 'parquet', %llu, %llu, %s, %s, %s, %s, %s, %s);",
		// 	    identifier_suffix, static_cast<unsigned long long>(local_file_id),
		// 	    static_cast<unsigned long long>(table_id.index), static_cast<unsigned long long>(file_order),
		// 	    SQLString(file.file_name), static_cast<unsigned long long>(file.row_count),
		// 	    static_cast<unsigned long long>(file.file_size_bytes), OptionalIdxOrNull(file.footer_size),
		// 	    OptionalIdxOrNull(file.flush_row_id_start), OptionalIdxOrNull(file.partition_id),
		// 	    EncryptionKeyLiteral(file.encryption_key), MappingIdOrNull(file.mapping_id),
		// 	    OptionalIdxOrNull(file.max_partial_file_snapshot));
		// 	for (auto &stat : file.column_stats) {
		// 		auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
		// 		batch += StringUtil::Format("INSERT INTO ducklake_staged_data_file_column_stats_%s VALUES "
		// 		                            "(%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
		// 		                            identifier_suffix, static_cast<unsigned long long>(local_file_id),
		// 		                            static_cast<unsigned long long>(table_id.index),
		// 		                            static_cast<unsigned long long>(info.column_id.index),
		// 		                            info.column_size_bytes, info.value_count, info.null_count, info.min_val,
		// 		                            info.max_val, info.contains_nan, info.extra_stats);
		// 	}
		// 	local_file_id++;
		// 	file_order++;
		// }
	}

	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to create DuckLake staging tables: ");
	}
}

void DuckLakeStagedCommit::Drop() {
	string batch;
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_commit_%s;", identifier_suffix);
	batch += ChangeInfo<set<TableIndex>, ChangeKind::INSERTED_INTO>().Drop(identifier_suffix);
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_%s;", identifier_suffix);
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_column_stats_%s;", identifier_suffix);
	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to drop DuckLake staging tables: ");
	}
}

} // namespace duckdb
