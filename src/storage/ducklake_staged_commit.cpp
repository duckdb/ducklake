#include "storage/ducklake_staged_commit.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/main/query_result.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {

DuckLakeStagedCommit::DuckLakeStagedCommit(DuckLakeMetadataManager &manager, string commit_uuid)
    : manager(manager), commit_uuid(std::move(commit_uuid)),
      identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
}

void DuckLakeStagedCommit::Write() {
	string batch;
	batch += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_commit_%s("
	                            "transaction_snapshot_id BIGINT, transaction_schema_version BIGINT, "
	                            "commit_author VARCHAR, commit_message VARCHAR);",
	                            identifier_suffix);
	batch += StringUtil::Format("CREATE TEMP TABLE ducklake_staged_change_touch_%s("
	                            "kind VARCHAR, entity_id BIGINT);",
	                            identifier_suffix);
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
	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to create DuckLake staging tables: ");
	}
}

void DuckLakeStagedCommit::Drop() {
	string batch;
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_commit_%s;", identifier_suffix);
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_change_touch_%s;", identifier_suffix);
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_%s;", identifier_suffix);
	batch += StringUtil::Format("DROP TABLE IF EXISTS ducklake_staged_data_file_column_stats_%s;", identifier_suffix);
	auto result = manager.Query(batch);
	if (result && result->HasError()) {
		result->GetErrorObject().Throw("Failed to drop DuckLake staging tables: ");
	}
}

} // namespace duckdb
