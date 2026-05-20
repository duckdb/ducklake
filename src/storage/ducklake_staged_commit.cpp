#include "storage/ducklake_staged_commit.hpp"

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"

namespace duckdb {

DuckLakeStagedCommit::DuckLakeStagedCommit(string commit_uuid)
    : commit_uuid(std::move(commit_uuid)), identifier_suffix(StringUtil::Replace(this->commit_uuid, "-", "")) {
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

static string EmitStagedChangeTouch(const TransactionChangeInformation &changes, const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_change_touch_%s("
	                          "kind VARCHAR, entity_id BIGINT);",
	                          sfx);
	for (auto &table_id : changes.tables_inserted_into) {
		sql += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_change_touch_%s VALUES ('inserted_into', %llu);", sfx,
		    table_id.index);
	}
	for (auto &table_id : changes.tables_inserted_inlined) {
		sql += StringUtil::Format(
		    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_change_touch_%s VALUES ('inserted_inlined', %llu);", sfx,
		    table_id.index);
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
			    sfx, local_file_id, table_id.index, file_order, SQLString(file.file_name), file.row_count,
			    file.file_size_bytes, DuckLakeUtil::OptionalIdxOrNull(file.footer_size),
			    DuckLakeUtil::OptionalIdxOrNull(file.flush_row_id_start),
			    DuckLakeUtil::OptionalIdxOrNull(file.partition_id),
			    DuckLakeUtil::EncryptionKeyLiteral(file.encryption_key),
			    DuckLakeUtil::MappingIdOrNull(file.mapping_id),
			    DuckLakeUtil::OptionalIdxOrNull(file.max_partial_file_snapshot));
			for (auto &stat : file.column_stats) {
				auto info = DuckLakeColumnStatsInfo::FromColumnStats(stat.first, stat.second);
				sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_data_file_column_stats_%s "
				                          "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s);",
				                          sfx, local_file_id, table_id.index, info.column_id.index,
				                          info.column_size_bytes, info.value_count, info.null_count, info.min_val,
				                          info.max_val, info.contains_nan, info.extra_stats);
			}
			local_file_id++;
			file_order++;
		}
	}
	return sql;
}

static string EmitStagedInlinedData(const LocalTableChanges &local_changes, DuckLakeTransaction &transaction,
                                    const string &sfx) {
	string sql;
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_data_%s("
	                          "table_id BIGINT, has_preserved_row_ids BOOLEAN);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_row_%s("
	                          "table_id BIGINT, row_order BIGINT, preserved_row_id BIGINT, value_literals VARCHAR);",
	                          sfx);
	sql += StringUtil::Format("CREATE TABLE IF NOT EXISTS {METADATA_CATALOG}.ducklake_staged_inlined_column_stats_%s("
	                          "table_id BIGINT, column_id BIGINT, "
	                          "column_size_bytes BIGINT, has_num_values BOOLEAN, num_values BIGINT, "
	                          "has_null_count BOOLEAN, null_count BIGINT, "
	                          "has_min BOOLEAN, min_value VARCHAR, has_max BOOLEAN, max_value VARCHAR, "
	                          "has_contains_nan BOOLEAN, contains_nan BOOLEAN, "
	                          "any_valid BOOLEAN, extra_stats VARCHAR);",
	                          sfx);

	auto context_ref = transaction.context.lock();
	auto &context = *context_ref;
	auto &metadata_manager = transaction.GetMetadataManager();

	for (auto &entry : local_changes.Changes()) {
		auto table_id = entry.GetTableIndex();
		auto &table_changes = entry.GetTableChanges();
		if (!table_changes.new_inlined_data) {
			continue;
		}
		auto &inlined = *table_changes.new_inlined_data;
		bool has_preserved = inlined.HasPreservedRowIds();
		sql += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_data_%s VALUES (%llu, %s);",
		                          sfx, table_id.index, has_preserved ? "true" : "false");
		idx_t row_order = 0;
		idx_t global_row_idx = 0;
		for (auto &chunk : inlined.data->Chunks()) {
			for (idx_t r = 0; r < chunk.size(); r++) {
				string tuple = "(";
				for (idx_t c = 0; c < chunk.ColumnCount(); c++) {
					if (c > 0) {
						tuple += ", ";
					}
					tuple += DuckLakeUtil::ValueToSQL(metadata_manager, context, chunk.GetValue(c, r));
				}
				tuple += ")";
				string preserved_row_id = "NULL";
				if (has_preserved) {
					auto rid = inlined.row_ids[global_row_idx];
					if (!DuckLakeConstants::IsTransactionLocalRowId(rid)) {
						preserved_row_id = std::to_string(rid);
					}
				}
				sql += StringUtil::Format(
				    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_row_%s VALUES (%llu, %llu, %s, %s);", sfx,
				    table_id.index, row_order, preserved_row_id, SQLString(tuple));
				row_order++;
				global_row_idx++;
			}
		}
		for (auto &stat : inlined.column_stats) {
			auto &s = stat.second;
			string num_values = s.has_num_values ? std::to_string(s.num_values) : "NULL";
			string null_count = s.has_null_count ? std::to_string(s.null_count) : "NULL";
			string min_val = s.has_min ? DuckLakeUtil::StatsToString(s.min) : "NULL";
			string max_val = s.has_max ? DuckLakeUtil::StatsToString(s.max) : "NULL";
			bool has_min_emit = s.has_min && min_val != "NULL";
			bool has_max_emit = s.has_max && max_val != "NULL";
			string contains_nan = s.has_contains_nan ? (s.contains_nan ? "true" : "false") : "NULL";
			string extra_stats = "NULL";
			if (s.extra_stats) {
				string serialized;
				if (s.extra_stats->TrySerialize(serialized) && !serialized.empty()) {
					extra_stats = DuckLakeUtil::SQLLiteralToString(serialized);
				}
			}
			sql += StringUtil::Format(
			    "INSERT INTO {METADATA_CATALOG}.ducklake_staged_inlined_column_stats_%s "
			    "VALUES (%llu, %llu, %llu, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
			    sfx, table_id.index, stat.first.index, s.column_size_bytes, s.has_num_values ? "true" : "false",
			    num_values, s.has_null_count ? "true" : "false", null_count, has_min_emit ? "true" : "false", min_val,
			    has_max_emit ? "true" : "false", max_val, s.has_contains_nan ? "true" : "false", contains_nan,
			    s.any_valid ? "true" : "false", extra_stats);
		}
	}
	return sql;
}

string DuckLakeStagedCommit::Build(DuckLakeTransaction &transaction,
                                   const TransactionChangeInformation &transaction_changes,
                                   const DuckLakeSnapshot &transaction_snapshot,
                                   const DuckLakeRetryConfig &retry_config) const {
	auto &ducklake_catalog = transaction.GetCatalog();
	auto schema_name = ducklake_catalog.MetadataSchemaName();
	string batch;
	batch += EmitStagedCommitHeader(transaction.GetCommitInfo(), identifier_suffix);
	batch += EmitStagedChangeTouch(transaction_changes, identifier_suffix);
	batch += EmitStagedDataFiles(transaction.GetLocalChanges(), identifier_suffix);
	batch += EmitStagedInlinedData(transaction.GetLocalChanges(), transaction, identifier_suffix);
	batch += StringUtil::Format(
	    "SELECT * FROM ducklake_commit(%s, %s, %lld, "
	    "max_retry_count => %llu, retry_wait_ms => %llu, retry_backoff => %f);",
	    DuckLakeUtil::SQLLiteralToString(identifier_suffix), DuckLakeUtil::SQLLiteralToString(schema_name),
	    transaction_snapshot.schema_version, retry_config.max_retry_count, retry_config.retry_wait_ms,
	    retry_config.retry_backoff);
	return batch;
}

} // namespace duckdb
