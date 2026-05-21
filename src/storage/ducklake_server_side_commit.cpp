#include "storage/ducklake_server_side_commit.hpp"

#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/blob.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"

#include <utility>

namespace duckdb {

namespace {

LogicalType ResolveType(const string &column_type) {
	try {
		return DuckLakeTypes::FromString(column_type);
	} catch (...) {
		return LogicalType::VARCHAR;
	}
}

DuckLakeColumnStats StatsFromRow(const LogicalType &type, const Value &column_size_bytes, const Value &value_count,
                                 const Value &null_count, const Value &min_value, const Value &max_value,
                                 const Value &contains_nan) {
	DuckLakeColumnStats stats(type);
	if (!column_size_bytes.IsNull()) {
		stats.column_size_bytes = static_cast<idx_t>(column_size_bytes.GetValue<int64_t>());
	}
	if (!value_count.IsNull()) {
		stats.num_values = static_cast<idx_t>(value_count.GetValue<int64_t>());
		stats.has_num_values = true;
	}
	if (!null_count.IsNull()) {
		stats.null_count = static_cast<idx_t>(null_count.GetValue<int64_t>());
		stats.has_null_count = true;
	}
	if (!min_value.IsNull()) {
		stats.min = min_value.ToString();
		stats.has_min = true;
	}
	if (!max_value.IsNull()) {
		stats.max = max_value.ToString();
		stats.has_max = true;
	}
	if (!contains_nan.IsNull()) {
		stats.contains_nan = contains_nan.GetValue<bool>();
		stats.has_contains_nan = true;
	}
	if (stats.has_num_values && stats.has_null_count) {
		stats.any_valid = stats.num_values > stats.null_count;
	}
	return stats;
}

DuckLakeColumnStats StatsFromGlobal(const LogicalType &type, const DuckLakeGlobalColumnStatsInfo &col) {
	DuckLakeColumnStats stats(type);
	stats.has_null_count = col.has_contains_null;
	if (col.has_contains_null) {
		stats.null_count = col.contains_null ? 1 : 0;
	}
	stats.has_contains_nan = col.has_contains_nan;
	if (col.has_contains_nan) {
		stats.contains_nan = col.contains_nan;
	}
	stats.has_min = col.has_min;
	if (col.has_min) {
		stats.min = col.min_val;
	}
	stats.has_max = col.has_max;
	if (col.has_max) {
		stats.max = col.max_val;
	}
	stats.any_valid = stats.has_min || stats.has_max;
	if (col.has_extra_stats && stats.extra_stats) {
		stats.extra_stats->Deserialize(col.extra_stats);
	}
	return stats;
}

} // namespace

//===--------------------------------------------------------------------===//
// Construction
//===--------------------------------------------------------------------===//

DuckLakeServerSideCommit::DuckLakeServerSideCommit(ClientContext &context_p, string metadata_schema_name_p,
                                                   string identifier_suffix_p, int64_t schema_version_p)
    : context(context_p), metadata_schema_name(std::move(metadata_schema_name_p)),
      schema_id(DuckLakeUtil::SQLIdentifierToString(metadata_schema_name)),
      identifier_suffix(std::move(identifier_suffix_p)), schema_version(schema_version_p), fresh_conn(*context_p.db) {
}

void DuckLakeServerSideCommit::SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config_p) {
	retry_config = retry_config_p;
}

//===--------------------------------------------------------------------===//
// Run — hydrate state from staging, build context, delegate to DuckLakeTransactionState::Commit
//===--------------------------------------------------------------------===//

DuckLakeServerSideCommitResult DuckLakeServerSideCommit::Run() {
	// Reconstruct the DuckLakeTransactionState from staging — each method fills a slice.
	ReadCommitHeader();       // creates state, fills commit_info/data_path/separator, sets transaction_snapshot
	ReadColumnTypes();        // fills column_types (used by stats reconstruction below + retry closure)
	ReadStagedDataFiles();    // fills state->local_changes + derives transaction_changes from it
	ReadExistingTableStats(); // fills existing_table_stats (first-attempt path for get_table_stats)

	// Delegate to the unified commit loop. Closures capture committed_* by reference so we can
	// return them; the state itself owns everything else needed for retry / conflict / SQL emission.
	idx_t committed_snapshot_id = 0;
	idx_t committed_schema_version = static_cast<idx_t>(schema_version);
	state->Commit(transaction_snapshot, transaction_changes, retry_config,
	              BuildContext(committed_snapshot_id, committed_schema_version));

	RunQuery(EmitDropStagingSql(), "drop staging");
	DuckLakeServerSideCommitResult result;
	result.committed_snapshot_id = static_cast<int64_t>(committed_snapshot_id);
	result.committed_schema_version = static_cast<int64_t>(committed_schema_version);
	return result;
}

//===--------------------------------------------------------------------===//
// Hydration — each method populates a slice of `state` (or its sibling inputs)
//===--------------------------------------------------------------------===//

void DuckLakeServerSideCommit::ReadCommitHeader() {
	auto query = StringUtil::Format("SELECT commit_author, commit_message, commit_extra_info, "
	                                "transaction_snapshot_id, data_path, path_separator "
	                                "FROM %s.ducklake_staged_commit_%s",
	                                schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged commit header");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		throw IOException("Server-side ducklake_commit: no staged commit header");
	}

	string data_path;
	string separator;
	if (!chunk->GetValue(4, 0).IsNull()) {
		data_path = chunk->GetValue(4, 0).ToString();
	}
	if (!chunk->GetValue(5, 0).IsNull()) {
		separator = chunk->GetValue(5, 0).ToString();
	}
	state = make_uniq<DuckLakeTransactionState>(*context.db, /*require_commit_message=*/false, new_name_maps,
	                                            std::move(data_path), std::move(separator));
	state->commit_info.author = chunk->GetValue(0, 0);
	state->commit_info.commit_message = chunk->GetValue(1, 0);
	state->commit_info.commit_extra_info = chunk->GetValue(2, 0);

	transaction_snapshot.snapshot_id = static_cast<idx_t>(chunk->GetValue(3, 0).GetValue<int64_t>());
	transaction_snapshot.schema_version = static_cast<idx_t>(schema_version);
}

void DuckLakeServerSideCommit::ReadColumnTypes() {
	// Load types for every column of every table touched in this commit — covers columns with staged
	// per-file stats AND columns whose cumulative ducklake_table_column_stats row we read back.
	auto query =
	    StringUtil::Format("SELECT col.table_id, col.column_id, col.column_type "
	                       "FROM %s.ducklake_column col "
	                       "WHERE col.end_snapshot IS NULL "
	                       "AND col.table_id IN (SELECT DISTINCT table_id FROM %s.ducklake_staged_data_file_%s)",
	                       schema_id, schema_id, identifier_suffix);
	auto result = RunQuery(query, "read column types");
	for (auto &row : *result) {
		column_types.emplace(ColumnKey {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))),
		                                FieldIndex(static_cast<idx_t>(row.GetValue<int64_t>(1)))},
		                     ResolveType(row.GetValue<string>(2)));
	}
}

void DuckLakeServerSideCommit::ReadStagedDataFiles() {
	// First fold per-file column stats so we can attach them to each DuckLakeDataFile in one pass.
	map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> per_file_stats;
	{
		auto stats_query =
		    StringUtil::Format("SELECT data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, "
		                       "min_value, max_value, contains_nan "
		                       "FROM %s.ducklake_staged_data_file_column_stats_%s",
		                       schema_id, identifier_suffix);
		auto stats_result = RunQuery(stats_query, "read staged column stats");
		for (auto &row : *stats_result) {
			DataFileIndex local_file_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
			ColumnKey key {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(1))),
			               FieldIndex(static_cast<idx_t>(row.GetValue<int64_t>(2)))};
			auto type_it = column_types.find(key);
			if (type_it == column_types.end()) {
				continue;
			}
			per_file_stats[local_file_id].emplace(
			    key.second, StatsFromRow(type_it->second, row.GetBaseValue(3), row.GetBaseValue(4), row.GetBaseValue(5),
			                             row.GetBaseValue(6), row.GetBaseValue(7), row.GetBaseValue(8)));
		}
	}

	// Read the files themselves, attach stats, push into state->local_changes per table.
	map<TableIndex, vector<DuckLakeDataFile>> files_per_table;
	auto files_query =
	    StringUtil::Format("SELECT data_file_id, table_id, path, record_count, file_size_bytes, footer_size, "
	                       "row_id_start, partition_id, encryption_key, mapping_id, partial_max "
	                       "FROM %s.ducklake_staged_data_file_%s ORDER BY table_id, file_order",
	                       schema_id, identifier_suffix);
	auto files_result = RunQuery(files_query, "read staged files");
	for (auto &row : *files_result) {
		DataFileIndex local_file_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
		DuckLakeDataFile f;
		f.file_name = row.GetValue<string>(2);
		f.row_count = static_cast<idx_t>(row.GetValue<int64_t>(3));
		f.file_size_bytes = static_cast<idx_t>(row.GetValue<int64_t>(4));
		if (!row.IsNull(5)) {
			f.footer_size = static_cast<idx_t>(row.GetValue<int64_t>(5));
		}
		if (!row.IsNull(6)) {
			f.flush_row_id_start = static_cast<idx_t>(row.GetValue<int64_t>(6));
		}
		if (!row.IsNull(7)) {
			f.partition_id = static_cast<idx_t>(row.GetValue<int64_t>(7));
		}
		if (!row.IsNull(8)) {
			f.encryption_key = Blob::FromBase64(string_t(row.GetValue<string>(8)));
		}
		if (!row.IsNull(9)) {
			f.mapping_id = MappingIndex(static_cast<idx_t>(row.GetValue<int64_t>(9)));
		}
		if (!row.IsNull(10)) {
			f.max_partial_file_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(10));
		}
		auto stats_it = per_file_stats.find(local_file_id);
		if (stats_it != per_file_stats.end()) {
			f.column_stats = std::move(stats_it->second);
		}
		files_per_table[TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(1)))].push_back(std::move(f));
	}

	for (auto &entry : files_per_table) {
		state->local_changes.AppendFiles(entry.first, std::move(entry.second));
	}

	// Derive transaction_changes the same way DuckLakeTransaction would — directly from local_changes.
	for (auto &entry : state->local_changes.Changes()) {
		DuckLakeTransaction::AddTableChanges(entry.GetTableIndex(), entry.GetTableChanges(), transaction_changes);
	}
}

void DuckLakeServerSideCommit::ReadExistingTableStats() {
	// Shared SQL + parser with DuckLakeMetadataManager::GetGlobalTableStats — single combined query
	// over ducklake_table_stats LEFT JOIN ducklake_table_column_stats folded into per-table entries.
	string sql = StringUtil::Replace(DuckLakeMetadataManager::GlobalTableStatsQuery(), "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read existing table stats");
	auto global_stats = DuckLakeMetadataManager::ParseGlobalTableStats(*result);

	for (auto &gs : global_stats) {
		auto entry = make_shared_ptr<DuckLakeTableStats>();
		entry->record_count = gs.record_count;
		entry->next_row_id = gs.next_row_id;
		entry->table_size_bytes = gs.table_size_bytes;
		for (auto &col : gs.column_stats) {
			auto type_it = column_types.find({gs.table_id, col.column_id});
			if (type_it == column_types.end()) {
				continue;
			}
			entry->column_stats.emplace(col.column_id, StatsFromGlobal(type_it->second, col));
		}
		existing_table_stats.emplace(gs.table_id, std::move(entry));
	}
}

//===--------------------------------------------------------------------===//
// Closure backends
//===--------------------------------------------------------------------===//

DuckLakeSnapshot DuckLakeServerSideCommit::ReadLatestSnapshot() {
	string sql = StringUtil::Replace(DuckLakeMetadataManager::LatestSnapshotQuery(), "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read latest snapshot");
	auto snapshot = DuckLakeMetadataManager::ParseSnapshot(*result);
	if (!snapshot) {
		throw IOException("Server-side ducklake_commit: no snapshot found");
	}
	return *snapshot;
}

unique_ptr<DuckLakeStats> DuckLakeServerSideCommit::BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats) {
	// Mirrors DuckLakeCatalog::ConstructStatsMap but resolves column types via our column_types
	// map instead of a DuckLakeCatalogSet (no catalog available server-side).
	auto result = make_uniq<DuckLakeStats>();
	for (auto &gs : global_stats) {
		auto entry = make_uniq<DuckLakeTableStats>();
		entry->record_count = gs.record_count;
		entry->next_row_id = gs.next_row_id;
		entry->table_size_bytes = gs.table_size_bytes;
		for (auto &col : gs.column_stats) {
			auto type_it = column_types.find({gs.table_id, col.column_id});
			if (type_it == column_types.end()) {
				continue;
			}
			entry->column_stats.emplace(col.column_id, StatsFromGlobal(type_it->second, col));
		}
		result->table_stats.emplace(gs.table_id, std::move(entry));
	}
	return result;
}

DuckLakeCommitContext DuckLakeServerSideCommit::BuildContext(idx_t &committed_snapshot_id,
                                                             idx_t &committed_schema_version) {
	DuckLakeCommitContext ctx;
	ctx.commit_info = state->commit_info;
	ctx.conflict_query_executor = [this](string q) -> unique_ptr<QueryResult> {
		auto sql = SubstitutePlaceholders(std::move(q), transaction_snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(sql));
	};
	ctx.get_snapshot = [this]() {
		return ReadLatestSnapshot();
	};
	ctx.execute_commit_batch = [this](DuckLakeSnapshot snapshot, string &query) -> unique_ptr<QueryResult> {
		query = SubstitutePlaceholders(query, snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(query));
	};
	ctx.flush_cache_if_pending = []() {
	};
	ctx.commit_connection = []() {
	};
	ctx.try_rollback = []() {
	};
	ctx.prepare_retry = []() {
	};
	ctx.query_metadata = [this](string q) -> unique_ptr<QueryResult> {
		auto sql = StringUtil::Replace(std::move(q), "{METADATA_CATALOG}", schema_id);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(sql));
	};
	ctx.query_metadata_with_snapshot = [this](DuckLakeSnapshot snapshot, string q) -> unique_ptr<QueryResult> {
		auto sql = SubstitutePlaceholders(std::move(q), snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(sql));
	};
	ctx.try_append_data_files = [](DuckLakeSnapshot &, const vector<DuckLakeFileInfo> &,
	                               const vector<DuckLakeTableInfo> &, vector<DuckLakeSchemaInfo> &) {
		return false;
	};
	// Inlined-data closures are unreachable today: the gate rejects tables_inserted_inlined, and
	// the data-only commit path never produces new_tables — so CommitChanges never invokes these.
	// Once we lift inlined emitters to statics they'll be wired here.
	ctx.write_inlined_tables = [](DuckLakeSnapshot, const vector<DuckLakeTableInfo> &) {
		return string();
	};
	ctx.write_inlined_data = [](DuckLakeSnapshot &, const vector<DuckLakeInlinedDataInfo> &,
	                            const vector<DuckLakeTableInfo> &, const vector<DuckLakeTableInfo> &) {
		return string();
	};
	ctx.get_table_stats = [this](TableIndex table_id) -> shared_ptr<DuckLakeTableStats> {
		auto it = existing_table_stats.find(table_id);
		if (it == existing_table_stats.end()) {
			return nullptr;
		}
		return it->second;
	};
	ctx.build_stats_map = [this](vector<DuckLakeGlobalStatsInfo> &stats) {
		return BuildStatsMap(stats);
	};
	ctx.invalidate_schema_cache = [](idx_t) {
	};
	ctx.set_catalog_version = [&committed_schema_version](idx_t v) {
		committed_schema_version = v;
	};
	ctx.set_committed_snapshot_id = [&committed_snapshot_id](idx_t v) {
		committed_snapshot_id = v;
	};
	return ctx;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

string DuckLakeServerSideCommit::SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const {
	sql = StringUtil::Replace(sql, "{METADATA_CATALOG}", schema_id);
	sql = StringUtil::Replace(sql, "{SNAPSHOT_ID}", std::to_string(snapshot.snapshot_id));
	sql = StringUtil::Replace(sql, "{SCHEMA_VERSION}", std::to_string(snapshot.schema_version));
	sql = StringUtil::Replace(sql, "{NEXT_CATALOG_ID}", std::to_string(snapshot.next_catalog_id));
	sql = StringUtil::Replace(sql, "{NEXT_FILE_ID}", std::to_string(snapshot.next_file_id));
	return sql;
}

string DuckLakeServerSideCommit::EmitDropStagingSql() const {
	string sql;
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_commit_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_data_file_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_data_file_column_stats_%s;", schema_id,
	                          identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_inlined_data_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_inlined_row_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_inlined_column_stats_%s;", schema_id,
	                          identifier_suffix);
	return sql;
}

unique_ptr<MaterializedQueryResult> DuckLakeServerSideCommit::RunQuery(const string &query, const char *what) {
	auto result = fresh_conn.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw(StringUtil::Format("Server-side ducklake_commit (%s) failed: ", what));
	}
	return result;
}

} // namespace duckdb
