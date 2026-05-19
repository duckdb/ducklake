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

//===--------------------------------------------------------------------===//
// Run — orchestrates the four phases and executes the resulting batch
//===--------------------------------------------------------------------===//

DuckLakeServerSideCommitResult DuckLakeServerSideCommit::Run() {
	// Phase 1 — pull staged data into C++ structures.
	auto column_types = ReadColumnTypes();
	auto per_file_column_stats = ReadStagedColumnStats(column_types);
	auto files_per_table = ReadStagedFiles(per_file_column_stats);
	SnapshotChangeInfo change_info;
	DuckLakeSnapshotCommit commit_info;
	ReadCommitHeader(change_info, commit_info);

	// Phase 2 — load existing metadata state needed for stats merging and ID allocation.
	auto existing_table_stats = ReadExistingTableStats(column_types);
	auto commit_snapshot = ReadLatestSnapshot();
	commit_snapshot.snapshot_id += 1;
	commit_snapshot.schema_version = static_cast<idx_t>(schema_version);

	// Phase 3 — allocate file IDs and merge per-table stats.
	auto commit_data = BuildCommitData(files_per_table, existing_table_stats, commit_snapshot);

	// Phase 4 — assemble + execute the SQL batch.
	string batch;
	batch += EmitDataFilesSql(commit_data.new_files, commit_snapshot.snapshot_id);
	batch += EmitTableStatsSql(commit_data.table_stats);
	batch += EmitSnapshotSql(commit_snapshot);
	batch += EmitSnapshotChangesSql(commit_snapshot, change_info, commit_info);
	batch += EmitDropStagingSql();
	RunQuery(batch, "commit batch");

	DuckLakeServerSideCommitResult result;
	result.committed_snapshot_id = static_cast<int64_t>(commit_snapshot.snapshot_id);
	result.committed_schema_version = schema_version;
	return result;
}

//===--------------------------------------------------------------------===//
// Phase 1 — read staged data
//===--------------------------------------------------------------------===//

map<DuckLakeServerSideCommit::ColumnKey, LogicalType> DuckLakeServerSideCommit::ReadColumnTypes() {
	// Load types for every column of every table we are touching in this commit. This covers both
	// columns with new staged stats AND columns whose existing cumulative stats we need to read back.
	map<ColumnKey, LogicalType> types;
	auto query = StringUtil::Format("SELECT col.table_id, col.column_id, col.column_type "
	                                 "FROM %s.ducklake_column col "
	                                 "WHERE col.end_snapshot IS NULL "
	                                 "AND col.table_id IN (SELECT DISTINCT table_id FROM %s.ducklake_staged_data_file_%s)",
	                                 schema_id, schema_id, identifier_suffix);
	auto result = RunQuery(query, "read column types");
	for (auto &row : *result) {
		types.emplace(ColumnKey {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))),
		                          FieldIndex(static_cast<idx_t>(row.GetValue<int64_t>(1)))},
		              ResolveType(row.GetValue<string>(2)));
	}
	return types;
}

map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>>
DuckLakeServerSideCommit::ReadStagedColumnStats(const map<ColumnKey, LogicalType> &column_types) {
	map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> per_file;
	auto query = StringUtil::Format(
	    "SELECT data_file_id, table_id, column_id, column_size_bytes, value_count, null_count, "
	    "min_value, max_value, contains_nan "
	    "FROM %s.ducklake_staged_data_file_column_stats_%s",
	    schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged column stats");
	for (auto &row : *result) {
		DataFileIndex local_file_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
		ColumnKey key {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(1))),
		               FieldIndex(static_cast<idx_t>(row.GetValue<int64_t>(2)))};
		auto type_it = column_types.find(key);
		if (type_it == column_types.end()) {
			continue;
		}
		per_file[local_file_id].emplace(key.second, StatsFromRow(type_it->second, row.GetBaseValue(3),
		                                                          row.GetBaseValue(4), row.GetBaseValue(5),
		                                                          row.GetBaseValue(6), row.GetBaseValue(7),
		                                                          row.GetBaseValue(8)));
	}
	return per_file;
}

map<TableIndex, vector<DuckLakeDataFile>>
DuckLakeServerSideCommit::ReadStagedFiles(map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> &per_file_column_stats) {
	map<TableIndex, vector<DuckLakeDataFile>> files_per_table;
	auto query = StringUtil::Format(
	    "SELECT data_file_id, table_id, path, record_count, file_size_bytes, footer_size, "
	    "row_id_start, partition_id, encryption_key, mapping_id, partial_max "
	    "FROM %s.ducklake_staged_data_file_%s ORDER BY table_id, file_order",
	    schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged files");
	for (auto &row : *result) {
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
		auto stats_it = per_file_column_stats.find(local_file_id);
		if (stats_it != per_file_column_stats.end()) {
			f.column_stats = std::move(stats_it->second);
		}
		files_per_table[TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(1)))].push_back(std::move(f));
	}
	return files_per_table;
}

void DuckLakeServerSideCommit::ReadCommitHeader(SnapshotChangeInfo &change_info, DuckLakeSnapshotCommit &commit_info) {
	auto query = StringUtil::Format(
	    "SELECT (SELECT string_agg('inserted_into_table:' || entity_id::VARCHAR, ',') "
	    "FROM %s.ducklake_staged_change_touch_%s WHERE kind = 'inserted_into') AS changes, "
	    "commit_author, commit_message, commit_extra_info "
	    "FROM %s.ducklake_staged_commit_%s",
	    schema_id, identifier_suffix, schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged commit header");
	auto chunk = result->Fetch();
	if (!chunk || chunk->size() == 0) {
		return;
	}
	if (!chunk->GetValue(0, 0).IsNull()) {
		change_info.changes_made = chunk->GetValue(0, 0).ToString();
	}
	commit_info.author = chunk->GetValue(1, 0);
	commit_info.commit_message = chunk->GetValue(2, 0);
	commit_info.commit_extra_info = chunk->GetValue(3, 0);
}

//===--------------------------------------------------------------------===//
// Phase 2 — read existing metadata state
//===--------------------------------------------------------------------===//

map<TableIndex, DuckLakeTableStats>
DuckLakeServerSideCommit::ReadExistingTableStats(const map<ColumnKey, LogicalType> &column_types) {
	// Shared SQL + parser with DuckLakeMetadataManager::GetGlobalTableStats — one combined query
	// over ducklake_table_stats LEFT JOIN ducklake_table_column_stats, then a fold into
	// per-table DuckLakeTableStats with typed DuckLakeColumnStats (regular path does this in
	// DuckLakeCatalog::ConstructStatsMap using catalog schema; we use our column_types map).
	string sql = StringUtil::Replace(DuckLakeMetadataManager::GlobalTableStatsQuery(), "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read existing table stats");
	auto global_stats = DuckLakeMetadataManager::ParseGlobalTableStats(*result);

	map<TableIndex, DuckLakeTableStats> table_stats;
	for (auto &gs : global_stats) {
		auto &entry = table_stats[gs.table_id];
		entry.record_count = gs.record_count;
		entry.next_row_id = gs.next_row_id;
		entry.table_size_bytes = gs.table_size_bytes;
		for (auto &col : gs.column_stats) {
			auto type_it = column_types.find({gs.table_id, col.column_id});
			if (type_it == column_types.end()) {
				continue;
			}
			DuckLakeColumnStats column_stats(type_it->second);
			column_stats.has_null_count = col.has_contains_null;
			if (col.has_contains_null) {
				column_stats.null_count = col.contains_null ? 1 : 0;
			}
			column_stats.has_contains_nan = col.has_contains_nan;
			if (col.has_contains_nan) {
				column_stats.contains_nan = col.contains_nan;
			}
			column_stats.has_min = col.has_min;
			if (col.has_min) {
				column_stats.min = col.min_val;
			}
			column_stats.has_max = col.has_max;
			if (col.has_max) {
				column_stats.max = col.max_val;
			}
			column_stats.any_valid = column_stats.has_min || column_stats.has_max;
			entry.column_stats.emplace(col.column_id, std::move(column_stats));
		}
	}
	return table_stats;
}

DuckLakeSnapshot DuckLakeServerSideCommit::ReadLatestSnapshot() {
	string sql = StringUtil::Replace(DuckLakeMetadataManager::LatestSnapshotQuery(), "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read latest snapshot");
	auto snapshot = DuckLakeMetadataManager::ParseSnapshot(*result);
	if (!snapshot) {
		throw IOException("Server-side ducklake_commit: no snapshot found");
	}
	return *snapshot;
}

//===--------------------------------------------------------------------===//
// Phase 3 — allocate file IDs and merge per-table stats (uses DuckLakeTableStats::MergeStats)
//===--------------------------------------------------------------------===//

DuckLakeServerSideCommit::CommitData
DuckLakeServerSideCommit::BuildCommitData(const map<TableIndex, vector<DuckLakeDataFile>> &files_per_table,
                                           const map<TableIndex, DuckLakeTableStats> &existing_table_stats,
                                           DuckLakeSnapshot &commit_snapshot) {
	CommitData result;
	for (auto &entry : files_per_table) {
		auto table_id = entry.first;
		DuckLakeNewGlobalStats new_globals;
		auto existing_it = existing_table_stats.find(table_id);
		if (existing_it != existing_table_stats.end()) {
			new_globals.stats = existing_it->second;
			new_globals.initialized = true;
		}
		auto &new_stats = new_globals.stats;
		for (auto &file : entry.second) {
			auto row_id_start =
			    file.flush_row_id_start.IsValid() ? file.flush_row_id_start.GetIndex() : new_stats.next_row_id;
			auto data_file = DuckLakeTransaction::BuildDataFileInfo(file, commit_snapshot, table_id, row_id_start);
			if (!file.max_partial_file_snapshot.IsValid()) {
				new_stats.record_count += file.row_count;
				new_stats.next_row_id += file.row_count;
			}
			new_stats.table_size_bytes += file.file_size_bytes;
			for (auto &col_stats : file.column_stats) {
				new_stats.MergeStats(col_stats.first, col_stats.second);
			}
			result.new_files.push_back(std::move(data_file));
		}
		result.table_stats.emplace(table_id, std::move(new_globals));
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Phase 4 — SQL emission
//===--------------------------------------------------------------------===//

string DuckLakeServerSideCommit::EmitDataFilesSql(const vector<DuckLakeFileInfo> &files,
                                                   idx_t commit_snapshot_id) const {
	if (files.empty()) {
		return string();
	}
	// Staged paths are always absolute today — staging-side relativization is a separate concern.
	vector<DuckLakePath> resolved_paths;
	resolved_paths.reserve(files.size());
	for (auto &file : files) {
		resolved_paths.push_back({file.file_name, false});
	}
	auto sql = DuckLakeMetadataManager::WriteNewDataFilesSqlBatch(files, resolved_paths);
	sql = StringUtil::Replace(sql, "{METADATA_CATALOG}", schema_id);
	sql = StringUtil::Replace(sql, "{SNAPSHOT_ID}", std::to_string(commit_snapshot_id));
	return sql;
}

string DuckLakeServerSideCommit::EmitTableStatsSql(const map<TableIndex, DuckLakeNewGlobalStats> &table_stats) const {
	string sql;
	for (auto &entry : table_stats) {
		auto stats = DuckLakeTransaction::ConvertNewGlobalStats(entry.first, entry.second);
		sql += DuckLakeMetadataManager::UpdateGlobalTableStatsSql(stats);
	}
	return StringUtil::Replace(sql, "{METADATA_CATALOG}", schema_id);
}

string DuckLakeServerSideCommit::EmitSnapshotSql(const DuckLakeSnapshot &commit_snapshot) const {
	return SubstitutePlaceholders(DuckLakeMetadataManager::InsertSnapshotSql(), commit_snapshot);
}

string DuckLakeServerSideCommit::EmitSnapshotChangesSql(const DuckLakeSnapshot &commit_snapshot,
                                                         const SnapshotChangeInfo &change_info,
                                                         const DuckLakeSnapshotCommit &commit_info) const {
	return SubstitutePlaceholders(DuckLakeMetadataManager::WriteSnapshotChangesSql(change_info, commit_info),
	                               commit_snapshot);
}

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
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_change_touch_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_data_file_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_data_file_column_stats_%s;", schema_id,
	                           identifier_suffix);
	return sql;
}

//===--------------------------------------------------------------------===//
// Helpers
//===--------------------------------------------------------------------===//

unique_ptr<MaterializedQueryResult> DuckLakeServerSideCommit::RunQuery(const string &query, const char *what) {
	auto result = fresh_conn.Query(query);
	if (result->HasError()) {
		result->GetErrorObject().Throw(StringUtil::Format("Server-side ducklake_commit (%s) failed: ", what));
	}
	return result;
}

} // namespace duckdb
