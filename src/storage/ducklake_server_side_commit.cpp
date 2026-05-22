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
	ReadCommitHeader();             // creates state, fills commit_info/data_path/separator, sets transaction_snapshot
	ReadColumnTypes();              // fills column_types (used by stats reconstruction below + retry closure)
	ReadStagedDataFiles();          // fills state->local_changes (data files)
	ReadStagedInlinedData();        // fills state->local_changes (inlined data) + staged_inlined_tuples
	ReadStagedInlinedDeletes();     // fills state->local_changes (inlined-data row deletes)
	ReadStagedInlinedFileDeletes(); // fills state->local_changes (inlined deletes against parquet files)
	ReadStagedDeleteFiles();        // fills state->local_changes (parquet delete-vector files)
	ReadStagedDroppedFiles();       // fills state->dropped_files + state->tables_deleted_from
	ReadStagedFlushedInlinedTables(); // fills state->flushed_inlined_tables (inlined → parquet flushes)
	ReadExistingTableStats();       // fills existing_table_stats (first-attempt path for get_table_stats)

	// Derive transaction_changes the same way DuckLakeTransaction would — directly from
	// the now-fully-hydrated local_changes (both data files and inlined data).
	for (auto &entry : state->local_changes.Changes()) {
		DuckLakeTransaction::AddTableChanges(entry.GetTableIndex(), entry.GetTableChanges(), transaction_changes);
	}
	// Whole-file drops live on state->tables_deleted_from rather than local_changes, so mirror them
	// into the transaction_changes set the same way state.WriteSnapshotChanges does at commit time.
	for (auto &table_id : state->tables_deleted_from) {
		transaction_changes.tables_deleted_from.insert(table_id);
	}

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
	result.had_flushes = !state->flushed_inlined_tables.empty();
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
	// per-file stats AND columns whose cumulative ducklake_table_column_stats row we read back, plus
	// columns of tables touched by staged inlined-data inserts.
	auto query = StringUtil::Format("SELECT col.table_id, col.column_id, col.column_type "
	                                "FROM %s.ducklake_column col "
	                                "WHERE col.end_snapshot IS NULL "
	                                "AND col.table_id IN ("
	                                "  SELECT table_id FROM %s.ducklake_staged_data_file_%s "
	                                "  UNION "
	                                "  SELECT table_id FROM %s.ducklake_staged_inlined_data_%s)",
	                                schema_id, schema_id, identifier_suffix, schema_id, identifier_suffix);
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

	// Read delete files attached to transaction-local data files (DELETE applied to rows of a file
	// inserted in the same transaction). Grouped by local_file_id so we can attach when building
	// each DuckLakeDataFile below.
	map<idx_t, vector<DuckLakeDeleteFile>> attached_deletes;
	{
		auto attached_query =
		    StringUtil::Format("SELECT attached_local_file_id, file_name, format, delete_count, file_size_bytes, "
		                       "footer_size, encryption_key, begin_snapshot, max_snapshot, source "
		                       "FROM %s.ducklake_staged_delete_file_%s "
		                       "WHERE attached_local_file_id IS NOT NULL",
		                       schema_id, identifier_suffix);
		auto attached_result = RunQuery(attached_query, "read attached delete files");
		for (auto &row : *attached_result) {
			idx_t local_file_id = static_cast<idx_t>(row.GetValue<int64_t>(0));
			DuckLakeDeleteFile f;
			f.file_name = row.GetValue<string>(1);
			f.format = DeleteFileFormatFromString(row.GetValue<string>(2));
			f.delete_count = static_cast<idx_t>(row.GetValue<int64_t>(3));
			f.file_size_bytes = static_cast<idx_t>(row.GetValue<int64_t>(4));
			f.footer_size = static_cast<idx_t>(row.GetValue<int64_t>(5));
			if (!row.IsNull(6)) {
				f.encryption_key = Blob::FromBase64(string_t(row.GetValue<string>(6)));
			}
			if (!row.IsNull(7)) {
				f.begin_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(7));
			}
			if (!row.IsNull(8)) {
				f.max_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(8));
			}
			f.source = row.GetValue<string>(9) == "FLUSH" ? DeleteFileSource::FLUSH : DeleteFileSource::REGULAR;
			attached_deletes[local_file_id].push_back(std::move(f));
		}
	}

	// Read the files themselves, attach stats + attached deletes, push into state->local_changes per table.
	map<TableIndex, vector<DuckLakeDataFile>> files_per_table;
	auto files_query =
	    StringUtil::Format("SELECT data_file_id, table_id, path, record_count, file_size_bytes, footer_size, "
	                       "row_id_start, partition_id, encryption_key, mapping_id, partial_max, begin_snapshot "
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
		if (!row.IsNull(11)) {
			f.begin_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(11));
		}
		auto stats_it = per_file_stats.find(local_file_id);
		if (stats_it != per_file_stats.end()) {
			f.column_stats = std::move(stats_it->second);
		}
		auto attached_it = attached_deletes.find(local_file_id.index);
		if (attached_it != attached_deletes.end()) {
			f.delete_files = std::move(attached_it->second);
		}
		files_per_table[TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(1)))].push_back(std::move(f));
	}

	for (auto &entry : files_per_table) {
		state->local_changes.AppendFiles(entry.first, std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedData() {
	// `ducklake_staged_inlined_data_<sfx>(table_id, has_preserved_row_ids)` — one row per table that
	// has inlined inserts in this commit.
	auto meta_query = StringUtil::Format("SELECT table_id, has_preserved_row_ids "
	                                     "FROM %s.ducklake_staged_inlined_data_%s",
	                                     schema_id, identifier_suffix);
	auto meta_result = RunQuery(meta_query, "read staged inlined data meta");
	struct PerTable {
		bool has_preserved = false;
	};
	map<TableIndex, PerTable> per_table;
	for (auto &row : *meta_result) {
		TableIndex table_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
		per_table[table_id].has_preserved = row.GetValue<bool>(1);
	}
	if (per_table.empty()) {
		return;
	}

	// `ducklake_staged_inlined_row_<sfx>(table_id, row_order, preserved_row_id, value_literals)` —
	// one row per inlined-data row; `value_literals` is the SQL tuple text produced by
	// DuckLakeUtil::ValueToSQL at staging time. Group by table_id and order by row_order.
	auto rows_query = StringUtil::Format("SELECT table_id, preserved_row_id, value_literals "
	                                     "FROM %s.ducklake_staged_inlined_row_%s "
	                                     "ORDER BY table_id, row_order",
	                                     schema_id, identifier_suffix);
	auto rows_result = RunQuery(rows_query, "read staged inlined rows");
	for (auto &row : *rows_result) {
		TableIndex table_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
		auto pt_it = per_table.find(table_id);
		if (pt_it == per_table.end()) {
			continue;
		}
		staged_inlined_tuples[table_id].push_back(row.GetValue<string>(2));
		if (pt_it->second.has_preserved) {
			// preserved_row_id is NULL for INSERTed rows (transaction-local placeholder), real id
			// for UPDATEd rows. Use TRANSACTION_LOCAL_ROW_ID_START as the placeholder so the state's
			// next_row_id accounting matches the client-side path.
			int64_t rid;
			if (row.IsNull(1)) {
				rid = static_cast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START);
			} else {
				rid = row.GetValue<int64_t>(1);
			}
			staged_inlined_row_ids[table_id].push_back(rid);
		}
	}

	// `ducklake_staged_inlined_column_stats_<sfx>` — one row per (table_id, column_id) of staged
	// inlined-data stats, fed into MergeStats during state.CommitChanges so global stats track
	// the new rows.
	auto stats_query = StringUtil::Format(
	    "SELECT table_id, column_id, column_size_bytes, has_num_values, num_values, has_null_count, null_count, "
	    "has_min, min_value, has_max, max_value, has_contains_nan, contains_nan, any_valid, extra_stats "
	    "FROM %s.ducklake_staged_inlined_column_stats_%s",
	    schema_id, identifier_suffix);
	auto stats_result = RunQuery(stats_query, "read staged inlined column stats");
	map<TableIndex, map<FieldIndex, DuckLakeColumnStats>> stats_per_table;
	for (auto &row : *stats_result) {
		TableIndex table_id(static_cast<idx_t>(row.GetValue<int64_t>(0)));
		FieldIndex column_id(static_cast<idx_t>(row.GetValue<int64_t>(1)));
		auto type_it = column_types.find({table_id, column_id});
		if (type_it == column_types.end()) {
			continue;
		}
		DuckLakeColumnStats s(type_it->second);
		if (!row.IsNull(2)) {
			s.column_size_bytes = static_cast<idx_t>(row.GetValue<int64_t>(2));
		}
		s.has_num_values = !row.IsNull(3) && row.GetValue<bool>(3);
		if (s.has_num_values && !row.IsNull(4)) {
			s.num_values = static_cast<idx_t>(row.GetValue<int64_t>(4));
		}
		s.has_null_count = !row.IsNull(5) && row.GetValue<bool>(5);
		if (s.has_null_count && !row.IsNull(6)) {
			s.null_count = static_cast<idx_t>(row.GetValue<int64_t>(6));
		}
		if (!row.IsNull(7) && row.GetValue<bool>(7) && !row.IsNull(8)) {
			s.has_min = true;
			s.min = row.GetValue<string>(8);
		}
		if (!row.IsNull(9) && row.GetValue<bool>(9) && !row.IsNull(10)) {
			s.has_max = true;
			s.max = row.GetValue<string>(10);
		}
		s.has_contains_nan = !row.IsNull(11) && row.GetValue<bool>(11);
		if (s.has_contains_nan && !row.IsNull(12)) {
			s.contains_nan = row.GetValue<bool>(12);
		}
		if (!row.IsNull(13)) {
			s.any_valid = row.GetValue<bool>(13);
		}
		if (!row.IsNull(14) && s.extra_stats) {
			s.extra_stats->Deserialize(row.GetValue<string>(14));
		}
		stats_per_table[table_id].emplace(column_id, std::move(s));
	}

	// Build a DuckLakeInlinedData per table. `data` is null — the closure reads from
	// `staged_inlined_tuples` instead of walking a ColumnDataCollection.
	for (auto &entry : per_table) {
		auto table_id = entry.first;
		auto tuples_it = staged_inlined_tuples.find(table_id);
		idx_t row_count = (tuples_it == staged_inlined_tuples.end()) ? 0 : tuples_it->second.size();

		auto inlined = make_uniq<DuckLakeInlinedData>();
		inlined->external_row_count = row_count;
		auto stats_it = stats_per_table.find(table_id);
		if (stats_it != stats_per_table.end()) {
			inlined->column_stats = std::move(stats_it->second);
		}
		if (entry.second.has_preserved) {
			auto row_ids_it = staged_inlined_row_ids.find(table_id);
			if (row_ids_it != staged_inlined_row_ids.end()) {
				inlined->row_ids = row_ids_it->second;
			}
		}
		state->local_changes.AppendInlinedData(context, table_id, std::move(inlined));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedDeletes() {
	// `ducklake_staged_inlined_delete_<sfx>(table_id, inlined_table_name, deleted_row_id)` — one row
	// per deleted inlined-data row. Group by (table_id, inlined_table_name) and feed each group
	// through LocalTableChanges::AddNewInlinedDeletes — same path the client uses for live deletes.
	auto query = StringUtil::Format("SELECT table_id, inlined_table_name, deleted_row_id "
	                                "FROM %s.ducklake_staged_inlined_delete_%s "
	                                "ORDER BY table_id, inlined_table_name",
	                                schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged inlined deletes");

	struct GroupKey {
		TableIndex table_id;
		string inlined_table_name;
		bool operator<(const GroupKey &other) const {
			if (table_id.index != other.table_id.index) {
				return table_id.index < other.table_id.index;
			}
			return inlined_table_name < other.inlined_table_name;
		}
	};
	map<GroupKey, set<idx_t>> grouped;
	for (auto &row : *result) {
		GroupKey key {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))), row.GetValue<string>(1)};
		grouped[key].insert(static_cast<idx_t>(row.GetValue<int64_t>(2)));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedDeletes(entry.first.table_id, entry.first.inlined_table_name,
		                                          std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedFileDeletes() {
	// `ducklake_staged_inlined_file_delete_<sfx>(table_id, file_id, deleted_row_id)` — one row per
	// row deleted from a parquet file that's recorded as metadata (in ducklake_inlined_delete_<id>)
	// rather than as a delete-vector parquet file. Group by (table_id, file_id) and feed each group
	// through LocalTableChanges::AddNewInlinedFileDeletes — same path the client uses.
	auto query = StringUtil::Format("SELECT table_id, file_id, deleted_row_id "
	                                "FROM %s.ducklake_staged_inlined_file_delete_%s "
	                                "ORDER BY table_id, file_id",
	                                schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged inlined file deletes");

	struct GroupKey {
		TableIndex table_id;
		idx_t file_id;
		bool operator<(const GroupKey &other) const {
			if (table_id.index != other.table_id.index) {
				return table_id.index < other.table_id.index;
			}
			return file_id < other.file_id;
		}
	};
	map<GroupKey, set<idx_t>> grouped;
	for (auto &row : *result) {
		GroupKey key {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))),
		              static_cast<idx_t>(row.GetValue<int64_t>(1))};
		grouped[key].insert(static_cast<idx_t>(row.GetValue<int64_t>(2)));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedFileDeletes(entry.first.table_id, entry.first.file_id,
		                                              std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedDeleteFiles() {
	// Loose delete-vector files (DELETE against pre-existing parquet files). Reconstruct
	// DuckLakeDeleteFile entries (including overwrite metadata) and push into local_changes via
	// AppendDeleteFiles — AddDeletes would RemoveFile() on overwritten files, which is the
	// client's job and has already happened. WHERE attached_local_file_id IS NULL excludes the
	// deletes that ReadStagedDataFiles already attached to their transaction-local data file.
	auto query =
	    StringUtil::Format("SELECT table_id, data_file_path, data_file_id, file_name, format, delete_count, "
	                       "file_size_bytes, footer_size, encryption_key, begin_snapshot, max_snapshot, source, "
	                       "overwrites_existing_delete, overwrite_delete_file_id, overwrite_delete_file_path "
	                       "FROM %s.ducklake_staged_delete_file_%s "
	                       "WHERE attached_local_file_id IS NULL "
	                       "ORDER BY table_id, data_file_path",
	                       schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged delete files");

	struct GroupKey {
		TableIndex table_id;
		string data_file_path;
		bool operator<(const GroupKey &other) const {
			if (table_id.index != other.table_id.index) {
				return table_id.index < other.table_id.index;
			}
			return data_file_path < other.data_file_path;
		}
	};
	map<GroupKey, vector<DuckLakeDeleteFile>> grouped;
	for (auto &row : *result) {
		GroupKey key {TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))), row.GetValue<string>(1)};
		DuckLakeDeleteFile f;
		f.data_file_id = DataFileIndex(static_cast<idx_t>(row.GetValue<int64_t>(2)));
		f.data_file_path = key.data_file_path;
		f.file_name = row.GetValue<string>(3);
		f.format = DeleteFileFormatFromString(row.GetValue<string>(4));
		f.delete_count = static_cast<idx_t>(row.GetValue<int64_t>(5));
		f.file_size_bytes = static_cast<idx_t>(row.GetValue<int64_t>(6));
		f.footer_size = static_cast<idx_t>(row.GetValue<int64_t>(7));
		if (!row.IsNull(8)) {
			f.encryption_key = Blob::FromBase64(string_t(row.GetValue<string>(8)));
		}
		if (!row.IsNull(9)) {
			f.begin_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(9));
		}
		if (!row.IsNull(10)) {
			f.max_snapshot = static_cast<idx_t>(row.GetValue<int64_t>(10));
		}
		f.source = row.GetValue<string>(11) == "FLUSH" ? DeleteFileSource::FLUSH : DeleteFileSource::REGULAR;
		f.overwrites_existing_delete = !row.IsNull(12) && row.GetValue<bool>(12);
		if (!row.IsNull(13)) {
			f.overwritten_delete_file.delete_file_id = DataFileIndex(static_cast<idx_t>(row.GetValue<int64_t>(13)));
		}
		if (!row.IsNull(14)) {
			f.overwritten_delete_file.path = row.GetValue<string>(14);
		}
		grouped[key].push_back(std::move(f));
	}
	for (auto &entry : grouped) {
		state->local_changes.AppendDeleteFiles(entry.first.table_id, entry.first.data_file_path,
		                                       std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedDroppedFiles() {
	// Whole-file drops. Populate state->dropped_files (the map state.CommitChanges iterates to
	// emit DROP rows) and mirror into state->tables_deleted_from (drives WriteSnapshotChanges).
	auto query = StringUtil::Format("SELECT t.path, t.data_file_id, f.table_id "
	                                "FROM %s.ducklake_staged_dropped_file_%s t "
	                                "LEFT JOIN %s.ducklake_data_file f ON f.data_file_id = t.data_file_id",
	                                schema_id, identifier_suffix, schema_id);
	auto result = RunQuery(query, "read staged dropped files");
	for (auto &row : *result) {
		auto path = row.GetValue<string>(0);
		DataFileIndex file_id(static_cast<idx_t>(row.GetValue<int64_t>(1)));
		state->dropped_files.emplace(std::move(path), file_id);
		if (!row.IsNull(2)) {
			state->tables_deleted_from.insert(TableIndex(static_cast<idx_t>(row.GetValue<int64_t>(2))));
		}
	}
}

void DuckLakeServerSideCommit::ReadStagedFlushedInlinedTables() {
	// `ducklake_staged_flushed_inlined_<sfx>(inlined_table_name, schema_version, flush_snapshot_id)` —
	// one row per inlined-data table that just got flushed to parquet in this commit. Drives
	// GenerateDeleteFlushedInlinedData (removes the inlined rows) and DropEmptySupersededInlinedTables
	// (post-commit cleanup of empty inlined tables superseded by a flush).
	auto query =
	    StringUtil::Format("SELECT inlined_table_name, schema_version, flush_snapshot_id "
	                       "FROM %s.ducklake_staged_flushed_inlined_%s",
	                       schema_id, identifier_suffix);
	auto result = RunQuery(query, "read staged flushed inlined tables");
	for (auto &row : *result) {
		FlushedInlinedTableInfo entry;
		entry.inlined_table.table_name = row.GetValue<string>(0);
		entry.inlined_table.schema_version = static_cast<idx_t>(row.GetValue<int64_t>(1));
		entry.flush_snapshot_id = static_cast<idx_t>(row.GetValue<int64_t>(2));
		state->flushed_inlined_tables.push_back(std::move(entry));
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
	// Server-side: skip the post-commit DropEmptySupersededInlinedTables. The server's fresh_conn
	// would happily drop the empty inlined tables, but the client's catalog cache still references
	// them and the next read fails. Leftover empty tables are a space optimization concern only.
	ctx.skip_drop_empty_inlined = true;
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
		// Use SubstitutePlaceholders so {METADATA_CATALOG_NAME_LITERAL} / {METADATA_SCHEMA_NAME_LITERAL}
		// also resolve — DropEmptySupersededInlinedTables uses them. Snapshot-scoped placeholders
		// are absent from this path; pass an empty snapshot so any stray substitution is a no-op.
		DuckLakeSnapshot empty {};
		auto sql = SubstitutePlaceholders(std::move(q), empty);
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
	// `write_inlined_tables` (the CREATE TABLE part) is still unreachable: the data-only commit
	// gate rejects `created_tables`, so `new_tables` is empty here. Once DDL lands this gets wired.
	ctx.write_inlined_tables = [](DuckLakeSnapshot, const vector<DuckLakeTableInfo> &) {
		return string();
	};
	// `write_inlined_data` emits INSERT INTO ducklake_inlined_data_<id>_<sv> VALUES (...).
	// Staged value_literals already carry SQL-formatted column tuples (produced by
	// DuckLakeUtil::ValueToSQL at staging time), so we splice them in directly rather than
	// reconstruct a ColumnDataCollection on the server.
	ctx.write_inlined_data = [this](DuckLakeSnapshot &, const vector<DuckLakeInlinedDataInfo> &new_data,
	                                const vector<DuckLakeTableInfo> &, const vector<DuckLakeTableInfo> &) -> string {
		string batch;
		for (auto &entry : new_data) {
			auto tuples_it = staged_inlined_tuples.find(entry.table_id);
			if (tuples_it == staged_inlined_tuples.end() || tuples_it->second.empty()) {
				continue;
			}
			auto &tuples = tuples_it->second;
			auto row_ids_it = staged_inlined_row_ids.find(entry.table_id);
			const bool has_preserved = row_ids_it != staged_inlined_row_ids.end();

			// Resolve the latest inlined-data table name for this table. Cache across retries so
			// we only hit the metadata DB once per table_id per commit.
			auto cache_it = inlined_table_name_cache.find(entry.table_id.index);
			if (cache_it == inlined_table_name_cache.end()) {
				auto lookup =
				    StringUtil::Replace(DuckLakeMetadataManager::LatestInlinedTableQuery(entry.table_id.index),
				                        "{METADATA_CATALOG}", schema_id) +
				    ";";
				auto result = RunQuery(lookup, "lookup inlined table name");
				string name;
				for (auto &row : *result) {
					name = row.GetValue<string>(0);
				}
				cache_it = inlined_table_name_cache.emplace(entry.table_id.index, std::move(name)).first;
			}
			const string &inlined_table_name = cache_it->second;
			if (inlined_table_name.empty()) {
				// No inlined table yet — only reachable via CREATE TABLE in this commit, which is
				// gated out. Skip rather than crash on a stray staged row.
				continue;
			}

			idx_t row_id = entry.row_id_start;
			string values;
			for (idx_t i = 0; i < tuples.size(); i++) {
				int64_t emit_rid;
				if (has_preserved) {
					int64_t staged_rid = row_ids_it->second[i];
					if (DuckLakeConstants::IsTransactionLocalRowId(staged_rid)) {
						emit_rid = static_cast<int64_t>(row_id++);
					} else {
						emit_rid = staged_rid;
					}
				} else {
					emit_rid = static_cast<int64_t>(row_id++);
				}
				// Staged value_literals are wrapped as "(v1, v2, ...)". Strip the outer parens
				// so we can splice the cells in after the (row_id, snapshot, NULL) prefix.
				auto &tuple = tuples[i];
				string inner = tuple.size() >= 2 ? tuple.substr(1, tuple.size() - 2) : tuple;
				if (!values.empty()) {
					values += ", ";
				}
				values += StringUtil::Format("(%lld, {SNAPSHOT_ID}, NULL, %s)", emit_rid, inner);
			}
			batch += StringUtil::Format("INSERT INTO {METADATA_CATALOG}.%s VALUES %s;",
			                            SQLIdentifier(inlined_table_name), values);
		}
		return batch;
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
	// State-emitted post-commit cleanup (e.g. DropEmptySupersededInlinedTables) joins against
	// duckdb_tables() filtered by database_name = {METADATA_CATALOG_NAME_LITERAL}. Server-side we
	// don't know the catalog name a priori, so resolve it via current_database() at execution time.
	sql = StringUtil::Replace(sql, "{METADATA_CATALOG_NAME_LITERAL}", "(SELECT current_database())");
	sql = StringUtil::Replace(sql, "{METADATA_SCHEMA_NAME_LITERAL}",
	                          DuckLakeUtil::SQLLiteralToString(metadata_schema_name));
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
	sql +=
	    StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_inlined_delete_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_inlined_file_delete_%s;", schema_id,
	                          identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_delete_file_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_dropped_file_%s;", schema_id, identifier_suffix);
	sql += StringUtil::Format("DROP TABLE IF EXISTS %s.ducklake_staged_flushed_inlined_%s;", schema_id,
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
