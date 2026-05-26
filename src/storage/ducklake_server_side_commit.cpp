#include "storage/ducklake_server_side_commit.hpp"

#include "common/ducklake_row_helpers.hpp"
#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/common/string_util.hpp"
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
                                 const Value &contains_nan, const Value &extra_stats) {
	DuckLakeColumnStats stats(type);
	if (!column_size_bytes.IsNull()) {
		stats.column_size_bytes = static_cast<idx_t>(column_size_bytes.GetValue<int64_t>());
	}
	// Staged value_count is the non-null count; rebuild num_values = value_count + null_count
	// so a later FromColumnStats round-trip preserves the original counts.
	if (!null_count.IsNull()) {
		stats.null_count = static_cast<idx_t>(null_count.GetValue<int64_t>());
		stats.has_null_count = true;
	}
	if (!value_count.IsNull()) {
		idx_t non_null = static_cast<idx_t>(value_count.GetValue<int64_t>());
		stats.num_values = non_null + (stats.has_null_count ? stats.null_count : 0);
		stats.has_num_values = true;
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
	// Only types that allocate stats.extra_stats can consume it.
	if (!extra_stats.IsNull() && stats.extra_stats) {
		stats.extra_stats->Deserialize(extra_stats.ToString());
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
	// extra_stats counts as validity for types whose stats live there (geo bbox, variant).
	stats.any_valid = stats.has_min || stats.has_max || col.has_extra_stats;
	if (col.has_extra_stats && stats.extra_stats) {
		stats.extra_stats->Deserialize(col.extra_stats);
	}
	return stats;
}

//! Read the 9 shared DuckLakeDeleteFile fields starting at `base` column.
//! Layout: file_name, format, delete_count, file_size_bytes, footer_size,
//! encryption_key, begin_snapshot, max_snapshot, source.
template <class ROW>
void FillDeleteFileCommon(DuckLakeDeleteFile &f, ROW &row, idx_t base) {
	f.file_name = row.template GetValue<string>(base + 0);
	f.format = DeleteFileFormatFromString(row.template GetValue<string>(base + 1));
	f.delete_count = AsIdx(row, base + 2);
	f.file_size_bytes = AsIdx(row, base + 3);
	f.footer_size = AsIdx(row, base + 4);
	ReadEncryptionKey(row, base + 5, f.encryption_key);
	if (!row.IsNull(base + 6)) {
		f.begin_snapshot = AsIdx(row, base + 6);
	}
	if (!row.IsNull(base + 7)) {
		f.max_snapshot = AsIdx(row, base + 7);
	}
	f.source = row.template GetValue<string>(base + 8) == "FLUSH" ? DeleteFileSource::FLUSH : DeleteFileSource::REGULAR;
}

template <class ROW>
DuckLakeColumnStats ReadInlinedStatsRow(ROW &row, const LogicalType &type) {
	DuckLakeColumnStats s(type);
	if (!row.IsNull(2)) {
		s.column_size_bytes = AsIdx(row, 2);
	}
	s.has_num_values = OptBoolFalse(row, 3);
	if (s.has_num_values && !row.IsNull(4)) {
		s.num_values = AsIdx(row, 4);
	}
	s.has_null_count = OptBoolFalse(row, 5);
	if (s.has_null_count && !row.IsNull(6)) {
		s.null_count = AsIdx(row, 6);
	}
	if (OptBoolFalse(row, 7) && !row.IsNull(8)) {
		s.has_min = true;
		s.min = row.template GetValue<string>(8);
	}
	if (OptBoolFalse(row, 9) && !row.IsNull(10)) {
		s.has_max = true;
		s.max = row.template GetValue<string>(10);
	}
	s.has_contains_nan = OptBoolFalse(row, 11);
	if (s.has_contains_nan && !row.IsNull(12)) {
		s.contains_nan = row.template GetValue<bool>(12);
	}
	if (!row.IsNull(13)) {
		s.any_valid = row.template GetValue<bool>(13);
	}
	if (!row.IsNull(14) && s.extra_stats) {
		s.extra_stats->Deserialize(row.template GetValue<string>(14));
	}
	return s;
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

string DuckLakeServerSideCommit::Staged(DuckLakeStagedTableType kind) const {
	return DuckLakeStagedTable(kind, schema_id, identifier_suffix).FullQualifiedName();
}

string DuckLakeServerSideCommit::Select(const char *columns, DuckLakeStagedTableType kind, const char *tail) const {
	return StringUtil::Format("SELECT %s FROM %s %s", columns, Staged(kind), tail);
}

//===--------------------------------------------------------------------===//
// Run
//===--------------------------------------------------------------------===//

DuckLakeServerSideCommitResult DuckLakeServerSideCommit::Run() {
	// Hydrate the DuckLakeTransactionState slice by slice.
	ReadCommitHeader();
	ReadColumnTypes();
	ReadStagedDataFiles();
	ReadStagedInlinedData();
	ReadStagedInlinedDeletes();
	ReadStagedInlinedFileDeletes();
	ReadStagedDeleteFiles();
	ReadStagedDroppedFiles();
	ReadStagedFlushedInlinedTables();
	ReadStagedCompactions();
	ReadStagedNameMaps();
	ReadExistingTableStats();

	// Derive transaction_changes from local_changes the same way DuckLakeTransaction does.
	for (auto &entry : state->local_changes.Changes()) {
		DuckLakeTransaction::AddTableChanges(entry.GetTableIndex(), entry.GetTableChanges(), transaction_changes);
	}
	// Mirror whole-file drops into the conflict-detection set.
	for (auto &table_id : state->tables_deleted_from) {
		transaction_changes.tables_deleted_from.insert(table_id);
	}

	idx_t committed_snapshot_id = 0;
	idx_t committed_schema_version = static_cast<idx_t>(schema_version);
	state->Commit(transaction_snapshot, transaction_changes, retry_config,
	              BuildContext(committed_snapshot_id, committed_schema_version));

	RunQuery(DuckLakeStagedTable::DropAllSql(schema_id, identifier_suffix), "drop staging");
	DuckLakeServerSideCommitResult result;
	result.committed_snapshot_id = static_cast<int64_t>(committed_snapshot_id);
	result.committed_schema_version = static_cast<int64_t>(committed_schema_version);
	result.had_flushes = !state->flushed_inlined_tables.empty();
	return result;
}

//===--------------------------------------------------------------------===//
// Hydration
//===--------------------------------------------------------------------===//

void DuckLakeServerSideCommit::ReadCommitHeader() {
	auto query = Select("commit_author, commit_message, commit_extra_info, transaction_snapshot_id, "
	                    "data_path, path_separator, transaction_next_catalog_id, transaction_next_file_id",
	                    DuckLakeStagedTableType::COMMIT_HEADER);
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
	transaction_snapshot.next_catalog_id = static_cast<idx_t>(chunk->GetValue(6, 0).GetValue<uint64_t>());
	transaction_snapshot.next_file_id = static_cast<idx_t>(chunk->GetValue(7, 0).GetValue<uint64_t>());
}

void DuckLakeServerSideCommit::ReadColumnTypes() {
	// Load types for every column of every table touched in this commit.
	auto query = StringUtil::Format("SELECT col.table_id, col.column_id, col.column_type "
	                                "FROM %s.ducklake_column col "
	                                "WHERE col.end_snapshot IS NULL "
	                                "AND col.table_id IN (SELECT table_id FROM %s UNION SELECT table_id FROM %s)",
	                                schema_id, Staged(DuckLakeStagedTableType::DATA_FILE),
	                                Staged(DuckLakeStagedTableType::INLINED_DATA));
	auto result = RunQuery(query, "read column types");
	for (auto &row : *result) {
		column_types.emplace(ColumnKey {TableIndex(AsIdx(row, 0)), FieldIndex(AsIdx(row, 1))},
		                     ResolveType(row.GetValue<string>(2)));
	}
}

void DuckLakeServerSideCommit::ReadStagedDataFiles() {
	// Per-file column stats keyed by local file id.
	map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> per_file_stats;
	{
		auto stats_result = RunQuery(Select("data_file_id, table_id, column_id, column_size_bytes, value_count, "
		                                    "null_count, min_value, max_value, contains_nan, extra_stats",
		                                    DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS),
		                             "read staged column stats");
		for (auto &row : *stats_result) {
			DataFileIndex local_file_id(AsIdx(row, 0));
			ColumnKey key {TableIndex(AsIdx(row, 1)), FieldIndex(AsIdx(row, 2))};
			auto type_it = column_types.find(key);
			if (type_it == column_types.end()) {
				continue;
			}
			per_file_stats[local_file_id].emplace(key.second, StatsFromRow(type_it->second, row.GetBaseValue(3),
			                                                               row.GetBaseValue(4), row.GetBaseValue(5),
			                                                               row.GetBaseValue(6), row.GetBaseValue(7),
			                                                               row.GetBaseValue(8), row.GetBaseValue(9)));
		}
	}

	// Partition values per local file id.
	map<idx_t, vector<DuckLakeFilePartition>> partition_values_by_file;
	{
		auto part_result = RunQuery(Select("local_file_id, partition_column_idx, partition_value",
		                                   DuckLakeStagedTableType::DATA_FILE_PARTITION,
		                                   "ORDER BY local_file_id, partition_column_idx"),
		                            "read staged partition values");
		for (auto &row : *part_result) {
			DuckLakeFilePartition entry;
			entry.partition_column_idx = AsIdx(row, 1);
			entry.partition_value = row.IsNull(2) ? Value() : Value(row.GetValue<string>(2));
			partition_values_by_file[AsIdx(row, 0)].push_back(std::move(entry));
		}
	}

	// Delete files attached to transaction-local data files.
	map<idx_t, vector<DuckLakeDeleteFile>> attached_deletes;
	{
		auto attached_result =
		    RunQuery(Select("attached_local_file_id, file_name, format, delete_count, file_size_bytes, "
		                    "footer_size, encryption_key, begin_snapshot, max_snapshot, source",
		                    DuckLakeStagedTableType::DELETE_FILE, "WHERE attached_local_file_id IS NOT NULL"),
		             "read attached delete files");
		for (auto &row : *attached_result) {
			DuckLakeDeleteFile f;
			FillDeleteFileCommon(f, row, /*base=*/1);
			attached_deletes[AsIdx(row, 0)].push_back(std::move(f));
		}
	}

	// Files themselves; attach stats/deletes/partitions; route compaction outputs aside.
	map<TableIndex, vector<DuckLakeDataFile>> files_per_table;
	auto files_result = RunQuery(Select("data_file_id, table_id, path, record_count, file_size_bytes, footer_size, "
	                                    "row_id_start, partition_id, encryption_key, mapping_id, partial_max, "
	                                    "begin_snapshot, compaction_id",
	                                    DuckLakeStagedTableType::DATA_FILE, "ORDER BY table_id, file_order"),
	                             "read staged files");
	for (auto &row : *files_result) {
		DataFileIndex local_file_id(AsIdx(row, 0));
		DuckLakeDataFile f;
		f.file_name = row.GetValue<string>(2);
		f.row_count = AsIdx(row, 3);
		f.file_size_bytes = AsIdx(row, 4);
		if (!row.IsNull(5)) {
			f.footer_size = AsIdx(row, 5);
		}
		if (!row.IsNull(6)) {
			f.flush_row_id_start = AsIdx(row, 6);
		}
		if (!row.IsNull(7)) {
			f.partition_id = AsIdx(row, 7);
		}
		ReadEncryptionKey(row, 8, f.encryption_key);
		if (!row.IsNull(9)) {
			f.mapping_id = MappingIndex(static_cast<idx_t>(row.GetValue<uint64_t>(9)));
		}
		if (!row.IsNull(10)) {
			f.max_partial_file_snapshot = AsIdx(row, 10);
		}
		if (!row.IsNull(11)) {
			f.begin_snapshot = AsIdx(row, 11);
		}
		auto stats_it = per_file_stats.find(local_file_id);
		if (stats_it != per_file_stats.end()) {
			f.column_stats = std::move(stats_it->second);
		}
		auto attached_it = attached_deletes.find(local_file_id.index);
		if (attached_it != attached_deletes.end()) {
			f.delete_files = std::move(attached_it->second);
		}
		auto part_it = partition_values_by_file.find(local_file_id.index);
		if (part_it != partition_values_by_file.end()) {
			f.partition_values = std::move(part_it->second);
		}
		// Compaction-output file: routed by ReadStagedCompactions instead.
		if (!row.IsNull(12)) {
			compaction_output_files.emplace(AsIdx(row, 12), std::move(f));
			continue;
		}
		files_per_table[TableIndex(AsIdx(row, 1))].push_back(std::move(f));
	}

	for (auto &entry : files_per_table) {
		state->local_changes.AppendFiles(entry.first, std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedData() {
	struct PerTable {
		bool has_preserved = false;
	};
	map<TableIndex, PerTable> per_table;

	auto meta_result = RunQuery(Select("table_id, has_preserved_row_ids", DuckLakeStagedTableType::INLINED_DATA),
	                            "read staged inlined data meta");
	for (auto &row : *meta_result) {
		per_table[TableIndex(AsIdx(row, 0))].has_preserved = row.GetValue<bool>(1);
	}
	if (per_table.empty()) {
		return;
	}

	// value_literals holds the SQL tuple text produced at staging time.
	auto rows_result = RunQuery(Select("table_id, preserved_row_id, value_literals",
	                                   DuckLakeStagedTableType::INLINED_ROW, "ORDER BY table_id, row_order"),
	                            "read staged inlined rows");
	for (auto &row : *rows_result) {
		TableIndex table_id(AsIdx(row, 0));
		auto pt_it = per_table.find(table_id);
		if (pt_it == per_table.end()) {
			continue;
		}
		staged_inlined_tuples[table_id].push_back(row.GetValue<string>(2));
		if (pt_it->second.has_preserved) {
			// NULL preserved_row_id = INSERTed row (placeholder); real id = UPDATEd row.
			int64_t rid = row.IsNull(1) ? static_cast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START)
			                            : row.GetValue<int64_t>(1);
			staged_inlined_row_ids[table_id].push_back(rid);
		}
	}

	auto stats_result =
	    RunQuery(Select("table_id, column_id, column_size_bytes, has_num_values, num_values, has_null_count, "
	                    "null_count, has_min, min_value, has_max, max_value, has_contains_nan, contains_nan, "
	                    "any_valid, extra_stats",
	                    DuckLakeStagedTableType::INLINED_COLUMN_STATS),
	             "read staged inlined column stats");
	map<TableIndex, map<FieldIndex, DuckLakeColumnStats>> stats_per_table;
	for (auto &row : *stats_result) {
		TableIndex table_id(AsIdx(row, 0));
		FieldIndex column_id(AsIdx(row, 1));
		auto type_it = column_types.find({table_id, column_id});
		if (type_it == column_types.end()) {
			continue;
		}
		stats_per_table[table_id].emplace(column_id, ReadInlinedStatsRow(row, type_it->second));
	}

	// Build a DuckLakeInlinedData per table; data is null because tuples are spliced as SQL text.
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
	auto result = RunQuery(Select("table_id, inlined_table_name, deleted_row_id",
	                              DuckLakeStagedTableType::INLINED_DELETE, "ORDER BY table_id, inlined_table_name"),
	                       "read staged inlined deletes");

	using Key = std::pair<idx_t, string>; // table_id, inlined_table_name
	map<Key, set<idx_t>> grouped;
	for (auto &row : *result) {
		grouped[{AsIdx(row, 0), row.GetValue<string>(1)}].insert(AsIdx(row, 2));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedDeletes(TableIndex(entry.first.first), entry.first.second,
		                                          std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedFileDeletes() {
	auto result = RunQuery(Select("table_id, file_id, deleted_row_id", DuckLakeStagedTableType::INLINED_FILE_DELETE,
	                              "ORDER BY table_id, file_id"),
	                       "read staged inlined file deletes");

	using Key = std::pair<idx_t, idx_t>; // table_id, file_id
	map<Key, set<idx_t>> grouped;
	for (auto &row : *result) {
		grouped[{AsIdx(row, 0), AsIdx(row, 1)}].insert(AsIdx(row, 2));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedFileDeletes(TableIndex(entry.first.first), entry.first.second,
		                                              std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedDeleteFiles() {
	// Loose deletes only — attached deletes are handled in ReadStagedDataFiles.
	auto result =
	    RunQuery(Select("table_id, data_file_path, data_file_id, file_name, format, delete_count, file_size_bytes, "
	                    "footer_size, encryption_key, begin_snapshot, max_snapshot, source, "
	                    "overwrites_existing_delete, overwrite_delete_file_id, overwrite_delete_file_path",
	                    DuckLakeStagedTableType::DELETE_FILE,
	                    "WHERE attached_local_file_id IS NULL ORDER BY table_id, data_file_path"),
	             "read staged delete files");

	using Key = std::pair<idx_t, string>; // table_id, data_file_path
	map<Key, vector<DuckLakeDeleteFile>> grouped;
	for (auto &row : *result) {
		Key key {AsIdx(row, 0), row.GetValue<string>(1)};
		DuckLakeDeleteFile f;
		f.data_file_id = DataFileIndex(AsIdx(row, 2));
		f.data_file_path = key.second;
		FillDeleteFileCommon(f, row, /*base=*/3);
		f.overwrites_existing_delete = OptBoolFalse(row, 12);
		if (!row.IsNull(13)) {
			f.overwritten_delete_file.delete_file_id = DataFileIndex(AsIdx(row, 13));
		}
		if (!row.IsNull(14)) {
			f.overwritten_delete_file.path = row.GetValue<string>(14);
		}
		grouped[key].push_back(std::move(f));
	}
	for (auto &entry : grouped) {
		state->local_changes.AppendDeleteFiles(TableIndex(entry.first.first), entry.first.second,
		                                       std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedDroppedFiles() {
	auto dropped =
	    RunQuery(Select("path, data_file_id", DuckLakeStagedTableType::DROPPED_FILE), "read staged dropped files");
	for (auto &row : *dropped) {
		state->dropped_files.emplace(row.GetValue<string>(0), DataFileIndex(AsIdx(row, 1)));
	}
	auto tables =
	    RunQuery(Select("table_id", DuckLakeStagedTableType::TABLES_DELETED_FROM), "read staged tables_deleted_from");
	for (auto &row : *tables) {
		state->tables_deleted_from.insert(TableIndex(AsIdx(row, 0)));
	}
}

void DuckLakeServerSideCommit::ReadStagedFlushedInlinedTables() {
	auto result = RunQuery(
	    Select("inlined_table_name, schema_version, flush_snapshot_id", DuckLakeStagedTableType::FLUSHED_INLINED),
	    "read staged flushed inlined tables");
	for (auto &row : *result) {
		FlushedInlinedTableInfo entry;
		entry.inlined_table.table_name = row.GetValue<string>(0);
		entry.inlined_table.schema_version = AsIdx(row, 1);
		entry.flush_snapshot_id = AsIdx(row, 2);
		state->flushed_inlined_tables.push_back(std::move(entry));
	}
}

void DuckLakeServerSideCommit::ReadStagedCompactions() {
	struct CompactionShell {
		TableIndex table_id;
		CompactionType type;
		optional_idx row_id_start;
		optional_idx output_local_file_id;
	};
	map<idx_t, CompactionShell> shells;
	auto header_result = RunQuery(Select("compaction_id, table_id, compaction_type, row_id_start, output_local_file_id",
	                                     DuckLakeStagedTableType::COMPACTION),
	                              "read staged compactions");
	for (auto &row : *header_result) {
		CompactionShell shell;
		shell.table_id = TableIndex(AsIdx(row, 1));
		shell.type = CompactionTypeFromString(row.GetValue<string>(2));
		shell.row_id_start = OptIdx(row, 3);
		shell.output_local_file_id = OptIdx(row, 4);
		shells.emplace(AsIdx(row, 0), std::move(shell));
	}

	map<idx_t, vector<DuckLakeCompactionFileEntry>> sources_by_compaction;
	auto sources_result =
	    RunQuery(Select("compaction_id, source_data_file_id, source_path, source_row_count, source_begin_snapshot, "
	                    "source_max_partial_file_snapshot, source_inlined_delete_count, last_delete_file_id, "
	                    "last_delete_path, last_delete_row_count, last_delete_end_snapshot",
	                    DuckLakeStagedTableType::COMPACTION_SOURCE, "ORDER BY compaction_id, source_order"),
	             "read staged compaction sources");
	for (auto &row : *sources_result) {
		DuckLakeCompactionFileEntry entry;
		entry.file.id = DataFileIndex(AsIdx(row, 1));
		entry.file.data.path = row.GetValue<string>(2);
		entry.file.row_count = AsIdx(row, 3);
		entry.file.begin_snapshot = AsIdx(row, 4);
		if (!row.IsNull(5)) {
			entry.max_partial_file_snapshot = AsIdx(row, 5);
		}
		// inlined_file_deletions stored as a count; fill with placeholders so size() is correct.
		auto inlined_count = AsIdx(row, 6);
		for (idx_t i = 0; i < inlined_count; i++) {
			entry.inlined_file_deletions.insert(i);
		}
		entry.has_inlined_deletions = inlined_count > 0;
		if (!row.IsNull(7)) {
			DuckLakeCompactionDeleteFileData del;
			del.delete_file_id = DataFileIndex(AsIdx(row, 7));
			if (!row.IsNull(8)) {
				del.data.path = row.GetValue<string>(8);
			}
			del.row_count = AsIdx(row, 9);
			if (!row.IsNull(10)) {
				del.end_snapshot = AsIdx(row, 10);
			}
			entry.delete_files.push_back(std::move(del));
		}
		sources_by_compaction[AsIdx(row, 0)].push_back(std::move(entry));
	}

	for (auto &kv : shells) {
		auto &shell = kv.second;
		DuckLakeCompactionEntry entry;
		entry.type = shell.type;
		entry.row_id_start = shell.row_id_start;
		if (shell.output_local_file_id.IsValid()) {
			auto it = compaction_output_files.find(shell.output_local_file_id.GetIndex());
			if (it != compaction_output_files.end()) {
				entry.written_file = std::move(it->second);
			}
		}
		auto src_it = sources_by_compaction.find(kv.first);
		if (src_it != sources_by_compaction.end()) {
			entry.source_files = std::move(src_it->second);
		}
		state->local_changes.AddCompaction(shell.table_id, std::move(entry));
	}
}

void DuckLakeServerSideCommit::ReadStagedNameMaps() {
	// Header (id, table_id) plus a flattened entry tree.
	struct EntryShell {
		idx_t entry_id;
		optional_idx parent_entry_id;
		string source_name;
		FieldIndex target_field_id;
		bool hive_partition;
	};
	map<idx_t, vector<EntryShell>> entries_by_map;
	{
		auto result = RunQuery(Select("map_id, entry_id, parent_entry_id, source_name, target_field_id, hive_partition",
		                              DuckLakeStagedTableType::NAME_MAP_ENTRY, "ORDER BY map_id, entry_id"),
		                       "read staged name map entries");
		for (auto &row : *result) {
			EntryShell shell;
			shell.entry_id = AsIdx(row, 1);
			shell.parent_entry_id = OptIdx(row, 2);
			shell.source_name = row.GetValue<string>(3);
			shell.target_field_id = FieldIndex(AsIdx(row, 4));
			shell.hive_partition = OptBoolFalse(row, 5);
			entries_by_map[static_cast<idx_t>(row.GetValue<uint64_t>(0))].push_back(std::move(shell));
		}
	}

	auto header_result =
	    RunQuery(Select("map_id, table_id", DuckLakeStagedTableType::NAME_MAP), "read staged name maps");
	for (auto &row : *header_result) {
		auto name_map = make_uniq<DuckLakeNameMap>();
		name_map->id = MappingIndex(static_cast<idx_t>(row.GetValue<uint64_t>(0)));
		name_map->table_id = TableIndex(AsIdx(row, 1));

		auto entries_it = entries_by_map.find(name_map->id.index);
		if (entries_it != entries_by_map.end()) {
			std::map<idx_t, const EntryShell *> shell_by_id;
			std::map<idx_t, vector<idx_t>> children_of;
			vector<idx_t> root_ids;
			for (auto &shell : entries_it->second) {
				shell_by_id[shell.entry_id] = &shell;
				if (shell.parent_entry_id.IsValid()) {
					children_of[shell.parent_entry_id.GetIndex()].push_back(shell.entry_id);
				} else {
					root_ids.push_back(shell.entry_id);
				}
			}
			std::function<unique_ptr<DuckLakeNameMapEntry>(idx_t)> build_entry = [&](idx_t id) {
				auto &shell = *shell_by_id.at(id);
				auto entry = make_uniq<DuckLakeNameMapEntry>();
				entry->source_name = shell.source_name;
				entry->target_field_id = shell.target_field_id;
				entry->hive_partition = shell.hive_partition;
				auto child_it = children_of.find(id);
				if (child_it != children_of.end()) {
					for (auto child_id : child_it->second) {
						entry->child_entries.push_back(build_entry(child_id));
					}
				}
				return entry;
			};
			for (auto root_id : root_ids) {
				name_map->column_maps.push_back(build_entry(root_id));
			}
		}
		new_name_maps.Add(std::move(name_map));
	}
}

void DuckLakeServerSideCommit::ReadExistingTableStats() {
	// Shared SQL with DuckLakeMetadataManager::GetGlobalTableStats.
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
	// Mirrors DuckLakeCatalog::ConstructStatsMap with server-side type resolution.
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

const string &DuckLakeServerSideCommit::ResolveInlinedTableName(TableIndex table_id) {
	auto it = inlined_table_name_cache.find(table_id.index);
	if (it != inlined_table_name_cache.end()) {
		return it->second;
	}
	auto lookup = StringUtil::Replace(DuckLakeMetadataManager::LatestInlinedTableQuery(table_id.index),
	                                  "{METADATA_CATALOG}", schema_id) +
	              ";";
	auto result = RunQuery(lookup, "lookup inlined table name");
	string name;
	for (auto &row : *result) {
		name = row.GetValue<string>(0);
	}
	return inlined_table_name_cache.emplace(table_id.index, std::move(name)).first->second;
}

string DuckLakeServerSideCommit::BuildInlinedDataInserts(const vector<DuckLakeInlinedDataInfo> &new_data) {
	// Strip the outer parens off staged tuples, then reuse the shared metadata-manager formatter.
	string batch;
	for (auto &entry : new_data) {
		auto tuples_it = staged_inlined_tuples.find(entry.table_id);
		if (tuples_it == staged_inlined_tuples.end() || tuples_it->second.empty()) {
			continue;
		}
		auto &tuples = tuples_it->second;
		auto row_ids_it = staged_inlined_row_ids.find(entry.table_id);
		const bool has_preserved = row_ids_it != staged_inlined_row_ids.end();

		const string &inlined_table_name = ResolveInlinedTableName(entry.table_id);
		if (inlined_table_name.empty()) {
			// No inlined table yet — only reachable via gated-out CREATE TABLE.
			continue;
		}

		vector<string> cells_per_row;
		cells_per_row.reserve(tuples.size());
		for (auto &tuple : tuples) {
			cells_per_row.push_back(tuple.size() >= 2 ? tuple.substr(1, tuple.size() - 2) : tuple);
		}
		batch += DuckLakeMetadataManager::FormatInlinedDataInsert(inlined_table_name, entry.row_id_start, has_preserved,
		                                                          has_preserved ? &row_ids_it->second : nullptr,
		                                                          cells_per_row);
	}
	return batch;
}

DuckLakeCommitContext DuckLakeServerSideCommit::BuildContext(idx_t &committed_snapshot_id,
                                                             idx_t &committed_schema_version) {
	DuckLakeCommitContext ctx;
	ctx.commit_info = state->commit_info;
	// Skip post-commit DropEmptySupersededInlinedTables: the client catalog cache still references them.
	ctx.skip_drop_empty_inlined = true;
	ctx.conflict_query_executor = [this](string q) -> unique_ptr<QueryResult> {
		auto sql = SubstitutePlaceholders(std::move(q), transaction_snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(sql));
	};
	// Return the transaction's snapshot (cached at BEGIN), NOT the latest.
	// Returning latest would let concurrent commits each pick distinct new snapshot_ids,
	// bypassing the PK collision that drives the retry loop.
	ctx.get_snapshot = [this]() {
		return transaction_snapshot;
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
		// Snapshot-scoped placeholders are absent on this path; empty snapshot is a no-op.
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
	// write_inlined_tables is unreachable while DDL is gated out of server-side commit.
	ctx.write_inlined_tables = [](DuckLakeSnapshot, const vector<DuckLakeTableInfo> &) {
		return string();
	};
	ctx.write_inlined_data = [this](DuckLakeSnapshot &, const vector<DuckLakeInlinedDataInfo> &new_data,
	                                const vector<DuckLakeTableInfo> &, const vector<DuckLakeTableInfo> &) -> string {
		return BuildInlinedDataInserts(new_data);
	};
	ctx.get_table_stats = [this](TableIndex table_id) -> shared_ptr<DuckLakeTableStats> {
		auto it = existing_table_stats.find(table_id);
		return it == existing_table_stats.end() ? nullptr : it->second;
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
	// State emits {METADATA_CATALOG_NAME_LITERAL} for duckdb_tables() joins; resolve at exec time.
	sql = StringUtil::Replace(sql, "{METADATA_CATALOG_NAME_LITERAL}", "(SELECT current_database())");
	sql = StringUtil::Replace(sql, "{METADATA_SCHEMA_NAME_LITERAL}",
	                          DuckLakeUtil::SQLLiteralToString(metadata_schema_name));
	sql = StringUtil::Replace(sql, "{SNAPSHOT_ID}", std::to_string(snapshot.snapshot_id));
	sql = StringUtil::Replace(sql, "{SCHEMA_VERSION}", std::to_string(snapshot.schema_version));
	sql = StringUtil::Replace(sql, "{NEXT_CATALOG_ID}", std::to_string(snapshot.next_catalog_id));
	sql = StringUtil::Replace(sql, "{NEXT_FILE_ID}", std::to_string(snapshot.next_file_id));
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
