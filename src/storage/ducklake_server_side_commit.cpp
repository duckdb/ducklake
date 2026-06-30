#include "storage/ducklake_server_side_commit.hpp"

#include "common/ducklake_row_helpers.hpp"
#include "common/ducklake_types.hpp"
#include "common/ducklake_util.hpp"
#include "common/ducklake_version.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/transaction/duck_transaction.hpp"

namespace duckdb {

struct TableFileKey {
	idx_t table_id;
	string file_path;
	bool operator<(const TableFileKey &other) const {
		if (table_id != other.table_id) {
			return table_id < other.table_id;
		}
		return file_path < other.file_path;
	}
};

struct TableFileIdKey {
	idx_t table_id;
	idx_t file_id;
	bool operator<(const TableFileIdKey &other) const {
		if (table_id != other.table_id) {
			return table_id < other.table_id;
		}
		return file_id < other.file_id;
	}
};

struct EntryShell {
	idx_t entry_id;
	optional_idx parent_entry_id;
	string source_name;
	FieldIndex target_field_id;
	bool hive_partition;
};

unique_ptr<DuckLakeNameMapEntry> BuildNameMapEntry(idx_t id, const std::map<idx_t, const EntryShell *> &shell_by_id,
                                                   const std::map<idx_t, vector<idx_t>> &children_of) {
	auto &shell = *shell_by_id.at(id);
	auto entry = make_uniq<DuckLakeNameMapEntry>();
	entry->source_name = shell.source_name;
	entry->target_field_id = shell.target_field_id;
	entry->hive_partition = shell.hive_partition;
	auto child_it = children_of.find(id);
	if (child_it != children_of.end()) {
		for (auto child_id : child_it->second) {
			entry->child_entries.push_back(BuildNameMapEntry(child_id, shell_by_id, children_of));
		}
	}
	return entry;
}

template <class ROW>
DuckLakeColumnStats ReadColumnStatsRow(ROW &row, idx_t base, const LogicalType &type) {
	DuckLakeColumnStats s(type);
	if (!row.IsNull(base + 0)) {
		s.column_size_bytes = AsIdx(row, base + 0);
	}
	s.has_num_values = OptBoolFalse(row, base + 1);
	if (s.has_num_values && !row.IsNull(base + 2)) {
		s.num_values = AsIdx(row, base + 2);
	}
	s.has_null_count = OptBoolFalse(row, base + 3);
	if (s.has_null_count && !row.IsNull(base + 4)) {
		s.null_count = AsIdx(row, base + 4);
	}
	if (OptBoolFalse(row, base + 5) && !row.IsNull(base + 6)) {
		s.has_min = true;
		s.min = row.template GetValue<string>(base + 6);
	}
	if (OptBoolFalse(row, base + 7) && !row.IsNull(base + 8)) {
		s.has_max = true;
		s.max = row.template GetValue<string>(base + 8);
	}
	s.has_contains_nan = OptBoolFalse(row, base + 9);
	if (s.has_contains_nan && !row.IsNull(base + 10)) {
		s.contains_nan = row.template GetValue<bool>(base + 10);
	}
	if (!row.IsNull(base + 11)) {
		s.any_valid = row.template GetValue<bool>(base + 11);
	}
	if (!row.IsNull(base + 12) && s.extra_stats) {
		s.extra_stats->Deserialize(row.template GetValue<string>(base + 12));
	}
	return s;
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

DuckLakeServerSideCommit::DuckLakeServerSideCommit(ClientContext &context_p, string metadata_schema_name_p,
                                                   int64_t schema_version_p)
    : context(context_p), metadata_schema_name(std::move(metadata_schema_name_p)),
      schema_id(DuckLakeUtil::SQLIdentifierToString(metadata_schema_name)), schema_version(schema_version_p),
      fresh_conn(*context_p.db) {
}

void DuckLakeServerSideCommit::SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config_p) {
	retry_config = retry_config_p;
}

DuckLakeServerSideCommitResult DuckLakeServerSideCommit::Run() {
	ReadCommitHeader();
	ReadColumnTypes();
	ReadStagedDeleteFiles();
	ReadStagedDataFiles();
	ReadStagedInlinedData();
	ReadStagedInlinedDeletes();
	ReadStagedInlinedFileDeletes();
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
	// Run the whole commit batch (snapshot + data/stats + changes) in one transaction
	fresh_conn.BeginTransaction();
	state->Commit(transaction_snapshot, transaction_changes, retry_config,
	              BuildContext(committed_snapshot_id, committed_schema_version));

	DuckLakeServerSideCommitResult result;
	result.committed_snapshot_id = static_cast<int64_t>(committed_snapshot_id);
	result.committed_schema_version = static_cast<int64_t>(committed_schema_version);
	result.had_flushes = !state->flushed_inlined_tables.empty();
	return result;
}

void DuckLakeServerSideCommit::ReadCommitHeader() {
	auto result = ScanStagedTable(DuckLakeStagedTableType::COMMIT_HEADER);
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

	if (chunk->GetValue(3, 0).IsNull()) {
		transaction_snapshot = ReadLatestSnapshot();
	} else {
		transaction_snapshot.snapshot_id = static_cast<idx_t>(chunk->GetValue(3, 0).GetValue<int64_t>());
		transaction_snapshot.next_catalog_id = chunk->GetValue(6, 0).GetValue<uint64_t>();
		transaction_snapshot.next_file_id = chunk->GetValue(7, 0).GetValue<uint64_t>();
		transaction_snapshot.schema_version = static_cast<idx_t>(schema_version);
	}
	if (schema_version < 0) {
		transaction_snapshot.schema_version =
		    transaction_snapshot.snapshot_id != DConstants::INVALID_INDEX ? transaction_snapshot.schema_version : 0;
	} else {
		transaction_snapshot.schema_version = static_cast<idx_t>(schema_version);
	}
}

void DuckLakeServerSideCommit::ReadColumnTypes() {
	unordered_set<idx_t> table_ids;
	auto data_files = ScanStagedTable(DuckLakeStagedTableType::DATA_FILE);
	for (auto &row : *data_files) {
		table_ids.insert(AsIdx(row, 1));
	}
	auto inlined = ScanStagedTable(DuckLakeStagedTableType::INLINED_DATA);
	for (auto &row : *inlined) {
		table_ids.insert(AsIdx(row, 0));
	}
	if (table_ids.empty()) {
		return;
	}
	string id_list;
	for (auto id : table_ids) {
		if (!id_list.empty()) {
			id_list += ",";
		}
		id_list += to_string(id);
	}
	auto query = StringUtil::Format("SELECT col.table_id, col.column_id, col.column_type "
	                                "FROM %s.ducklake_column col "
	                                "WHERE col.end_snapshot IS NULL "
	                                "AND col.table_id IN (%s)",
	                                schema_id, id_list);
	auto result = RunQuery(query, "read column types");
	for (auto &row : *result) {
		column_types.emplace(ColumnKey {TableIndex(AsIdx(row, 0)), FieldIndex(AsIdx(row, 1))},
		                     DuckLakeTypes::FromString(row.GetValue<string>(2)));
	}
}

void DuckLakeServerSideCommit::ReadStagedDataFiles() {
	// Per-file column stats keyed by local file id.
	map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> per_file_stats;
	{
		auto stats_result = ScanStagedTable(DuckLakeStagedTableType::DATA_FILE_COLUMN_STATS);
		for (auto &row : *stats_result) {
			DataFileIndex local_file_id(AsIdx(row, 0));
			ColumnKey key {TableIndex(AsIdx(row, 1)), FieldIndex(AsIdx(row, 2))};
			auto type_it = column_types.find(key);
			if (type_it == column_types.end()) {
				continue;
			}
			per_file_stats[local_file_id].emplace(key.column_id, ReadColumnStatsRow(row, 3, type_it->second));
		}
	}

	// Partition values per local file id.
	map<idx_t, vector<DuckLakeFilePartition>> partition_values_by_file;
	{
		auto part_result = ScanStagedTable(DuckLakeStagedTableType::DATA_FILE_PARTITION);
		for (auto &row : *part_result) {
			DuckLakeFilePartition entry;
			entry.partition_column_idx = AsIdx(row, 1);
			entry.partition_value = row.IsNull(2) ? Value() : Value(row.GetValue<string>(2));
			partition_values_by_file[AsIdx(row, 0)].push_back(std::move(entry));
		}
	}

	map<TableIndex, vector<DuckLakeDataFile>> files_per_table;
	auto files_result = ScanStagedTable(DuckLakeStagedTableType::DATA_FILE);
	for (auto &row : *files_result) {
		DataFileIndex local_file_id(AsIdx(row, 0));
		DuckLakeDataFile f;
		f.file_name = row.GetValue<string>(3);
		f.row_count = AsIdx(row, 6);
		f.file_size_bytes = AsIdx(row, 7);
		if (!row.IsNull(8)) {
			f.footer_size = AsIdx(row, 8);
		}
		if (!row.IsNull(9)) {
			f.flush_row_id_start = AsIdx(row, 9);
		}
		if (!row.IsNull(10)) {
			f.partition_id = AsIdx(row, 10);
		}
		ReadEncryptionKey(row, 11, f.encryption_key);
		if (!row.IsNull(12)) {
			f.mapping_id = MappingIndex(row.GetValue<uint64_t>(12));
		}
		if (!row.IsNull(13)) {
			f.max_partial_file_snapshot = AsIdx(row, 13);
		}
		if (!row.IsNull(14)) {
			f.begin_snapshot = AsIdx(row, 14);
		}
		if (!row.IsNull(16)) {
			f.row_group_count = AsIdx(row, 16);
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
		if (!row.IsNull(15)) {
			compaction_output_files.emplace(AsIdx(row, 15), std::move(f));
			continue;
		}
		files_per_table[TableIndex(AsIdx(row, 1))].push_back(std::move(f));
	}

	for (auto &entry : files_per_table) {
		state->local_changes.AppendFiles(entry.first, std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedData() {
	map<TableIndex, bool> has_preserved_row_ids;

	auto meta_result = ScanStagedTable(DuckLakeStagedTableType::INLINED_DATA);
	for (auto &row : *meta_result) {
		has_preserved_row_ids[TableIndex(AsIdx(row, 0))] = row.GetValue<bool>(1);
	}
	if (has_preserved_row_ids.empty()) {
		return;
	}

	auto rows_result = ScanStagedTable(DuckLakeStagedTableType::INLINED_ROW);
	for (auto &row : *rows_result) {
		TableIndex table_id(AsIdx(row, 0));
		auto pt_it = has_preserved_row_ids.find(table_id);
		if (pt_it == has_preserved_row_ids.end()) {
			continue;
		}
		staged_inlined_tuples[table_id].push_back(row.GetValue<string>(3));
		if (pt_it->second) {
			int64_t rid = row.IsNull(2) ? static_cast<int64_t>(DuckLakeConstants::TRANSACTION_LOCAL_ROW_ID_START)
			                            : row.GetValue<int64_t>(2);
			staged_inlined_row_ids[table_id].push_back(rid);
		}
	}

	auto stats_result = ScanStagedTable(DuckLakeStagedTableType::INLINED_COLUMN_STATS);
	map<TableIndex, map<FieldIndex, DuckLakeColumnStats>> stats_per_table;
	for (auto &row : *stats_result) {
		TableIndex table_id(AsIdx(row, 0));
		FieldIndex column_id(AsIdx(row, 1));
		auto type_it = column_types.find({table_id, column_id});
		if (type_it == column_types.end()) {
			continue;
		}
		stats_per_table[table_id].emplace(column_id, ReadColumnStatsRow(row, 2, type_it->second));
	}

	// Build a DuckLakeInlinedData per table, data is null because tuples are spliced as SQL text.
	for (auto &entry : has_preserved_row_ids) {
		auto table_id = entry.first;
		auto tuples_it = staged_inlined_tuples.find(table_id);
		idx_t row_count = (tuples_it == staged_inlined_tuples.end()) ? 0 : tuples_it->second.size();

		auto inlined = make_uniq<DuckLakeInlinedData>();
		inlined->external_row_count = row_count;
		auto stats_it = stats_per_table.find(table_id);
		if (stats_it != stats_per_table.end()) {
			inlined->column_stats = std::move(stats_it->second);
		}
		if (entry.second) {
			auto row_ids_it = staged_inlined_row_ids.find(table_id);
			if (row_ids_it != staged_inlined_row_ids.end()) {
				inlined->row_ids = row_ids_it->second;
			}
		}
		state->local_changes.AppendInlinedData(context, table_id, std::move(inlined));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedDeletes() {
	auto result = ScanStagedTable(DuckLakeStagedTableType::INLINED_DELETE);

	map<TableFileKey, set<idx_t>> grouped;
	for (auto &row : *result) {
		grouped[{AsIdx(row, 0), row.GetValue<string>(1)}].insert(AsIdx(row, 2));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedDeletes(TableIndex(entry.first.table_id), entry.first.file_path,
		                                          std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedInlinedFileDeletes() {
	auto result = ScanStagedTable(DuckLakeStagedTableType::INLINED_FILE_DELETE);
	map<TableFileIdKey, set<idx_t>> grouped;
	for (auto &row : *result) {
		grouped[{AsIdx(row, 0), AsIdx(row, 1)}].insert(AsIdx(row, 2));
	}
	for (auto &entry : grouped) {
		state->local_changes.AddNewInlinedFileDeletes(TableIndex(entry.first.table_id), entry.first.file_id,
		                                              std::move(entry.second));
	}
}

void DuckLakeServerSideCommit::ReadStagedDeleteFiles() {
	auto result = ScanStagedTable(DuckLakeStagedTableType::DELETE_FILE);

	map<idx_t, vector<DuckLakeDeleteFile>> attached_deletes_map;
	map<TableFileKey, vector<DuckLakeDeleteFile>> grouped;
	for (auto &row : *result) {
		if (!row.IsNull(15)) {
			DuckLakeDeleteFile f;
			FillDeleteFileCommon(f, row, /*base=*/3);
			if (!row.IsNull(16)) {
				f.row_group_count = AsIdx(row, 16);
			}
			attached_deletes_map[AsIdx(row, 15)].push_back(std::move(f));
			continue;
		}
		TableFileKey key {AsIdx(row, 0), row.GetValue<string>(1)};
		DuckLakeDeleteFile f;
		f.data_file_id = DataFileIndex(AsIdx(row, 2));
		f.data_file_path = key.file_path;
		FillDeleteFileCommon(f, row, /*base=*/3);
		if (!row.IsNull(16)) {
			f.row_group_count = AsIdx(row, 16);
		}
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
		state->local_changes.AppendDeleteFiles(TableIndex(entry.first.table_id), entry.first.file_path,
		                                       std::move(entry.second));
	}
	for (auto &entry : attached_deletes_map) {
		attached_deletes[entry.first] = std::move(entry.second);
	}
}

void DuckLakeServerSideCommit::ReadStagedDroppedFiles() {
	auto dropped = ScanStagedTable(DuckLakeStagedTableType::DROPPED_FILE);
	for (auto &row : *dropped) {
		state->dropped_files.emplace(row.GetValue<string>(0), DataFileIndex(AsIdx(row, 1)));
	}
	auto tables = ScanStagedTable(DuckLakeStagedTableType::TABLES_DELETED_FROM);
	for (auto &row : *tables) {
		state->tables_deleted_from.insert(TableIndex(AsIdx(row, 0)));
	}
}

void DuckLakeServerSideCommit::ReadStagedFlushedInlinedTables() {
	auto result = ScanStagedTable(DuckLakeStagedTableType::FLUSHED_INLINED);
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
	auto header_result = ScanStagedTable(DuckLakeStagedTableType::COMPACTION);
	for (auto &row : *header_result) {
		CompactionShell shell;
		shell.table_id = TableIndex(AsIdx(row, 1));
		shell.type = CompactionTypeFromString(row.GetValue<string>(2));
		shell.row_id_start = OptIdx(row, 3);
		shell.output_local_file_id = OptIdx(row, 4);
		shells.emplace(AsIdx(row, 0), std::move(shell));
	}

	map<idx_t, vector<DuckLakeCompactionFileEntry>> sources_by_compaction;
	auto sources_result = ScanStagedTable(DuckLakeStagedTableType::COMPACTION_SOURCE);
	for (auto &row : *sources_result) {
		DuckLakeCompactionFileEntry entry;
		entry.file.id = DataFileIndex(AsIdx(row, 2));
		entry.file.data.path = row.GetValue<string>(3);
		entry.file.row_count = AsIdx(row, 4);
		entry.file.begin_snapshot = AsIdx(row, 5);
		if (!row.IsNull(6)) {
			entry.max_partial_file_snapshot = AsIdx(row, 6);
		}
		auto inlined_count = AsIdx(row, 7);
		for (idx_t i = 0; i < inlined_count; i++) {
			entry.inlined_file_deletions.insert(i);
		}
		entry.has_inlined_deletions = inlined_count > 0;
		if (!row.IsNull(8)) {
			DuckLakeCompactionDeleteFileData del;
			del.delete_file_id = DataFileIndex(AsIdx(row, 8));
			if (!row.IsNull(9)) {
				del.data.path = row.GetValue<string>(9);
			}
			del.row_count = AsIdx(row, 10);
			if (!row.IsNull(11)) {
				del.end_snapshot = AsIdx(row, 11);
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
	map<idx_t, vector<EntryShell>> entries_by_map;
	{
		auto result = ScanStagedTable(DuckLakeStagedTableType::NAME_MAP_ENTRY);
		for (auto &row : *result) {
			EntryShell shell;
			shell.entry_id = AsIdx(row, 1);
			shell.parent_entry_id = OptIdx(row, 2);
			shell.source_name = row.GetValue<string>(3);
			shell.target_field_id = FieldIndex(AsIdx(row, 4));
			shell.hive_partition = OptBoolFalse(row, 5);
			entries_by_map[row.GetValue<uint64_t>(0)].push_back(std::move(shell));
		}
	}

	auto header_result = ScanStagedTable(DuckLakeStagedTableType::NAME_MAP);
	for (auto &row : *header_result) {
		auto name_map = make_uniq<DuckLakeNameMap>();
		name_map->id = MappingIndex(row.GetValue<uint64_t>(0));
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
			for (auto root_id : root_ids) {
				name_map->column_maps.push_back(BuildNameMapEntry(root_id, shell_by_id, children_of));
			}
		}
		new_name_maps.Add(std::move(name_map));
	}
}

unique_ptr<DuckLakeTableStats> DuckLakeServerSideCommit::BuildTableStats(const DuckLakeGlobalStatsInfo &gs) {
	auto entry = make_uniq<DuckLakeTableStats>();
	entry->record_count = gs.record_count;
	entry->next_row_id = gs.next_row_id;
	entry->table_size_bytes = gs.table_size_bytes;
	for (auto &col : gs.column_stats) {
		auto type_it = column_types.find({gs.table_id, col.column_id});
		if (type_it == column_types.end()) {
			continue;
		}
		entry->column_stats.emplace(col.column_id, DuckLakeColumnStats::FromGlobalStats(type_it->second, col));
	}
	return entry;
}

void DuckLakeServerSideCommit::ReadExistingTableStats() {
	string sql = StringUtil::Replace(DuckLakeMetadataManager::GlobalTableStatsQuery(), "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read existing table stats");
	auto global_stats = DuckLakeMetadataManager::ParseGlobalTableStats(*result);

	for (auto &gs : global_stats) {
		existing_table_stats.emplace(gs.table_id, BuildTableStats(gs));
	}
}

bool DuckLakeServerSideCommit::ReadSupportsV1_1Metadata() {
	string sql = StringUtil::Replace("SELECT value FROM {METADATA_CATALOG}.ducklake_metadata WHERE key = 'version'",
	                                 "{METADATA_CATALOG}", schema_id);
	auto result = RunQuery(sql, "read catalog version");
	for (auto &row : *result) {
		return DuckLakeVersionFromString(row.GetValue<string>(0)) >= DuckLakeVersion::V1_1_DEV_1;
	}
	return false;
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

unique_ptr<DuckLakeStats> DuckLakeServerSideCommit::BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats) {
	auto result = make_uniq<DuckLakeStats>();
	for (auto &gs : global_stats) {
		result->table_stats.emplace(gs.table_id, BuildTableStats(gs));
	}
	return result;
}

vector<string> DuckLakeServerSideCommit::LookupInlinedTableNames(TableIndex table_id) {
	vector<string> names;
	auto sql = SubstitutePlaceholders(DuckLakeMetadataManager::GetInlinedTableNamesSql(table_id), transaction_snapshot);
	auto result = RunQuery(sql, "lookup inlined table names");
	for (auto &row : *result) {
		names.push_back(row.GetValue<string>(0));
	}
	return names;
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
			throw InternalException("Server-side ducklake_commit: no inlined-data table exists for table %llu - this "
			                        "commit should have taken the client-side path",
			                        entry.table_id.index);
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
	ctx.skip_drop_empty_inlined = true;
	ctx.supports_v1_1_metadata = ReadSupportsV1_1Metadata();
	ctx.conflict_query_executor = [this](string q) -> unique_ptr<QueryResult> {
		auto sql = SubstitutePlaceholders(std::move(q), transaction_snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(sql));
	};
	ctx.get_snapshot = [this]() {
		return transaction_snapshot;
	};
	ctx.execute_commit_batch = [this](DuckLakeSnapshot snapshot, string &query) -> unique_ptr<QueryResult> {
		query = SubstitutePlaceholders(query, snapshot);
		return unique_ptr_cast<MaterializedQueryResult, QueryResult>(fresh_conn.Query(query));
	};
	ctx.commit_connection = [this]() {
		fresh_conn.Commit();
	};
	ctx.try_rollback = [this]() {
		if (fresh_conn.context->transaction.HasActiveTransaction()) {
			fresh_conn.Rollback();
		}
	};
	ctx.prepare_retry = [this]() {
		fresh_conn.BeginTransaction();
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
	ctx.write_inlined_data = [this](DuckLakeSnapshot &, const vector<DuckLakeInlinedDataInfo> &new_data,
	                                const vector<DuckLakeTableInfo> &, const vector<DuckLakeTableInfo> &) -> string {
		return BuildInlinedDataInserts(new_data);
	};
	ctx.write_inlined_file_deletes = [](const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes) -> string {
		bool created_new_table = false;
		return DuckLakeMetadataManager::WriteNewInlinedFileDeletesSql(new_deletes, created_new_table);
	};
	ctx.get_table_stats = [this](TableIndex table_id) -> shared_ptr<DuckLakeTableStats> {
		auto it = existing_table_stats.find(table_id);
		return it == existing_table_stats.end() ? nullptr : it->second;
	};
	ctx.build_stats_map = [this](vector<DuckLakeGlobalStatsInfo> &stats) {
		return BuildStatsMap(stats);
	};
	ctx.get_table_column_schema = [this](TableIndex table_id) {
		vector<DuckLakeColumnSchemaEntry> schema;
		auto sql =
		    SubstitutePlaceholders(DuckLakeMetadataManager::GetTableColumnSchemaSql(table_id), transaction_snapshot);
		auto result = RunQuery(sql, "read table column schema");
		for (auto &row : *result) {
			// parent_column IS NULL => top-level root; otherwise a nested leaf carrying its own leaf type.
			bool is_root = row.IsNull(3);
			schema.push_back({FieldIndex(static_cast<idx_t>(row.GetValue<int64_t>(0))), row.GetValue<string>(1),
			                  DuckLakeTypes::FromString(row.GetValue<string>(2)), is_root});
		}
		return schema;
	};
	ctx.get_inlined_table_names = [this](TableIndex table_id) {
		return LookupInlinedTableNames(table_id);
	};
	ctx.get_net_data_file_row_count = [this](TableIndex table_id) -> idx_t {
		// The inlined-file-deletion table is deterministically named but created lazily.
		// Probe its existence via the catalog (an erroring probe would abort the transaction);
		// if absent, the SQL omits the inlined-deletion subterm.
		auto inlined_deletion_table = DuckLakeMetadataManager::InlinedFileDeletionTableName(table_id);
		auto probe_sql = SubstitutePlaceholders(
		    StringUtil::Format("SELECT 1 FROM duckdb_tables() WHERE database_name = current_database() AND "
		                       "schema_name = {METADATA_SCHEMA_NAME_LITERAL} AND table_name = %s",
		                       DuckLakeUtil::SQLLiteralToString(inlined_deletion_table)),
		    transaction_snapshot);
		auto probe = fresh_conn.Query(probe_sql);
		if (!probe || probe->HasError() || probe->RowCount() == 0) {
			inlined_deletion_table.clear();
		}
		auto sql = SubstitutePlaceholders(
		    DuckLakeMetadataManager::GetNetDataFileRowCountSql(table_id, inlined_deletion_table), transaction_snapshot);
		auto result = RunQuery(sql, "read net data file row count");
		for (auto &row : *result) {
			return row.GetValue<idx_t>(0);
		}
		return 0;
	};
	ctx.get_net_inlined_row_count = [this](TableIndex table_id) -> idx_t {
		idx_t total = 0;
		for (auto &name : LookupInlinedTableNames(table_id)) {
			auto sql =
			    SubstitutePlaceholders(DuckLakeMetadataManager::GetNetInlinedRowCountSql(name), transaction_snapshot);
			auto result = RunQuery(sql, "read net inlined row count");
			for (auto &row : *result) {
				total += row.GetValue<idx_t>(0);
				break;
			}
		}
		return total;
	};
	ctx.set_catalog_version = [&committed_schema_version](idx_t v) {
		committed_schema_version = v;
	};
	ctx.set_committed_snapshot_id = [&committed_snapshot_id](idx_t v) {
		committed_snapshot_id = v;
	};
	return ctx;
}

string DuckLakeServerSideCommit::SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const {
	sql = StringUtil::Replace(sql, "{METADATA_CATALOG}", schema_id);
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

unique_ptr<MaterializedQueryResult> DuckLakeServerSideCommit::ScanStagedTable(DuckLakeStagedTableType kind) {
	string table_name = DuckLakeStagedTable::BaseName(kind);
	auto &temp_catalog = Catalog::GetCatalog(context, TEMP_CATALOG);
	auto &table_entry = temp_catalog.GetEntry<TableCatalogEntry>(context, DEFAULT_SCHEMA, Identifier(table_name))
	                        .Cast<DuckTableEntry>();
	auto &storage = table_entry.GetStorage();

	auto types = storage.GetTypes();
	auto &columns = storage.Columns();
	vector<string> names;
	vector<StorageIndex> column_ids;
	for (idx_t i = 0; i < columns.size(); i++) {
		names.push_back(columns[i].Name().GetIdentifierName());
		column_ids.emplace_back(i);
	}

	auto &duck_transaction = DuckTransaction::Get(context, temp_catalog);
	TableScanState scan_state;
	storage.InitializeScan(context, duck_transaction, scan_state, column_ids);

	auto collection = make_uniq<ColumnDataCollection>(context, types);
	DataChunk chunk;
	chunk.Initialize(context, types);
	while (true) {
		chunk.Reset();
		storage.Scan(duck_transaction, chunk, scan_state);
		if (chunk.size() == 0) {
			break;
		}
		collection->Append(chunk);
	}
	StatementProperties properties;
	properties.return_type = StatementReturnType::QUERY_RESULT;
	return make_uniq<MaterializedQueryResult>(StatementType::SELECT_STATEMENT, properties, std::move(names),
	                                          std::move(collection), context.GetClientProperties());
}

} // namespace duckdb
