//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_transaction_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct DuckLakeColumnSchemaEntry {
	FieldIndex field_index;
	string column_name;
	LogicalType column_type;
	//! false for nested struct/list/map/array leaves. Only top-level roots are safe to feed to the inlined-data
	//! aggregate merge (which references the column by name); nested leaves still carry their own per-file stats.
	//! (No default initializer: this struct must stay a C++11 aggregate; both producers set it explicitly.)
	bool is_root;
};

struct DuckLakeCommitContext {
	//! Runs a metadata-DB query during conflict resolution.
	std::function<unique_ptr<QueryResult>(string)> conflict_query_executor;
	//! Returns the latest snapshot for the first commit attempt.
	std::function<DuckLakeSnapshot()> get_snapshot;
	//! Executes the batched snapshot/changes SQL against the metadata DB.
	std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string &)> execute_commit_batch;
	//! Optional hooks below default to a no-op/constant; callers override only the ones they need.
	//! Clears the metadata manager cache if a clear was pending.
	std::function<void()> flush_cache_if_pending = []() {
	};
	//! Commits the underlying metadata connection.
	std::function<void()> commit_connection = []() {
	};
	//! Rolls back the metadata connection if a transaction is active.
	std::function<void()> try_rollback = []() {
	};
	//! Resets per-attempt state before a retry.
	std::function<void()> prepare_retry = []() {
	};
	//! Runs a metadata-DB query during post-commit cleanup.
	std::function<unique_ptr<QueryResult>(string)> query_metadata;
	//! Runs a snapshot-templated metadata-DB query (handles {SNAPSHOT_ID} substitution).
	std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string)> query_metadata_with_snapshot;
	//! Optional Appender fast-path.
	std::function<bool(DuckLakeSnapshot &, const vector<DuckLakeFileInfo> &, const vector<DuckLakeTableInfo> &,
	                   vector<DuckLakeSchemaInfo> &)>
	    try_append_data_files = [](DuckLakeSnapshot &, const vector<DuckLakeFileInfo> &,
	                               const vector<DuckLakeTableInfo> &, vector<DuckLakeSchemaInfo> &) {
		    return false;
	    };
	//! Emits the SQL that registers new inlined data tables (CREATE TABLE + ducklake_inlined_data_tables INSERT).
	std::function<string(DuckLakeSnapshot, const vector<DuckLakeTableInfo> &)> write_inlined_tables =
	    [](DuckLakeSnapshot, const vector<DuckLakeTableInfo> &) {
		    return string();
	    };
	//! Emits the SQL that appends inlined data for the given table changes (handles new inlined-table creation,
	//! per-tx cache, and the per-row INSERT VALUES).
	std::function<string(DuckLakeSnapshot &, const vector<DuckLakeInlinedDataInfo> &, const vector<DuckLakeTableInfo> &,
	                     const vector<DuckLakeTableInfo> &)>
	    write_inlined_data;
	//! Returns the current global table stats for a single table id (first-attempt path).
	std::function<shared_ptr<DuckLakeTableStats>(TableIndex)> get_table_stats;
	//! Top-level columns of a table at the commit snapshot — needed by stats-refresh to iterate
	//! columns and look up types when merging per-file stats.
	std::function<vector<DuckLakeColumnSchemaEntry>(TableIndex)> get_table_column_schema =
	    [](TableIndex) {
		    return vector<DuckLakeColumnSchemaEntry>{};
	    };
	//! Names of the inlined-data tables associated with a table id at the commit snapshot.
	std::function<vector<string>(TableIndex)> get_inlined_table_names = [](TableIndex) {
		return vector<string>{};
	};
	//! Net (delete-adjusted) row count of a table's regular data files.
	std::function<idx_t(TableIndex)> get_net_data_file_row_count = [](TableIndex) {
		return 0;
	};
	//! Net (delete-adjusted) row count of a table's inlined-data tables.
	std::function<idx_t(TableIndex)> get_net_inlined_row_count = [](TableIndex) {
		return 0;
	};
	//! Builds a DuckLakeStats map from a vector of per-snapshot global stats (retry path).
	std::function<unique_ptr<DuckLakeStats>(vector<DuckLakeGlobalStatsInfo> &)> build_stats_map;
	//! Invalidates the cached schema in the catalog for a given schema version.
	std::function<void(idx_t)> invalidate_schema_cache = [](idx_t) {
	};
	//! Re-keys an in-memory table-scoped config option from the local id to the committed id
	//! (client-side only - the server-side commit path never handles new tables).
	std::function<void(const DuckLakeConfigOption &)> set_config_option = [](const DuckLakeConfigOption &) {
	};
	//! Publishes the new schema version onto the transaction.
	std::function<void(idx_t)> set_catalog_version;
	//! Records the committed snapshot id on the catalog.
	std::function<void(idx_t)> set_committed_snapshot_id;
	//! Author / message / extra info for the snapshot row.
	DuckLakeSnapshotCommit commit_info;
	//! When true, Commit() skips the post-commit DropEmptySupersededInlinedTables cleanup.
	bool skip_drop_empty_inlined = false;
};

//! Holds the per-transaction mutable change state (new/dropped/renamed catalog entries, local file
//! changes, flushed inlined tables) and owns the Commit() loop that flushes them into the metadata
//! database. Owned by DuckLakeTransaction via a back-reference.
class DuckLakeTransactionState {
public:
	DuckLakeTransactionState(DatabaseInstance &db, bool require_commit_message, DuckLakeNameMapSet &new_name_maps,
	                         string data_path, string separator);
	~DuckLakeTransactionState();

	void Commit(DuckLakeSnapshot transaction_snapshot, const TransactionChangeInformation &transaction_changes,
	            const DuckLakeRetryConfig &retry_config, const DuckLakeCommitContext &context);

	SnapshotAndStats CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
	                                   const TransactionChangeInformation &changes,
	                                   const std::function<unique_ptr<QueryResult>(string)> &executor);
	void CheckForConflicts(const TransactionChangeInformation &changes, const SnapshotChangeInformation &other_changes,
	                       DuckLakeSnapshot transaction_snapshot,
	                       const std::function<unique_ptr<QueryResult>(string)> &executor) const;

	static SnapshotDeletedFromFiles
	GetFilesDeletedOrDroppedAfterSnapshot(const std::function<unique_ptr<QueryResult>(string)> &executor);

	string WriteSnapshotChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &changes,
	                            const DuckLakeSnapshotCommit &commit_info) const;

	string CommitChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes,
	                     optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats, const DuckLakeCommitContext &context);

	vector<DuckLakeSchemaInfo> GetNewSchemas(DuckLakeCommitState &commit_state);
	NewTableInfo GetNewTables(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes);
	void GetNewTableInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
	                     reference<CatalogEntry> table_entry, NewTableInfo &result,
	                     TransactionChangeInformation &transaction_changes);
	void GetNewViewInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
	                    reference<CatalogEntry> view_entry, NewTableInfo &result,
	                    TransactionChangeInformation &transaction_changes);
	NewMacroInfo GetNewMacros(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes);
	NewDataInfo GetNewDataFiles(string &batch_query, DuckLakeCommitState &commit_state,
	                            optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats,
	                            const DuckLakeCommitContext &context);
	CompactionInformation GetCompactionChanges(DuckLakeCommitState &commit_state, CompactionType type);
	//! After a REWRITE_DELETES compaction, recompute EXACT global stats for `table_id` from the post-rewrite file set
	//! (+ committed inlined data) and append the UpdateGlobalTableStats SQL to `batch_query`. No-op (leaving the
	//! existing stale stats, and the scan fallback) if the table is not fully delete-free post-rewrite or the
	//! inlined data cannot be accounted for exactly.
	void RecomputeGlobalStatsAfterRewrite(string &batch_query, TableIndex table_id, DuckLakeSnapshot snapshot,
	                                      const CompactionInformation &rewrite_changes,
	                                      const set<DataFileIndex> &removed_source_ids,
	                                      const DuckLakeCommitContext &context);
	//! Merge committed inlined data's per-column min/max into `target` via typed SQL aggregates. Returns false if the
	//! inlined data cannot be accounted for exactly (e.g. a non-scalar column), in which case the caller must not
	//! claim the recomputed stats are exact.
	bool TryMergeInlinedStats(const vector<DuckLakeColumnSchemaEntry> &columns,
	                          const vector<string> &inlined_table_names, DuckLakeSnapshot snapshot,
	                          DuckLakeTableStats &target, const DuckLakeCommitContext &context);
	vector<DuckLakeDeleteFileInfo>
	GetNewDeleteFiles(const DuckLakeCommitState &commit_state,
	                  vector<DuckLakeOverwrittenDeleteFile> &overwritten_delete_files) const;
	vector<DuckLakeDeletedInlinedDataInfo> GetNewInlinedDeletes(DuckLakeCommitState &commit_state) const;
	vector<DuckLakeInlinedFileDeletionInfo> GetNewInlinedFileDeletes(DuckLakeCommitState &commit_state);
	NewNameMapInfo GetNewNameMaps(DuckLakeCommitState &commit_state);
	static DuckLakeFileInfo GetNewDataFile(const DuckLakeDataFile &file, DuckLakeCommitState &commit_state,
	                                       TableIndex table_id, optional_idx row_id_start);

	static void DropEmptySupersededInlinedTables(const DuckLakeCommitContext &context);

	void CleanupFiles();

	void EnsureCommitInfoProvided(const DuckLakeSnapshotCommit &commit_info) const;

	DuckLakePath GetRelativePath(const string &path) const;

	bool SchemaChangesMade() const;

public:
	DatabaseInstance &db;
	bool require_commit_message;
	DuckLakeNameMapSet &new_name_maps;
	string data_path;
	string separator;
	DuckLakeSnapshotCommit commit_info;

	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_tables;
	set<TableIndex> dropped_tables;

	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_scalar_macros;
	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_table_macros;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;

	set<TableIndex> renamed_tables;
	set<TableIndex> renamed_views;
	set<TableIndex> dropped_views;
	unordered_map<string, DataFileIndex> dropped_files;
	set<TableIndex> tables_deleted_from;
	unique_ptr<DuckLakeCatalogSet> new_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	LocalTableChanges local_changes;
	vector<FlushedInlinedTableInfo> flushed_inlined_tables;
};

} // namespace duckdb
