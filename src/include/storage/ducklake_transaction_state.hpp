//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_transaction_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_transaction.hpp"

namespace duckdb {

struct DuckLakeCommitContext {
	//! Runs a metadata-DB query during conflict resolution.
	std::function<unique_ptr<QueryResult>(string)> conflict_query_executor;
	//! Returns the latest snapshot for the first commit attempt.
	std::function<DuckLakeSnapshot()> get_snapshot;
	//! Executes the batched snapshot/changes SQL against the metadata DB.
	std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string &)> execute_commit_batch;
	//! Clears the metadata manager cache if a clear was pending.
	std::function<void()> flush_cache_if_pending;
	//! Commits the underlying metadata connection.
	std::function<void()> commit_connection;
	//! Rolls back the metadata connection if a transaction is active.
	std::function<void()> try_rollback;
	//! Resets per-attempt state before a retry.
	std::function<void()> prepare_retry;
	//! Runs a metadata-DB query during post-commit cleanup.
	std::function<unique_ptr<QueryResult>(string)> query_metadata;
	//! Invalidates the cached schema in the catalog for a given schema version.
	std::function<void(idx_t)> invalidate_schema_cache;
	//! Publishes the new schema version onto the transaction.
	std::function<void(idx_t)> set_catalog_version;
	//! Records the committed snapshot id on the catalog.
	std::function<void(idx_t)> set_committed_snapshot_id;
	//! Author / message / extra info for the snapshot row.
	DuckLakeSnapshotCommit commit_info;
};

//! Holds the per-transaction mutable change state (new/dropped/renamed catalog entries, local file
//! changes, flushed inlined tables) and owns the Commit() loop that flushes them into the metadata
//! database. Owned by DuckLakeTransaction via a back-reference.
class DuckLakeTransactionState {
public:
	DuckLakeTransactionState(DuckLakeTransaction &transaction, DatabaseInstance &db);
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
	                     optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats);

	static void DropEmptySupersededInlinedTables(const DuckLakeCommitContext &context);

	void CleanupFiles();

	bool SchemaChangesMade() const;

public:
	DuckLakeTransaction &transaction;
	DatabaseInstance &db;

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
