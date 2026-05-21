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
	std::function<unique_ptr<QueryResult>(string)> conflict_query_executor;
	std::function<DuckLakeSnapshot()> get_snapshot;
	std::function<bool()> take_pending_cache_clear;
	std::function<void()> clear_cache;
	std::function<void()> commit_connection;
	DuckLakeSnapshotCommit commit_info;
};

//! Holds the per-transaction mutable change state (new/dropped/renamed catalog entries, local file
//! changes, flushed inlined tables) and owns the Commit() loop that flushes them into the metadata
//! database. Owned by DuckLakeTransaction via a back-reference.
class DuckLakeTransactionState {
public:
	explicit DuckLakeTransactionState(DuckLakeTransaction &transaction);
	~DuckLakeTransactionState();

	void Commit(DuckLakeSnapshot transaction_snapshot, const TransactionChangeInformation &transaction_changes,
	            const DuckLakeRetryConfig &retry_config, const DuckLakeCommitContext &context);

	SnapshotAndStats CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
	                                   const TransactionChangeInformation &changes,
	                                   const std::function<unique_ptr<QueryResult>(string)> &executor);
	void CheckForConflicts(const TransactionChangeInformation &changes, const SnapshotChangeInformation &other_changes,
	                       DuckLakeSnapshot transaction_snapshot) const;

	string WriteSnapshotChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &changes,
	                            const DuckLakeSnapshotCommit &commit_info) const;

	bool SchemaChangesMade() const;

public:
	DuckLakeTransaction &transaction;

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
