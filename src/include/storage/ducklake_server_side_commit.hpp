#pragma once

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_transaction_changes.hpp"
#include "storage/ducklake_transaction_state.hpp"

#include <map>
#include <vector>

namespace duckdb {
class ClientContext;

struct DuckLakeServerSideCommitResult {
	int64_t committed_snapshot_id = 0;
	int64_t committed_schema_version = 0;
};

//! Executes a DuckLake commit on the server side.
//!
//! Reads staged commit data from `{METADATA_CATALOG}.ducklake_staged_*_<suffix>`, hydrates a
//! `DuckLakeTransactionState` from it, builds a `DuckLakeCommitContext` whose closures are backed
//! by a fresh `Connection` on the same `DatabaseInstance`, and calls `DuckLakeTransactionState::Commit`
//! to drive the retry loop, conflict detection, snapshot allocation, and SQL emission.
class DuckLakeServerSideCommit {
public:
	DuckLakeServerSideCommit(ClientContext &context, string metadata_schema_name, string identifier_suffix,
	                         int64_t schema_version);

	DuckLakeServerSideCommitResult Run();

	//! Override the retry configuration. If not called, defaults from DuckLakeRetryConfig apply.
	void SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config);

private:
	using ColumnKey = std::pair<TableIndex, FieldIndex>;

	// Each hydration step populates a slice of `state` (or of the side inputs to Commit:
	// transaction_snapshot, transaction_changes, column_types, existing_table_stats).
	void ReadCommitHeader();
	void ReadColumnTypes();
	void ReadStagedDataFiles();
	void ReadStagedInlinedData();
	void ReadStagedInlinedDeletes();
	void ReadStagedInlinedFileDeletes();
	void ReadStagedDeleteFiles();
	void ReadStagedDroppedFiles();
	void ReadExistingTableStats();

	// Closure backends.
	DuckLakeSnapshot ReadLatestSnapshot();
	unique_ptr<DuckLakeStats> BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats);
	DuckLakeCommitContext BuildContext(idx_t &committed_snapshot_id, idx_t &committed_schema_version);

	string EmitDropStagingSql() const;
	string SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const;
	unique_ptr<MaterializedQueryResult> RunQuery(const string &query, const char *what);

private:
	ClientContext &context;
	const string metadata_schema_name;
	const string schema_id; //! SQL-identifier form of `metadata_schema_name`
	const string identifier_suffix;
	const int64_t schema_version;
	Connection fresh_conn;
	DuckLakeRetryConfig retry_config;

	// Hydration outputs — populated during Run(), consumed by state->Commit().
	DuckLakeNameMapSet new_name_maps;
	unique_ptr<DuckLakeTransactionState> state;
	DuckLakeSnapshot transaction_snapshot;
	TransactionChangeInformation transaction_changes;
	map<ColumnKey, LogicalType> column_types;
	map<TableIndex, shared_ptr<DuckLakeTableStats>> existing_table_stats;

	//! Per-table SQL literal tuples staged for inlined-data inserts. `write_inlined_data` reads
	//! these directly instead of walking a ColumnDataCollection — the client has already
	//! produced the per-cell SQL via DuckLakeUtil::ValueToSQL at staging time.
	map<TableIndex, vector<string>> staged_inlined_tuples;
	//! Parallel to `staged_inlined_tuples` — row_ids for update-inlining (transaction-local
	//! placeholder for INSERTed rows, real id for UPDATEd rows). Empty if !HasPreservedRowIds.
	map<TableIndex, vector<int64_t>> staged_inlined_row_ids;
	//! Cache of inlined_table_name lookups (keyed by table_id.index) so we hit the metadata DB
	//! once per table across the commit retry loop.
	map<idx_t, string> inlined_table_name_cache;
};

} // namespace duckdb
