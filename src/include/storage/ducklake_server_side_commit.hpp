#pragma once

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_staged_commit.hpp"
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
	bool had_flushes = false;
};

//! Executes a DuckLake commit server-side from staged tables.
class DuckLakeServerSideCommit {
public:
	DuckLakeServerSideCommit(ClientContext &context, string metadata_schema_name, string identifier_suffix,
	                         int64_t schema_version);

	DuckLakeServerSideCommitResult Run();

	//! Override the retry configuration; otherwise defaults apply.
	void SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config);

private:
	using ColumnKey = std::pair<TableIndex, FieldIndex>;

	// Each step hydrates a slice of `state` or its sibling inputs.
	void ReadCommitHeader();
	void ReadColumnTypes();
	void ReadStagedDataFiles();
	void ReadStagedInlinedData();
	void ReadStagedInlinedDeletes();
	void ReadStagedInlinedFileDeletes();
	void ReadStagedDeleteFiles();
	void ReadStagedDroppedFiles();
	void ReadStagedFlushedInlinedTables();
	void ReadStagedCompactions();
	void ReadStagedNameMaps();
	void ReadExistingTableStats();

	// Closure backends.
	DuckLakeSnapshot ReadLatestSnapshot();
	unique_ptr<DuckLakeStats> BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats);
	DuckLakeCommitContext BuildContext(idx_t &committed_snapshot_id, idx_t &committed_schema_version);

	//! Build the INSERT batch returned by the write_inlined_data closure.
	string BuildInlinedDataInserts(const vector<DuckLakeInlinedDataInfo> &new_data);
	//! Resolve and cache the latest inlined-data table name for a table.
	const string &ResolveInlinedTableName(TableIndex table_id);

	string SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const;
	unique_ptr<MaterializedQueryResult> RunQuery(const string &query, const char *what);
	//! Fully-qualified name of a staging table for this commit.
	string Staged(DuckLakeStagedTableType kind) const;
	//! Convenience: `SELECT <columns> FROM <staged> <tail>`.
	string Select(const char *columns, DuckLakeStagedTableType kind, const char *tail = "") const;

private:
	ClientContext &context;
	const string metadata_schema_name;
	//! SQL-identifier form of `metadata_schema_name`.
	const string schema_id;
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

	//! Per-table SQL literal tuples for inlined-data inserts.
	map<TableIndex, vector<string>> staged_inlined_tuples;
	//! Parallel row_ids for update-inlining; empty if !HasPreservedRowIds.
	map<TableIndex, vector<int64_t>> staged_inlined_row_ids;
	//! Cache of inlined_table_name lookups across the commit retry loop.
	map<idx_t, string> inlined_table_name_cache;
	//! Compaction-output files indexed by compaction_id.
	map<idx_t, DuckLakeDataFile> compaction_output_files;
};

} // namespace duckdb
