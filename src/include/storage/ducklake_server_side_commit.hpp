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
	DuckLakeServerSideCommit(ClientContext &context, string metadata_schema_name, int64_t schema_version);

	//! Transform staged tables to Objects and commit.
	DuckLakeServerSideCommitResult Run();
	//! Override the retry configuration
	void SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config);

private:
	struct ColumnKey {
		TableIndex table_id;
		FieldIndex column_id;
		bool operator<(const ColumnKey &other) const {
			if (table_id != other.table_id) {
				return table_id < other.table_id;
			}
			return column_id < other.column_id;
		}
	};

	//! Read commit metadata (author, message, snapshot ids).
	void ReadCommitHeader();
	//! Read staged dropped file rows once and cache them.
	void ReadStagedDroppedFileEntries();
	//! Load column types for every table touched in this commit.
	void ReadColumnTypes();
	//! Read staged data files and their per-file column stats.
	void ReadStagedDataFiles();
	//! Read staged inlined data rows, row ids, and column stats.
	void ReadStagedInlinedData();
	//! Read staged inlined row deletes grouped by table.
	void ReadStagedInlinedDeletes();
	//! Read staged inlined file-level deletes grouped by table.
	void ReadStagedInlinedFileDeletes();
	//! Read staged delete files not attached to data files.
	void ReadStagedDeleteFiles();
	//! Read staged dropped file paths and tables deleted from.
	void ReadStagedDroppedFiles();
	//! Read staged flushed inlined table entries.
	void ReadStagedFlushedInlinedTables();
	//! Read staged compaction headers and their source files.
	void ReadStagedCompactions();
	//! Read staged name maps and rebuild entry trees.
	void ReadStagedNameMaps();
	//! Load current global table stats from the metadata catalog.
	void ReadExistingTableStats();

	//! Query the metadata catalog for the latest snapshot.
	DuckLakeSnapshot ReadLatestSnapshot();
	//! Whether the metadata schema has the row_group_count columns (DuckLake >= 1.1).
	bool ReadSupportsRowGroupCount();
	//! Build a DuckLakeTableStats from parsed global stats.
	unique_ptr<DuckLakeTableStats> BuildTableStats(const DuckLakeGlobalStatsInfo &gs);
	//! Build a full DuckLakeStats map from global stats.
	unique_ptr<DuckLakeStats> BuildStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats);
	//! Assemble the DuckLakeCommitContext with all closures.
	DuckLakeCommitContext BuildContext(idx_t &committed_snapshot_id, idx_t &committed_schema_version);
	//! Build INSERT SQL from staged inlined tuples.
	string BuildInlinedDataInserts(const vector<DuckLakeInlinedDataInfo> &new_data);
	//! Resolve and cache the latest inlined-data table name.
	const string &ResolveInlinedTableName(TableIndex table_id);
	//! All inlined-data table names registered for a table id (any schema version).
	vector<string> LookupInlinedTableNames(TableIndex table_id);
	//! Replace {METADATA_CATALOG}, {SNAPSHOT_ID}, etc. in SQL.
	string SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const;
	//! Execute a query on the fresh connection; throw on error.
	unique_ptr<MaterializedQueryResult> RunQuery(const string &query, const char *what);
	//! Scan a temporary staging table via the catalog API (no SQL, no lock).
	unique_ptr<MaterializedQueryResult> ScanStagedTable(DuckLakeStagedTableType kind);

private:
	ClientContext &context;
	const string metadata_schema_name;
	const string schema_id;
	const int64_t schema_version;
	Connection fresh_conn;
	DuckLakeRetryConfig retry_config;

	DuckLakeNameMapSet new_name_maps;
	unique_ptr<DuckLakeTransactionState> state;
	DuckLakeSnapshot transaction_snapshot;
	TransactionChangeInformation transaction_changes;
	map<ColumnKey, LogicalType> column_types;
	map<TableIndex, shared_ptr<DuckLakeTableStats>> existing_table_stats;
	bool staged_dropped_files_read = false;
	vector<pair<string, idx_t>> staged_dropped_files;
	map<TableIndex, DroppedDataFileStats> staged_dropped_file_stats;

	//! Per-table SQL literal tuples for inlined-data inserts.
	map<TableIndex, vector<string>> staged_inlined_tuples;
	//! Parallel row_ids for update-inlining; empty if !HasPreservedRowIds.
	map<TableIndex, vector<int64_t>> staged_inlined_row_ids;
	//! Cache of inlined_table_name lookups across the commit retry loop.
	map<idx_t, string> inlined_table_name_cache;
	//! Delete files attached to transaction-local data files.
	map<idx_t, vector<DuckLakeDeleteFile>> attached_deletes;
	//! Compaction-output files indexed by compaction_id, then ordered by file_order.
	map<idx_t, map<idx_t, DuckLakeDataFile>> compaction_output_files;
};

} // namespace duckdb
