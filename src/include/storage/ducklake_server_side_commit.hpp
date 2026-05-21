#pragma once

#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/main/connection.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "storage/ducklake_stats.hpp"
#include "storage/ducklake_transaction.hpp"

#include <map>
#include <vector>

namespace duckdb {
class ClientContext;

struct DuckLakeServerSideCommitResult {
	int64_t committed_snapshot_id = 0;
	int64_t committed_schema_version = 0;
};

//! Executes a DuckLake data-only commit on the server side.
//!
//! Reads staged commit data from `{METADATA_CATALOG}.ducklake_staged_*_<suffix>`, performs the
//! type-aware stats merge in C++ (`DuckLakeColumnStats::MergeStats` via `DuckLakeTransaction::
//! BuildDataOnlyCommitData`), emits the commit SQL batch, executes it on a fresh `Connection` on
//! the same `DatabaseInstance`, drops staging, and returns the new committed snapshot.
class DuckLakeServerSideCommit {
public:
	DuckLakeServerSideCommit(ClientContext &context, string metadata_schema_name, string identifier_suffix,
	                         int64_t schema_version);

	DuckLakeServerSideCommitResult Run();

	//! Override the retry configuration. If not called, defaults from DuckLakeRetryConfig apply.
	void SetRetryConfigOverride(const DuckLakeRetryConfig &retry_config);

private:
	using ColumnKey = std::pair<TableIndex, FieldIndex>;

	struct CommitData {
		vector<DuckLakeFileInfo> new_files;
		map<TableIndex, DuckLakeNewGlobalStats> table_stats;
	};

	// Phase 1 — read staged data into C++ structures.
	map<ColumnKey, LogicalType> ReadColumnTypes();
	map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>>
	ReadStagedColumnStats(const map<ColumnKey, LogicalType> &column_types);
	map<TableIndex, vector<DuckLakeDataFile>>
	ReadStagedFiles(map<DataFileIndex, map<FieldIndex, DuckLakeColumnStats>> &per_file_column_stats);
	void ReadCommitHeader(SnapshotChangeInfo &change_info, DuckLakeSnapshotCommit &commit_info);

	// Phase 2 — read existing metadata state (shares the SQL templates and parsers in DuckLakeMetadataManager).
	map<TableIndex, DuckLakeTableStats> ReadExistingTableStats(const map<ColumnKey, LogicalType> &column_types);
	DuckLakeSnapshot ReadLatestSnapshot();

	// Phase 3 — merge file stats / allocate file IDs.
	CommitData BuildCommitData(const map<TableIndex, vector<DuckLakeDataFile>> &files_per_table,
	                           const map<TableIndex, DuckLakeTableStats> &existing_table_stats,
	                           DuckLakeSnapshot &commit_snapshot);

	// Phase 4 — SQL emission. EmitTableStatsSql / EmitSnapshotSql / EmitSnapshotChangesSql delegate
	// to DuckLakeMetadataManager statics (placeholder strings) and substitute {METADATA_CATALOG} /
	// {SNAPSHOT_ID} locally. EmitDataFilesSql stays inlined for now — the regular path's emitter has
	// schema-relative paths, appender, variant/partition stats that the server-side staging doesn't
	// yet feed in. EmitDropStagingSql is server-side-only.
	string EmitDataFilesSql(const vector<DuckLakeFileInfo> &files, idx_t commit_snapshot_id) const;
	string EmitTableStatsSql(const map<TableIndex, DuckLakeNewGlobalStats> &table_stats) const;
	string EmitSnapshotSql(const DuckLakeSnapshot &commit_snapshot) const;
	string EmitSnapshotChangesSql(const DuckLakeSnapshot &commit_snapshot, const SnapshotChangeInfo &change_info,
	                              const DuckLakeSnapshotCommit &commit_info) const;
	string EmitDropStagingSql() const;
	//! Substitute {METADATA_CATALOG} and snapshot placeholders ({SNAPSHOT_ID}, {SCHEMA_VERSION},
	//! {NEXT_CATALOG_ID}, {NEXT_FILE_ID}) in SQL produced by DuckLakeMetadataManager statics.
	string SubstitutePlaceholders(string sql, const DuckLakeSnapshot &snapshot) const;

	// Throws if the query fails.
	unique_ptr<MaterializedQueryResult> RunQuery(const string &query, const char *what);

private:
	ClientContext &context;
	const string metadata_schema_name;
	const string schema_id; //! SQL-identifier form of `metadata_schema_name`
	const string identifier_suffix;
	const int64_t schema_version;
	Connection fresh_conn;
	DuckLakeRetryConfig retry_config;
};

} // namespace duckdb
