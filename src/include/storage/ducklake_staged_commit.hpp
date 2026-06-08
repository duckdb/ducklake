//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_staged_commit.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"
#include "duckdb/common/common.hpp"

#include <vector>

namespace duckdb {

class ClientContext;
class DataChunk;
class DuckLakeMetadataManager;
class DuckLakeTransaction;
class Value;
enum class CompactionType;
struct DuckLakeColumnStats;
struct DuckLakeDataFile;
struct DuckLakeDeleteFile;
struct DuckLakeNameMapEntry;
struct DuckLakeNameMapSet;
struct DuckLakeRetryConfig;
struct DuckLakeSnapshotCommit;
struct FlushedInlinedTableInfo;
struct LocalTableChanges;
struct TransactionChangeInformation;

enum class DuckLakeStagedTableType : uint8_t {
	COMMIT_HEADER,
	DATA_FILE,
	DATA_FILE_COLUMN_STATS,
	DATA_FILE_PARTITION,
	DELETE_FILE,
	INLINED_DATA,
	INLINED_ROW,
	INLINED_COLUMN_STATS,
	INLINED_DELETE,
	INLINED_FILE_DELETE,
	DROPPED_FILE,
	TABLES_DELETED_FROM,
	FLUSHED_INLINED,
	COMPACTION,
	COMPACTION_SOURCE,
	NAME_MAP,
	NAME_MAP_ENTRY,
};

//! A staging table bound to a specific commit (schema + identifier suffix).
class DuckLakeStagedTable {
public:
	static const char *BaseName(DuckLakeStagedTableType type);
	static string Columns(DuckLakeStagedTableType type);
	static vector<string> ColumnNames(DuckLakeStagedTableType type);
	static const vector<DuckLakeStagedTableType> &AllTypes();
	static string CreateAllSql();
	static string TruncateAllSql();
};

//! Builds the SQL batch that stages a commit into temporary tables and calls ducklake_commit.
class DuckLakeStagedCommit {
public:
	//! Build the full SQL batch from transaction state.
	string Build(DuckLakeTransaction &transaction, const DuckLakeSnapshot &transaction_snapshot,
	             const DuckLakeRetryConfig &retry_config) const;

private:
	//! Emits INSERT for a single data file, its column stats, and partition values.
	void EmitDataFileRow(string &sql, const DuckLakeDataFile &file, idx_t local_file_id, TableIndex table_id,
	                     idx_t file_order, const string &compaction_id_literal) const;
	//! Emits INSERT for a loose delete file against a pre-existing data file.
	void EmitDeleteFileRow(string &sql, const DuckLakeDeleteFile &file, TableIndex table_id,
	                       const string &data_file_path) const;
	//! Emits INSERT for a delete file attached to a transaction-local data file.
	void EmitAttachedDeleteRow(string &sql, const DuckLakeDeleteFile &del, TableIndex table_id,
	                           idx_t local_file_id) const;
	//! Emits INSERT for a single inlined column stats row.
	void EmitInlinedColumnStatsRow(string &sql, TableIndex table_id, FieldIndex column_id,
	                               const DuckLakeColumnStats &s) const;
	//! Builds the shared 13-value column-stats payload (column_size_bytes .. extra_stats) used by both
	//! the data-file and inlined column-stats staging rows. Read back by ReadColumnStatsRow.
	static string EmitColumnStatsValues(const DuckLakeColumnStats &s);
	//! Recursively emits INSERT rows for a name map entry and its children.
	void FlattenNameMapEntry(const DuckLakeNameMapEntry &entry, idx_t map_id, idx_t parent_id, idx_t &next_entry_id,
	                         string &sql) const;

	//! Emits the commit header row (author, message, snapshot info, paths).
	string EmitCommitHeader(const DuckLakeSnapshotCommit &h, const DuckLakeSnapshot &snap, const string &data_path,
	                        const string &separator) const;
	//! Emits all new data files and their attached deletes.
	string EmitDataFiles(const LocalTableChanges &local_changes, idx_t &local_file_id) const;
	//! Emits inlined data rows, tuple literals, and per-column stats.
	string EmitInlinedData(const LocalTableChanges &local_changes, DuckLakeTransaction &transaction) const;
	//! Emits inlined row-level deletes (by inlined table name).
	string EmitInlinedDeletes(const LocalTableChanges &local_changes) const;
	//! Emits inlined file-level deletes (by file id + row id).
	string EmitInlinedFileDeletes(const LocalTableChanges &local_changes) const;
	//! Emits loose delete files against pre-existing parquet files.
	string EmitDeleteFiles(const LocalTableChanges &local_changes) const;
	//! Emits compaction headers, output files, and source file entries.
	string EmitCompactions(const LocalTableChanges &local_changes, idx_t &local_file_id) const;
	//! Emits column name mapping entries.
	string EmitNameMaps(const DuckLakeNameMapSet &name_maps) const;
	//! Emits flushed inlined table references.
	string EmitFlushedInlinedTables(const vector<FlushedInlinedTableInfo> &flushed) const;
	//! Emits dropped file paths and tables-deleted-from markers.
	string EmitDroppedFiles(DuckLakeTransaction &transaction) const;
};

} // namespace duckdb
