//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/ducklake_snapshot.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_stats.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "common/ducklake_encryption.hpp"
#include "common/index.hpp"

namespace duckdb {
class DuckLakeCatalogSet;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeTransaction;
class BoundAtClause;
class QueryResult;
class FileSystem;

// The DuckLake metadata manger is the communication layer between the system and the metadata catalog
class DuckLakeMetadataManager {
public:
	explicit DuckLakeMetadataManager(DuckLakeTransaction &transaction);
	virtual ~DuckLakeMetadataManager();

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction);

	DuckLakeMetadataManager &Get(DuckLakeTransaction &transaction);

	//! Initialize a new DuckLake
	virtual void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);
	virtual DuckLakeMetadata LoadDuckLake();
	//! Get the catalog information for a specific snapshot
	virtual DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListEntry> GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                                       const string &filter);
	virtual vector<DuckLakeFileListEntry> GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                         DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeDeleteScanEntry>
	GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot, const string &filter);
	virtual vector<DuckLakeCompactionFileEntry> GetFilesForCompaction(DuckLakeTableEntry &table);
	virtual vector<DuckLakeFileScheduledForCleanup> GetFilesScheduledForCleanup(const string &filter);
	virtual void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileScheduledForCleanup> &cleaned_up_files);
	virtual void DropSchemas(DuckLakeSnapshot commit_snapshot, const set<SchemaIndex> &ids);
	virtual void DropTables(DuckLakeSnapshot commit_snapshot, const set<TableIndex> &ids);
	virtual void DropViews(DuckLakeSnapshot commit_snapshot, const set<TableIndex> &ids);
	virtual void WriteNewSchemas(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeSchemaInfo> &new_schemas);
	virtual void WriteNewTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &new_tables);
	virtual void WriteNewViews(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeViewInfo> &new_views);
	virtual void WriteNewPartitionKeys(DuckLakeSnapshot commit_snapshot,
	                                   const vector<DuckLakePartitionInfo> &new_partitions);
	virtual void WriteDroppedColumns(DuckLakeSnapshot commit_snapshot,
	                                 const vector<DuckLakeDroppedColumn> &dropped_columns);
	virtual void WriteNewColumns(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeNewColumn> &new_columns);
	virtual void WriteNewTags(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTagInfo> &new_tags);
	virtual void WriteNewColumnTags(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeColumnTagInfo> &new_tags);
	virtual void WriteNewDataFiles(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeFileInfo> &new_files);
	virtual void WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
	                                 const vector<DuckLakeInlinedDataInfo> &new_data);
	virtual void WriteNewInlinedDeletes(DuckLakeSnapshot commit_snapshot,
	                                    const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes);
	virtual void WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &tables);
	virtual string GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
	                                      string &inlined_tables, string &inlined_table_queries);
	virtual void ExecuteInlinedTableQueries(DuckLakeSnapshot commit_snapshot, string &inlined_tables,
	                                        const string &inlined_table_queries);
	virtual void DropDataFiles(DuckLakeSnapshot commit_snapshot, const set<DataFileIndex> &dropped_files);
	virtual void DropDeleteFiles(DuckLakeSnapshot commit_snapshot, const set<DataFileIndex> &dropped_files);
	virtual void WriteNewDeleteFiles(DuckLakeSnapshot commit_snapshot,
	                                 const vector<DuckLakeDeleteFileInfo> &new_delete_files);
	virtual vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from);
	virtual void WriteNewColumnMappings(DuckLakeSnapshot commit_snapshot,
	                                    const vector<DuckLakeColumnMappingInfo> &new_column_mappings);
	virtual void WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions);
	virtual void InsertSnapshot(DuckLakeSnapshot commit_snapshot);
	virtual void WriteSnapshotChanges(DuckLakeSnapshot commit_snapshot, const SnapshotChangeInfo &change_info);
	virtual void UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats);
	virtual SnapshotChangeInfo GetChangesMadeAfterSnapshot(DuckLakeSnapshot start_snapshot);
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot();
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause);
	virtual idx_t GetNextColumnId(TableIndex table_id);
	virtual shared_ptr<DuckLakeInlinedData> ReadInlinedData(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                                        const vector<string> &columns_to_read);
	virtual shared_ptr<DuckLakeInlinedData> ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
	                                                                  DuckLakeSnapshot end_snapshot,
	                                                                  const string &inlined_table_name,
	                                                                  const vector<string> &columns_to_read);
	virtual shared_ptr<DuckLakeInlinedData> ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
	                                                                 DuckLakeSnapshot end_snapshot,
	                                                                 const string &inlined_table_name,
	                                                                 const vector<string> &columns_to_read);

	virtual vector<DuckLakeSnapshotInfo> GetAllSnapshots(const string &filter = string());
	virtual void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	virtual vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot);
	virtual void SetConfigOption(const DuckLakeConfigOption &option);
	virtual string GetPathForSchema(SchemaIndex schema_id);
	virtual string GetPathForTable(TableIndex table_id);

	virtual void MigrateV01();
	virtual void MigrateV02();

	string LoadPath(string path);
	string StorePath(string path);

protected:
	virtual string GetLatestSnapshotQuery() const;

protected:
	string GetInlinedTableQuery(const DuckLakeTableInfo &table, const string &table_name);
	string GetColumnType(const DuckLakeColumnInfo &col);
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result);

	//! Get path relative to catalog path
	DuckLakePath GetRelativePath(const string &path);
	//! Get path relative to schema path
	DuckLakePath GetRelativePath(SchemaIndex schema_id, const string &path);
	//! Get path relative to table path
	DuckLakePath GetRelativePath(TableIndex table_id, const string &path);
	DuckLakePath GetRelativePath(const string &path, const string &data_path);
	string FromRelativePath(const DuckLakePath &path, const string &base_path);
	string FromRelativePath(const DuckLakePath &path);
	string FromRelativePath(TableIndex table_id, const DuckLakePath &path);
	string GetPath(SchemaIndex schema_id);
	string GetPath(TableIndex table_id);
	FileSystem &GetFileSystem();

private:
	template <class T>
	void FlushDrop(DuckLakeSnapshot commit_snapshot, const string &metadata_table_name, const string &id_name,
	               const set<T> &dropped_entries);
	template <class T>
	DuckLakeFileData ReadDataFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx, bool is_encrypted);

	bool IsEncrypted() const;
	string GetFileSelectList(const string &prefix);

protected:
	DuckLakeTransaction &transaction;
	mutex paths_lock;
	map<SchemaIndex, string> schema_paths;
	map<TableIndex, string> table_paths;
};

} // namespace duckdb
