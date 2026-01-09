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
#include "common/ducklake_options.hpp"
#include "common/index.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class DuckLakeCatalogSet;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeTransaction;
class BoundAtClause;
class QueryResult;
class FileSystem;
class ConstantFilter;

struct SnapshotAndStats;

enum class SnapshotBound { LOWER_BOUND, UPPER_BOUND };

struct CTERequirement {
	idx_t column_field_index;
	unordered_set<string> referenced_stats;
	idx_t reference_count = 1;

	CTERequirement(idx_t col_idx, unordered_set<string> stats)
	    : column_field_index(col_idx), referenced_stats(std::move(stats)) {
	}
};

struct FilterSQLResult {
	string where_conditions;
	unordered_map<idx_t, CTERequirement> required_ctes;

	FilterSQLResult() = default;
	FilterSQLResult(string conditions) : where_conditions(std::move(conditions)) {
	}
};

struct ColumnFilterInfo {
	idx_t column_field_index;
	LogicalType column_type;
	unique_ptr<TableFilter> table_filter;

	ColumnFilterInfo(idx_t col_idx, LogicalType type, unique_ptr<TableFilter> filter)
	    : column_field_index(col_idx), column_type(std::move(type)), table_filter(std::move(filter)) {
	}

	ColumnFilterInfo(const ColumnFilterInfo &other)
	    : column_field_index(other.column_field_index), column_type(other.column_type),
	      table_filter(other.table_filter->Copy()) {
	}
};

struct FilterPushdownInfo {
	unordered_map<idx_t, ColumnFilterInfo> column_filters;

	FilterPushdownInfo() = default;

	unique_ptr<FilterPushdownInfo> Copy() const {
		auto result = make_uniq<FilterPushdownInfo>();
		for (const auto &entry : column_filters) {
			result->column_filters.emplace(entry.first, entry.second);
		}
		return result;
	}
};

struct FilterPushdownQueryComponents {
	string cte_section;
	string where_clause;
};

//! The DuckLake metadata manger is the communication layer between the system and the metadata catalog
class DuckLakeMetadataManager {
public:
	explicit DuckLakeMetadataManager(DuckLakeTransaction &transaction);
	virtual ~DuckLakeMetadataManager();

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction);

	virtual bool TypeIsNativelySupported(const LogicalType &type);

	virtual string GetColumnTypeInternal(const LogicalType &column_type);

	DuckLakeMetadataManager &Get(DuckLakeTransaction &transaction);

	//! Initialize a new DuckLake
	virtual void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);
	virtual DuckLakeMetadata LoadDuckLake();

	virtual unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query);

	virtual unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query);
	//! Get the catalog information for a specific snapshot
	virtual DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListEntry> GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                                       const FilterPushdownInfo *filter_info = nullptr);
	virtual vector<DuckLakeFileListEntry> GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                         DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeDeleteScanEntry>
	GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr);
	virtual vector<DuckLakeCompactionFileEntry> GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type,
	                                                                  double deletion_threshold,
	                                                                  DuckLakeSnapshot snapshot,
	                                                                  DuckLakeFileSizeOptions options);
	virtual idx_t GetCatalogIdForSchema(idx_t schema_id);
	virtual vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &filter);
	virtual vector<DuckLakeFileForCleanup> GetOrphanFilesForCleanup(const string &filter, const string &separator);
	virtual vector<DuckLakeFileForCleanup> GetFilesForCleanup(const string &filter, CleanupType type,
	                                                          const string &separator);

	virtual void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files);
	virtual string DropSchemas(const set<SchemaIndex> &ids);
	virtual string DropTables(const set<TableIndex> &ids, bool renamed);
	virtual string DropViews(const set<TableIndex> &ids);
	virtual string DropMacros(const set<MacroIndex> &ids);

	virtual string WriteNewSchemas(const vector<DuckLakeSchemaInfo> &new_schemas);
	virtual string WriteNewTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &new_tables,
	                              vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewViews(const vector<DuckLakeViewInfo> &new_views);
	virtual string WriteNewPartitionKeys(DuckLakeSnapshot commit_snapshot,
	                                     const vector<DuckLakePartitionInfo> &new_partitions);
	virtual string WriteNewSortKeys(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeSortInfo> &new_sorts);
	virtual string WriteDroppedColumns(const vector<DuckLakeDroppedColumn> &dropped_columns);
	virtual string WriteNewColumns(const vector<DuckLakeNewColumn> &new_columns);
	virtual string WriteNewTags(const vector<DuckLakeTagInfo> &new_tags);
	virtual string WriteNewColumnTags(const vector<DuckLakeColumnTagInfo> &new_tags);
	virtual string WriteNewDataFiles(const vector<DuckLakeFileInfo> &new_files,
	                                 const vector<DuckLakeTableInfo> &new_tables,
	                                 vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
	                                   const vector<DuckLakeInlinedDataInfo> &new_data,
	                                   const vector<DuckLakeTableInfo> &new_tables,
	                                   const vector<DuckLakeTableInfo> &new_inlined_data_tables_result);
	virtual string WriteNewInlinedDeletes(const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes);
	virtual string WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &tables);
	virtual string GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
	                                      string &inlined_tables, string &inlined_table_queries);
	virtual string DropDataFiles(const set<DataFileIndex> &dropped_files);
	virtual string DropDeleteFiles(const set<DataFileIndex> &dropped_files);
	virtual string WriteNewDeleteFiles(const vector<DuckLakeDeleteFileInfo> &new_delete_files,
	                                   const vector<DuckLakeTableInfo> &new_tables,
	                                   vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewMacros(const vector<DuckLakeMacroInfo> &new_macros);

	virtual vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from);
	virtual string WriteNewColumnMappings(const vector<DuckLakeColumnMappingInfo> &new_column_mappings);
	virtual string WriteMergeAdjacent(const vector<DuckLakeCompactedFileInfo> &compactions);
	virtual string WriteDeleteRewrites(const vector<DuckLakeCompactedFileInfo> &compactions);
	virtual string WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions, CompactionType type);
	virtual string InsertSnapshot();
	virtual string WriteSnapshotChanges(const SnapshotChangeInfo &change_info,
	                                    const DuckLakeSnapshotCommit &commit_info);
	virtual string UpdateGlobalTableStats(const DuckLakeGlobalStatsInfo &stats);
	virtual SnapshotChangeInfo GetSnapshotAndStatsAndChanges(DuckLakeSnapshot start_snapshot,
	                                                         SnapshotAndStats &current_snapshot);
	SnapshotDeletedFromFiles GetFilesDeletedOrDroppedAfterSnapshot(const DuckLakeSnapshot &start_snapshot) const;
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot();
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound);
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
	virtual void DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table);
	virtual string InsertNewSchema(const DuckLakeSnapshot &snapshot, const set<TableIndex> &table_ids);

	virtual vector<DuckLakeSnapshotInfo> GetAllSnapshots(const string &filter = string());
	virtual void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	virtual vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot);
	virtual void SetConfigOption(const DuckLakeConfigOption &option);
	virtual string GetPathForSchema(SchemaIndex schema_id, vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string GetPathForTable(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	                               const vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual bool IsColumnCreatedWithTable(const string &table_name, const string &column_name);
	virtual void MigrateV01();
	virtual void MigrateV02(bool allow_failures = false);
	virtual void MigrateV03(bool allow_failures = false);
	virtual void ExecuteMigration(string migrate_query, bool allow_failures);

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
	DuckLakePath GetRelativePath(SchemaIndex schema_id, const string &path,
	                             vector<DuckLakeSchemaInfo> &new_schemas_result);
	//! Get path relative to table path
	DuckLakePath GetRelativePath(TableIndex table_id, const string &path, const vector<DuckLakeTableInfo> &new_tables,
	                             vector<DuckLakeSchemaInfo> &new_schemas_result);
	DuckLakePath GetRelativePath(const string &path, const string &data_path);
	string FromRelativePath(const DuckLakePath &path, const string &base_path);
	string FromRelativePath(const DuckLakePath &path);
	string FromRelativePath(TableIndex table_id, const DuckLakePath &path);
	string GetPath(SchemaIndex schema_id, vector<DuckLakeSchemaInfo> &new_schemas_result);
	string GetPath(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	               const vector<DuckLakeSchemaInfo> &new_schemas_result);
	FileSystem &GetFileSystem();

private:
	template <class T>
	string FlushDrop(const string &metadata_table_name, const string &id_name, const set<T> &dropped_entries);
	template <class T>
	DuckLakeFileData ReadDataFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx, bool is_encrypted);

	bool IsEncrypted() const;
	string GetFileSelectList(const string &prefix);
	FilterPushdownQueryComponents GenerateFilterPushdownComponents(const FilterPushdownInfo &filter_info,
	                                                               TableIndex table_id);
	virtual FilterSQLResult ConvertFilterPushdownToSQL(const FilterPushdownInfo &filter_info);
	virtual string GenerateCTESectionFromRequirements(const unordered_map<idx_t, CTERequirement> &requirements,
	                                                  TableIndex table_id);
	virtual string GenerateFilterFromTableFilter(const TableFilter &filter, const LogicalType &type,
	                                             unordered_set<string> &referenced_stats);
	virtual bool ValueIsFinite(const Value &val);
	virtual string CastValueToTarget(const Value &val, const LogicalType &type);
	virtual string CastStatsToTarget(const string &stats, const LogicalType &type);
	virtual string GenerateConstantFilter(const ConstantFilter &constant_filter, const LogicalType &type,
	                                      unordered_set<string> &referenced_stats);
	virtual string GenerateConstantFilterDouble(const ConstantFilter &constant_filter, const LogicalType &type,
	                                            unordered_set<string> &referenced_stats);
	virtual string GenerateFilterPushdown(const TableFilter &filter, unordered_set<string> &referenced_stats);

private:
	unordered_map<idx_t, string> inlined_table_name_cache;

protected:
	DuckLakeTransaction &transaction;
	mutex paths_lock;
	map<SchemaIndex, string> schema_paths;
	map<TableIndex, string> table_paths;
};

} // namespace duckdb
