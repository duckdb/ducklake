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

#include <functional>

namespace duckdb {
class ColumnList;
class DuckLakeCatalogSet;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeTransaction;
struct DuckLakeRetryConfig;
struct TransactionChangeInformation;
class BoundAtClause;
class QueryResult;
class FileSystem;
class ConstantFilter;

struct SnapshotAndStats;
struct FlushedInlinedTableInfo;

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
	string order_by_clause;
};

//! The DuckLake metadata manger is the communication layer between the system and the metadata catalog
class DuckLakeMetadataManager {
public:
	explicit DuckLakeMetadataManager(DuckLakeTransaction &transaction);
	virtual ~DuckLakeMetadataManager();

	typedef unique_ptr<DuckLakeMetadataManager> (*create_t)(DuckLakeTransaction &transaction);
	static void Register(const string &name, create_t);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction);

	virtual bool TypeIsNativelySupported(const LogicalType &type);
	//! Check if a type supports data inlining on this metadata backend
	virtual bool SupportsInlining(const LogicalType &type);
	//! Check if this metadata manager supports the DuckDB Appender API for fast inserts
	//! Returns true for DuckDB metadata, false for external databases (Postgres, SQLite)
	virtual bool SupportsAppender() const {
		return true;
	}

	//! Probe the metadata server for optional capabilities, for now we only check for server-side retries
	virtual void ProbeServerCapabilities() {
	}
	//! Whether or not the commit retry loop should be executed on the metadata server rather than the client.
	bool ExecuteRetrialsServerSide() const;
	//! Whether the client can skip the snapshot fetch and let the server read it.
	virtual bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const;

	//! Run the commit retry loop with the metadata server handling retries.
	virtual void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                                    const TransactionChangeInformation &transaction_changes,
	                                    const DuckLakeRetryConfig &retry_config);

	virtual void ClearCache() {
	}

	void MarkPendingCacheClear() {
		pending_cache_clear = true;
	}
	bool TakePendingCacheClear() {
		bool pending = pending_cache_clear;
		pending_cache_clear = false;
		return pending;
	}
	//! Maximum identifier length in bytes supported by this backend
	virtual idx_t MaxIdentifierLength() const {
		return NumericLimits<idx_t>::Maximum();
	}
	//! Check if columns (stored as DuckLakeColumnInfo) support inlining, recursing into children
	bool SupportsInliningColumns(const vector<DuckLakeColumnInfo> &columns);

	//! Check whether a table with the given columns can be inlined
	bool CanInlineColumns(const ColumnList &columns);
	bool CanInlineColumns(const vector<DuckLakeColumnInfo> &columns);

	virtual string GetColumnTypeInternal(const LogicalType &column_type);
	virtual string CastColumnToTarget(const string &column, const LogicalType &type);

	DuckLakeMetadataManager &Get(DuckLakeTransaction &transaction);

	virtual unique_ptr<QueryResult> AttachMetadata(const string &attach_query);

	virtual bool MetadataExists();

	virtual string MetadataExistsQuery() const;

	//! Initialize a new DuckLake
	virtual void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);
	virtual DuckLakeMetadata LoadDuckLake();

	virtual unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query);

	virtual unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query);
	virtual unique_ptr<QueryResult> Query(string &query);

protected:
	void SubstituteCatalogPlaceholders(string &query) const;
	void SubstituteSnapshotPlaceholders(DuckLakeSnapshot snapshot, string &query) const;

public:
	//! Pure SQL templates (use `{METADATA_CATALOG}` placeholder) — caller substitutes + executes.
	//! Both used by the regular metadata-manager methods and by server-side commit, which runs the
	//! SQL on a fresh Connection without going through the metadata-manager wrapper.
	static string LatestSnapshotQuery();
	static string GlobalTableStatsQuery();
	//! Pure parsers for the results of the above queries.
	static unique_ptr<DuckLakeSnapshot> ParseSnapshot(QueryResult &result);
	static vector<DuckLakeGlobalStatsInfo> ParseGlobalTableStats(QueryResult &result);

	//! Get the catalog information for a specific snapshot
	virtual DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot);
	//! Transaction-free build of the catalog snapshot. Caller supplies a snapshot-aware query
	//! executor (responsible for `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` substitution) plus the
	//! data path and separator used for resolving stored relative paths.
	static DuckLakeCatalogInfo
	BuildCatalogForSnapshot(DuckLakeSnapshot snapshot,
	                        const std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string)> &query_executor,
	                        const string &base_data_path, const string &separator);
	virtual vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot, TableIndex table_id);
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
	virtual idx_t GetBeginSnapshotForTable(TableIndex table_id);
	virtual idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version);
	virtual idx_t GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot);
	virtual idx_t GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot);
	//! SQL builders for stats-refresh metadata lookups; caller substitutes placeholders + executes.
	static string GetNetDataFileRowCountSql(TableIndex table_id, const string &inlined_deletion_table);
	static string GetNetInlinedRowCountSql(const string &inlined_table_name);
	static string GetTableColumnSchemaSql(TableIndex table_id);
	static string GetInlinedTableNamesSql(TableIndex table_id);
	virtual vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &filter);
	virtual vector<DuckLakeFileForCleanup> GetOrphanFilesForCleanup(const string &filter, const string &separator);
	virtual vector<DuckLakeFileForCleanup> GetFilesForCleanup(const string &filter, CleanupType type,
	                                                          const string &separator);

	virtual void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files);
	static string DropSchemas(const set<SchemaIndex> &ids);
	static string DropTables(const set<TableIndex> &ids, bool renamed);
	static string DropViews(const set<TableIndex> &ids, bool renamed);
	static string DropMacros(const set<MacroIndex> &ids);

	//! Emits the INSERT for new schemas. Caller supplies resolved paths (one per schema, same order)
	//! since path resolution depends on the catalog's data_path / separator (instance state).
	static string WriteNewSchemas(const vector<DuckLakeSchemaInfo> &new_schemas,
	                              const vector<DuckLakePath> &resolved_paths);
	//! Emits the INSERT for new tables and their columns. Caller supplies resolved paths (one per
	//! table, same order). commit_snapshot is currently unused by the body — kept off the signature.
	static string WriteNewTables(const vector<DuckLakeTableInfo> &new_tables,
	                             const vector<DuckLakePath> &resolved_paths);
	static string WriteNewViews(const vector<DuckLakeViewInfo> &new_views);
	//! Emits the partition-key diff SQL. Caller supplies the existing partition state (fetched
	//! via GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewPartitionKeys(const vector<DuckLakePartitionInfo> &existing_partitions,
	                                    const vector<DuckLakePartitionInfo> &new_partitions);
	//! Emits the sort-key diff SQL. Caller supplies the existing sort state (fetched via
	//! GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewSortKeys(const vector<DuckLakeSortInfo> &existing_sorts,
	                               const vector<DuckLakeSortInfo> &new_sorts);
	static string WriteOptionsInCreateWith(const vector<DuckLakeConfigOption> &options_in_create_with);
	static string WriteDroppedColumns(const vector<DuckLakeDroppedColumn> &dropped_columns);
	static string WriteNewColumns(const vector<DuckLakeNewColumn> &new_columns);
	static string WriteNewTags(const vector<DuckLakeTagInfo> &new_tags);
	static string WriteNewColumnTags(const vector<DuckLakeColumnTagInfo> &new_tags);
	virtual string WriteNewDataFiles(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                                 const vector<DuckLakeTableInfo> &new_tables,
	                                 vector<DuckLakeSchemaInfo> &new_schemas_result);
	//! SQL branch of WriteNewDataFiles, shared with the server-side commit path. Returns SQL with
	//! {METADATA_CATALOG} / {SNAPSHOT_ID} placeholders. Caller supplies resolved paths (one per file,
	//! same order) since path policy differs across callers (schema-relative vs. always-absolute).
	static string WriteNewDataFilesSqlBatch(const vector<DuckLakeFileInfo> &new_files,
	                                        const vector<DuckLakePath> &resolved_paths);
	//! Opt-in fast-path: if this backend supports the DuckDB Appender API, write the files directly
	bool TryAppendDataFiles(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                        const vector<DuckLakeTableInfo> &new_tables,
	                        vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
	                                   const vector<DuckLakeInlinedDataInfo> &new_data,
	                                   const vector<DuckLakeTableInfo> &new_tables,
	                                   const vector<DuckLakeTableInfo> &new_inlined_data_tables_result);
	static string WriteNewInlinedDeletes(const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes);
	//! Creates the INSERT INTO {METADATA_CATALOG}.<inlined_table_name> VALUES (...) batch.
	static string FormatInlinedDataInsert(const string &inlined_table_name, idx_t row_id_start,
	                                      bool has_preserved_row_ids, const vector<int64_t> *row_ids,
	                                      const vector<string> &cells_per_row);
	virtual string WriteNewInlinedFileDeletes(DuckLakeSnapshot &commit_snapshot,
	                                          const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes);
	//! Static deterministic name of the per-table inlined deletion table.
	static string InlinedFileDeletionTableName(TableIndex table_id);
	//! SQL branch of WriteNewInlinedFileDeletes — returns the INSERT statements only. Callers must
	//! ensure the per-table deletion tables already exist (the instance overload does that via
	//! GetInlinedDeletionTableName with create_if_not_exists=true).
	static string WriteNewInlinedFileDeletesSqlBatch(const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes);
	//! Get the name of the inlined deletion table for a given table ID
	virtual string GetInlinedDeletionTableName(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                           bool create_if_not_exists = false);
	virtual string WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &tables);
	virtual string GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
	                                      string &inlined_tables, string &inlined_table_queries);
	static string InlinedTableNameFor(idx_t table_id, idx_t schema_version);
	static string InlinedTableDdlSql(const string &table_name, const string &column_defs);
	static string InlinedTableRegistrationTuple(idx_t table_id, const string &table_name, idx_t schema_version);
	static string LatestInlinedTableQuery(idx_t table_id);
	static string DropDataFiles(const set<DataFileIndex> &dropped_files);
	static string DropDeleteFiles(const set<DataFileIndex> &dropped_files);
	//! Caller supplies one resolved path per overwritten file, in the same order.
	static string DeleteOverwrittenDeleteFiles(const vector<DuckLakeOverwrittenDeleteFile> &overwritten_files,
	                                           const vector<DuckLakePath> &resolved_paths);
	//! Caller supplies one resolved path per new delete file, in the same order.
	static string WriteNewDeleteFiles(const vector<DuckLakeDeleteFileInfo> &new_delete_files,
	                                  const vector<DuckLakePath> &resolved_paths);
	static string WriteNewMacros(const vector<DuckLakeMacroInfo> &new_macros);

	virtual vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from);
	static string WriteNewColumnMappings(const vector<DuckLakeColumnMappingInfo> &new_column_mappings);
	//! Caller supplies one resolved path per compaction, in the same order.
	static string WriteMergeAdjacent(const vector<DuckLakeCompactedFileInfo> &compactions,
	                                 const vector<DuckLakePath> &resolved_paths);
	static string WriteDeleteRewrites(const vector<DuckLakeCompactedFileInfo> &compactions);
	//! For MERGE_ADJACENT_TABLES, resolved_paths is one path per compaction; for REWRITE_DELETES it
	//! is ignored.
	static string WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions, CompactionType type,
	                               const vector<DuckLakePath> &resolved_paths);
	//! SQL templates with {METADATA_CATALOG} / {SNAPSHOT_ID} placeholders, shared with the
	//! server-side commit path.
	static string InsertSnapshotSql();
	static string WriteSnapshotChangesSql(const SnapshotChangeInfo &change_info,
	                                      const DuckLakeSnapshotCommit &commit_info);
	static string UpdateGlobalTableStatsSql(const DuckLakeGlobalStatsInfo &stats);
	static SnapshotChangeInfo
	GetSnapshotAndStatsAndChanges(SnapshotAndStats &current_snapshot,
	                              const std::function<unique_ptr<QueryResult>(string)> &executor);
	static string GetSnapshotAndStatsAndChangesQuery();
	static SnapshotChangeInfo ParseSnapshotAndStatsAndChanges(QueryResult &result, SnapshotAndStats &current_snapshot);
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot();
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound);

	virtual idx_t GetNextColumnId(TableIndex table_id);
	virtual unique_ptr<QueryResult> ReadInlinedData(DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                                const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
	                                                          DuckLakeSnapshot end_snapshot,
	                                                          const string &inlined_table_name,
	                                                          const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
	                                                         DuckLakeSnapshot end_snapshot,
	                                                         const string &inlined_table_name,
	                                                         const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot,
	                                                           const string &inlined_table_name,
	                                                           const vector<string> &columns_to_read);
	//! SQL builders for the stats-refresh queries used by DuckLakeTransactionState::RecomputeGlobalStatsAfterRewrite.
	//! Caller substitutes `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` and executes via the commit context's executor.
	static string ReadInlinedDataAggregatesSql(const string &inlined_table_name, const string &select_list);
	static string ReadFileColumnStatsForTableSql(TableIndex table_id);
	virtual shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result,
	                                                             const vector<LogicalType> &expected_types);

	virtual void DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table);
	//! We delete at the flush
	virtual void DeleteFlushedInlinedData(const DuckLakeInlinedTableInfo &inlined_table, idx_t flush_snapshot_id);
	//! If it conflicts we batch everything at the retry
	static string GenerateDeleteFlushedInlinedData(const vector<FlushedInlinedTableInfo> &flushed_tables);
	static string InsertNewSchema(const DuckLakeSnapshot &snapshot, const set<TableIndex> &table_ids);

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
	virtual void MigrateV04();
	virtual void ExecuteMigration(string migrate_query, bool allow_failures, const string &from_version,
	                              const string &to_version);

	string LoadPath(string path);
	string StorePath(string path);
	string GetPathSeparator(const string &path);

protected:
	virtual string GetLatestSnapshotQuery() const;

	virtual string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id);

	//! Wrap field selections with list aggregation of struct objects (DBMS-specific)
	//! For DuckDB: LIST({'key1': val1, 'key2': val2, ...})
	//! For Postgres: jsonb_agg(jsonb_build_object('key1', val1, 'key2', val2, ...))
	static string ListAggregation(const vector<pair<string, string>> &fields);
	//! Parse tag list from ListAggregation value
	static vector<DuckLakeTag> LoadTags(const Value &tag_map);
	//! Parse inlined data tables list from ListAggregation value
	static vector<DuckLakeInlinedTableInfo> LoadInlinedDataTables(const Value &list);
	//! Parse macro implementations list from ListAggregation value
	static vector<DuckLakeMacroImplementation> LoadMacroImplementations(const Value &list);

public:
	//! Get path relative to catalog path
	DuckLakePath GetRelativePath(const string &path);
	//! Get path relative to schema path
	DuckLakePath GetRelativePath(SchemaIndex schema_id, const string &path,
	                             vector<DuckLakeSchemaInfo> &new_schemas_result);
	//! Get path relative to table path
	DuckLakePath GetRelativePath(TableIndex table_id, const string &path, const vector<DuckLakeTableInfo> &new_tables,
	                             vector<DuckLakeSchemaInfo> &new_schemas_result);

	static DuckLakePath GetRelativePath(SchemaIndex schema_id, const string &path,
	                                    const vector<DuckLakeSchemaInfo> &new_schemas_result,
	                                    const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                                    const string &base_data_path, const string &separator);
	static DuckLakePath GetRelativePath(TableIndex table_id, const string &path,
	                                    const vector<DuckLakeTableInfo> &new_tables,
	                                    const vector<DuckLakeSchemaInfo> &new_schemas_result,
	                                    const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                                    const string &base_data_path, const string &separator);

protected:
	string GetInlinedTableQuery(const DuckLakeTableInfo &table, const string &table_name);
	string GetColumnType(const DuckLakeColumnInfo &col);
	string GetKnownFilesForCleanupQuery(const string &separator) const;

	//! Optimized data file writing using DuckDB Appender API (only for DuckDB metadata manager)
	string WriteNewDataFilesWithAppender(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                                     const vector<DuckLakeTableInfo> &new_tables,
	                                     vector<DuckLakeSchemaInfo> &new_schemas_result);
	DuckLakePath GetRelativePath(const string &path, const string &data_path);
	string FromRelativePath(const DuckLakePath &path, const string &base_path);
	string FromRelativePath(const DuckLakePath &path);
	string FromRelativePath(TableIndex table_id, const DuckLakePath &path);
	string GetPath(SchemaIndex schema_id, vector<DuckLakeSchemaInfo> &new_schemas_result);
	string GetPath(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	               const vector<DuckLakeSchemaInfo> &new_schemas_result);
	FileSystem &GetFileSystem();

	static string StorePath(string path, const string &separator);
	static string LoadPath(string path, const string &separator);
	static string FromRelativePath(const DuckLakePath &path, const string &base_path, const string &separator);
	static DuckLakePath GetRelativePath(const string &path, const string &data_path, const string &separator);
	static string GetPathForSchema(SchemaIndex schema_id, const vector<DuckLakeSchemaInfo> &new_schemas_result,
	                               const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                               const string &base_data_path, const string &separator);
	static string GetPathForTable(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	                              const vector<DuckLakeSchemaInfo> &new_schemas_result,
	                              const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                              const string &base_data_path, const string &separator);

private:
	template <class T>
	static string FlushDrop(const string &metadata_table_name, const string &id_name, const set<T> &dropped_entries);
	template <class T>
	DuckLakeFileData ReadDataFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx, bool is_encrypted);
	template <class T>
	DuckLakeFileData ReadDeleteFile(DuckLakeTableEntry &table, T &row, idx_t &col_idx, bool is_encrypted);

	bool IsEncrypted() const;
	string GetFileSelectList(const string &prefix);
	string GetDeleteFileSelectList(const string &prefix);
	FilterPushdownQueryComponents GenerateFilterPushdownComponents(const FilterPushdownInfo &filter_info,
	                                                               TableIndex table_id);
	//! Build an additional WHERE fragment that prunes files by bucket() partition value.
	//! Returns "" when no foldable equality / IN-list predicate exists on a bucket-partitioned column.
	//! The fragment uses only string equality against ducklake_file_partition_value, so it works against
	//! any metadata backend (DuckDB / Postgres / SQLite). Bucket hashes are pre-computed in C++.
	string BuildBucketPartitionPruningClause(DuckLakeTableEntry &table, const FilterPushdownInfo &filter_info);
	string GenerateCTESectionFromRequirements(const unordered_map<idx_t, CTERequirement> &requirements,
	                                          TableIndex table_id);
	virtual FilterSQLResult ConvertFilterPushdownToSQL(const FilterPushdownInfo &filter_info);
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

public:
	//! Read inlined file deletions for regular table scans (no snapshot info per row)
	map<idx_t, set<idx_t>> ReadInlinedFileDeletions(TableIndex table_id, DuckLakeSnapshot snapshot);
	//! Clear inlined table caches (needed after rollback so retry re-creates the tables)
	void ClearInlinedTableCaches();

private:
	static unordered_map<string /* name */, create_t> metadata_managers;
	static mutex metadata_managers_lock;

	//! Check which file IDs have inlined deletions (returns set of file IDs that have deletions)
	unordered_set<idx_t> GetFileIdsWithInlinedDeletions(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                                    const vector<idx_t> &file_ids);
	//! Read inlined file deletions for deletion scans (includes snapshot info per row)
	map<idx_t, unordered_map<idx_t, idx_t>> ReadInlinedFileDeletionsForRange(TableIndex table_id,
	                                                                         DuckLakeSnapshot start_snapshot,
	                                                                         DuckLakeSnapshot end_snapshot);

	unordered_map<idx_t, string> insert_inlined_table_name_cache;
	unordered_set<idx_t> delete_inlined_table_cache;

protected:
	DuckLakeTransaction &transaction;
	mutex paths_lock;
	map<SchemaIndex, string> schema_paths;
	map<TableIndex, string> table_paths;
	bool pending_cache_clear = false;
};

} // namespace duckdb
