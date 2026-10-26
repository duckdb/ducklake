//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/ducklake_row_helpers.hpp"
#include "common/ducklake_snapshot.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_stats.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "storage/ducklake_metadata_info.hpp"
#include "common/ducklake_encryption.hpp"
#include "common/ducklake_options.hpp"
#include "common/index.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
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
class SQLStatement;
class FileSystem;

struct SnapshotAndStats;
struct FlushedInlinedTableInfo;

enum class SnapshotBound { LOWER_BOUND, UPPER_BOUND };

//! A check whether the live rows of an inlined data table hold a value of a column
struct DuckLakeInlinedValueCheck {
	string table_name;
	//! The rows up to this snapshot are flushed by the commit
	idx_t flushed_up_to;
	FieldIndex column_id;
	//! The name of the column in the inlined data table, empty when any live row counts as a value
	string column_name;
};

//! Metadata column names inside ducklake_inlined_data_<table_id>_<schema_version> tables.
struct DuckLakeInlinedColNames {
	//! Column name prefix reserved for DuckLake internal use
	static constexpr const char *PREFIX = "_ducklake_";
	//! Number of metadata columns an inlined data table adds
	static constexpr idx_t COLUMN_COUNT = 3;

	explicit DuckLakeInlinedColNames(bool prefixed_inlined_columns) {
		if (prefixed_inlined_columns) {
			row_id = PREFIX + row_id;
			begin_snapshot = PREFIX + begin_snapshot;
			end_snapshot = PREFIX + end_snapshot;
		}
	}

	//! Whether a user column name collides with the metadata columns written for inlining
	bool ConflictsWith(const string &name) const;

	string row_id = "row_id";
	string begin_snapshot = "begin_snapshot";
	string end_snapshot = "end_snapshot";
};

struct CTERequirement {
	idx_t column_field_index;
	unordered_set<string> referenced_stats;

	CTERequirement(idx_t col_idx, unordered_set<string> stats)
	    : column_field_index(col_idx), referenced_stats(std::move(stats)) {
	}
};

struct FilterSQLResult {
	string where_conditions;
	//! Ordered by column field index so the generated SQL does not depend on hash iteration order
	map<idx_t, CTERequirement> required_ctes;

	FilterSQLResult() = default;
};

struct ColumnFilterInfo {
	idx_t column_field_index;
	LogicalType column_type;
	unique_ptr<ExpressionFilter> table_filter;

	ColumnFilterInfo(idx_t col_idx, LogicalType type, unique_ptr<ExpressionFilter> filter)
	    : column_field_index(col_idx), column_type(std::move(type)), table_filter(std::move(filter)) {
	}

	ColumnFilterInfo(const ColumnFilterInfo &other)
	    : column_field_index(other.column_field_index), column_type(other.column_type),
	      table_filter(other.table_filter ? other.table_filter->Copy() : nullptr) {
	}

	ColumnFilterInfo(ColumnFilterInfo &&other) = default;
	ColumnFilterInfo &operator=(ColumnFilterInfo &&other) = default;
};

enum class DuckLakeFilterNodeType : uint8_t { COLUMN_FILTER, CONJUNCTION_AND, CONJUNCTION_OR, MATCH_NONE };

//! A node in a filter tree - leaves filter a single column, inner nodes combine them across columns
struct DuckLakeFilterNode {
	DuckLakeFilterNodeType type;
	//! Set for COLUMN_FILTER nodes
	unique_ptr<ColumnFilterInfo> column_filter;
	//! Set for conjunction nodes
	vector<unique_ptr<DuckLakeFilterNode>> children;

	explicit DuckLakeFilterNode(DuckLakeFilterNodeType type_p) : type(type_p) {
	}
	explicit DuckLakeFilterNode(ColumnFilterInfo filter)
	    : type(DuckLakeFilterNodeType::COLUMN_FILTER), column_filter(make_uniq<ColumnFilterInfo>(std::move(filter))) {
	}

	unique_ptr<DuckLakeFilterNode> Copy() const {
		if (type == DuckLakeFilterNodeType::COLUMN_FILTER) {
			return make_uniq<DuckLakeFilterNode>(*column_filter);
		}
		auto result = make_uniq<DuckLakeFilterNode>(type);
		for (const auto &child : children) {
			result->children.push_back(child->Copy());
		}
		return result;
	}
};

//! A filter tree together with the expression it was derived from
struct DuckLakeFilterTree {
	unique_ptr<DuckLakeFilterNode> root;
	//! Used to recognize the same filter being pushed down again
	unique_ptr<Expression> source;

	DuckLakeFilterTree Copy() const {
		DuckLakeFilterTree result;
		result.root = root->Copy();
		result.source = source->Copy();
		return result;
	}
};

struct FilterPushdownInfo {
	unordered_map<idx_t, ColumnFilterInfo> column_filters;
	//! Filter trees, which must hold alongside the single-column filters above. Usually cross-column -
	//! a single-column tree is only kept when it expresses something the per-column filters cannot.
	vector<DuckLakeFilterTree> filter_trees;

	FilterPushdownInfo() = default;

	bool Empty() const {
		return column_filters.empty() && filter_trees.empty();
	}

	unique_ptr<FilterPushdownInfo> Copy() const {
		auto result = make_uniq<FilterPushdownInfo>();
		for (const auto &entry : column_filters) {
			result->column_filters.emplace(entry.first, entry.second);
		}
		for (const auto &tree : filter_trees) {
			result->filter_trees.push_back(tree.Copy());
		}
		return result;
	}
};

struct DuckLakeFileListDynamicFilter {
	idx_t column_field_index;
	ExpressionType comparison_type;
	LogicalType column_type;
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
	//! Check if the inlined data tables of this metadata backend can store the values of the chunk
	virtual bool CanInlineValues(DataChunk &chunk);
	//! Check if this metadata manager supports the DuckDB Appender API for fast inserts
	//! Returns true for DuckDB and SQLite metadata, false for Postgres and Quack
	virtual bool SupportsAppender() const {
		return true;
	}

	//! Whether the metadata catalog commits each statement on its own, so a rollback cannot undo them
	virtual bool CommitsEachStatement() const {
		return false;
	}
	//! Whether transactions that started before a metadata table was dropped still read it
	virtual bool DroppedTablesStayReadable() const {
		return true;
	}
	//! Whether a commit inserts inlined rows after its batch instead of sending them as VALUES text in it
	virtual bool InsertsInlinedRowsAfterBatch(idx_t row_count) const {
		return !CommitsEachStatement();
	}
	//! Probe the metadata server for optional capabilities, for now we only check for server-side retries
	virtual void ProbeServerCapabilities() {
	}
	//! Whether or not the commit retry loop should be executed on the metadata server rather than the client.
	bool ExecuteRetrialsServerSide() const;
	//! Whether the client can skip the snapshot fetch and let the server read it.
	virtual bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const;
	virtual bool IsRetryableCommitError(const string &) const {
		return false;
	}
	//! Advice added to the error of a commit that failed for good
	virtual string CommitErrorHint(const string &) const {
		return string();
	}

	//! Run the commit retry loop with the metadata server handling retries.
	virtual void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                                    const TransactionChangeInformation &transaction_changes,
	                                    const DuckLakeRetryConfig &retry_config);

	virtual void ClearCache() {
	}
	//! Whether a table that a commit creates is only found once the cache is cleared
	virtual bool NewTablesRequireCacheClear() const {
		return true;
	}

	void MarkPendingCacheClear() {
		if (NewTablesRequireCacheClear()) {
			pending_cache_clear = true;
		}
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
	//! Maximum number of columns of a table supported by this backend
	virtual idx_t MaxColumnCount() const {
		return NumericLimits<idx_t>::Maximum();
	}
	//! Names of the system columns that the tables of this backend have
	virtual vector<string> SystemColumnNames() const {
		return {};
	}

	//! Why the rows of a table with the given columns cannot be inlined, empty when they can
	string GetInliningObstacle(const ColumnList &columns);
	//! Whether the inlined data table of a table with this many columns fits the backend
	bool InlinedTableFits(idx_t column_count) const;
	bool CanInlineColumns(const vector<DuckLakeColumnInfo> &columns);

	virtual string GetColumnTypeInternal(const LogicalType &column_type);
	string CastColumnToTarget(const string &column, const LogicalType &type);
	//! The inlined rows to flush with typed columns, without those this transaction deleted
	string InlinedFlushSource(const string &inlined_table_name, const DuckLakeTableEntry &table);
	//! The order of the rows in a flushed file
	string InlinedFlushOrder(const string &sort_order_sql) const;

	virtual unique_ptr<QueryResult> AttachMetadata(const string &attach_query);

	virtual bool MetadataExists();

	virtual string MetadataExistsQuery() const;

	//! Initialize a new DuckLake
	virtual void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);
	//! Get the CREATE TABLE statements for all metadata tables
	virtual string GetCreateTableStatements();
	virtual string GetSchemaTableStatement();
	virtual string GetDataFileTableStatement();
	virtual string GetDeleteFileTableStatement();
	virtual string GetFileColumnStatsTableStatement();
	virtual string GetTableColumnStatsTableStatement();
	//! Get the version string written to ducklake_metadata
	virtual string GetVersionString();
	virtual DuckLakeMetadata LoadDuckLake();

	virtual unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query);
	virtual unique_ptr<QueryResult> Execute(string &query);
	//! Runs the statements in a transaction of their own, rolled back when one of them fails
	virtual unique_ptr<QueryResult> ExecuteInTransaction(string &query);
	//! Runs statements that drop metadata tables atomically, failing where the drop would wait for their readers
	virtual unique_ptr<QueryResult> ExecuteDropInTransaction(string &query);

	virtual unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query);
	virtual unique_ptr<QueryResult> Query(string &query);

	//! Rvalue sugar so call sites can pass `R"(...)"` and `StringUtil::Format(...)` directly.
	//! Named-rvalue decays to an lvalue inside, so the virtual dispatch still picks up the
	//! string-ref overrides without derived classes needing to add anything.
	unique_ptr<QueryResult> Execute(string &&query);
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &&query);
	unique_ptr<QueryResult> Query(string &&query);

protected:
	void SubstituteCatalogPlaceholders(string &query) const;
	void SubstituteCatalogPlaceholders(string &query, const string &metadata_catalog) const;
	void SubstituteTransactionPlaceholders(DuckLakeSnapshot snapshot, string &query) const;

public:
	static void SubstituteSnapshotPlaceholders(const DuckLakeSnapshot &snapshot, string &query);
	//! Pure SQL templates (use `{METADATA_CATALOG}` placeholder) — caller substitutes + executes.
	//! Both used by the regular metadata-manager methods and by server-side commit, which runs the
	//! SQL on a fresh Connection without going through the metadata-manager wrapper.
	static string LatestSnapshotQuery();
	//! With table ids only those tables' stats are read
	static string GlobalTableStatsQuery(bool include_exactness, const vector<TableIndex> &table_ids = {});
	//! Pure parsers for the results of the above queries.
	static unique_ptr<DuckLakeSnapshot> ParseSnapshot(QueryResult &result,
	                                                  optional_ptr<string> catalog_version = nullptr);
	static vector<DuckLakeGlobalStatsInfo> ParseGlobalTableStats(QueryResult &result);
	//! Take the table sizes that stats rows lack from the live data files
	static void FillMissingTableSizes(vector<DuckLakeGlobalStatsInfo> &stats,
	                                  const std::function<unique_ptr<QueryResult>(string)> &executor);
	//! Whether the result contains a column with the given name
	static bool ResultHasColumn(QueryResult &result, const string &name);

	//! Get the catalog information for a specific snapshot
	virtual DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot);
	//! Transaction-free build of the catalog snapshot. Caller supplies a snapshot-aware query
	//! executor (responsible for `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` substitution) plus the
	//! data path and separator used for resolving stored relative paths.
	static DuckLakeCatalogInfo
	BuildCatalogForSnapshot(DuckLakeSnapshot snapshot,
	                        const std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string)> &query_executor,
	                        const string &base_data_path, const string &separator, bool supports_v1_1_metadata = false);
	//! The global stats of every table of the snapshot, or of the given tables
	virtual vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot,
	                                                            const vector<TableIndex> &table_ids = {});
	//! Get the record count of every table that has global stats
	virtual map<TableIndex, idx_t> GetTableRecordCounts(DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListEntry> GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                                       const FilterPushdownInfo *filter_info = nullptr,
	                                                       bool include_local_changes = true);
	virtual vector<DuckLakeFileListEntry> GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                         DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeDeleteScanEntry> GetTableDeletions(DuckLakeTableEntry &table,
	                                                          DuckLakeSnapshot start_snapshot,
	                                                          DuckLakeSnapshot snapshot,
	                                                          bool include_local_changes = true);
	virtual vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr);
	//! Sets the schema version of the partition spec of the files written with an older spec
	void SetPartitionSchemaVersions(DuckLakeTableEntry &table, vector<DuckLakeCompactionFileEntry> &files);
	virtual vector<DuckLakeCompactionFileEntry> GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type,
	                                                                  double deletion_threshold,
	                                                                  DuckLakeSnapshot snapshot,
	                                                                  DuckLakeFileSizeOptions options);
	virtual idx_t GetBeginSnapshotForTable(TableIndex table_id);
	virtual idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version);
	DuckLakeTableSchemaVersions GetTableSchemaVersions(TableIndex table_id, DuckLakeSnapshot snapshot);
	//! The net row count of the data files that were committed before the snapshot's next file id
	virtual idx_t GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot);
	optional_idx GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot);
	virtual idx_t GetNetInlinedRowCount(const string &inlined_table_name, DuckLakeSnapshot snapshot);
	//! SQL builders for stats-refresh metadata lookups; caller substitutes placeholders + executes.
	//! With require_exact, the count is NULL if any visible file is only partially visible to the snapshot.
	//! With exclude_new_files, the files of the commit, which can already be in the catalog, are not counted.
	static string GetNetDataFileRowCountSql(TableIndex table_id, const string &inlined_deletion_table,
	                                        bool require_exact = false, bool exclude_new_files = false);
	static string GetNetInlinedRowCountSql(const string &inlined_table_name, const DuckLakeInlinedColNames &col_names);
	//! The rows that are stored, deleted ones included, and the rows that are live
	static string GetInlinedRowCountsSql(const string &inlined_table_name, const DuckLakeInlinedColNames &col_names);
	static string GetTableColumnSchemaSql(TableIndex table_id);
	static string GetInlinedTableNamesSql(TableIndex table_id);
	static string GetInlinedTablesBeforeSchemaChangeSql(TableIndex table_id);
	//! The top-level columns of each inlined data table at the schema version of that table
	static string GetInlinedTableColumnsSql(optional_idx table_id = optional_idx());
	//! The inserts of the given rows, in batches
	static string InsertValuesSql(const string &table_name, const vector<string> &values);
	//! The inserts of the given rows into the given table, in batches
	static string InsertValuesIntoSql(const string &qualified_table_name, const vector<string> &values);
	unordered_set<string> GetInlinedTableNames(TableIndex table_id);
	virtual vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &filter);
	virtual vector<DuckLakeFileForCleanup> GetOrphanFilesForCleanup(const string &filter);
	virtual vector<DuckLakeFileForCleanup> GetFilesForCleanup(const string &filter, CleanupType type);

	virtual void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files);
	static string DropSchemas(const set<SchemaIndex> &ids);
	static string DropTables(const set<TableIndex> &ids, bool renamed);
	static string DropViews(const set<TableIndex> &ids, bool renamed, bool drop_view_column_tags = false);
	static string DropMacros(const set<MacroIndex> &ids);

	//! Emits the INSERT for new schemas. Caller supplies resolved paths (one per schema, same order)
	//! since path resolution depends on the catalog's data_path / separator (instance state).
	static string WriteNewSchemas(const vector<DuckLakeSchemaInfo> &new_schemas,
	                              const vector<DuckLakePath> &resolved_paths, bool supports_v1_1_metadata);
	//! Emits the INSERT for new tables and their columns. Caller supplies resolved paths (one per
	//! table, same order). commit_snapshot is currently unused by the body — kept off the signature.
	static string WriteNewTables(const vector<DuckLakeTableInfo> &new_tables,
	                             const vector<DuckLakePath> &resolved_paths);
	static string WriteNewViews(const vector<DuckLakeViewInfo> &new_views);
	//! Emits the partition-key diff SQL. Caller supplies the existing partition state (fetched
	//! via GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewPartitionKeys(const vector<DuckLakePartitionInfo> &existing_partitions,
	                                    const vector<DuckLakePartitionInfo> &new_partitions);
	//! Maps the id of a last new spec that equals the committed spec, which is not written, to the committed id
	static unordered_map<idx_t, idx_t>
	GetUnchangedPartitionIds(const vector<DuckLakePartitionInfo> &existing_partitions,
	                         const vector<DuckLakePartitionInfo> &new_partitions);
	//! Emits the sort-key diff SQL. Caller supplies the existing sort state (fetched via
	//! GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewSortKeys(const vector<DuckLakeSortInfo> &existing_sorts,
	                               const vector<DuckLakeSortInfo> &new_sorts);
	static string WriteDroppedColumns(const vector<DuckLakeDroppedColumn> &dropped_columns);
	//! Formats a column set as a SQL list of (table_id, column_id) rows
	static string WriteColumnIdPairs(const map<TableIndex, set<FieldIndex>> &columns);
	//! Rewrite the bounds of committed files and tables written while a widened column was FLOAT
	static string WriteFloatWidenedStats(const map<TableIndex, set<FieldIndex>> &widened_columns,
	                                     const map<TableIndex, set<FieldIndex>> &cleared_columns, idx_t next_file_id);
	//! Clear the bounds of added files of the columns retyped to a finer timestamp unit, and of their tables
	static string WriteRefinedTimestampStats(const map<TableIndex, set<FieldIndex>> &refined_columns,
	                                         idx_t next_file_id);
	static string WriteExpiredColumnTags(const vector<DuckLakeDroppedColumn> &dropped_columns);
	static string WriteNewColumns(const vector<DuckLakeNewColumn> &new_columns);
	//! Columns added and dropped in the same commit, which begin and end in its snapshot
	static string WriteDroppedNewColumns(const vector<DuckLakeNewColumn> &columns);
	static string WriteNewTags(const vector<DuckLakeTagInfo> &new_tags);
	static string WriteNewTableOptions(const vector<DuckLakeConfigOption> &new_options);
	static string WriteNewColumnTags(const vector<DuckLakeColumnTagInfo> &new_tags);
	static string WriteNewViewColumnTags(const vector<DuckLakeViewColumnTagInfo> &new_tags);
	//! SQL that writes new data files, shared with the server-side commit path. Returns SQL with
	//! {METADATA_CATALOG} / {SNAPSHOT_ID} placeholders. Caller supplies resolved paths (one per file,
	//! same order) since path policy differs across callers (schema-relative vs. always-absolute).
	static string WriteNewDataFilesSqlBatch(const vector<DuckLakeFileInfo> &new_files,
	                                        const vector<DuckLakePath> &resolved_paths, bool supports_v1_1_metadata);
	//! Opt-in fast-path: if this backend supports the DuckDB Appender API, write the files directly
	bool TryAppendDataFiles(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                        const vector<DuckLakeTableInfo> &new_tables,
	                        vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
	                                   const vector<DuckLakeInlinedDataInfo> &new_data,
	                                   const vector<DuckLakeTableInfo> &new_tables,
	                                   const vector<DuckLakeTableInfo> &new_inlined_data_tables_result,
	                                   vector<unique_ptr<SQLStatement>> &inlined_inserts);
	static string WriteNewInlinedDeletes(const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes,
	                                     const DuckLakeInlinedColNames &col_names);
	//! Creates the INSERT INTO {METADATA_CATALOG}.<inlined_table_name> VALUES (...) batch.
	static string FormatInlinedDataInsert(const string &inlined_table_name, idx_t row_id_start,
	                                      bool has_preserved_row_ids, const vector<int64_t> *row_ids,
	                                      const vector<string> &cells_per_row);
	//! Static deterministic name of the per-table inlined deletion table.
	static string InlinedFileDeletionTableName(TableIndex table_id);
	//! Pure SQL builder for inlined file deletions (CREATE TABLE IF NOT EXISTS + INSERT). Sets
	//! created_new_table when a new ducklake_inlined_delete_<id> table was emitted.
	static string WriteNewInlinedFileDeletesSql(const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes,
	                                            bool &created_new_table);
	//! SQL branch of WriteNewInlinedFileDeletes — returns the CREATE/INSERT statements and marks the
	//! metadata-manager cache for clearing when a new per-table deletion table is created.
	string WriteNewInlinedFileDeletesSqlBatch(const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes);
	//! Get the name of the inlined deletion table for a given table ID
	virtual string GetInlinedDeletionTableName(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                           bool include_local_changes = true);
	//! Probe for the physical inlined-deletion table without aborting the active metadata transaction.
	virtual bool InlinedDeletionTableExists(const string &table_name);
	//! Also drops the empty inlined data tables of the replaced tables
	virtual string WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &tables,
	                                     const set<TableIndex> &replaced_tables);
	//! Drop the current inlined data table of a table if it holds no rows
	string DropInlinedTableIfEmpty(TableIndex table_id, const DuckLakeTableEntry &table);
	//! A query that returns a row if the inlined data table holds any row
	static string InlinedTableHasRowsQuery(const string &table_name);
	static string DropInlinedTableSql(idx_t table_id, idx_t schema_version, const string &table_name);
	virtual string GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
	                                      vector<string> &inlined_tables, string &inlined_table_queries);
	static string InlinedTableNameFor(idx_t table_id, idx_t schema_version);
	static string InlinedTableDdlSql(const string &table_name, const string &column_defs,
	                                 const DuckLakeInlinedColNames &col_names);
	DuckLakeInlinedColNames InlinedColNames() const;
	static string InlinedTableRegistrationTuple(idx_t table_id, const string &table_name, idx_t schema_version);
	static string LatestInlinedTableQuery(idx_t table_id);
	static string DropDataFiles(const set<DataFileIndex> &dropped_files);
	//! Selects the given data files that are still in the metadata
	static string GetExistingDataFilesSql(const set<DataFileIndex> &files);
	//! Selects the given columns that a live data file of the table records values of, or holds the default of
	static string GetColumnsWithRecordedValuesSql(TableIndex table_id, const set<FieldIndex> &column_ids);
	//! Selects the given columns that may hold values other than NULL in a live data file of the table
	static string GetColumnsWithFileValuesSql(TableIndex table_id, const set<FieldIndex> &column_ids,
	                                          const map<FieldIndex, idx_t> &null_default_columns);
	//! Selects the versions of the given columns with their begin snapshot, whether their initial default is NULL and
	//! whether they are top level columns
	static string GetColumnDefaultsSql(TableIndex table_id, const set<FieldIndex> &column_ids);
	//! Selects the number of live inlined values of each check, the checks of a table are consecutive
	static string GetInlinedValuesSql(const vector<DuckLakeInlinedValueCheck> &checks,
	                                  const DuckLakeInlinedColNames &col_names);
	//! Selects the given tables or views renamed after the snapshot
	static string GetRenamedAfterSnapshotSql(const string &metadata_table_name, const string &id_name,
	                                         const set<TableIndex> &ids);
	//! Caller supplies one resolved path per overwritten file, in the same order.
	static string DeleteOverwrittenDeleteFiles(const vector<DuckLakeOverwrittenDeleteFile> &overwritten_files,
	                                           const vector<DuckLakePath> &resolved_paths);
	//! Caller supplies one resolved path per new delete file, in the same order.
	static string WriteNewDeleteFiles(const vector<DuckLakeDeleteFileInfo> &new_delete_files,
	                                  const vector<DuckLakePath> &resolved_paths, bool write_row_group_count);
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
	static string UpdateGlobalTableStatsSql(const DuckLakeGlobalStatsInfo &stats, bool write_stats_exactness);
	static string InsertTableColumnStatsSql(const DuckLakeGlobalStatsInfo &stats, bool write_stats_exactness);
	//! Set the stored table sizes to the size of the live data files
	static string RefreshTableSizesSql(const set<TableIndex> &table_ids);
	static SnapshotChangeInfo
	GetSnapshotAndStatsAndChanges(SnapshotAndStats &current_snapshot,
	                              const std::function<unique_ptr<QueryResult>(string)> &executor, const string &query);
	static string SnapshotAndStatsAndChangesQuery(bool include_exactness);
	//! The query of the conflict check, which must read the latest snapshot and the table stats consistently
	virtual string GetSnapshotAndStatsAndChangesQuery(DuckLakeSnapshot snapshot, bool include_exactness) const;
	static SnapshotChangeInfo ParseSnapshotAndStatsAndChanges(QueryResult &result, SnapshotAndStats &current_snapshot);
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot();
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound,
	                                                 idx_t newest_snapshot_id);

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
	                                                           const DuckLakeTableEntry &table,
	                                                           const string &sort_order_sql,
	                                                           const vector<string> &columns_to_read);
	//! SQL builders for the stats-refresh queries used by DuckLakeTransactionState::RecomputeGlobalStatsAfterRewrite.
	//! Caller substitutes `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` and executes via the commit context's executor.
	static string ReadInlinedDataAggregatesSql(const string &inlined_table_name, const string &select_list,
	                                           const DuckLakeInlinedColNames &col_names);
	//! The column stats of the data files committed before the snapshot's next file id
	static string ReadFileColumnStatsForTableSql(TableIndex table_id, bool include_exactness);
	//! Throws on a failed inlined data read, hinting at the migration for legacy named catalogs
	void CheckInlinedDataReadError(QueryResult &result, const string &inlined_table_name);
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types,
	                                                     const string &inlined_table_name);

	//! The statements deleting the inlined rows and inlined file deletions that the transaction flushed
	static string GenerateDeleteFlushedInlinedData(const vector<FlushedInlinedTableInfo> &flushed_tables,
	                                               const map<TableIndex, idx_t> &flushed_file_deletions,
	                                               const DuckLakeInlinedColNames &col_names);
	static string InsertNewSchema(const DuckLakeSnapshot &snapshot, const set<TableIndex> &table_ids);

	virtual vector<DuckLakeSnapshotInfo> GetAllSnapshots(const string &filter = string());
	virtual void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	virtual vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot);
	//! The tables with data files that are not dropped
	unordered_set<idx_t> GetTablesWithLiveDataFiles();
	virtual void SetConfigOption(const DuckLakeConfigOption &option);
	virtual bool ResetConfigOption(const DuckLakeConfigOption &option);
	virtual void MigrateV01();
	virtual void MigrateV02(bool allow_failures = false);
	virtual void MigrateV03(bool allow_failures = false);
	virtual void MigrateV04();
	virtual void MigrateV10(bool from_dev1 = false);
	//! Whether the catalog already has every metadata table and column of v1.1
	bool HasV1_1Additions();
	//! Renames inlined metadata columns to the prefixed variants, skipping already renamed tables
	virtual void MigrateInlinedColumnNames(bool probe_renamed);
	//! Renames the given (old name, new name) columns of each inlined table
	virtual void RenameInlinedColumns(const map<string, vector<pair<string, string>>> &renames);
	//! Adds the schema version rows of inlined tables that older versions registered without one
	void MigrateInlinedSchemaVersions();
	//! Restores the defaults that older versions dropped when they renamed a struct field
	void MigrateRenamedFieldDefaults();
	//! Clears the bounds that older versions may have stored in the type of a file or of an earlier column type
	void MigrateRetypedBounds();
	//! Rewrites inlined tables whose columns were created with the storage types of an older DuckLake version
	virtual void MigrateInlinedDataTypes() {
	}
	virtual void ExecuteMigration(string migrate_query, bool allow_failures, const string &from_version,
	                              const string &to_version);

	string LoadPath(string path);
	string StorePath(string path);
	string GetPathSeparator(const string &path);

protected:
	enum class FileListType : uint8_t { SCAN, EXTENDED };
	enum class StatsCastType : uint8_t { ORDERING, MIN, MAX };

	virtual string GetLatestSnapshotQuery() const;

	virtual string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id,
	                                              const string &metadata_table_prefix);
	//! The snapshot placeholders of the query are replaced later, unless an override does it
	virtual string GenerateFileListQuery(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                     const FilterPushdownInfo *filter_info,
	                                     const vector<DuckLakeFileListDynamicFilter> &dynamic_filters,
	                                     const vector<idx_t> &runtime_filter_stats_columns,
	                                     FileListType file_list_type = FileListType::SCAN,
	                                     const string &metadata_table_prefix = "{METADATA_CATALOG}");

	//! Wrap field selections with list aggregation of struct objects (DBMS-specific)
	//! For DuckDB: LIST({'key1': val1, 'key2': val2, ...})
	//! For Postgres: jsonb_agg(jsonb_build_object('key1', val1, 'key2', val2, ...))
	static string ListAggregation(const vector<pair<string, string>> &fields, const string &order = string());
	//! Parse tag list from ListAggregation value
	static vector<DuckLakeTag> LoadTags(const Value &tag_map);
	static vector<DuckLakeViewColumnTag> LoadViewColumnTags(const Value &list);
	//! Parse inlined data tables list from ListAggregation value
	static vector<DuckLakeInlinedTableInfo> LoadInlinedDataTables(const Value &list);
	//! Parse macro implementations list from ListAggregation value
	static vector<DuckLakeMacroImplementation> LoadMacroImplementations(const Value &list);

public:
	//! Get path relative to catalog path
	DuckLakePath GetRelativePath(const string &path);
	//! Get path relative to table path
	DuckLakePath GetRelativePath(TableIndex table_id, const string &path, const vector<DuckLakeTableInfo> &new_tables,
	                             vector<DuckLakeSchemaInfo> &new_schemas_result);

	static string StorePath(string path, const string &separator);
	static string LoadPath(string path, const string &separator);
	static string FromRelativePath(const DuckLakePath &path, const string &base_path, const string &separator);
	static DuckLakePath GetRelativePath(const string &path, const string &data_path, const string &separator);
	static string GetPathForSchema(SchemaIndex schema_id, const vector<DuckLakeSchemaInfo> &new_schemas_result,
	                               const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                               const string &base_data_path, const string &separator);
	static string GetPathForTable(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	                              const std::function<unique_ptr<QueryResult>(string)> &query_executor,
	                              const string &base_data_path, const string &separator);

protected:
	string GetInlinedTableQuery(const DuckLakeTableInfo &table, const string &table_name);
	bool CanInlineColumn(const string &name, const LogicalType &type);
	string GetInliningObstacle(const string &name, const LogicalType &type);
	string GetColumnType(const DuckLakeColumnInfo &col);
	string GetColumnDefinitions(const vector<DuckLakeColumnInfo> &columns);
	string GetKnownFilesForCleanupQuery() const;

	//! Optimized data file writing using DuckDB Appender API (DuckDB and SQLite metadata managers)
	string WriteNewDataFilesWithAppender(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                                     const vector<DuckLakeTableInfo> &new_tables,
	                                     vector<DuckLakeSchemaInfo> &new_schemas_result);
	DuckLakePath GetRelativePath(const string &path, const string &data_path);
	string FromRelativePath(const DuckLakePath &path, const string &base_path);
	string FromRelativePath(const DuckLakePath &path);
	string FromRelativePath(TableIndex table_id, const DuckLakePath &path);
	string GetPath(TableIndex table_id, const vector<DuckLakeTableInfo> &new_tables,
	               const vector<DuckLakeSchemaInfo> &new_schemas_result);
	FileSystem &GetFileSystem();

private:
	template <class T>
	static string FlushDrop(const string &metadata_table_name, const string &id_name, const set<T> &dropped_entries);
	DuckLakeFileData ReadFile(DuckLakeTableEntry &table, const QueryResultRow &row, idx_t &col_idx, bool is_encrypted);
	DuckLakeFileData ReadDataFile(DuckLakeTableEntry &table, const QueryResultRow &row, idx_t &col_idx,
	                              bool is_encrypted);
	DuckLakeFileData ReadDeleteFile(DuckLakeTableEntry &table, const QueryResultRow &row, idx_t &col_idx,
	                                bool is_encrypted);

	bool IsEncrypted() const;

protected:
	string GetFileSelectList(const string &prefix);
	string GetDataFileSelectList(const string &prefix);
	string GetDeleteFileSelectList(const string &prefix);
	//! Build an additional WHERE fragment that prunes files by bucket() partition value.
	//! Returns "" when no foldable equality / IN-list predicate exists on a bucket-partitioned column.
	//! The fragment uses only string equality against ducklake_file_partition_value, so it works against
	//! any metadata backend (DuckDB / Postgres / SQLite). Bucket hashes are pre-computed in C++.
	string BuildBucketPartitionPruningClause(
	    DuckLakeTableEntry &table, const FilterPushdownInfo &filter_info,
	    const string &partition_value_table = "{METADATA_CATALOG}.ducklake_file_partition_value");
	//! Emit the condition for a single-column filter, registering the stats its CTE must project
	string GenerateColumnFilterCondition(const ColumnFilterInfo &column_filter, FilterSQLResult &result);
	//! Emit the condition for a filter tree, combining the per-column conditions of its leaves.
	//! A node under a parent of the same conjunction type is spliced in without parens of its own.
	string GenerateFilterTreeCondition(const DuckLakeFilterNode &node, FilterSQLResult &result,
	                                   bool splice_into_parent = false);
	virtual FilterSQLResult ConvertFilterPushdownToSQL(const FilterPushdownInfo &filter_info);
	string GenerateCTESectionFromRequirements(const map<idx_t, CTERequirement> &requirements, TableIndex table_id,
	                                          const string &metadata_table_prefix);
	//! Join each column's stats CTE once. Leading newline per join, empty when there are none.
	static string GenerateStatsJoinList(const map<idx_t, CTERequirement> &requirements);
	virtual bool ValueIsFinite(const Value &val);
	//! Unknown bounds must keep the file
	static string BoundOrInfinity(const string &bound, const string &type_name, StatsCastType cast_type);

private:
	virtual string GenerateFilterFromExpression(const Expression &expr, const LogicalType *type,
	                                            unordered_set<string> &referenced_stats, const string &stats_alias);
	virtual string CastValueToTarget(const Value &val, const LogicalType &type);
	virtual string CastStatsToTarget(const string &stats, const LogicalType &type,
	                                 StatsCastType cast_type = StatsCastType::ORDERING);
	virtual string GenerateConstantFilter(ExpressionType comparison_type, const Value &constant,
	                                      const LogicalType &type, unordered_set<string> &referenced_stats,
	                                      const string &stats_alias);
	virtual string GenerateConstantFilterDouble(ExpressionType comparison_type, const Value &constant,
	                                            const LogicalType &type, unordered_set<string> &referenced_stats,
	                                            const string &stats_alias);
	virtual string GenerateFilterPushdown(const ExpressionFilter &filter, unordered_set<string> &referenced_stats,
	                                      const string &stats_alias);

public:
	//! Read inlined file deletions for regular table scans (no snapshot info per row)
	map<idx_t, set<idx_t>> ReadInlinedFileDeletions(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                                bool include_local_changes = true);
	//! Clear inlined table caches (needed after rollback so retry re-creates the tables)
	void ClearInlinedTableCaches();

private:
	static unordered_map<string /* name */, create_t> metadata_managers;
	static mutex metadata_managers_lock;

	unordered_set<idx_t> GetFileIdsWithInlinedDeletions(TableIndex table_id, DuckLakeSnapshot snapshot);
	unique_ptr<QueryResult> ReadInlinedDataChanges(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
	                                               const string &inlined_table_name,
	                                               const vector<string> &columns_to_read,
	                                               const string &snapshot_column);

	unordered_map<idx_t, string> insert_inlined_table_name_cache;
	unordered_set<idx_t> delete_inlined_table_cache;

protected:
	DuckLakeTransaction &transaction;
	mutex paths_lock;
	map<TableIndex, string> table_paths;
	bool pending_cache_clear = false;
};

} // namespace duckdb
