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
#include "duckdb/common/set.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/types/value.hpp"
#include "common/ducklake_branch_chain.hpp"
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
struct DuckLakeMetadataProfile;
struct TransactionChangeInformation;
class BoundAtClause;
class Connection;
class SQLStatement;
class FileSystem;
struct CreateTableInfo;
struct AlterInfo;
class DuckLakeViewEntry;
class MacroCatalogEntry;

struct SnapshotAndStats;
struct FlushedInlinedTableInfo;

//! How an AT clause picks a snapshot: the bounds of a change feed take the nearest snapshot after or before a
//! timestamp, time travel takes the newest snapshot at or before it and refuses times before expired snapshots
enum class SnapshotBound { LOWER_BOUND, UPPER_BOUND, TIME_TRAVEL };

//! A check whether the live rows of an inlined data table hold a value of a column
struct DuckLakeInlinedValueCheck {
	Identifier table_name;
	//! The rows up to this snapshot are flushed by the commit
	idx_t flushed_up_to;
	FieldIndex column_id;
	//! The name of the column in the inlined data table, empty when any live row counts as a value
	Identifier column_name;
};

//! Metadata column names inside ducklake_inlined_data_<table_id>_<schema_version> tables.
struct DuckLakeInlinedColNames {
	//! Column name prefix reserved for DuckLake internal use
	static constexpr const char *PREFIX = "_ducklake_";
	//! Number of metadata columns an inlined data table adds
	static constexpr idx_t COLUMN_COUNT = 3;

	explicit DuckLakeInlinedColNames(bool prefixed_inlined_columns) : prefixed(prefixed_inlined_columns) {
		if (prefixed_inlined_columns) {
			row_id = Identifier(PREFIX + row_id);
			begin_snapshot = Identifier(PREFIX + begin_snapshot);
			end_snapshot = Identifier(PREFIX + end_snapshot);
			branch_id = Identifier(PREFIX + branch_id);
		}
	}

	//! Whether a user column name collides with the metadata columns written for inlining
	bool ConflictsWith(const Identifier &name) const;
	//! The names the 2.0 readers give the joined companion columns, reserved like the metadata columns
	static const vector<Identifier> &DerivedColumnNames();

	Identifier row_id = "row_id";
	Identifier begin_snapshot = "begin_snapshot";
	//! The end column of the range rows before 2.0, also the alias of the end the 2.0 readers derive
	Identifier end_snapshot = "end_snapshot";
	//! The birth branch of a row version from 2.0 on
	Identifier branch_id = "branch_id";
	//! Whether the names carry the prefix, which 1.1 introduced
	bool prefixed;
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
	//! The columns with a filter the conditions do not express completely
	set<idx_t> unexpressed_columns;

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

//! An order of the files: by the minimum or by the maximum of a column in the files
struct DuckLakeFileListOrder {
	idx_t column_field_index;
	//! Order by the maximum of the column in the file instead of the minimum
	bool use_max;
	bool descending;
	LogicalType column_type;
};

struct FilterPushdownInfo {
	unordered_map<idx_t, ColumnFilterInfo> column_filters;
	//! Filter trees, which must hold alongside the single-column filters above. Usually cross-column -
	//! a single-column tree is only kept when it expresses something the per-column filters cannot.
	vector<DuckLakeFilterTree> filter_trees;
	//! Columns whose file min/max statistics are returned with the files, for filters evaluated at runtime
	vector<idx_t> runtime_filter_columns;
	//! The order of the files, e.g. to make the dynamic filters of a top-N prune early
	vector<DuckLakeFileListOrder> file_order;

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
		result->runtime_filter_columns = runtime_filter_columns;
		result->file_order = file_order;
		return result;
	}
};

//! The DuckLake metadata manger is the communication layer between the system and the metadata catalog
class DuckLakeMetadataManager {
public:
	explicit DuckLakeMetadataManager(DuckLakeTransaction &transaction);
	virtual ~DuckLakeMetadataManager();

	typedef unique_ptr<DuckLakeMetadataManager> (*create_t)(DuckLakeTransaction &transaction);
	static void Register(const string &name, create_t);
	//! Whether a metadata manager of the given name is built in, so that no profile can be registered for it
	static bool HasBuiltInManager(const string &name);

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction);
	//! The profile of the metadata database, which the defaults of the metadata manager follow
	const DuckLakeMetadataProfile &GetMetadataProfile() const;

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
	//! Whether the metadata is served remotely (e.g. over REST) rather than stored in an attached database
	virtual bool IsRemote() const {
		return false;
	}
	//! Whether the file lists read the row counts and the row group counts of the files, which REST responses hold
	void SetReadFileCounts(bool read_file_counts_p) {
		read_file_counts = read_file_counts_p;
	}
	//! Whether the backend has changes of the transaction that are only written when the transaction commits
	virtual bool HasPendingChanges() const {
		return false;
	}
	//! Invoked when the transaction creates a schema or table, for backends that replay catalog changes on commit
	virtual void OnCreateSchema(DuckLakeSchemaEntry &schema) {
	}
	virtual void OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info,
	                           const DuckLakeTableEntry &table) {
	}
	virtual void OnCreateView(DuckLakeViewEntry &view) {
	}
	virtual void OnCreateMacro(MacroCatalogEntry &macro) {
	}
	virtual void OnAlterEntry(CatalogEntry &entry, const AlterInfo &info) {
	}
	virtual void OnDropEntry(CatalogEntry &entry) {
	}
	//! Invoked with the new version of an entry that the transaction created or altered
	virtual void OnEntryChanged(CatalogEntry &entry) {
	}
	virtual void OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path, idx_t row_count,
	                        idx_t file_size_bytes) {
	}
	//! Whether metadata reads inside a transaction can show commits of other transactions
	virtual bool ShowsConcurrentCommits() const {
		return CommitsEachStatement();
	}
	//! Whether transactions that started before a metadata table was dropped still read it
	virtual bool DroppedTablesStayReadable() const;
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
	virtual bool IsRetryableCommitError(const string &message) const;
	//! The error of a failed commit without the text of the failed query that it repeats
	virtual string CommitErrorWithoutQuery(const string &message) const {
		return message;
	}
	//! Advice added to the error of a commit that failed for good
	virtual string CommitErrorHint(const string &message) const;
	//! Whether the catalog holds any branch besides main, memoized while metadata reads are repeatable
	bool HasBranches();
	//! Drops the cached file listings, called when the transaction writes metadata
	void InvalidateFileListCache();
	//! Reads the transaction scoped inlined deletions and attaches them to the listed files
	void AttachInlinedFileDeletions(TableIndex table_id, DuckLakeSnapshot snapshot, bool include_local_changes,
	                                vector<DuckLakeFileListEntry> &files);
	//! Runs the deletion scan of GetTableDeletions, which caches it
	vector<DuckLakeDeleteScanEntry> ReadTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                                   DuckLakeSnapshot end_snapshot, bool include_local_changes);
	//! SQL that the commit of a transaction that changed options runs, so two commits cannot both insert an option
	virtual string ConfigOptionLockSql() const {
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
	virtual idx_t MaxColumnCount() const;
	//! Names of the system columns that the tables of this backend have
	virtual vector<string> SystemColumnNames() const;

	//! Why the rows of a table with the given columns cannot be inlined, empty when they can
	string GetInliningObstacle(const ColumnList &columns);
	//! Whether the inlined data table of a table with this many columns fits the backend
	bool InlinedTableFits(idx_t column_count) const;
	bool CanInlineColumns(const vector<DuckLakeColumnInfo> &columns);

	virtual string GetColumnTypeInternal(const LogicalType &column_type);
	string CastColumnToTarget(const string &column, const LogicalType &type);
	//! The inlined rows to flush with typed columns, without those this transaction deleted
	string InlinedFlushSource(const Identifier &inlined_table_name, const DuckLakeTableEntry &table);
	//! The versions of an inlined table under the alias inlined_data born in the view of the attached chain
	string InlinedBirthView(const DuckLakeInlinedColNames &col_names, const string &read_snapshot) const;
	//! The order of the rows in a flushed file
	string InlinedFlushOrder(const string &sort_order_sql) const;

	virtual unique_ptr<QueryResult> AttachMetadata(const string &attach_query);

	virtual bool MetadataExists();

	virtual string MetadataExistsQuery() const;

	//! Initialize a new DuckLake
	virtual void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption);
	//! Get the CREATE TABLE statements for all metadata tables
	virtual string GetCreateTableStatements();
	static string GetSnapshotTableColumns();
	//! The snapshot tables keyed by branch and snapshot id, shared by the 2.0 manager and the heal
	static string BranchedSnapshotTableColumns();
	static string BranchedSnapshotChangesTableColumns();
	virtual string GetSnapshotTableStatement();
	virtual string GetSnapshotChangesTableStatement();
	virtual string GetSchemaVersionsTableStatement();
	//! Rows inserted after the metadata tables are created
	virtual string GetInitialDataStatements();
	virtual string GetSchemaTableStatement();
	virtual string GetTableTableStatement();
	virtual string GetViewTableStatement();
	virtual string GetColumnTableStatement();
	virtual string GetPartitionInfoTableStatement();
	virtual string GetMacroTableStatement();
	virtual string GetSortInfoTableStatement();
	virtual string GetTagTableStatement();
	virtual string GetColumnTagTableStatement();
	virtual string GetViewColumnTagTableStatement();
	//! A table whose version rows end in a companion table, with the key columns naming an entity
	struct RowVisibilityKind {
		RowVisibilityKind() = default;
		RowVisibilityKind(string kind, vector<string> key_columns);

		//! The companion is ducklake_<kind>_visibility, the row table is RowTableName(kind)
		string kind;
		//! The row table when it is not named after the kind, the payload of the inlined rows
		string row_table;
		//! The key columns of the companion
		vector<string> key_columns;
		//! The constant of a key column in every statement of the kind, empty for a column of the row table
		vector<string> key_literals;
		//! The key columns of the row table when named unlike the companion's, which the inlined rows prefix
		vector<string> payload_key_columns;
		//! The row table column of the key column at the index
		const string &PayloadKey(idx_t i) const;
		//! Whether the key column at the index is a constant of the kind
		bool IsKeyLiteral(idx_t i) const;
		//! The birth columns of the row table
		string branch_column = "branch_id";
		string begin_column = "begin_snapshot";
		//! The end column of the range rows before the kind took the companion
		string end_column = "end_snapshot";
		//! The prefix of the companion columns the live row join projects, so they never shadow a row column
		string companion_alias_prefix;
		//! When set the live row join also projects the ending snapshot under this alias
		string end_snapshot_alias;
		//! Whether the heal keeps the rows superseded in their birth transaction, which the inlined writers never bear
		bool skip_superseded_deletes = false;
	};
	static const vector<RowVisibilityKind> &RowVisibilityKinds();
	//! The catalog kind with the name
	static const RowVisibilityKind &RowKind(const string &kind);
	//! The kind of an inlined data table, whose shared companion names the row versions by table id and row id
	static RowVisibilityKind InlinedRowKind(const Identifier &table_name, const DuckLakeInlinedColNames &col_names);
	//! The row table of a kind, the partition and sort specs keep their info suffix
	static string RowTableName(const string &kind);
	static string RowTableName(const RowVisibilityKind &kind);
	//! The key column at the index as the row alias reads it, the literal of a constant key
	static string KeyValue(const RowVisibilityKind &kind, idx_t i, const string &row_alias);
	//! The type of a key column, the tag keys and names are text
	static string RowKeyType(const string &key);
	//! Matches a key column of two aliases, text keys compare byte for byte like the tag writers
	static string RowKeyMatch(const string &left, const string &right, const string &key);
	//! Matches the key, projected as left_column under the left alias, with the row column of the right alias
	static string RowKeyMatch(const string &left, const string &left_column, const string &right,
	                          const string &right_column, const string &key);
	//! The companion table holding the end events of the version rows of a catalog kind
	static string GetRowVisibilityTableStatement(const RowVisibilityKind &kind, bool if_not_exists = false);
	//! The companion of every inlined data table, keyed by the table and the birth of the row version
	static string GetInlinedVisibilityTableStatement();
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
	//! The snapshot time of a commit, the current time when its batch runs
	virtual string SnapshotTimeSql() const;

public:
	static void SubstituteSnapshotPlaceholders(const DuckLakeSnapshot &snapshot, string &query);
	//! Pure SQL templates (use `{METADATA_CATALOG}` placeholder) — caller substitutes + executes.
	//! Both used by the regular metadata-manager methods and by server-side commit, which runs the
	//! SQL on a fresh Connection without going through the metadata-manager wrapper.
	static string LatestSnapshotQuery();
	//! With table ids only those tables' stats are read
	static string GlobalTableStatsQuery(bool include_exactness, const vector<TableIndex> &table_ids = {});
	//! The filter that selects the head of the snapshots the predicate matches
	static string HeadSnapshotFilter(const string &head_predicate);
	//! The head of the snapshots the predicate selects with the catalog version, its branch and the newest snapshot
	//! of each given branch
	static string HeadSnapshotQuery(const string &head_predicate, const vector<idx_t> &lineage_branches = {});
	//! Pure parsers for the results of the above queries.
	static unique_ptr<DuckLakeSnapshot> ParseSnapshot(QueryResult &result,
	                                                  optional_ptr<string> catalog_version = nullptr,
	                                                  optional_ptr<vector<idx_t>> lineage_heads = nullptr);
	//! The newest snapshot of every branch of the chain as of the last snapshot read, root first
	const vector<idx_t> &GetLineageHeads() const {
		return lineage_heads;
	}
	//! Drops the heads when the transaction releases the metadata view that read them
	void ClearLineageHeads() {
		lineage_heads.clear();
	}
	static vector<DuckLakeGlobalStatsInfo> ParseGlobalTableStats(QueryResult &result);
	//! Take the table sizes that stats rows lack from the live data files
	static void FillMissingTableSizes(vector<DuckLakeGlobalStatsInfo> &stats,
	                                  const std::function<unique_ptr<QueryResult>(string)> &executor, bool branch_aware,
	                                  const string &chain_predicate);
	//! Whether the result contains a column with the given name
	static bool ResultHasColumn(QueryResult &result, const string &name);

	//! A list aggregate column of a catalog load statement with the type the parser reads it as
	struct CatalogListColumn {
		string name;
		LogicalType type;
	};
	//! The SQL of a catalog load, DuckDB SQL run as rendered unless a backend supplies its own
	struct CatalogLoadDialect {
		std::function<string(const vector<pair<string, string>> &, const string &)> render_list_aggregation;
		std::function<string(const string &, const vector<CatalogListColumn> &)> wrap_statement;

		//! The aggregate of the fields as a list of structs, in the order when one is given
		string ListAggregation(const vector<pair<string, string>> &fields, const string &order = string()) const;
		//! The statement the executor runs, which returns the list columns with the given types
		string Statement(const string &statement, const vector<CatalogListColumn> &list_columns) const;
	};
	//! Get the catalog information for a specific snapshot
	virtual DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot);
	//! Transaction-free build of the catalog snapshot. Caller supplies a snapshot-aware query
	//! executor (responsible for `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` substitution) plus the
	//! data path and separator used for resolving stored relative paths and the chain the rows are read through.
	static DuckLakeCatalogInfo
	BuildCatalogForSnapshot(DuckLakeSnapshot snapshot,
	                        const std::function<unique_ptr<QueryResult>(DuckLakeSnapshot, string)> &query_executor,
	                        const string &base_data_path, const string &separator, bool supports_v1_1_metadata,
	                        bool supports_v2_0_metadata, const DuckLakeBranchChain &chain,
	                        const CatalogLoadDialect &dialect = CatalogLoadDialect());
	//! The global stats of every table of the snapshot, or of the given tables
	virtual vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot,
	                                                            const vector<TableIndex> &table_ids = {});
	//! Get the record count of every table that has global stats
	virtual map<TableIndex, idx_t> GetTableRecordCounts(DuckLakeSnapshot snapshot);
	//! The projection lists the top-level columns that the scan reads, all of them if it is not set
	virtual vector<DuckLakeFileListEntry>
	GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                 const FilterPushdownInfo *filter_info = nullptr, bool include_local_changes = true,
	                 optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr);
	virtual vector<DuckLakeFileListEntry>
	GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot,
	                   optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr);
	virtual vector<DuckLakeDeleteScanEntry>
	GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot,
	                  bool include_local_changes = true,
	                  optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr);
	//! The snapshots of the range that rewrote data files of the table without changing its rows
	set<idx_t> GetTableRewriteSnapshots(TableIndex table_id, DuckLakeSnapshot start_snapshot,
	                                    DuckLakeSnapshot snapshot);
	virtual vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr,
	                         optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr);
	//! Sets the schema version of the partition spec of the files written with an older spec
	void SetPartitionSchemaVersions(DuckLakeTableEntry &table, vector<DuckLakeCompactionFileEntry> &files);
	virtual vector<DuckLakeCompactionFileEntry> GetFilesForCompaction(DuckLakeTableEntry &table, CompactionType type,
	                                                                  double deletion_threshold,
	                                                                  DuckLakeSnapshot snapshot,
	                                                                  DuckLakeFileSizeOptions options);
	virtual idx_t GetBeginSnapshotForTable(TableIndex table_id);
	virtual idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version);
	virtual DuckLakeTableSchemaVersions GetTableSchemaVersions(TableIndex table_id, DuckLakeSnapshot snapshot);
	//! The net row count of the data files that were committed before the snapshot's next file id
	virtual idx_t GetNetDataFileRowCount(TableIndex table_id, DuckLakeSnapshot snapshot);
	virtual optional_idx GetNetDataFileRowCountForStats(TableIndex table_id, DuckLakeSnapshot snapshot);
	virtual idx_t GetNetInlinedRowCount(const Identifier &inlined_table_name, DuckLakeSnapshot snapshot);
	//! The number of rows of a table, in data files and inlined, or invalid if it cannot be computed exactly
	virtual optional_idx GetNetRowCount(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot);
	//! SQL builders for stats-refresh metadata lookups; caller substitutes placeholders + executes.
	//! With require_exact, the count is NULL if any visible file is only partially visible to the snapshot.
	//! With exclude_new_files, the files of the commit, which can already be in the catalog, are not counted.
	static string GetNetDataFileRowCountSql(TableIndex table_id, const Identifier &inlined_deletion_table,
	                                        bool branch_aware, const DuckLakeBranchChain &chain,
	                                        bool require_exact = false, bool exclude_new_files = false);
	static string GetNetInlinedRowCountSql(const Identifier &inlined_table_name,
	                                       const DuckLakeInlinedColNames &col_names, bool branch_aware,
	                                       const DuckLakeBranchChain &chain);
	//! The rows that are stored, deleted ones included, and the rows that are live
	static string GetInlinedRowCountsSql(const Identifier &inlined_table_name, const DuckLakeInlinedColNames &col_names,
	                                     bool branch_aware, const DuckLakeBranchChain &chain);
	//! The columns of the table at the snapshot, read through the chain from 2.0 on
	static string GetTableColumnSchemaSql(TableIndex table_id, bool branch_aware, const DuckLakeBranchChain &chain);
	//! The inlined table registrations the chain reads, filtered on a branch alone, a valid table id is pushed down
	static string InlinedTableRegistrations(bool branch_aware, const DuckLakeBranchChain &chain,
	                                        optional_idx table_id = optional_idx());
	//! The inlined data tables of the table, read through the chain from 2.0 on
	static string GetInlinedTableNamesSql(TableIndex table_id, bool branch_aware, const DuckLakeBranchChain &chain);
	//! The schema version rows are read through the chain from 2.0 on
	static string GetInlinedTablesBeforeSchemaChangeSql(TableIndex table_id, bool branch_aware,
	                                                    const DuckLakeBranchChain &chain);
	//! The top-level columns of each inlined data table at the schema version of that table
	static string GetInlinedTableColumnsSql(optional_idx table_id, bool branch_aware, const DuckLakeBranchChain &chain);
	//! The inserts of the given rows, in batches
	static string InsertValuesSql(const string &table_name, const vector<string> &values);
	//! The inserts of the given rows into the given table, in batches
	static string InsertValuesIntoSql(const string &qualified_table_name, const vector<string> &values);
	virtual identifier_set_t GetInlinedTableNames(TableIndex table_id);
	//! The inlined data tables of a table at a snapshot: the tables that the catalog entry of the table holds, without
	//! the tables that another attach dropped
	virtual vector<DuckLakeInlinedTableInfo> GetInlinedDataTables(const DuckLakeTableEntry &table,
	                                                              DuckLakeSnapshot snapshot);
	//! The files scheduled for deletion before an ISO 8601 timestamp (all of them if it is empty)
	virtual vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &older_than);
	//! The files in the data path that are not referenced by the metadata
	virtual vector<DuckLakeFileForCleanup> GetOrphanFilesForCleanup(const string &older_than);
	virtual vector<DuckLakeFileForCleanup> GetFilesForCleanup(const string &older_than, CleanupType type);
	//! The paths of all files the metadata refers to
	virtual vector<string> GetKnownFilePaths();
	//! The names of the tables of the metadata catalog
	virtual vector<string> GetMetadataTableNames();
	//! The contents of a table of the metadata catalog
	virtual unique_ptr<QueryResult> ReadMetadataTable(const string &table_name);

	virtual void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files);
	//! The chain predicate over a snapshot column, capped by the read snapshot unless that is empty
	static string ChainView(const DuckLakeBranchChain &chain, const string &prefix, const string &snapshot_column,
	                        const string &read_snapshot, const string &branch_column = "branch_id");
	//! The version rows of a kind born in the chain view at the read snapshot and not ended in it
	static string RowVisibility(const DuckLakeBranchChain &chain, const string &alias, const RowVisibilityKind &kind,
	                            const string &read_snapshot);
	static string RowVisibility(const DuckLakeBranchChain &chain, const string &alias, const string &kind,
	                            const string &read_snapshot);
	//! The range predicate of the version rows before 2.0, which carry begin_snapshot and end_snapshot
	static string RangeVisibility(const string &alias, const string &read_snapshot);
	static string RangeVisibility(const string &alias, const RowVisibilityKind &kind, const string &read_snapshot);
	//! The visibility predicate of the version rows under the layout of the catalog
	static string RowFilter(bool branch_aware, const DuckLakeBranchChain &chain, const string &alias,
	                        const RowVisibilityKind &kind, const string &read_snapshot);
	static string RowFilter(bool branch_aware, const DuckLakeBranchChain &chain, const string &alias,
	                        const string &kind, const string &read_snapshot);
	//! The version rows of a kind joined with the ends of the chain view at the read snapshot
	struct LiveRowSource {
		//! The row table under its alias, from 2.0 on with the ends joined under the alias suffixed by _end
		string from;
		//! The rows born in the view that no end of the view closes
		string filter;
	};
	//! The shapes of the end join that differ from the live row default
	struct LiveRowOptions {
		LiveRowOptions() : ended_only(false) {
		}
		//! Keeps the ended rows alone, an inner join instead of the left join
		bool ended_only;
		//! A predicate on the companion columns inside its derived table, preceded by AND when set
		string companion_filter;
	};
	//! The live version rows of a kind under the alias, the ends are joined once instead of a correlated probe
	static LiveRowSource LiveRows(bool branch_aware, const DuckLakeBranchChain &chain, const RowVisibilityKind &kind,
	                              const string &alias, const string &read_snapshot,
	                              const LiveRowOptions &options = LiveRowOptions());
	static LiveRowSource LiveRows(bool branch_aware, const DuckLakeBranchChain &chain, const string &kind,
	                              const string &alias, const string &read_snapshot);
	//! The live rows of an inlined data table under the alias, the join columns keep the reserved prefix
	static LiveRowSource InlinedRows(bool branch_aware, const DuckLakeBranchChain &chain, const Identifier &table,
	                                 const string &alias, const string &read_snapshot,
	                                 const DuckLakeInlinedColNames &col_names,
	                                 const LiveRowOptions &options = LiveRowOptions());
	//! The per payload companion of an older dev build, which the heal folds into the shared companion
	static Identifier InlinedCompanionName(const Identifier &table_name);
	//! Whether the name is that of a per payload companion, the name of a payload followed by _visibility
	static bool IsInlinedCompanionName(const string &name);
	//! The companion of a kind under the alias with the filter, which carries the constant keys of the kind
	static string CompanionEnds(const RowVisibilityKind &kind, const string &alias, const string &filter);
	//! The versions of a table over its payloads as one derived table of row id, branch and begin snapshot
	static string InlinedVersionsOf(const vector<Identifier> &payloads, const DuckLakeInlinedColNames &col_names);
	//! The end events of the version rows the filter selects under the alias t that the commit chain sees
	static string EndRows(const DuckLakeBranchChain &chain, const RowVisibilityKind &kind, const string &filter);
	static string EndRows(const DuckLakeBranchChain &chain, const string &kind, const string &filter);
	//! The delete events at the snapshot on the files the filter selects that the commit chain reads without an end
	static string FileEndEvents(const string &visibility_table, const string &file_id_column,
	                            const string &filter_column, const string &id_list, const string &snapshot,
	                            const DuckLakeBranchChain &chain);
	static string DropSchemas(const set<SchemaIndex> &ids, bool branch_aware, const DuckLakeBranchChain &chain);
	static string DropTables(const set<TableIndex> &ids, bool renamed, bool branch_aware,
	                         const DuckLakeBranchChain &chain);
	static string DropViews(const set<TableIndex> &ids, bool renamed, bool drop_view_column_tags, bool branch_aware,
	                        const DuckLakeBranchChain &chain);
	static string DropMacros(const set<MacroIndex> &ids, bool branch_aware, const DuckLakeBranchChain &chain);

	//! Emits the INSERT for new schemas. Caller supplies resolved paths (one per schema, same order)
	//! since path resolution depends on the catalog's data_path / separator (instance state).
	static string WriteNewSchemas(const vector<DuckLakeSchemaInfo> &new_schemas,
	                              const vector<DuckLakePath> &resolved_paths, bool supports_v1_1_metadata,
	                              bool branch_aware);
	//! Emits the INSERT for new tables and their columns. Caller supplies resolved paths (one per
	//! table, same order). commit_snapshot is currently unused by the body — kept off the signature.
	static string WriteNewTables(const vector<DuckLakeTableInfo> &new_tables,
	                             const vector<DuckLakePath> &resolved_paths, bool branch_aware);
	static string WriteNewViews(const vector<DuckLakeViewInfo> &new_views, bool branch_aware);
	//! Emits the partition-key diff SQL. Caller supplies the existing partition state (fetched
	//! via GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewPartitionKeys(const vector<DuckLakePartitionInfo> &existing_partitions,
	                                    const vector<DuckLakePartitionInfo> &new_partitions, bool branch_aware,
	                                    const DuckLakeBranchChain &chain);
	//! Maps the id of a last new spec that equals the committed spec, which is not written, to the committed id
	static unordered_map<idx_t, idx_t>
	GetUnchangedPartitionIds(const vector<DuckLakePartitionInfo> &existing_partitions,
	                         const vector<DuckLakePartitionInfo> &new_partitions);
	//! Emits the sort-key diff SQL. Caller supplies the existing sort state (fetched via
	//! GetCatalogForSnapshot) since the diff is computed against it.
	static string WriteNewSortKeys(const vector<DuckLakeSortInfo> &existing_sorts,
	                               const vector<DuckLakeSortInfo> &new_sorts, bool branch_aware,
	                               const DuckLakeBranchChain &chain);
	static string WriteDroppedColumns(const vector<DuckLakeDroppedColumn> &dropped_columns, bool branch_aware,
	                                  const DuckLakeBranchChain &chain);
	//! Formats a column set as a SQL list of (table_id, column_id) rows
	static string WriteColumnIdPairs(const map<TableIndex, set<FieldIndex>> &columns);
	//! Rewrite the bounds of committed files and tables written while a widened column was FLOAT
	static string WriteFloatWidenedStats(const map<TableIndex, set<FieldIndex>> &widened_columns,
	                                     const map<TableIndex, set<FieldIndex>> &cleared_columns, idx_t next_file_id);
	//! Clear the bounds of added files of the columns retyped to a finer timestamp unit, and of their tables
	static string WriteRefinedTimestampStats(const map<TableIndex, set<FieldIndex>> &refined_columns,
	                                         idx_t next_file_id);
	static string WriteExpiredColumnTags(const vector<DuckLakeDroppedColumn> &dropped_columns, bool branch_aware,
	                                     const DuckLakeBranchChain &chain);
	static string WriteNewColumns(const vector<DuckLakeNewColumn> &new_columns, bool branch_aware);
	//! Columns added and dropped in the same commit, which begin and end in its snapshot
	static string WriteDroppedNewColumns(const vector<DuckLakeNewColumn> &columns, bool branch_aware);
	//! A new tag value writes a version row and ends the one it overwrites in the same commit
	static string WriteNewTags(const vector<DuckLakeTagInfo> &new_tags, bool branch_aware,
	                           const DuckLakeBranchChain &chain);
	static string WriteNewTableOptions(const vector<DuckLakeConfigOption> &new_options);
	static string WriteNewColumnTags(const vector<DuckLakeColumnTagInfo> &new_tags, bool branch_aware,
	                                 const DuckLakeBranchChain &chain);
	static string WriteNewViewColumnTags(const vector<DuckLakeViewColumnTagInfo> &new_tags, bool branch_aware,
	                                     const DuckLakeBranchChain &chain);
	//! SQL that writes new data files, shared with the server-side commit path. Returns SQL with
	//! {METADATA_CATALOG} / {SNAPSHOT_ID} placeholders. Caller supplies resolved paths (one per file,
	//! same order) since path policy differs across callers (schema-relative vs. always-absolute).
	static string WriteNewDataFilesSqlBatch(const vector<DuckLakeFileInfo> &new_files,
	                                        const vector<DuckLakePath> &resolved_paths, bool supports_v1_1_metadata,
	                                        bool write_visibility);
	//! Opt-in fast-path: if this backend supports the DuckDB Appender API, write the files directly
	bool TryAppendDataFiles(DuckLakeSnapshot &commit_snapshot, const vector<DuckLakeFileInfo> &new_files,
	                        const vector<DuckLakeTableInfo> &new_tables,
	                        vector<DuckLakeSchemaInfo> &new_schemas_result);
	virtual string WriteNewInlinedData(DuckLakeSnapshot &commit_snapshot,
	                                   const vector<DuckLakeInlinedDataInfo> &new_data,
	                                   const vector<DuckLakeTableInfo> &new_tables,
	                                   const vector<DuckLakeTableInfo> &new_inlined_data_tables_result,
	                                   vector<unique_ptr<SQLStatement>> &inlined_inserts);
	//! The ends of the deleted row versions from 2.0 on, the in place update of their end column before
	static string WriteNewInlinedDeletes(const vector<DuckLakeDeletedInlinedDataInfo> &new_deletes,
	                                     const DuckLakeInlinedColNames &col_names, bool branch_aware,
	                                     const DuckLakeBranchChain &chain);
	//! Creates the INSERT INTO {METADATA_CATALOG}.<inlined_table_name> VALUES (...) batch.
	static string FormatInlinedDataInsert(const Identifier &inlined_table_name, idx_t row_id_start,
	                                      bool has_preserved_row_ids, const vector<int64_t> *row_ids,
	                                      const vector<string> &cells_per_row, bool branch_aware);
	//! Static deterministic name of the per-table inlined deletion table.
	static Identifier InlinedFileDeletionTableName(TableIndex table_id);
	//! The CREATE IF NOT EXISTS and INSERT of the inlined file deletions, with the branch of the commit from 2.0 on
	static string WriteNewInlinedFileDeletesSql(const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes,
	                                            bool &created_new_table, bool branch_aware);
	//! The inlined deletion rows of the chain view at the read snapshot, the rows at or below it before 2.0
	static string InlinedDeletionView(bool branch_aware, const DuckLakeBranchChain &chain, const string &alias,
	                                  const string &read_snapshot);
	//! The inlined deletion rows the attached chain reads at the read snapshot
	string InlinedDeletionView(const string &alias, const string &read_snapshot) const;
	//! SQL branch of WriteNewInlinedFileDeletes — returns the CREATE/INSERT statements and marks the
	//! metadata-manager cache for clearing when a new per-table deletion table is created.
	string WriteNewInlinedFileDeletesSqlBatch(const vector<DuckLakeInlinedFileDeletionInfo> &new_deletes);
	//! Get the name of the inlined deletion table for a given table ID
	virtual Identifier GetInlinedDeletionTableName(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                               bool include_local_changes = true);
	//! Probe for the physical inlined-deletion table without aborting the active metadata transaction.
	virtual bool InlinedDeletionTableExists(const Identifier &table_name);
	//! Whether the listing of the snapshot's inlined deletion tables, run once per snapshot, names the table
	bool ListedInlinedDeletionTableExists(const Identifier &table_name,
	                                      const std::function<unique_ptr<QueryResult>()> &list);
	//! Also drops the empty inlined data tables of the replaced tables
	virtual string WriteNewInlinedTables(DuckLakeSnapshot commit_snapshot, const vector<DuckLakeTableInfo> &tables,
	                                     const set<TableIndex> &replaced_tables);
	//! Drop the current inlined data table of a table if it holds no rows
	string DropInlinedTableIfEmpty(TableIndex table_id, const DuckLakeTableEntry &table);
	//! A query that returns a row if the inlined data table holds any row
	static string InlinedTableHasRowsQuery(const Identifier &table_name);
	//! Unregisters and drops an inlined data table, its ends in the shared companion name no row
	static string DropInlinedTableSql(idx_t table_id, idx_t schema_version, const Identifier &table_name);
	virtual Identifier GetInlinedTableQueries(DuckLakeSnapshot commit_snapshot, const DuckLakeTableInfo &table,
	                                          vector<string> &inlined_tables, string &inlined_table_queries);
	static Identifier InlinedTableNameFor(idx_t table_id, idx_t schema_version);
	//! The table id of an inlined data table named by InlinedTableNameFor, invalid for any other name
	static optional_idx ParseInlinedTableName(const string &name, idx_t &schema_version);
	//! The payload table of inlined rows, from 2.0 on with its birth branch
	static string InlinedTableDdlSql(const Identifier &table_name, const string &column_defs,
	                                 const DuckLakeInlinedColNames &col_names, bool branch_aware);
	DuckLakeInlinedColNames InlinedColNames() const;
	static string InlinedTableRegistrationTuple(idx_t table_id, const Identifier &table_name, idx_t schema_version);
	//! The registration with the highest schema version in the chain view
	static string LatestInlinedTableQuery(idx_t table_id, bool branch_aware, const DuckLakeBranchChain &chain);
	static string DropDataFiles(const set<DataFileIndex> &dropped_files, bool write_visibility,
	                            const DuckLakeBranchChain &chain);
	//! The deletes recorded against the given data files of the table at the snapshot, read through the chain
	static string GetDeleteCountForDataFilesSql(TableIndex table_id, const set<DataFileIndex> &file_ids,
	                                            const Identifier &inlined_deletion_table, bool branch_aware,
	                                            const DuckLakeBranchChain &chain);
	//! Selects the given data files that are still in the metadata
	static string GetExistingDataFilesSql(const set<DataFileIndex> &files);
	//! Selects the given columns that a live data file of the table records values of, or holds the default of
	static string GetColumnsWithRecordedValuesSql(TableIndex table_id, const set<FieldIndex> &column_ids,
	                                              bool branch_aware, const DuckLakeBranchChain &chain);
	//! Selects the given columns that may hold values other than NULL in a live data file of the table
	static string GetColumnsWithFileValuesSql(TableIndex table_id, const set<FieldIndex> &column_ids,
	                                          const map<FieldIndex, idx_t> &null_default_columns, bool branch_aware,
	                                          const string &chain_predicate);
	//! Selects the versions of the given columns with their begin snapshot, whether their initial default is NULL and
	//! whether they are top level columns
	static string GetColumnDefaultsSql(TableIndex table_id, const set<FieldIndex> &column_ids, bool branch_aware,
	                                   const DuckLakeBranchChain &chain);
	//! Selects the number of live inlined values of each check, the checks of a table are consecutive
	static string GetInlinedValuesSql(const vector<DuckLakeInlinedValueCheck> &checks,
	                                  const DuckLakeInlinedColNames &col_names, bool branch_aware,
	                                  const DuckLakeBranchChain &chain);
	//! Selects the given tables or views with a version row born after the snapshot on the commit branch
	static string GetRenamedAfterSnapshotSql(const string &metadata_table_name, const string &id_name,
	                                         const set<TableIndex> &ids, bool branch_aware,
	                                         const DuckLakeBranchChain &chain);
	//! Caller supplies one resolved path per overwritten file, in the same order.
	static string DeleteOverwrittenDeleteFiles(const vector<DuckLakeOverwrittenDeleteFile> &overwritten_files,
	                                           const vector<DuckLakePath> &resolved_paths);
	//! Caller supplies one resolved path per new delete file, in the same order
	static string WriteNewDeleteFiles(const vector<DuckLakeDeleteFileInfo> &new_delete_files,
	                                  const vector<DuckLakePath> &resolved_paths, bool write_row_group_count,
	                                  bool write_visibility);
	static string WriteNewMacros(const vector<DuckLakeMacroInfo> &new_macros, bool branch_aware);

	virtual vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from);
	//! The column mappings with the ids
	vector<DuckLakeColumnMappingInfo> GetColumnMappings(const set<idx_t> &mapping_ids);
	static string WriteNewColumnMappings(const vector<DuckLakeColumnMappingInfo> &new_column_mappings);
	//! Caller supplies one resolved path per compaction, in the same order.
	static string WriteMergeAdjacent(const vector<DuckLakeCompactedFileInfo> &compactions,
	                                 const vector<DuckLakePath> &resolved_paths, bool write_visibility);
	static string WriteDeleteRewrites(const vector<DuckLakeCompactedFileInfo> &compactions, bool write_visibility,
	                                  const DuckLakeBranchChain &chain);
	//! For MERGE_ADJACENT_TABLES, resolved_paths is one path per compaction; for REWRITE_DELETES it
	//! is ignored.
	static string WriteCompactions(const vector<DuckLakeCompactedFileInfo> &compactions, CompactionType type,
	                               const vector<DuckLakePath> &resolved_paths, bool write_visibility,
	                               const DuckLakeBranchChain &chain);
	//! SQL templates with {METADATA_CATALOG} / {SNAPSHOT_ID} placeholders, shared with the
	//! server-side commit path.
	static string InsertSnapshotSql(bool write_branch_id);
	static string WriteSnapshotChangesSql(const SnapshotChangeInfo &change_info,
	                                      const DuckLakeSnapshotCommit &commit_info, bool write_branch_id);
	static string UpdateGlobalTableStatsSql(const DuckLakeGlobalStatsInfo &stats, bool write_stats_exactness);
	static string InsertTableColumnStatsSql(const DuckLakeGlobalStatsInfo &stats, bool write_stats_exactness);
	//! Set the stored table sizes to the size of the live data files, and the known record counts of the given tables
	//! to the rows of the live data files
	static string RefreshTableSizesSql(const set<TableIndex> &table_ids, const set<TableIndex> &record_count_tables,
	                                   bool branch_aware, const string &chain_predicate);
	//! Add the rows stored in the inlined data tables to a known record count
	static string AddInlinedRowCountsSql(TableIndex table_id, const vector<Identifier> &inlined_table_names);
	static SnapshotChangeInfo
	GetSnapshotAndStatsAndChanges(SnapshotAndStats &current_snapshot,
	                              const std::function<unique_ptr<QueryResult>(string)> &executor, const string &query,
	                              bool branch_aware, const string &chain_predicate);
	//! The head predicate selects the snapshots of the commit lineage and the commit branch the changes that
	//! conflict, both unused before 2.0
	static string SnapshotAndStatsAndChangesQuery(bool include_exactness, bool branch_aware,
	                                              const string &head_predicate, idx_t commit_branch_id);
	//! The query of the conflict check, which must read the latest snapshot and the table stats consistently
	virtual string GetSnapshotAndStatsAndChangesQuery(DuckLakeSnapshot snapshot, bool include_exactness) const;
	static SnapshotChangeInfo ParseSnapshotAndStatsAndChanges(QueryResult &result, SnapshotAndStats &current_snapshot);
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot();
	//! Throws when the newest expired snapshot a time travel by timestamp could see is its resolved snapshot or newer
	void CheckSnapshotAfterExpiredGap(const DuckLakeSnapshot &snapshot, const Value &timestamp,
	                                  idx_t newest_snapshot_id, const set<idx_t> &expired_in_transaction);
	virtual unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound,
	                                                 idx_t newest_snapshot_id);

	virtual idx_t GetNextColumnId(TableIndex table_id);
	//! The next free column id of every table that has columns
	unordered_map<idx_t, idx_t> GetNextColumnIds();
	virtual unique_ptr<QueryResult> ReadInlinedData(DuckLakeSnapshot snapshot, const Identifier &inlined_table_name,
	                                                const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot,
	                                                          DuckLakeSnapshot end_snapshot,
	                                                          const Identifier &inlined_table_name,
	                                                          const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot,
	                                                         DuckLakeSnapshot end_snapshot,
	                                                         const Identifier &inlined_table_name,
	                                                         const vector<string> &columns_to_read);
	virtual unique_ptr<QueryResult> ReadAllInlinedDataForFlush(DuckLakeSnapshot snapshot,
	                                                           const Identifier &inlined_table_name,
	                                                           const DuckLakeTableEntry &table,
	                                                           const string &sort_order_sql,
	                                                           const vector<string> &columns_to_read);
	//! The deleted versions of the inlined rows that a flush writes, and the ends other branches wrote on its versions
	virtual DuckLakeDeletedRowVersions GetDeletedInlinedRowVersions(DuckLakeTableEntry &table,
	                                                                const Identifier &inlined_table_name,
	                                                                DuckLakeSnapshot snapshot,
	                                                                DuckLakeRehomedRowEnds &rehomed_rows);
	//! The inlined deletions of rows of data files, which a flush writes to delete files
	virtual vector<DuckLakeInlinedFileDeletionEntry> GetInlinedFileDeletionsForFlush(DuckLakeTableEntry &table,
	                                                                                 DuckLakeSnapshot snapshot);
	//! SQL builders for the stats-refresh queries used by DuckLakeTransactionState::RecomputeGlobalStatsAfterRewrite.
	//! Caller substitutes `{METADATA_CATALOG}` / `{SNAPSHOT_ID}` and executes via the commit context's executor.
	static string ReadInlinedDataAggregatesSql(const Identifier &inlined_table_name, const string &select_list,
	                                           const DuckLakeInlinedColNames &col_names, bool branch_aware,
	                                           const DuckLakeBranchChain &chain);
	//! The column stats of the data files committed before the snapshot's next file id
	static string ReadFileColumnStatsForTableSql(TableIndex table_id, bool include_exactness, bool branch_aware,
	                                             const string &chain_predicate);
	//! Throws on a failed inlined data read, hinting at the migration for legacy named catalogs
	void CheckInlinedDataReadError(QueryResult &result, const Identifier &inlined_table_name);
	shared_ptr<DuckLakeInlinedData> TransformInlinedData(QueryResult &result, const vector<LogicalType> &expected_types,
	                                                     const Identifier &inlined_table_name);

	//! The statements deleting the flushed inlined rows and the flushed inlined file deletions of the committing branch
	static string GenerateDeleteFlushedInlinedData(const vector<FlushedInlinedTableInfo> &flushed_tables,
	                                               const map<TableIndex, idx_t> &flushed_file_deletions,
	                                               const DuckLakeInlinedColNames &col_names, bool branch_aware);
	static string InsertNewSchema(const DuckLakeSnapshot &snapshot, const set<TableIndex> &table_ids,
	                              bool write_branch_id);

	virtual vector<DuckLakeSnapshotInfo> GetAllSnapshots(const DuckLakeSnapshotFilter &filter = {});
	//! The snapshots in the column order of ducklake_snapshots, then next_file_id
	unique_ptr<QueryResult> ReadSnapshots(const string &filter);
	//! The snapshot tags born in the view of the attached branch, with the times of their snapshots if requested
	virtual vector<DuckLakeSnapshotTagInfo> GetSnapshotTags(bool with_snapshot_time = true);
	//! The live snapshot tag with the name, if any
	unique_ptr<DuckLakeSnapshotTagInfo> GetTaggedSnapshot(const string &name);
	//! The snapshot id of a VERSION value, a snapshot id or the name of a snapshot tag
	idx_t ResolveSnapshotVersion(const Value &version);
	static string WriteSnapshotTagChanges(const case_insensitive_map_t<DuckLakeSnapshotTagInfo> &created,
	                                      const case_insensitive_map_t<DuckLakeSnapshotTagInfo> &dropped,
	                                      const DuckLakeBranchChain &chain);
	//! Ends the row of a committed tag at the snapshot of the commit
	static string EndSnapshotTagSql(const DuckLakeSnapshotTagInfo &tag, const DuckLakeBranchChain &chain);
	//! The branch id is a placeholder in a commit batch and a literal where no snapshot substitutes it
	static string DeleteSnapshotsSql(const vector<DuckLakeSnapshotInfo> &snapshots, bool branch_aware,
	                                 const string &branch_id = "{BRANCH_ID}");
	//! Deletes the snapshots once the caller checked the catalog version
	virtual void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	//! Deletes the metadata that only the deleted snapshots referenced, once the caller checked the catalog version
	virtual void CleanupExpiredSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	//! Deletes the ends whose version row is gone
	void DeleteOrphanRowEnds(const RowVisibilityKind &kind);
	//! Deletes the ends of a table whose version is in none of its payloads
	void DeleteOrphanInlinedRowEnds(idx_t table_id, const vector<Identifier> &payloads);
	virtual vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot);
	//! The tables with data files that are not dropped
	unordered_set<idx_t> GetTablesWithLiveDataFiles();
	//! The tables in scope a delete rewrite can touch, which hold live delete files or inlined deletions
	unordered_set<idx_t> GetTablesWithRewriteCandidates(DuckLakeSnapshot snapshot, const vector<TableIndex> &table_ids);
	//! Returns the rows the option is expected to have after it is written
	virtual idx_t SetConfigOption(const DuckLakeConfigOption &option);
	virtual bool ResetConfigOption(const DuckLakeConfigOption &option);
	idx_t ConfigOptionRowCount(const DuckLakeConfigOption &option, optional_ptr<const string> value = nullptr);
	virtual void MigrateV01();
	virtual void MigrateV02(bool allow_failures = false);
	virtual void MigrateV03(bool allow_failures = false);
	virtual void MigrateV04();
	virtual void MigrateV10(bool from_dev1 = false);
	virtual void MigrateV11();
	//! Every branch of the catalog
	vector<DuckLakeBranchInfo> LoadBranches();
	//! The resolved chain of every branch other than main
	vector<DuckLakeBranchChain> LoadBranchChains();
	//! The snapshots of a branch that the chains of the other branches continue from, the root caps for main
	set<idx_t> LoadBranchCaps(idx_t branch_id);
	//! The snapshots of a branch an expiry keeps, its head, the forks and the unexpired tag targets of other branches
	set<idx_t> GetBranchProtectedSnapshots(idx_t branch_id, timestamp_tz_t start_time);
	string FileVisibilityGuard(const string &visibility_table, const string &file_id_column, const string &alias = "f",
	                           bool include_inherited = true);
	//! The chain predicate of the attached branch with bare column names, preceded by AND, empty before 2.0
	string BranchLineageFilter() const;
	//! The WHERE conditions of a snapshot filter, the latest snapshot is the head of the attached branch from 2.0 on
	string SnapshotFilterSql(const DuckLakeSnapshotFilter &filter) const;
	//! The chain predicate over the begin snapshots of schema version rows, preceded by AND, empty before 2.0
	string SchemaVersionsLineageFilter(const string &prefix) const;
	//! Selects the rows of main, preceded by AND, empty before 2.0
	string MainSnapshotFilter() const;
	bool HasBranchTable();
	//! Whether the branch table has parent columns rather than the fork columns of an older dev build
	bool HasBranchTableShape();
	bool HasSnapshotRemapTable();
	//! Whether the remap table keys its snapshots by branch
	bool HasSnapshotRemapShape();
	bool HasSnapshotTable();
	bool HasSnapshotBranchColumn();
	bool HasSnapshotTagTable();
	bool HasSnapshotChangesTable();
	bool HasSnapshotChangesBranchColumn();
	bool HasSchemaVersionsBranchColumn();
	//! The rebuild and old tables of an interrupted heal of the snapshot tables
	bool HasSnapshotRebuildTable();
	bool HasSnapshotOldTable();
	bool HasSnapshotChangesRebuildTable();
	bool HasSnapshotChangesOldTable();
	//! Whether any table holds rows of a branch other than main
	bool HasBranchRows();
	bool HasFileVisibilityTable();
	bool HasDeleteFileVisibilityTable();
	//! The per file ranges a branch derives from its visibility events
	static string VisibilityEventRanges(const string &visibility_table, const string &file_id_column,
	                                    const string &chain_predicate, const string &prefix);
	//! The ranges with begin_segment, the chain position of the earliest add event, the segment that wrote the file
	//! A file holds one add event in every lineage view that counts it as its own, so the earliest one is the writer
	//! No read snapshot caps the aggregate, the compaction and conflict readers run the sources without one
	static string VisibilityEventRanges(const string &visibility_table, const string &file_id_column,
	                                    const DuckLakeBranchChain &chain, const string &prefix);
	//! The chain predicate the file sources filter the visibility events with, under the alias v, empty on main
	static string FileSourceChain(const DuckLakeBranchChain &chain);
	//! The delete files the filter selects, on a branch one per data file (and begin) with the writer nearest the leaf
	static string ChosenDeleteFiles(const DuckLakeBranchChain &chain, const string &source, const string &filter,
	                                bool per_begin_snapshot = false);
	//! The file events the commit chain reads under the alias, branch 0 alone on main
	static string FileEventFilter(const DuckLakeBranchChain &chain, const string &alias);
	//! A valid table id is pushed into the file scan and the visibility aggregate
	static string DataFileSource(bool branch_aware, const DuckLakeBranchChain &chain, const string &prefix,
	                             optional_idx table_id = optional_idx());
	static string DataFileSource(bool branch_aware, const string &chain_predicate, const string &prefix,
	                             optional_idx table_id = optional_idx());
	static string DeleteFileSource(bool branch_aware, const DuckLakeBranchChain &chain, const string &prefix,
	                               optional_idx table_id = optional_idx());
	static string DeleteFileSource(bool branch_aware, const string &chain_predicate, const string &prefix,
	                               optional_idx table_id = optional_idx());
	string DataFiles() const;
	string DeleteFiles() const;
	//! The sources filtered to one table, pushed into the visibility aggregate
	string DataFiles(TableIndex table_id) const;
	string DeleteFiles(TableIndex table_id) const;
	bool HasMetadataColumn(const string &table, const string &column);
	//! Scans the metadata schema into the memo, once per probe pass
	void ProbeMetadataSchema();
	//! Reads the columns of every table of the metadata schema into the memo, false when the catalog lists none
	virtual bool ProbeMetadataColumns();
	//! Fills the memo from the table and column names a listing returns, false when the metadata table is not listed
	bool ProbeMetadataColumnsFromListing(string query);
	//! Drops the memo of a probe pass
	void ResetProbedColumns();
	bool HasAllFileRangeColumns();
	bool HasBranchZeroFileVisibility(const string &file_table, const string &file_column, bool has_visibility_table,
	                                 const string &visibility_table);
	bool HasBranchZeroDataVisibility();
	bool HasBranchZeroDeleteVisibility();
	bool HasBranchZeroVisibility();
	bool HasVisibilityEventShape(const string &visibility_table);
	bool HasVisibilityMigrationTable(const string &visibility_table);
	//! Whether the loaded metadata holds the marker of a completed heal of the current development layout
	static bool HasHealMarker(const DuckLakeMetadata &metadata);
	//! Reads the metadata table for the marker
	bool HasHealMarker();
	//! Writes the marker, the last statements of a heal
	static string HealMarkerSql();
	virtual void AddV2_0Dev1Additions();
	//! The heal steps a versioned catalog table still needs, the branch column, the companion and the end column drop
	struct RowVisibilityAdditions {
		RowVisibilityKind kind;
		//! A missing row table is left to its reader, so no heal step is emitted for it
		bool present = true;
		bool branch_column = true;
		//! The snapshot tags gain the branch of the tagged snapshot
		bool target_branch_column = false;
		bool visibility_table = true;
		//! The end column is still present, read into the companion and then dropped
		bool drop_end = true;
	};
	//! Whether a metadata table holds a row
	bool TableHasRows(const string &table);
	//! Whether the version rows of a catalog table carry a birth branch and end in a companion
	bool HasRowVisibilityShape(const RowVisibilityKind &kind);
	//! Whether every versioned catalog table has that shape
	bool HasRowVisibilityShapes();
	RowVisibilityAdditions ProbeRowVisibilityAdditions(const RowVisibilityKind &kind);
	static string RowVisibilityMigrationSql(const RowVisibilityAdditions &add);
	//! The heal step an inlined deletion table still needs, the branch column of its rows
	struct InlinedDeletionAdditions {
		Identifier table_name;
		bool branch_column = true;
	};
	//! Whether the name is that of an inlined deletion table, ducklake_inlined_delete_<table id>
	static bool IsInlinedDeletionTableName(const string &name);
	//! Whether the name is that of an inlined data table, ducklake_inlined_data_<table id>_<schema version>
	static bool IsInlinedDataTableName(const string &name);
	//! The inlined deletion tables of the catalog with their heal steps, from the memo of the pass when complete
	vector<InlinedDeletionAdditions> ProbeInlinedDeletionTables();
	//! Whether every inlined deletion table carries the branch column
	bool HasInlinedDeletionShapes();
	static string InlinedDeletionMigrationSql(const InlinedDeletionAdditions &add);
	//! A per payload companion of an older dev build, folded into the shared companion and dropped
	struct InlinedCompanionFold {
		Identifier companion;
		idx_t table_id;
		//! A companion without payload is dropped without the fold, its ends name no row
		bool payload_present;
	};
	//! The inlined data tables of the catalog with their heal steps and the per payload companions to fold
	vector<RowVisibilityAdditions> ProbeInlinedDataTables(vector<InlinedCompanionFold> &companions);
	//! Whether every inlined data table and deletion table has the 2.0 shape and no per payload companion is left
	bool HasInlinedRowShapes();
	//! Whether the shared companion of the inlined rows exists
	bool HasInlinedVisibilityTable();
	static string InlinedCompanionFoldSql(const InlinedCompanionFold &fold);
	//! The inlined data tables of the catalog, from the memo of the pass when complete, registered ones otherwise
	vector<Identifier> ListInlinedDataTables();
	//! The user table names of the inlined data tables, keyed by the inlined table name
	identifier_map_t<Identifier> UserTableNamesOf(const vector<Identifier> &inlined_tables);
	//! The branch additions a dev catalog is missing, all of them for the 1.1 migration
	struct V2_0Dev1Additions {
		//! The snapshot tag table of 2.0 is created
		bool snapshot_tag_table = true;
		//! The branch and remap tables are created, after dropping the shapes of an older dev build
		bool branch_table = true;
		bool branch_table_old_shape = false;
		bool remap_table = true;
		bool remap_table_old_shape = false;
		//! The snapshot tables are rebuilt with their branch key through rebuild tables, the replaced table moves
		//! aside as the old table, so an interrupted heal resumes from any of its statements
		//! The old layout numbered the branch snapshots in the sequence of main, the changes table marks it
		bool old_snapshot_layout = true;
		bool rebuild_snapshots = true;
		bool has_snapshot_table = true;
		bool snapshot_has_branch_column = false;
		bool has_snapshot_rebuild = false;
		bool has_snapshot_old = false;
		bool rebuild_snapshot_changes = true;
		bool has_changes_table = true;
		bool changes_has_branch_column = false;
		bool has_changes_rebuild = false;
		bool has_changes_old = false;
		bool schema_versions_branch_column = true;
		bool file_visibility_table = true;
		bool delete_file_visibility_table = true;
		//! The inline range columns still present, read into visibility and then dropped
		bool drop_data_begin = true;
		bool drop_data_end = true;
		bool drop_delete_begin = true;
		bool drop_delete_end = true;
		//! Visibility tables of an older dev build with range rows, rewritten into events
		bool file_visibility_ranges = false;
		bool delete_file_visibility_ranges = false;
		//! Backup tables of an interrupted rewrite, consumed and dropped
		bool file_visibility_migration = false;
		bool delete_file_visibility_migration = false;
		//! The catalog tables whose version rows end in companion tables, one entry per kind
		vector<RowVisibilityAdditions> row_additions;
		//! The shared companion of the inlined rows is created
		bool inlined_visibility_table = true;
		//! The inlined data tables, whose row versions end in the shared companion like the catalog rows
		vector<RowVisibilityAdditions> inlined_additions;
		//! The inlined deletion tables, whose rows gain the branch they were deleted on
		vector<InlinedDeletionAdditions> deletion_tables;
		//! The per payload companions of an older dev build, folded into the shared companion
		vector<InlinedCompanionFold> inlined_companions;
	};
	V2_0Dev1Additions ProbeV2_0Dev1Additions();
	void VerifyHealableV2_0Dev1(const V2_0Dev1Additions &missing);
	//! Refuses a legacy user column of an inlined data table named like a column the 2.0 layout adds
	void VerifyInlinedColumnsHealable(const V2_0Dev1Additions &missing);
	static string V2_0Dev1AdditionsSql(const V2_0Dev1Additions &add);
	static string VisibilityEventBackfillSql(const string &visibility_table, const string &file_table,
	                                         const string &file_id_column);
	static string VisibilityEventRewriteSql(bool table_missing, bool has_ranges, bool has_migration,
	                                        const string &visibility_table, const string &file_id_column,
	                                        const string &create_statement);
	static string GetSnapshotTagTableStatement();
	static string GetFileVisibilityTableStatement();
	static string GetDeleteFileVisibilityTableStatement();
	static string GetBranchTableStatement();
	//! A row (b, m, w, s, n) maps the snapshot (w, s) to (b, n) for every reader whose view holds (b, m), n <= m
	//! The merge commit alone writes rows, the readers of a merged file map its embedded ids, AT (VERSION) never does
	static string GetSnapshotRemapTableStatement();
	static string GetMainBranchRowStatement();
	//! Whether the catalog already has every metadata table and column of v1.1
	bool HasV1_1Additions();
	//! Renames inlined metadata columns to the prefixed variants, skipping already renamed tables
	virtual void MigrateInlinedColumnNames(bool probe_renamed);
	//! Renames the given (old name, new name) columns of each inlined table
	virtual void RenameInlinedColumns(const map<Identifier, vector<pair<Identifier, Identifier>>> &renames);
	//! Renames by copying the tables, for a database that parses its whole schema for every renamed column
	void RenameInlinedColumnsByCopy(const map<Identifier, vector<pair<Identifier, Identifier>>> &renames);
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
	//! The changes of the snapshots from first_snapshot to last_snapshot that recorded a rewrite
	virtual string GetRewriteSnapshotsQuery(idx_t first_snapshot, idx_t last_snapshot) const;

	virtual string GenerateFileColumnStatsCTEBody(const CTERequirement &req, TableIndex table_id,
	                                              const string &metadata_table_prefix);
	//! The snapshot placeholders of the query are replaced later, unless an override does it
	virtual string GenerateFileListQuery(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                     const FilterPushdownInfo *filter_info,
	                                     const vector<DuckLakeFileListOrder> &file_order,
	                                     vector<idx_t> &runtime_filter_stats_columns,
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
	vector<DuckLakeColumnMappingInfo> GetColumnMappingsWhere(const string &filter);
	string GetInlinedTableQuery(const DuckLakeTableInfo &table, const Identifier &table_name);
	bool CanInlineColumn(const Identifier &name, const LogicalType &type);
	string GetInliningObstacle(const Identifier &name, const LogicalType &type);
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
	//! Loads the paths of tables that are not in the current snapshot with a single query
	void LoadTablePaths(const vector<TableIndex> &table_ids);
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
	string GetFileCountsSelectList(const string &prefix, const string &row_count_column);
	void ReadFileCounts(const QueryResultRow &row, idx_t &col_idx, DuckLakeFileData &data);
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
	string GenerateColumnFilterCondition(const ColumnFilterInfo &column_filter, FilterSQLResult &result,
	                                     bool &complete);
	//! Emit the condition for a filter tree, combining the per-column conditions of its leaves.
	//! A node under a parent of the same conjunction type is spliced in without parens of its own.
	string GenerateFilterTreeCondition(const DuckLakeFilterNode &node, FilterSQLResult &result,
	                                   bool splice_into_parent = false);
	virtual FilterSQLResult ConvertFilterPushdownToSQL(const FilterPushdownInfo &filter_info);
	string GenerateCTESectionFromRequirements(const map<idx_t, CTERequirement> &requirements, TableIndex table_id,
	                                          const string &metadata_table_prefix);
	//! Whether the stats CTEs of the file list query are materialized, for a database that plans the query itself
	virtual bool MaterializeStatsCTEs() const;
	//! Join each column's stats CTE once. Leading newline per join, empty when there are none.
	static string GenerateStatsJoinList(const map<idx_t, CTERequirement> &requirements);
	virtual bool ValueIsFinite(const Value &val);
	//! Unknown bounds must keep the file
	static string BoundOrInfinity(const string &bound, const string &type_name, StatsCastType cast_type);

private:
	virtual string GenerateFilterFromExpression(const Expression &expr, const LogicalType *type,
	                                            unordered_set<string> &referenced_stats, const string &stats_alias,
	                                            bool &complete);
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
	                                      const string &stats_alias, bool &complete);

public:
	//! Read inlined file deletions for regular table scans (no snapshot info per row)
	//! The delete files of data files of a table at a snapshot, by the id of the data file
	map<idx_t, DuckLakeOverwrittenDeleteFile> GetDeleteFiles(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                                                         const set<idx_t> &data_file_ids);
	//! Sets the partitioning and the partition values of the files of a table at a snapshot
	void ReadFilePartitions(TableIndex table_id, DuckLakeSnapshot snapshot, vector<DuckLakeFileListEntry> &files);
	map<idx_t, set<idx_t>> ReadInlinedFileDeletions(TableIndex table_id, DuckLakeSnapshot snapshot,
	                                                bool include_local_changes = true);
	//! Clear inlined table caches (needed after rollback so retry re-creates the tables)
	void ClearInlinedTableCaches();

private:
	static unordered_map<string /* name */, create_t> metadata_managers;
	static mutex metadata_managers_lock;
	//! The newest snapshot of every branch of the chain, read along with the latest snapshot
	vector<idx_t> lineage_heads;
	//! Whether the catalog holds any branch besides main, read once when reads are repeatable
	optional_idx catalog_has_branches;
	//! Whether the pass scanned the metadata schema, a scan that listed no tables leaves the probes in the memo
	bool probed_metadata_schema = false;
	//! Whether the pass read the columns of every metadata table at once
	bool probed_all_columns = false;
	//! Guards the three caches below, the scans of one statement initialize in parallel
	mutex listing_cache_lock;
	//! Parsed file listings keyed by their query, statements repeat identical listings several times
	unordered_map<string, shared_ptr<vector<DuckLakeFileListEntry>>> file_list_cache;
	//! Parsed deletion scans of table_changes, which executes its queries twice per call
	unordered_map<string, shared_ptr<vector<DuckLakeDeleteScanEntry>>> delete_scan_cache;
	//! Net data file row counts, which every filtered count of a statement reads again
	unordered_map<string, optional_idx> row_count_cache;

	unordered_set<idx_t> GetFileIdsWithInlinedDeletions(TableIndex table_id, DuckLakeSnapshot snapshot);
	unordered_map<idx_t, Identifier> insert_inlined_table_name_cache;
	unordered_set<idx_t> delete_inlined_table_cache;

protected:
	DuckLakeTransaction &transaction;
	mutex paths_lock;
	map<TableIndex, string> table_paths;
	bool pending_cache_clear = false;
	//! The columns of the metadata tables probed in the current pass, a missing table has none
	unordered_map<string, vector<Identifier>> probed_columns;
	bool read_file_counts = false;
};

} // namespace duckdb
