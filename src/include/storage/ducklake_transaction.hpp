//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "ducklake_macro_entry.hpp"
#include "common/ducklake_data_file.hpp"
#include "common/ducklake_snapshot.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/types/value_map.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "storage/ducklake_catalog_set.hpp"
#include "storage/ducklake_inlined_data.hpp"
#include "storage/ducklake_metadata_manager.hpp"

namespace duckdb {
struct NewMacroInfo;
class DuckLakeCatalog;
class DuckLakeCatalogSet;
class DuckLakeMetadataManager;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;
class DuckLakeViewEntry;
struct DuckLakeNewGlobalStats;
struct DuckLakeTableStats;
struct SnapshotChangeInfo;
struct SnapshotChangeInformation;
struct TransactionChangeInformation;
struct NewDataInfo;
struct NewTableInfo;
struct NewNameMapInfo;
struct CompactionInformation;
struct DuckLakePath;
struct DuckLakeCommitState;
class DuckLakeFieldId;
class LocalTableChangeIterationHelper;
class DuckLakeTransactionState;

struct FlushedInlinedTableInfo {
	DuckLakeInlinedTableInfo inlined_table;
	idx_t flush_snapshot_id;
};

struct LocalTableDataChanges {
	vector<DuckLakeDataFile> new_data_files;
	unique_ptr<DuckLakeInlinedData> new_inlined_data;
	unordered_map<string, vector<DuckLakeDeleteFile>> new_delete_files;
	unordered_map<string, unique_ptr<DuckLakeInlinedDataDeletes>> new_inlined_data_deletes;
	unique_ptr<DuckLakeInlinedFileDeletes> new_inlined_file_deletes;
	vector<DuckLakeCompactionEntry> compactions;
	bool IsEmpty() const;
};

struct DuckLakeNewGlobalStats {
	DuckLakeTableStats stats;
	bool initialized = false;
};

struct LocalTableChanges {
public:
	void Clear();
	bool HasChanges() const;
	LocalTableChangeIterationHelper Changes() const;
	void CleanupFiles(DatabaseInstance &db);
	void CleanupFiles(ClientContext &context, TableIndex table_id);
	bool HasTransactionLocalInserts(TableIndex table_id) const;
	bool HasTransactionInlinedData(TableIndex table_id) const;
	vector<DuckLakeDataFile> GetTransactionLocalFiles(TableIndex table_id) const;
	shared_ptr<DuckLakeInlinedData> GetTransactionLocalInlinedData(ClientContext &context, TableIndex table_id) const;
	void DropTransactionLocalFile(ClientContext &context, TableIndex table_id, const string &path);
	void AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files);
	void AppendInlinedData(ClientContext &context, TableIndex table_id, unique_ptr<DuckLakeInlinedData> new_data);
	void AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes);
	void DeleteFromLocalInlinedData(ClientContext &context, TableIndex table_id, set<idx_t> new_deletes);
	void AddColumnToLocalInlinedData(ClientContext &context, TableIndex table_id, const LogicalType &new_column_type,
	                                 FieldIndex new_field_index, const Value &default_value);
	void RemoveColumnFromLocalInlinedData(ClientContext &context, TableIndex table_id,
	                                      LogicalIndex removed_column_index, const DuckLakeFieldId &field_id);
	optional_ptr<DuckLakeInlinedDataDeletes> GetInlinedDeletes(TableIndex table_id, const string &table_name) const;
	void AddNewInlinedFileDeletes(TableIndex table_id, idx_t file_id, set<idx_t> new_deletes);
	void AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry);
	bool HasLocalDeletes(TableIndex table_id) const;
	bool HasLocalDeleteForFile(TableIndex table_id, const string &path) const;
	bool HasAnyLocalChanges(TableIndex table_id) const;

	void GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &result) const;
	bool HasLocalInlinedFileDeletes(TableIndex table_id) const;

	void GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id, set<idx_t> &result) const;

	void TransactionLocalDelete(ClientContext &context, TableIndex table_id, const string &data_file_path,
	                            DuckLakeDeleteFile delete_file);
	void AddDeletes(ClientContext &context, TableIndex table_id, vector<DuckLakeDeleteFile> files);
	static void AddDeletesToMap(ClientContext &context, vector<DuckLakeDeleteFile> new_deletes,
	                            unordered_map<string, vector<DuckLakeDeleteFile>> &delete_file_map);

private:
	mutable mutex lock;
	map<TableIndex, LocalTableDataChanges> changes;
};

class LocalTableChangeIterationHelper {
public:
	LocalTableChangeIterationHelper(mutex &local_changes_lock, const map<TableIndex, LocalTableDataChanges> &changes);

private:
	unique_lock<mutex> lock;
	const map<TableIndex, LocalTableDataChanges> &changes;

private:
	struct LocalTableChangeIteratorEntry {
		friend class LocalTableChangeIterationHelper;

	public:
		LocalTableChangeIteratorEntry();
		TableIndex GetTableIndex() const;
		const LocalTableDataChanges &GetTableChanges() const;

	private:
		TableIndex table_id;
		optional_ptr<const LocalTableDataChanges> changes;
	};
	class LocalTableChangeIterator {
	public:
		explicit LocalTableChangeIterator(map<TableIndex, LocalTableDataChanges>::const_iterator it,
		                                  map<TableIndex, LocalTableDataChanges>::const_iterator end_it);
		map<TableIndex, LocalTableDataChanges>::const_iterator it;
		map<TableIndex, LocalTableDataChanges>::const_iterator end_it;
		LocalTableChangeIteratorEntry entry;

	public:
		LocalTableChangeIterator &operator++();
		bool operator!=(const LocalTableChangeIterator &other) const;
		const LocalTableChangeIteratorEntry &operator*() const;
	};

public:
	LocalTableChangeIterator begin() { // NOLINT: match stl API
		return LocalTableChangeIterator(changes.begin(), changes.end());
	}
	LocalTableChangeIterator end() { // NOLINT: match stl API
		return LocalTableChangeIterator(changes.end(), changes.end());
	}
};

struct SnapshotAndStats {
	vector<DuckLakeGlobalStatsInfo> stats;
	DuckLakeSnapshot snapshot;
};

struct DuckLakeRetryConfig {
	idx_t max_retry_count = 10;
	idx_t retry_wait_ms = 100;
	double retry_backoff = 1.5;

	static DuckLakeRetryConfig FromContext(ClientContext &context);
};

class DuckLakeTransaction : public Transaction, public enable_shared_from_this<DuckLakeTransaction> {
	friend class DuckLakeTransactionState;

public:
	DuckLakeTransaction(DuckLakeCatalog &ducklake_catalog, TransactionManager &manager, ClientContext &context);
	~DuckLakeTransaction() override;

public:
	virtual void Start();
	virtual void Commit();
	virtual void Rollback();

	//! Returns true if `message` indicates a retryable conflict (PK/unique/conflict/concurrent).
	static bool RetryOnError(const string &message);

	DuckLakeCatalog &GetCatalog() {
		return ducklake_catalog;
	}
	DuckLakeMetadataManager &GetMetadataManager() {
		return *metadata_manager;
	}

	DuckLakeSnapshotCommit &GetCommitInfo() {
		return commit_info;
	}
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string query);
	unique_ptr<QueryResult> Query(string query);
	//! Execute SQL on the metadata connection without placeholder substitution or metadata-manager wrapping.
	unique_ptr<QueryResult> ExecuteRaw(string query);
	Connection &GetConnection();

	DuckLakeSnapshot GetSnapshot();
	DuckLakeSnapshot GetSnapshot(optional_ptr<BoundAtClause> at_clause,
	                             SnapshotBound bound = SnapshotBound::UPPER_BOUND);

	static DuckLakeTransaction &Get(ClientContext &context, Catalog &catalog);

	void CreateEntry(unique_ptr<CatalogEntry> entry);
	void DropEntry(CatalogEntry &entry);
	bool IsDeleted(CatalogEntry &entry);
	bool IsRenamed(CatalogEntry &entry);
	optional_ptr<CatalogEntry> GetLocalEntryById(SchemaIndex schema_id);
	optional_ptr<CatalogEntry> GetLocalEntryById(TableIndex table_id);

	void AlterEntry(CatalogEntry &old_entry, unique_ptr<CatalogEntry> new_entry);

	DuckLakeCatalogSet &GetOrCreateTransactionLocalEntries(CatalogEntry &entry);
	optional_ptr<DuckLakeCatalogSet> GetTransactionLocalSchemas();
	optional_ptr<DuckLakeCatalogSet> GetTransactionLocalEntries(CatalogType type, const string &schema_name);
	optional_ptr<CatalogEntry> GetTransactionLocalEntry(CatalogType catalog_type, const string &schema_name,
	                                                    const string &entry_name);
	vector<DuckLakeDataFile> GetTransactionLocalFiles(TableIndex table_id) const;
	shared_ptr<DuckLakeInlinedData> GetTransactionLocalInlinedData(TableIndex table_id) const;
	void DropTransactionLocalFile(TableIndex table_id, const string &path);
	bool HasTransactionLocalInserts(TableIndex table_id) const;
	bool HasTransactionInlinedData(TableIndex table_id) const;
	void AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files);
	void AddDeletes(TableIndex table_id, vector<DuckLakeDeleteFile> files);
	void AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry);

	MappingIndex AddNameMap(unique_ptr<DuckLakeNameMap> name_map);
	const DuckLakeNameMap &GetMappingById(MappingIndex mapping_id);
	NewNameMapInfo GetNewNameMaps(DuckLakeCommitState &commit_state);

	void AppendInlinedData(TableIndex table_id, unique_ptr<DuckLakeInlinedData> collection);
	void AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes);
	void DeleteFromLocalInlinedData(TableIndex table_id, set<idx_t> new_deletes);
	void AddColumnToLocalInlinedData(TableIndex table_id, const LogicalType &new_column_type,
	                                 FieldIndex new_field_index, const Value &default_value = Value());
	void RemoveColumnFromLocalInlinedData(TableIndex table_id, LogicalIndex removed_column_index,
	                                      const DuckLakeFieldId &field_id);
	optional_ptr<DuckLakeInlinedDataDeletes> GetInlinedDeletes(TableIndex table_id, const string &table_name) const;
	vector<DuckLakeDeletedInlinedDataInfo> GetNewInlinedDeletes(DuckLakeCommitState &commit_state) const;

	//! Add inlined file deletions (deletions from parquet files stored in metadata)
	void AddNewInlinedFileDeletes(TableIndex table_id, idx_t file_id, set<idx_t> new_deletes);
	//! Get all inlined file deletions for commit
	vector<DuckLakeInlinedFileDeletionInfo> GetNewInlinedFileDeletes(DuckLakeCommitState &commit_state);

	void DropSchema(DuckLakeSchemaEntry &schema);
	void DropTable(DuckLakeTableEntry &table);
	void DropView(DuckLakeViewEntry &view);
	void DropScalarMacro(DuckLakeScalarMacroEntry &macro);
	void DropTableMacro(DuckLakeTableMacroEntry &macro);
	void DropFile(TableIndex table_id, DataFileIndex data_file_id, string path);

	void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots);
	void DeleteInlinedData(const DuckLakeInlinedTableInfo &inlined_table);
	//! Delete inlined data rows with begin_snapshot <= flush_snapshot_id
	void DeleteFlushedInlinedData(const DuckLakeInlinedTableInfo &inlined_table, idx_t flush_snapshot_id);
	//! Marks that inlined data have been deleted in a flush if retries are necessary
	void MarkInlinedDataForDeletion(DuckLakeInlinedTableInfo inlined_table, idx_t flush_snapshot_id);

	bool ChangesMade() const;
	idx_t GetLocalCatalogId();
	static bool IsTransactionLocal(idx_t id) {
		return id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
	void SetConfigOption(const DuckLakeConfigOption &option);

	void SetCommitMessage(const DuckLakeSnapshotCommit &option);

	string GetDefaultSchemaName();

	bool HasLocalDeletes(TableIndex table_id) const;
	bool HasLocalDeleteForFile(TableIndex table_id, const string &path) const;
	void GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &delete_file) const;
	void TransactionLocalDelete(TableIndex table_id, const string &data_path, DuckLakeDeleteFile delete_file);

	bool HasLocalInlinedFileDeletes(TableIndex table_id) const;
	void GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id, set<idx_t> &result) const;

	bool HasDroppedFiles() const;
	bool FileIsDropped(const string &path) const;
	//! Check if there are any uncommitted changes for this table (inserts, deletes, or dropped files)
	bool HasAnyLocalChanges(TableIndex table_id) const;

	string GenerateUUID() const;
	static string GenerateUUIDv7();

	const LocalTableChanges &GetLocalChanges() const;
	const set<TableIndex> &GetDroppedTables();
	const set<TableIndex> &GetDroppedViews();
	const set<MacroIndex> &GetDroppedScalarMacros();
	const set<MacroIndex> &GetDroppedTableMacros();
	const set<TableIndex> &GetRenamedTables();
	const case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> &GetNewTables();
	//! Returns the current version of the catalog:
	//! If there are no uncommitted changes, this is the schema version of the snapshot.
	//! Otherwise, it is an id that is incremented whenever the schema changes (not stored between restarts)
	idx_t GetCatalogVersion();

protected:
	void SetMetadataManager(unique_ptr<DuckLakeMetadataManager> metadata_manager) {
		this->metadata_manager = std::move(metadata_manager);
	}

public:
	void RunCommitLoop(DuckLakeSnapshot transaction_snapshot, const TransactionChangeInformation &transaction_changes,
	                   const DuckLakeRetryConfig &retry_config);
	void ApplyServerSideCommit(idx_t schema_version);

	static DuckLakeGlobalStatsInfo ConvertNewGlobalStats(TableIndex table_id,
	                                                     const DuckLakeNewGlobalStats &new_global_stats);

	static DuckLakeFileInfo BuildDataFileInfo(const DuckLakeDataFile &file, DuckLakeSnapshot &commit_snapshot,
	                                          TableIndex table_id, optional_idx row_id_start);

private:
	void FlushChanges();
	NewMacroInfo GetNewMacros(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes);
	static DuckLakePartitionInfo GetNewPartitionKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table);
	static DuckLakeSortInfo GetNewSortKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table);
	static DuckLakeTableInfo GetNewTable(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table);
	static DuckLakeViewInfo GetNewView(DuckLakeCommitState &commit_state, DuckLakeViewEntry &view);
	DuckLakeFileInfo GetNewDataFile(const DuckLakeDataFile &file, DuckLakeCommitState &commit_state,
	                                TableIndex table_id, optional_idx row_id_start);
	NewDataInfo GetNewDataFiles(string &batch_query, DuckLakeCommitState &commit_state,
	                            optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats);
	vector<DuckLakeDeleteFileInfo>
	GetNewDeleteFiles(const DuckLakeCommitState &commit_state,
	                  vector<DuckLakeOverwrittenDeleteFile> &overwritten_delete_files) const;
	DuckLakeDeleteFileInfo GetNewDeleteFile(TableIndex table_id, const DuckLakeCommitState &commit_state,
	                                        const DuckLakeDeleteFile &file) const;
	string UpdateGlobalTableStats(TableIndex table_id, const DuckLakeNewGlobalStats &new_stats);
	//! Return the set of changes made by this transaction
	TransactionChangeInformation GetTransactionChanges() const;
	void GetNewMacroInfo(DuckLakeCommitState &commit_state, reference<CatalogEntry> macro_entry, NewMacroInfo &result);
	CompactionInformation GetCompactionChanges(DuckLakeCommitState &commit_state, CompactionType type);

	void AlterEntryInternal(DuckLakeTableEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	void AlterEntryInternal(DuckLakeViewEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	static void AddTableChanges(TableIndex table_id, const LocalTableDataChanges &table_changes,
	                            TransactionChangeInformation &changes);
	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> &GetNewMacroMap(CatalogType type);

private:
	DuckLakeCatalog &ducklake_catalog;
	DuckLakeSnapshotCommit commit_info;
	DatabaseInstance &db;
	unique_ptr<DuckLakeMetadataManager> metadata_manager;
	mutex connection_lock;
	unique_ptr<Connection> connection;
	//! The snapshot of the transaction (latest snapshot in DuckLake)
	mutex snapshot_lock;
	unique_ptr<DuckLakeSnapshot> snapshot;
	idx_t local_catalog_id;
	//! Per-transaction mutable change state (new/dropped/renamed entries, local file changes, flushed
	//! inlined tables) and the Commit loop. Owns the data formerly held directly on the transaction.
	unique_ptr<DuckLakeTransactionState> state;
	//! Snapshot cache for the AT (...) conditions that are referenced in the transaction
	value_map_t<DuckLakeSnapshot> snapshot_cache;
	//! New set of transaction-local name maps
	DuckLakeNameMapSet new_name_maps;

	atomic<idx_t> catalog_version;
};

} // namespace duckdb
