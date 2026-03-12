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
struct SnapshotChangeInformation;
struct TransactionChangeInformation;
struct NewDataInfo;
struct NewTableInfo;
struct NewNameMapInfo;
struct CompactionInformation;
struct DuckLakePath;
struct DuckLakeCommitState;
class DuckLakeFieldId;

struct LocalTableDataChanges {
	vector<DuckLakeDataFile> new_data_files;
	unique_ptr<DuckLakeInlinedData> new_inlined_data;
	unordered_map<string, vector<DuckLakeDeleteFile>> new_delete_files;
	unordered_map<string, unique_ptr<DuckLakeInlinedDataDeletes>> new_inlined_data_deletes;
	unique_ptr<DuckLakeInlinedFileDeletes> new_inlined_file_deletes;
	vector<DuckLakeCompactionEntry> compactions;
	bool IsEmpty() const;
};

struct SnapshotAndStats {
	vector<DuckLakeGlobalStatsInfo> stats;
	DuckLakeSnapshot snapshot;
};
class DuckLakeTransaction : public Transaction, public enable_shared_from_this<DuckLakeTransaction> {
public:
	DuckLakeTransaction(DuckLakeCatalog &ducklake_catalog, TransactionManager &manager, ClientContext &context);
	~DuckLakeTransaction() override;

public:
	virtual void Start();
	virtual void Commit();
	virtual void Rollback();

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
	vector<DuckLakeDataFile> GetTransactionLocalFiles(TableIndex table_id);
	shared_ptr<DuckLakeInlinedData> GetTransactionLocalInlinedData(TableIndex table_id);
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
	optional_ptr<DuckLakeInlinedDataDeletes> GetInlinedDeletes(TableIndex table_id, const string &table_name);
	vector<DuckLakeDeletedInlinedDataInfo> GetNewInlinedDeletes(DuckLakeCommitState &commit_state);

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

	bool SchemaChangesMade();
	bool ChangesMade();
	idx_t GetLocalCatalogId();
	static bool IsTransactionLocal(idx_t id) {
		return id >= DuckLakeConstants::TRANSACTION_LOCAL_ID_START;
	}
	void SetConfigOption(const DuckLakeConfigOption &option);

	void SetCommitMessage(const DuckLakeSnapshotCommit &option);

	string GetDefaultSchemaName();

	bool HasLocalDeletes(TableIndex table_id);
	void GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &delete_file);
	void TransactionLocalDelete(TableIndex table_id, const string &data_path, DuckLakeDeleteFile delete_file);

	bool HasLocalInlinedFileDeletes(TableIndex table_id);
	void GetLocalInlinedFileDeletesForFile(TableIndex table_id, idx_t file_id, set<idx_t> &result);

	bool HasDroppedFiles() const;
	bool FileIsDropped(const string &path) const;
	//! Check if there are any uncommitted changes for this table (inserts, deletes, or dropped files)
	bool HasAnyLocalChanges(TableIndex table_id);

	string GenerateUUID() const;
	static string GenerateUUIDv7();

	const set<TableIndex> &GetDroppedTables() {
		return dropped_tables;
	}
	const set<MacroIndex> &GetDroppedScalarMacros() {
		return dropped_scalar_macros;
	}
	const set<MacroIndex> &GetDroppedTableMacros() {
		return dropped_table_macros;
	}
	const set<TableIndex> &GetRenamedTables() {
		return renamed_tables;
	}
	const case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> &GetNewTables() {
		return new_tables;
	}
	//! Returns the current version of the catalog:
	//! If there are no uncommitted changes, this is the schema version of the snapshot.
	//! Otherwise, it is an id that is incremented whenever the schema changes (not stored between restarts)
	idx_t GetCatalogVersion();

protected:
	void SetMetadataManager(unique_ptr<DuckLakeMetadataManager> metadata_manager) {
		this->metadata_manager = std::move(metadata_manager);
	}

private:
	void CleanupFiles();
	void FlushChanges();
	void FlushSettingChanges();
	string CommitChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes,
	                     optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats);
	void CommitCompaction(DuckLakeSnapshot &commit_snapshot, TransactionChangeInformation &transaction_changes);
	void FlushDrop(DuckLakeSnapshot commit_snapshot, const string &metadata_table_name, const string &id_name,
	               unordered_set<idx_t> &dropped_entries);
	vector<DuckLakeSchemaInfo> GetNewSchemas(DuckLakeCommitState &commit_state);
	NewTableInfo GetNewTables(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes);
	NewMacroInfo GetNewMacros(DuckLakeCommitState &commit_state, TransactionChangeInformation &transaction_changes);
	DuckLakePartitionInfo GetNewPartitionKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &tabletable_id);
	DuckLakeSortInfo GetNewSortKey(DuckLakeCommitState &commit_state, DuckLakeTableEntry &tabletable_id);
	DuckLakeTableInfo GetNewTable(DuckLakeCommitState &commit_state, DuckLakeTableEntry &table);
	DuckLakeViewInfo GetNewView(DuckLakeCommitState &commit_state, DuckLakeViewEntry &view);
	void FlushNewPartitionKey(DuckLakeSnapshot &commit_snapshot, DuckLakeTableEntry &table);
	DuckLakeFileInfo GetNewDataFile(DuckLakeDataFile &file, DuckLakeSnapshot &commit_snapshot, TableIndex table_id,
	                                optional_idx row_id_start);
	NewDataInfo GetNewDataFiles(string &batch_query, DuckLakeCommitState &commit_state,
	                            optional_ptr<vector<DuckLakeGlobalStatsInfo>> stats);
	vector<DuckLakeDeleteFileInfo>
	GetNewDeleteFiles(const DuckLakeCommitState &commit_state,
	                  vector<DuckLakeOverwrittenDeleteFile> &overwritten_delete_files) const;
	string UpdateGlobalTableStats(TableIndex table_id, const DuckLakeNewGlobalStats &new_stats);
	SnapshotAndStats CheckForConflicts(DuckLakeSnapshot transaction_snapshot,
	                                   const TransactionChangeInformation &changes);
	void CheckForConflicts(const TransactionChangeInformation &changes, const SnapshotChangeInformation &other_changes,
	                       DuckLakeSnapshot transaction_snapshot);
	string WriteSnapshotChanges(DuckLakeCommitState &commit_state, TransactionChangeInformation &changes);
	//! Return the set of changes made by this transaction
	TransactionChangeInformation GetTransactionChanges();
	void GetNewTableInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
	                     reference<CatalogEntry> table_entry, NewTableInfo &result,
	                     TransactionChangeInformation &transaction_changes);
	void GetNewMacroInfo(DuckLakeCommitState &commit_state, reference<CatalogEntry> macro_entry, NewMacroInfo &result);
	void GetNewViewInfo(DuckLakeCommitState &commit_state, DuckLakeCatalogSet &catalog_set,
	                    reference<CatalogEntry> table_entry, NewTableInfo &result,
	                    TransactionChangeInformation &transaction_changes);
	CompactionInformation GetCompactionChanges(DuckLakeSnapshot &commit_snapshot, CompactionType type);

	void AlterEntryInternal(DuckLakeTableEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	void AlterEntryInternal(DuckLakeViewEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	void AddTableChanges(TableIndex table_id, const LocalTableDataChanges &table_changes,
	                     TransactionChangeInformation &changes);

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
	//! New tables added by this transaction
	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_tables;
	set<TableIndex> dropped_tables;

	//! New macros added by this transaction
	case_insensitive_map_t<unique_ptr<DuckLakeCatalogSet>> new_macros;
	set<MacroIndex> dropped_scalar_macros;
	set<MacroIndex> dropped_table_macros;

	set<TableIndex> renamed_tables;
	set<TableIndex> dropped_views;
	unordered_map<string, DataFileIndex> dropped_files;
	set<TableIndex> tables_deleted_from;
	//! Schemas added by this transaction
	unique_ptr<DuckLakeCatalogSet> new_schemas;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> dropped_schemas;
	//! Local changes made to tables
	mutex table_data_changes_lock;
	map<TableIndex, LocalTableDataChanges> table_data_changes;
	//! Snapshot cache for the AT (...) conditions that are referenced in the transaction
	value_map_t<DuckLakeSnapshot> snapshot_cache;
	//! New set of transaction-local name maps
	DuckLakeNameMapSet new_name_maps;

	atomic<idx_t> catalog_version;
};

} // namespace duckdb
