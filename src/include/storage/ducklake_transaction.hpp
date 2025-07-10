//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_transaction.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

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

struct LocalTableDataChanges {
	vector<DuckLakeDataFile> new_data_files;
	unique_ptr<DuckLakeInlinedData> new_inlined_data;
	unordered_map<string, DuckLakeDeleteFile> new_delete_files;
	unordered_map<string, unique_ptr<DuckLakeInlinedDataDeletes>> new_inlined_data_deletes;
	vector<DuckLakeCompactionEntry> compactions;

	bool IsEmpty() const;
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
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string query);
	unique_ptr<QueryResult> Query(string query);
	Connection &GetConnection();

	DuckLakeSnapshot GetSnapshot();
	DuckLakeSnapshot GetSnapshot(optional_ptr<BoundAtClause> at_clause);

	static DuckLakeTransaction &Get(ClientContext &context, Catalog &catalog);

	void CreateEntry(unique_ptr<CatalogEntry> entry);
	void DropEntry(CatalogEntry &entry);
	bool IsDeleted(CatalogEntry &entry);
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
	bool HasTransactionLocalChanges(TableIndex table_id) const;
	bool HasTransactionInlinedData(TableIndex table_id) const;
	void AppendFiles(TableIndex table_id, vector<DuckLakeDataFile> files);
	void AddDeletes(TableIndex table_id, vector<DuckLakeDeleteFile> files);
	void AddCompaction(TableIndex table_id, DuckLakeCompactionEntry entry);

	MappingIndex AddNameMap(unique_ptr<DuckLakeNameMap> name_map);
	const DuckLakeNameMap &GetMappingById(MappingIndex mapping_id);
	NewNameMapInfo GetNewNameMaps(DuckLakeSnapshot &commit_snapshot);

	void AppendInlinedData(TableIndex table_id, unique_ptr<DuckLakeInlinedData> collection);
	void AddNewInlinedDeletes(TableIndex table_id, const string &table_name, set<idx_t> new_deletes);
	void DeleteFromLocalInlinedData(TableIndex table_id, set<idx_t> new_deletes);
	optional_ptr<DuckLakeInlinedDataDeletes> GetInlinedDeletes(TableIndex table_id, const string &table_name);
	vector<DuckLakeDeletedInlinedDataInfo> GetNewInlinedDeletes(DuckLakeSnapshot &commit_snapshot);

	void DropSchema(DuckLakeSchemaEntry &schema);
	void DropTable(DuckLakeTableEntry &table);
	void DropView(DuckLakeViewEntry &view);
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

	string GetDefaultSchemaName();

	bool HasLocalDeletes(TableIndex table_id);
	void GetLocalDeleteForFile(TableIndex table_id, const string &path, DuckLakeFileData &delete_file);
	void TransactionLocalDelete(TableIndex table_id, const string &data_path, DuckLakeDeleteFile delete_file);

	bool HasDroppedFiles() const;
	bool FileIsDropped(const string &path) const;

	string GenerateUUID() const;
	static string GenerateUUIDv7();

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
	void CommitChanges(DuckLakeSnapshot &commit_snapshot, TransactionChangeInformation &transaction_changes);
	void CommitCompaction(DuckLakeSnapshot &commit_snapshot, TransactionChangeInformation &transaction_changes);
	void FlushDrop(DuckLakeSnapshot commit_snapshot, const string &metadata_table_name, const string &id_name,
	               unordered_set<idx_t> &dropped_entries);
	vector<DuckLakeSchemaInfo> GetNewSchemas(DuckLakeSnapshot &commit_snapshot);
	NewTableInfo GetNewTables(DuckLakeSnapshot &commit_snapshot, TransactionChangeInformation &transaction_changes);
	DuckLakePartitionInfo GetNewPartitionKey(DuckLakeSnapshot &commit_snapshot, DuckLakeTableEntry &tabletable_id);
	DuckLakeTableInfo GetNewTable(DuckLakeSnapshot &commit_snapshot, DuckLakeTableEntry &table);
	DuckLakeViewInfo GetNewView(DuckLakeSnapshot &commit_snapshot, DuckLakeViewEntry &view);
	void FlushNewPartitionKey(DuckLakeSnapshot &commit_snapshot, DuckLakeTableEntry &table);
	DuckLakeFileInfo GetNewDataFile(DuckLakeDataFile &file, DuckLakeSnapshot &commit_snapshot, TableIndex table_id,
	                                optional_idx row_id_start);
	NewDataInfo GetNewDataFiles(DuckLakeSnapshot &commit_snapshot);
	vector<DuckLakeDeleteFileInfo> GetNewDeleteFiles(DuckLakeSnapshot &commit_snapshot,
	                                                 set<DataFileIndex> &overwritten_delete_files);
	void UpdateGlobalTableStats(TableIndex table_id, const DuckLakeNewGlobalStats &new_stats);
	void CheckForConflicts(DuckLakeSnapshot transaction_snapshot, const TransactionChangeInformation &changes);
	void CheckForConflicts(const TransactionChangeInformation &changes, const SnapshotChangeInformation &other_changes);
	void WriteSnapshotChanges(DuckLakeSnapshot commit_snapshot, TransactionChangeInformation &changes);
	//! Return the set of changes made by this transaction
	TransactionChangeInformation GetTransactionChanges();
	void GetNewTableInfo(DuckLakeSnapshot &commit_snapshot, DuckLakeCatalogSet &catalog_set,
	                     reference<CatalogEntry> table_entry, NewTableInfo &result,
	                     TransactionChangeInformation &transaction_changes);
	void GetNewViewInfo(DuckLakeSnapshot &commit_snapshot, DuckLakeCatalogSet &catalog_set,
	                    reference<CatalogEntry> table_entry, NewTableInfo &result,
	                    TransactionChangeInformation &transaction_changes);
	CompactionInformation GetCompactionChanges(DuckLakeSnapshot &commit_snapshot);

	void AlterEntryInternal(DuckLakeTableEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	void AlterEntryInternal(DuckLakeViewEntry &old_entry, unique_ptr<CatalogEntry> new_entry);
	void AddTableChanges(TableIndex table_id, const LocalTableDataChanges &table_changes,
	                     TransactionChangeInformation &changes);

private:
	DuckLakeCatalog &ducklake_catalog;
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
