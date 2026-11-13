//===----------------------------------------------------------------------===//
//                         DuckDB
//
// metadata_manager/rest_metadata_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "storage/ducklake_metadata_manager.hpp"
#include "rest/ducklake_rest_client.hpp"
#include "metadata_manager/rest_inlined_rows_cache.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {
class DuckLakeTableEntry;
class DuckLakeSchemaEntry;
struct CreateTableInfo;
struct CreateSchemaInfo;

//! Communicates with a DuckLake REST server instead of a metadata database
class RESTMetadataManager : public DuckLakeMetadataManager {
public:
	explicit RESTMetadataManager(DuckLakeTransaction &transaction);
	~RESTMetadataManager() override;

	static unique_ptr<DuckLakeMetadataManager> Create(DuckLakeTransaction &transaction) {
		return make_uniq<RESTMetadataManager>(transaction);
	}

	bool IsRemote() const override {
		return true;
	}
	bool CommitsEachStatement() const override {
		return true;
	}
	bool CanSkipSnapshotFetch(const TransactionChangeInformation &changes) const override {
		return true;
	}
	bool HasPendingChanges() const override;
	void FlushChangesServerSide(DuckLakeTransaction &transaction, DuckLakeSnapshot transaction_snapshot,
	                            const TransactionChangeInformation &transaction_changes,
	                            const DuckLakeRetryConfig &retry_config) override;

	unique_ptr<QueryResult> AttachMetadata(const string &attach_query) override;
	bool MetadataExists() override;
	void InitializeDuckLake(bool has_explicit_schema, DuckLakeEncryption encryption) override;
	DuckLakeMetadata LoadDuckLake() override;

	using DuckLakeMetadataManager::Execute;
	using DuckLakeMetadataManager::Query;
	unique_ptr<QueryResult> Execute(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Execute(string &query) override;
	unique_ptr<QueryResult> ExecuteInTransaction(string &query) override;
	unique_ptr<QueryResult> Query(DuckLakeSnapshot snapshot, string &query) override;
	unique_ptr<QueryResult> Query(string &query) override;

	unique_ptr<DuckLakeSnapshot> GetSnapshot() override;
	unique_ptr<DuckLakeSnapshot> GetSnapshot(BoundAtClause &at_clause, SnapshotBound bound,
	                                         idx_t newest_snapshot_id) override;
	DuckLakeCatalogInfo GetCatalogForSnapshot(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeGlobalStatsInfo> GetGlobalTableStats(DuckLakeSnapshot snapshot,
	                                                    const vector<TableIndex> &table_ids = {}) override;
	map<TableIndex, idx_t> GetTableRecordCounts(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeFileListEntry>
	GetFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                 const FilterPushdownInfo *filter_info = nullptr, bool include_local_changes = true,
	                 optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr) override;
	vector<DuckLakeFileListExtendedEntry>
	GetExtendedFilesForTable(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot,
	                         const FilterPushdownInfo *filter_info = nullptr,
	                         optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr) override;
	unique_ptr<QueryResult> ReadInlinedDataInsertions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
	                                                  const Identifier &inlined_table_name,
	                                                  const vector<string> &columns_to_read) override;
	unique_ptr<QueryResult> ReadInlinedDataDeletions(DuckLakeSnapshot start_snapshot, DuckLakeSnapshot end_snapshot,
	                                                 const Identifier &inlined_table_name,
	                                                 const vector<string> &columns_to_read) override;
	unique_ptr<QueryResult> ReadInlinedData(DuckLakeSnapshot snapshot, const Identifier &inlined_table_name,
	                                        const vector<string> &columns_to_read) override;
	vector<DuckLakeSnapshotInfo> GetAllSnapshots(const DuckLakeSnapshotFilter &filter = {}) override;
	void DeleteSnapshots(const vector<DuckLakeSnapshotInfo> &snapshots) override;
	vector<DuckLakeSnapshotTagInfo> GetSnapshotTags(bool with_snapshot_time = true) override;
	vector<DuckLakeFileForCleanup> GetOldFilesForCleanup(const string &older_than) override;
	unique_ptr<QueryResult> ReadMetadataTable(const string &table_name) override;
	vector<string> GetKnownFilePaths() override;
	idx_t SetConfigOption(const DuckLakeConfigOption &option) override;
	bool ResetConfigOption(const DuckLakeConfigOption &option) override;
	vector<DuckLakeTableSizeInfo> GetTableSizes(DuckLakeSnapshot snapshot) override;
	vector<DuckLakeColumnMappingInfo> GetColumnMappings(optional_idx start_from) override;
	vector<DuckLakeFileListEntry>
	GetTableInsertions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot,
	                   optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr) override;
	vector<DuckLakeDeleteScanEntry>
	GetTableDeletions(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot, DuckLakeSnapshot snapshot,
	                  bool include_local_changes = true,
	                  optional_ptr<const vector<DuckLakeProjectedColumn>> projection = nullptr) override;
	void RemoveFilesScheduledForCleanup(const vector<DuckLakeFileForCleanup> &cleaned_up_files) override;
	optional_idx GetNetRowCount(DuckLakeTableEntry &table, DuckLakeSnapshot snapshot) override;
	vector<DuckLakeInlinedTableInfo> GetInlinedDataTables(const DuckLakeTableEntry &table,
	                                                      DuckLakeSnapshot snapshot) override;

	void OnCreateSchema(DuckLakeSchemaEntry &schema) override;
	void OnCreateTable(DuckLakeSchemaEntry &schema, const CreateTableInfo &info,
	                   const DuckLakeTableEntry &table) override;
	void OnCreateView(DuckLakeViewEntry &view) override;
	void OnCreateMacro(MacroCatalogEntry &macro) override;
	void OnAlterEntry(CatalogEntry &entry, const AlterInfo &info) override;
	void OnDropEntry(CatalogEntry &entry) override;
	void OnEntryChanged(CatalogEntry &entry) override;
	void OnDropFile(TableIndex table_id, DataFileIndex data_file_id, const string &path, idx_t row_count,
	                idx_t file_size_bytes) override;

private:
	DuckLakeRESTClient &GetClient();
	//! Adds a catalog change to the requests that are sent when the transaction commits
	template <class T>
	T &AddCommitRequest() {
		auto request = make_uniq<T>();
		auto &result = *request;
		catalog_requests.push_back(std::move(request));
		return result;
	}
	unique_ptr<QueryResult> ToQueryResult(RESTRowSet rows);
	//! Whether a scan has to ask the server for the inlined rows, and for which columns: other scans of the table at
	//! the snapshot can have the rows already, and scans that need other columns ask for all columns
	struct InlinedRowsPlan {
		bool fetch;
		RESTInlinedProjection projection;
	};
	InlinedRowsPlan PlanInlinedRows(const RESTInlinedRowsKey &key,
	                                optional_ptr<const vector<DuckLakeProjectedColumn>> projection);
	//! Fetches the inlined rows of a table with all columns, if a scan needs rows that the cache does not have
	shared_ptr<RESTInlinedRowsResponse> FetchInlinedRows(const RESTInlinedRowsKey &key, DuckLakeSnapshot start_snapshot,
	                                                     DuckLakeSnapshot snapshot);
	//! Reads the columns from the cached rows of the inlined data, or fetches the rows from the server
	unique_ptr<QueryResult> ReadCachedInlinedRows(RESTInlinedRowsKind kind, DuckLakeSnapshot start_snapshot,
	                                              DuckLakeSnapshot snapshot, const string &inlined_table_name,
	                                              const vector<string> &columns_to_read);
	RESTSnapshotData GetSnapshotData(DuckLakeSnapshot snapshot, bool include_catalog, bool include_stats);
	void AddDeletes(const TransactionChangeInformation &changes, vector<unique_ptr<RESTCommitRequest>> &requests);
	void AddInserts(const TransactionChangeInformation &changes, vector<unique_ptr<RESTCommitRequest>> &requests);
	RESTTableChangesResponse GetTableChanges(DuckLakeTableEntry &table, DuckLakeSnapshot start_snapshot,
	                                         DuckLakeSnapshot snapshot, const string &changes,
	                                         optional_ptr<const vector<DuckLakeProjectedColumn>> projection);
	vector<RESTDroppedFile> GetDroppedFiles(TableIndex table_id);

private:
	//! The catalog changes of the transaction, in the order in which they were made
	vector<unique_ptr<RESTCommitRequest>> catalog_requests;
	//! The oids of the entries that the transaction created, by their placeholder ids
	map<int64_t, idx_t> created_oids;
	//! The placeholder ids of the macros that the transaction created
	reference_map_t<CatalogEntry, int64_t> local_macros;
	mutex cache_lock;
	//! The catalog of a snapshot that the transaction loaded, which came along with the snapshot
	unique_ptr<DuckLakeCatalogInfo> prefetched_catalog;
	idx_t prefetched_metadata_version = DConstants::INVALID_INDEX;
	//! The inlined rows that came along with file lists, which scans read the columns they need from
	RESTInlinedRowsCache inlined_rows_cache;
	//! The table statistics read by the transaction, by snapshot
	unordered_map<idx_t, vector<DuckLakeGlobalStatsInfo>> table_stats;
	//! The data files dropped by the transaction, by table
	map<TableIndex, vector<RESTDroppedFile>> dropped_files;
};

} // namespace duckdb
