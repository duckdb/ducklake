//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_catalog.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/ducklake_encryption.hpp"
#include "common/ducklake_options.hpp"
#include "common/ducklake_name_map.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/storage/object_cache.hpp"
#include "storage/ducklake_catalog_set.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_stats.hpp"

#include <chrono>
#include <functional>

namespace duckdb {
struct DuckLakeGlobalStatsInfo;
class ColumnList;
class DuckLakeFieldData;
struct DuckLakeFileListEntry;
struct DuckLakeConfigOption;
struct DeleteFileMap;
class LogicalGet;

//! Cache entry for DuckLake table statistics
struct DuckLakeStatsCacheEntry : public ObjectCacheEntry {
	static constexpr idx_t ESTIMATED_BYTES_PER_COLUMN_STATS = 256;

	explicit DuckLakeStatsCacheEntry(unique_ptr<DuckLakeStats> stats_p) : stats(std::move(*stats_p)) {
	}

	DuckLakeStats stats;

	static string ObjectType() {
		return "ducklake_stats";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override;
};

//! Cache entry for a DuckLake schema version
struct DuckLakeSchemaCacheEntry : public ObjectCacheEntry {
	static constexpr idx_t ESTIMATED_BYTES_PER_ENTRY = 4096;

	explicit DuckLakeSchemaCacheEntry(unique_ptr<DuckLakeCatalogSet> catalog_set_p)
	    : catalog_set(std::move(*catalog_set_p)) {
	}

	DuckLakeCatalogSet catalog_set;

	static string ObjectType() {
		return "ducklake_schema";
	}
	string GetObjectType() override {
		return ObjectType();
	}
	optional_idx GetEstimatedCacheMemory() const override;
};

//! Query-scoped pin for DuckLake schema cache entries, which guarantee memory safety before transaction finishes.
class DuckLakeSchemaPinState : public ClientContextState {
public:
	void QueryEnd(ClientContext &context) override;
	void Pin(shared_ptr<DuckLakeSchemaCacheEntry> entry);

private:
	mutex lock;
	// Maps from address of the schema cache entry to the schema cache entry.
	unordered_map<DuckLakeSchemaCacheEntry *, shared_ptr<DuckLakeSchemaCacheEntry>> pins;
};

enum class InlinedDeletionCacheResult { EXISTS, DOES_NOT_EXIST, UNKNOWN };

class DuckLakeCatalog : public Catalog {
public:
	// default target file size: 512MB
	static constexpr const idx_t DEFAULT_TARGET_FILE_SIZE = 1 << 29;

public:
	DuckLakeCatalog(AttachedDatabase &db_p, DuckLakeOptions options);
	~DuckLakeCatalog() override;

public:
	void Initialize(bool load_builtin) override;
	void Initialize(optional_ptr<ClientContext> context, bool load_builtin) override;
	void FinalizeLoad(optional_ptr<ClientContext> context) override;
	string GetCatalogType() override {
		return "ducklake";
	}
	const string &MetadataDatabaseName() const {
		return options.metadata_database;
	}
	const string &MetadataSchemaName() const {
		return options.metadata_schema;
	}
	const string &MetadataPath() const {
		return options.metadata_path;
	}
	const string &DataPath() const {
		return options.data_path;
	}
	const string &MetadataType() const {
		return metadata_type;
	}
	idx_t DataInliningRowLimit(SchemaIndex schema_index, TableIndex table_index) const;
	idx_t DataInliningRowLimit(ClientContext &context, SchemaIndex schema_index, TableIndex table_index) const;
	//! Returns the inlining limit (0 if the table is not eligible)
	idx_t GetInliningLimit(ClientContext &context, DuckLakeTableEntry &table);
	string &Separator() {
		return separator;
	}
	void SetConfigOption(const DuckLakeConfigOption &option);
	bool TryGetConfigOption(const string &option, string &result, SchemaIndex schema_id, TableIndex table_id) const;
	template <class T>
	T GetConfigOption(const string &option, SchemaIndex schema_id, TableIndex table_id, T default_value) const {
		string value_str;
		if (TryGetConfigOption(option, value_str, schema_id, table_id)) {
			return Value(value_str).GetValue<T>();
		}
		return default_value;
	}
	bool TryGetConfigOption(const string &option, string &result, DuckLakeTableEntry &table) const;

	optional_ptr<BoundAtClause> CatalogSnapshot() const;

	optional_ptr<CatalogEntry> CreateSchema(CatalogTransaction transaction, CreateSchemaInfo &info) override;

	void ScanSchemas(ClientContext &context, std::function<void(SchemaCatalogEntry &)> callback) override;

	optional_ptr<SchemaCatalogEntry> LookupSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup,
	                                              OnEntryNotFound if_not_found) override;

	PhysicalOperator &PlanInsert(ClientContext &context, PhysicalPlanGenerator &planner, LogicalInsert &op,
	                             optional_ptr<PhysicalOperator> plan) override;
	PhysicalOperator &PlanCreateTableAs(ClientContext &context, PhysicalPlanGenerator &planner, LogicalCreateTable &op,
	                                    PhysicalOperator &plan) override;
	PhysicalOperator &PlanDelete(ClientContext &context, PhysicalPlanGenerator &planner, LogicalDelete &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanUpdate(ClientContext &context, PhysicalPlanGenerator &planner, LogicalUpdate &op,
	                             PhysicalOperator &plan) override;
	PhysicalOperator &PlanMergeInto(ClientContext &context, PhysicalPlanGenerator &planner, LogicalMergeInto &op,
	                                PhysicalOperator &plan) override;
	unique_ptr<LogicalOperator> BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
	                                            unique_ptr<LogicalOperator> plan) override;
	unique_ptr<LogicalOperator> BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
	                                              unique_ptr<LogicalOperator> plan,
	                                              unique_ptr<CreateIndexInfo> create_info,
	                                              unique_ptr<AlterTableInfo> alter_info) override;
	DatabaseSize GetDatabaseSize(ClientContext &context) override;
	shared_ptr<DuckLakeTableStats> GetTableStats(DuckLakeTransaction &transaction, TableIndex table_id);
	shared_ptr<DuckLakeTableStats> GetTableStats(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
	                                             TableIndex table_id);

	optional_ptr<CatalogEntry> GetEntryById(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
	                                        SchemaIndex schema_id);
	optional_ptr<CatalogEntry> GetEntryById(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
	                                        TableIndex table_id);
	string GeneratePathFromName(const string &uuid, const string &name);

	bool InMemory() override;
	string GetDBPath() override;

	string GetDataPath();

	bool SupportsTimeTravel() const override {
		return true;
	}

	DuckLakeEncryption Encryption() const {
		return options.encryption;
	}

	bool IsEncrypted() const override {
		return Encryption() == DuckLakeEncryption::ENCRYPTED;
	}

	bool IsCommitInfoRequired() const {
		auto require = GetConfigOption<string>("require_commit_message", {}, {}, "false");
		return require == "true";
	}

	bool UseHiveFilePattern(bool default_value, SchemaIndex schema_id, TableIndex table_id) const {
		auto hive_file_pattern =
		    GetConfigOption<string>("hive_file_pattern", schema_id, table_id, default_value ? "true" : "false");
		return hive_file_pattern == "true";
	}

	bool WriteDeletionVectors(SchemaIndex schema_id, TableIndex table_id) const {
		auto write_dv = GetConfigOption<string>("write_deletion_vectors", schema_id, table_id, "false");
		return write_dv == "true";
	}

	void SetEncryption(DuckLakeEncryption encryption);
	// Generate an encryption key for writing (or empty if encryption is disabled)
	string GenerateEncryptionKey(ClientContext &context) const;

	void OnDetach(ClientContext &context) override;

	optional_idx GetCatalogVersion(ClientContext &context) override;

	idx_t GetNewUncommittedCatalogVersion() {
		return ++last_uncommitted_catalog_version;
	}

	void SetCommittedSnapshotId(idx_t value) {
		lock_guard<mutex> guard(commit_lock);
		// Max-update: sequence allocation decoupled from commit order.
		if (!last_committed_snapshot.IsValid() || value > last_committed_snapshot.GetIndex()) {
			last_committed_snapshot = value;
		}
	}

	Value GetLastCommittedSnapshotId() const {
		lock_guard<mutex> guard(commit_lock);
		if (last_committed_snapshot.IsValid()) {
			return Value::UBIGINT(last_committed_snapshot.GetIndex());
		}
		return Value();
	}

	optional_ptr<const DuckLakeNameMap> TryGetMappingById(DuckLakeTransaction &transaction, MappingIndex mapping_id);
	MappingIndex TryGetCompatibleNameMap(DuckLakeTransaction &transaction, const DuckLakeNameMap &name_map);
	idx_t GetBeginSnapshotForTable(TableIndex table_id, DuckLakeTransaction &transaction);
	idx_t GetBeginSnapshotForSchemaVersion(TableIndex table_id, idx_t schema_version, DuckLakeTransaction &transaction);

	static unique_ptr<DuckLakeStats> ConstructStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats,
	                                                   DuckLakeCatalogSet &schema);
	//! Return the schema for the given snapshot - loading it if it is not yet loaded
	DuckLakeCatalogSet &GetSchemaForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);

	//! Callback type for instrumenting metadata queries
	using QueryCallback = std::function<void(const string &query, std::chrono::steady_clock::duration elapsed)>;

	void SetQueryCallback(QueryCallback callback) {
		query_callback = std::move(callback);
	}
	const QueryCallback &GetQueryCallback() const {
		return query_callback;
	}

	//! Check if an inlined deletion table is known to exist or not exist for the given table and snapshot
	InlinedDeletionCacheResult CheckInlinedDeletionTableCache(TableIndex table_id, DuckLakeSnapshot snapshot);
	//! Cache the result of an inlined deletion table existence check
	void CacheInlinedDeletionTableResult(TableIndex table_id, DuckLakeSnapshot snapshot, bool exists);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
	unique_ptr<DuckLakeCatalogSet> LoadSchemaForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);
	//! Look up (or load) the ObjectCache entry for a given snapshot.
	shared_ptr<DuckLakeSchemaCacheEntry> GetSchemaCacheEntry(DuckLakeTransaction &transaction,
	                                                         DuckLakeSnapshot snapshot);
	shared_ptr<DuckLakeStatsCacheEntry> GetStatsForSnapshot(DuckLakeTransaction &transaction,
	                                                        DuckLakeSnapshot snapshot);
	//! Pin a schema cache entry for the duration of the current query to ensure safe memory access.
	void PinSchemaForQuery(DuckLakeTransaction &transaction, shared_ptr<DuckLakeSchemaCacheEntry> entry);
	unique_ptr<DuckLakeStats> LoadStatsForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
	                                               DuckLakeCatalogSet &schema);
	void LoadNameMaps(DuckLakeTransaction &transaction);
	//! Generate a cache key for the ObjectCache
	string StatsCacheKey(idx_t next_file_id) const;
	string SchemaCacheKey(idx_t schema_version) const;
	string SchemaPinStateKey() const;
	ObjectCache &GetObjectCacheInstance();

private:
	mutex name_maps_lock;
	//! Map of mapping index -> name map
	DuckLakeNameMapSet name_maps;
	//! The maximum name map index we have loaded so far
	optional_idx loaded_name_map_index;
	//! The configuration lock
	mutable mutex config_lock;
	//! The DuckLake options
	DuckLakeOptions options;
	//! The path separator
	string separator = "/";
	//! A unique tracker for catalog changes in uncommitted transactions.
	atomic<idx_t> last_uncommitted_catalog_version;
	//! The metadata server type
	string metadata_type;
	//! A per-instance identifier used to scope ObjectCache keys.
	string instance_id;
	//! Whether or not the catalog is initialized
	bool initialized = false;
	//! Cache for inlined deletion table existence checks
	mutex inlined_deletion_cache_lock;
	//! Table IDs where the inlined deletion table is known to exist (permanent - never invalidated)
	unordered_set<idx_t> inlined_deletion_exists;
	//! Table IDs where the inlined deletion table is known to NOT exist, with the snapshot_id at which we checked
	//! Valid as long as current snapshot.snapshot_id <= cached snapshot_id
	unordered_map<idx_t, idx_t> inlined_deletion_not_exists;
	//! The id of the last committed snapshot, set at FlushChanges on a successful commit
	mutable mutex commit_lock;
	optional_idx last_committed_snapshot;
	//! Optional callback for instrumenting metadata queries
	QueryCallback query_callback;
};

} // namespace duckdb
