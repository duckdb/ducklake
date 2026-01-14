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
#include "storage/ducklake_catalog_set.hpp"
#include "storage/ducklake_partition_data.hpp"
#include "storage/ducklake_stats.hpp"

namespace duckdb {
struct DuckLakeGlobalStatsInfo;
class ColumnList;
class DuckLakeFieldData;
struct DuckLakeFileListEntry;
struct DuckLakeConfigOption;
struct DeleteFileMap;
class LogicalGet;

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
	optional_ptr<DuckLakeTableStats> GetTableStats(DuckLakeTransaction &transaction, TableIndex table_id);
	optional_ptr<DuckLakeTableStats> GetTableStats(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
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

	bool IsCommitInfoRequired() const {
		auto require = GetConfigOption<string>("require_commit_message", {}, {}, "false");
		return require == "true";
	}

	bool UseHiveFilePattern(bool default_value) const {
		auto hive_file_pattern = GetConfigOption<string>("hive_file_pattern", {}, {}, default_value ? "true" : "false");
		return hive_file_pattern == "true";
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
		last_committed_snapshot = value;
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

	static unique_ptr<DuckLakeStats> ConstructStatsMap(vector<DuckLakeGlobalStatsInfo> &global_stats,
	                                                   DuckLakeCatalogSet &schema);
	//! Return the schema for the given snapshot - loading it if it is not yet loaded
	DuckLakeCatalogSet &GetSchemaForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);

private:
	void DropSchema(ClientContext &context, DropInfo &info) override;
	unique_ptr<DuckLakeCatalogSet> LoadSchemaForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);
	DuckLakeStats &GetStatsForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot);
	unique_ptr<DuckLakeStats> LoadStatsForSnapshot(DuckLakeTransaction &transaction, DuckLakeSnapshot snapshot,
	                                               DuckLakeCatalogSet &schema);
	void LoadNameMaps(DuckLakeTransaction &transaction);

private:
	mutex schemas_lock;
	//! Map of schema index -> schema
	unordered_map<idx_t, unique_ptr<DuckLakeCatalogSet>> schemas;
	//! Map of data file index -> table stats
	unordered_map<idx_t, unique_ptr<DuckLakeStats>> stats;
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
	//! Whether or not the catalog is initialized
	bool initialized = false;
	//! The id of the last committed snapshot, set at FlushChanges on a successful commit
	mutable mutex commit_lock;
	optional_idx last_committed_snapshot;
};

} // namespace duckdb
