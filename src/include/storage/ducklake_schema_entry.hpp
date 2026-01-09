//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_schema_entry.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "storage/ducklake_catalog_set.hpp"

namespace duckdb {
class DuckLakeTransaction;
struct DefaultTableMacro;

class DuckLakeSchemaEntry : public SchemaCatalogEntry {
public:
	DuckLakeSchemaEntry(Catalog &catalog, CreateSchemaInfo &info, SchemaIndex schema_id, string schema_uuid,
	                    string data_path);

public:
	SchemaIndex GetSchemaId() const {
		return schema_id;
	}
	const string &GetSchemaUUID() const {
		return schema_uuid;
	}
	const string &DataPath() const {
		return data_path;
	}

public:
	optional_ptr<CatalogEntry> CreateTableExtended(CatalogTransaction transaction, BoundCreateTableInfo &info,
	                                               string table_uuid, string table_data_path);
	optional_ptr<CatalogEntry> CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) override;
	optional_ptr<CatalogEntry> CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info,
	                                       TableCatalogEntry &table) override;
	optional_ptr<CatalogEntry> CreateView(CatalogTransaction transaction, CreateViewInfo &info) override;
	optional_ptr<CatalogEntry> CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) override;
	optional_ptr<CatalogEntry> CreateTableFunction(CatalogTransaction transaction,
	                                               CreateTableFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCopyFunction(CatalogTransaction transaction,
	                                              CreateCopyFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreatePragmaFunction(CatalogTransaction transaction,
	                                                CreatePragmaFunctionInfo &info) override;
	optional_ptr<CatalogEntry> CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) override;
	optional_ptr<CatalogEntry> CreateType(CatalogTransaction transaction, CreateTypeInfo &info) override;
	void Alter(CatalogTransaction transaction, AlterInfo &info) override;
	void Scan(ClientContext &context, CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void Scan(CatalogType type, const std::function<void(CatalogEntry &)> &callback) override;
	void DropEntry(ClientContext &context, DropInfo &info) override;
	optional_ptr<CatalogEntry> LookupEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;
	SimilarCatalogEntry GetSimilarEntry(CatalogTransaction transaction, const EntryLookupInfo &lookup_info) override;

	void AddEntry(CatalogType type, unique_ptr<CatalogEntry> entry);
	void TryDropSchema(DuckLakeTransaction &transaction, bool cascade);

	static bool CatalogTypeIsSupported(CatalogType type);

	static string GeneratePathFromName(const string &uuid, const string &name);

private:
	DuckLakeCatalogSet &GetCatalogSet(CatalogType type);
	bool HandleCreateConflict(CatalogTransaction transaction, CatalogType type, const string &name,
	                          OnCreateConflict on_conflict);

	optional_ptr<CatalogEntry> TryLoadBuiltInFunction(const string &entry_name);
	optional_ptr<CatalogEntry> LoadBuiltInFunction(DefaultTableMacro macro);

private:
	SchemaIndex schema_id;
	string schema_uuid;
	string data_path;
	DuckLakeCatalogSet tables;
	mutex default_function_lock;
	case_insensitive_map_t<unique_ptr<CatalogEntry>> default_function_map;
};

} // namespace duckdb
