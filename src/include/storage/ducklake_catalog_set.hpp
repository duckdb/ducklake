//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_catalog_set.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/catalog/catalog_entry.hpp"
#include "common/ducklake_snapshot.hpp"
#include "common/index.hpp"

namespace duckdb {
class DuckLakeTransaction;
class DuckLakeSchemaEntry;
class DuckLakeTableEntry;

using ducklake_entries_map_t = case_insensitive_map_t<unique_ptr<CatalogEntry>>;

//! The DuckLakeCatalogSet contains a set of catalog entries for a given schema version of the DuckLake
//! Note that we don't need any locks to access this - the catalog set is constant for a given snapshot
class DuckLakeCatalogSet {
public:
	DuckLakeCatalogSet();
	explicit DuckLakeCatalogSet(ducklake_entries_map_t catalog_entries_p);

	void CreateEntry(unique_ptr<CatalogEntry> entry);
	optional_ptr<CatalogEntry> GetEntry(const string &name);
	unique_ptr<CatalogEntry> DropEntry(const string &name);
	optional_ptr<CatalogEntry> GetEntryById(SchemaIndex index);
	optional_ptr<CatalogEntry> GetEntryById(TableIndex index);
	void AddEntry(DuckLakeSchemaEntry &schema, TableIndex id, unique_ptr<CatalogEntry> entry);
	void AddEntry(DuckLakeSchemaEntry &schema, MacroIndex id, unique_ptr<CatalogEntry> entry);
	template <class T>
	optional_ptr<T> GetEntry(const string &name) {
		auto entry = GetEntry(name);
		if (!entry) {
			return nullptr;
		}
		return entry->Cast<T>();
	}

	const ducklake_entries_map_t &GetEntries() {
		return catalog_entries;
	}
	const map<SchemaIndex, reference<DuckLakeSchemaEntry>> &GetSchemaIdMap() {
		return schema_entry_map;
	}

private:
	ducklake_entries_map_t catalog_entries;
	map<SchemaIndex, reference<DuckLakeSchemaEntry>> schema_entry_map;
	map<TableIndex, reference<CatalogEntry>> table_entry_map;
	map<MacroIndex, reference<CatalogEntry>> macro_entry_map;
};

} // namespace duckdb
