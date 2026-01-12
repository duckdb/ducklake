#include "storage/ducklake_catalog_set.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_transaction.hpp"

namespace duckdb {

DuckLakeCatalogSet::DuckLakeCatalogSet() {
}
DuckLakeCatalogSet::DuckLakeCatalogSet(ducklake_entries_map_t catalog_entries_p)
    : catalog_entries(std::move(catalog_entries_p)) {
	for (auto &entry : catalog_entries) {
		auto &schema_entry = entry.second->Cast<DuckLakeSchemaEntry>();
		schema_entry_map.insert(make_pair(schema_entry.GetSchemaId(), reference<DuckLakeSchemaEntry>(schema_entry)));
	}
}

void DuckLakeCatalogSet::CreateEntry(unique_ptr<CatalogEntry> catalog_entry) {
	auto name = catalog_entry->name;
	auto entry = catalog_entries.find(name);
	if (entry != catalog_entries.end()) {
		catalog_entry->SetChild(std::move(entry->second));
	}
	catalog_entries[std::move(name)] = std::move(catalog_entry);
}

unique_ptr<CatalogEntry> DuckLakeCatalogSet::DropEntry(const string &name) {
	auto entry = catalog_entries.find(name);
	auto catalog_entry = std::move(entry->second);
	catalog_entries.erase(entry);
	return catalog_entry;
}

optional_ptr<CatalogEntry> DuckLakeCatalogSet::GetEntry(const string &name) {
	auto entry = catalog_entries.find(name);
	if (entry == catalog_entries.end()) {
		return nullptr;
	}
	return entry->second.get();
}

optional_ptr<CatalogEntry> DuckLakeCatalogSet::GetEntryById(SchemaIndex index) {
	auto entry = schema_entry_map.find(index);
	if (entry == schema_entry_map.end()) {
		return nullptr;
	}
	D_ASSERT(entry->second.get().type == CatalogType::SCHEMA_ENTRY);
	return entry->second.get();
}

optional_ptr<CatalogEntry> DuckLakeCatalogSet::GetEntryById(TableIndex index) {
	auto entry = table_entry_map.find(index);
	if (entry == table_entry_map.end()) {
		return nullptr;
	}
	D_ASSERT(entry->second.get().type == CatalogType::TABLE_ENTRY);
	return entry->second.get();
}

void DuckLakeCatalogSet::AddEntry(DuckLakeSchemaEntry &schema, TableIndex id, unique_ptr<CatalogEntry> entry) {
	auto catalog_type = entry->type;
	table_entry_map.insert(make_pair(id, reference<CatalogEntry>(*entry)));
	schema.AddEntry(catalog_type, std::move(entry));
}

void DuckLakeCatalogSet::AddEntry(DuckLakeSchemaEntry &schema, MacroIndex id, unique_ptr<CatalogEntry> entry) {
	auto catalog_type = entry->type;
	macro_entry_map.insert(make_pair(id, reference<CatalogEntry>(*entry)));
	schema.AddEntry(catalog_type, std::move(entry));
}

} // namespace duckdb
