#include "duckdb/catalog/default/default_table_functions.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "storage/ducklake_transaction_manager.hpp"

namespace duckdb {

// clang-format off
static const DefaultTableMacro ducklake_table_macros[] = {
	{DEFAULT_SCHEMA, "merge_adjacent_files", {nullptr}, {{nullptr, nullptr}},  "FROM ducklake_merge_adjacent_files({CATALOG})"},
	{DEFAULT_SCHEMA, "set_option", {"option", "value", nullptr}, {{"table_name", "NULL"}, {"schema", "NULL"}, {nullptr, nullptr}},  "FROM ducklake_set_option({CATALOG}, option, value, table_name => table_name, schema => schema)"},
	{DEFAULT_SCHEMA, "set_commit_message", {"author", "commit_message", nullptr}, {{"extra_info", "NULL"},{nullptr, nullptr}},  "FROM ducklake_set_commit_message({CATALOG}, author, commit_message, extra_info => extra_info)"},
	{DEFAULT_SCHEMA, "options", {nullptr}, {{nullptr, nullptr}}, "FROM ducklake_options({CATALOG})"},
	{DEFAULT_SCHEMA, "current_snapshot", {nullptr}, {{nullptr, nullptr}},  "FROM ducklake_current_snapshot({CATALOG})"},
{DEFAULT_SCHEMA, "last_committed_snapshot", {nullptr}, {{nullptr, nullptr}}, "FROM ducklake_last_committed_snapshot({CATALOG})"},
	{DEFAULT_SCHEMA, "snapshots", {nullptr}, {{nullptr, nullptr}},  "FROM ducklake_snapshots({CATALOG})"},
	{DEFAULT_SCHEMA, "table_info", {nullptr}, {{nullptr, nullptr}},  "FROM ducklake_table_info({CATALOG})"},
	{DEFAULT_SCHEMA, "table_changes", {"table_name", "start_snapshot", "end_snapshot", nullptr}, {{nullptr, nullptr}},  "FROM ducklake_table_changes({CATALOG}, {SCHEMA}, table_name, start_snapshot, end_snapshot)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
};
// clang-format on

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::LoadBuiltInFunction(DefaultTableMacro macro) {
	string macro_def = macro.macro;
	macro_def = StringUtil::Replace(macro_def, "{CATALOG}", KeywordHelper::WriteQuoted(catalog.GetName(), '\''));
	macro_def = StringUtil::Replace(macro_def, "{SCHEMA}", KeywordHelper::WriteQuoted(name, '\''));
	macro.macro = macro_def.c_str();
	auto info = DefaultTableFunctionGenerator::CreateTableMacroInfo(macro);
	auto table_macro =
	    make_uniq_base<CatalogEntry, TableMacroCatalogEntry>(catalog, *this, info->Cast<CreateMacroInfo>());
	auto result = table_macro.get();
	default_function_map.emplace(macro.name, std::move(table_macro));
	return result;
}

optional_ptr<CatalogEntry> DuckLakeSchemaEntry::TryLoadBuiltInFunction(const string &entry_name) {
	lock_guard<mutex> guard(default_function_lock);
	auto entry = default_function_map.find(entry_name);
	if (entry != default_function_map.end()) {
		return entry->second.get();
	}
	for (idx_t index = 0; ducklake_table_macros[index].name != nullptr; index++) {
		if (StringUtil::CIEquals(ducklake_table_macros[index].name, entry_name)) {
			return LoadBuiltInFunction(ducklake_table_macros[index]);
		}
	}
	return nullptr;
}

} // namespace duckdb
