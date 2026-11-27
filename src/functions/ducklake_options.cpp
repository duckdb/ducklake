#include "duckdb/parser/qualified_name.hpp"
#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_metadata_manager.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"

namespace duckdb {

struct DuckLakeOptionsData : public TableFunctionData {
	explicit DuckLakeOptionsData(DuckLakeCatalog &catalog) : catalog(catalog) {
	}

	DuckLakeCatalog &catalog;
};

static unique_ptr<FunctionData> DuckLakeOptionsBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);

	names.emplace_back("option_name");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("description");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("value");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("scope_entry");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<DuckLakeOptionsData>(catalog);
}

static vector<Value> GetOptionRow(const DuckLakeTag &tag, const string &scope, const string &scope_entry) {
	return {Value(tag.key), DuckLakeUtil::GetConfigOptionDescription(tag.key), Value(tag.value), Value(scope),
	        scope_entry.empty() ? Value() : Value(scope_entry)};
}

unique_ptr<GlobalTableFunctionState> DuckLakeOptionsInit(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<DuckLakeOptionsData>();
	auto &ducklake_catalog = bind_data.catalog;
	auto &transaction = DuckLakeTransaction::Get(context, ducklake_catalog);
	auto &metadata_manager = transaction.GetMetadataManager();

	auto result = make_uniq<DuckLakeRunOnceState>();
	auto metadata = metadata_manager.LoadDuckLake();

	// Global options
	for (auto &tag : metadata.tags) {
		result->rows.push_back(GetOptionRow(tag, "GLOBAL", string()));
	}

	auto snapshot = transaction.GetSnapshot();

	// Schema options
	for (auto &schema_setting : metadata.schema_settings) {
		string scope_entry;
		auto schema_entry = ducklake_catalog.GetEntryById(transaction, snapshot, schema_setting.schema_id);
		if (schema_entry && !transaction.IsDeleted(*schema_entry)) {
			scope_entry = schema_entry->Cast<SchemaCatalogEntry>().GetSchemaName();
		}
		result->rows.push_back(GetOptionRow(schema_setting.tag, "SCHEMA", scope_entry));
	}

	// Table options
	for (auto &table_setting : metadata.table_settings) {
		string scope_entry;
		auto table_entry = transaction.GetLocalEntryById(table_setting.table_id);
		if (!table_entry) {
			table_entry = ducklake_catalog.GetEntryById(transaction, snapshot, table_setting.table_id);
		}
		if (table_entry && !transaction.IsDeleted(*table_entry)) {
			auto &table_catalog_entry = table_entry->Cast<TableCatalogEntry>();
			scope_entry =
			    QualifiedName(table_catalog_entry.ParentSchema().GetSchemaPath(), table_entry->name).ToString();
		}
		result->rows.push_back(GetOptionRow(table_setting.tag, "TABLE", scope_entry));
	}

	std::sort(result->rows.begin(), result->rows.end(), [](const vector<Value> &a, const vector<Value> &b) {
		return StringValue::Get(a[0]) < StringValue::Get(b[0]);
	});
	return std::move(result);
}

void DuckLakeOptionsExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeRunOnceState>();
	DuckLakeBaseMetadataFunction::ScanRows(state.rows, state.offset, output);
}

DuckLakeOptionsFunction::DuckLakeOptionsFunction()
    : DuckLakeBaseMetadataFunction("ducklake_options", DuckLakeOptionsBind) {
	init_global = DuckLakeOptionsInit;
	function = DuckLakeOptionsExecute;
}

} // namespace duckdb
