#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"

namespace duckdb {

static void ValidateTableScope(ClientContext &context, Catalog &catalog, const string &schema_name,
                               const string &table_name) {
	auto table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(
	    context, Identifier(schema_name), Identifier(table_name), OnEntryNotFound::THROW_EXCEPTION);
	auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
	DuckLakeUtil::ValidateCanEnableInlining(ducklake_table.GetColumns(),
	                                        catalog.Cast<DuckLakeCatalog>().SupportsV1_1Metadata(),
	                                        ducklake_table.name.GetIdentifierName());
}

static void ValidateTablesInSchema(ClientContext &context, DuckLakeCatalog &duck_catalog,
                                   DuckLakeSchemaEntry &schema_entry, SchemaIndex override_scope_id) {
	schema_entry.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		auto &ducklake_table = entry.Cast<DuckLakeTableEntry>();
		string override_val;
		if (duck_catalog.TryGetScopedConfigOption("data_inlining_row_limit", override_val, override_scope_id,
		                                          ducklake_table.GetTableId(), &ducklake_table.GetTableOptions()) &&
		    std::stoull(override_val) == 0) {
			return;
		}
		DuckLakeUtil::ValidateCanEnableInlining(ducklake_table.GetColumns(), duck_catalog.SupportsV1_1Metadata(),
		                                        ducklake_table.name.GetIdentifierName());
	});
}

static void ValidateSchemaScope(ClientContext &context, Catalog &catalog, const string &schema_name) {
	auto &duck_catalog = catalog.Cast<DuckLakeCatalog>();
	auto schema_catalog_entry = catalog.GetSchema(context, Identifier(schema_name), OnEntryNotFound::THROW_EXCEPTION);
	ValidateTablesInSchema(context, duck_catalog, schema_catalog_entry->Cast<DuckLakeSchemaEntry>(), SchemaIndex());
}

static void ValidateGlobalScope(ClientContext &context, Catalog &catalog) {
	auto &duck_catalog = catalog.Cast<DuckLakeCatalog>();
	duck_catalog.ScanSchemas(context, [&](SchemaCatalogEntry &schema) {
		auto &schema_entry = schema.Cast<DuckLakeSchemaEntry>();
		ValidateTablesInSchema(context, duck_catalog, schema_entry, schema_entry.GetSchemaId());
	});
}

static void ValidateNoReservedInliningColumns(ClientContext &context, Catalog &catalog,
                                              const TableFunctionBindInput &input) {
	auto table_name_entry = input.named_parameters.find("table_name");
	auto schema_param = input.named_parameters.find("schema");
	bool has_table = table_name_entry != input.named_parameters.end() && !table_name_entry->second.IsNull();
	bool has_schema = schema_param != input.named_parameters.end() && !schema_param->second.IsNull();
	if (has_table) {
		string schema_name = has_schema ? StringValue::Get(schema_param->second) : "";
		ValidateTableScope(context, catalog, schema_name, StringValue::Get(table_name_entry->second));
	} else if (has_schema) {
		ValidateSchemaScope(context, catalog, StringValue::Get(schema_param->second));
	} else {
		ValidateGlobalScope(context, catalog);
	}
}

struct DuckLakeSetOptionData : public TableFunctionData {
	DuckLakeSetOptionData(Catalog &catalog, DuckLakeConfigOption option_p)
	    : catalog(catalog), option(std::move(option_p)) {
	}

	Catalog &catalog;
	DuckLakeConfigOption option;
};

static unique_ptr<FunctionData> DuckLakeSetOptionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	DuckLakeConfigOption config_option;
	auto &option = config_option.option.key;
	auto &value = config_option.option.value;

	option = StringUtil::Lower(StringValue::Get(input.inputs[1]));
	auto &val = input.inputs[2];

	value = DuckLakeUtil::ParseConfigOptionValue(context, option, val);
	if (option == "data_inlining_row_limit" && std::stoull(value) > 0) {
		ValidateNoReservedInliningColumns(context, catalog, input);
	}

	string schema;
	string table;
	auto schema_entry = input.named_parameters.find("schema");
	if (schema_entry != input.named_parameters.end() && !schema_entry->second.IsNull()) {
		schema = StringValue::Get(schema_entry->second);
	}
	auto table_entry = input.named_parameters.find("table_name");
	if (table_entry != input.named_parameters.end() && !table_entry->second.IsNull()) {
		table = StringValue::Get(table_entry->second);
	}
	DuckLakeUtil::ValidateConfigOptionScope(option, !schema.empty(), !table.empty());
	if (!table.empty()) {
		auto table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(
		    context, QualifiedName(catalog.GetName(), Identifier(schema), Identifier(table)),
		    OnEntryNotFound::THROW_EXCEPTION);
		auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
		config_option.table_id = ducklake_table.GetTableId();
		if (IsTransactionLocal(config_option.table_id)) {
			throw NotImplementedException("Settings cannot be set for transaction-local tables");
		}
		if (option == "skip_stats_columns") {
			value = DuckLakeTableEntry::ResolveSkippedStatsColumns(ducklake_table, val);
		}
	} else if (!schema.empty()) {
		auto schema_catalog_entry = catalog.GetSchema(context, Identifier(schema), OnEntryNotFound::THROW_EXCEPTION);
		auto &ducklake_schema = schema_catalog_entry->Cast<DuckLakeSchemaEntry>();
		config_option.schema_id = ducklake_schema.GetSchemaId();
		if (config_option.schema_id.IsTransactionLocal()) {
			throw NotImplementedException("Settings cannot be set for transaction-local schemas");
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return make_uniq<DuckLakeSetOptionData>(catalog, std::move(config_option));
}

struct DuckLakeSetOptionState : public GlobalTableFunctionState {
	DuckLakeSetOptionState() {
	}

	bool finished = false;
};

unique_ptr<GlobalTableFunctionState> DuckLakeSetOptionInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<DuckLakeSetOptionState>();
}

void DuckLakeSetOptionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<DuckLakeSetOptionState>();
	auto &bind_data = data_p.bind_data->Cast<DuckLakeSetOptionData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	transaction.SetConfigOption(bind_data.option);
	state.finished = true;
}

DuckLakeSetOptionFunction::DuckLakeSetOptionFunction()
    : TableFunction("ducklake_set_option", {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::ANY},
                    DuckLakeSetOptionExecute, DuckLakeSetOptionBind, DuckLakeSetOptionInit) {
	named_parameters["table_name"] = LogicalType::VARCHAR;
	named_parameters["schema"] = LogicalType::VARCHAR;
}

} // namespace duckdb
