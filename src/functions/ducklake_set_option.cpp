#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"

namespace duckdb {

static void ValidateCanEnableInlining(const DuckLakeCatalog &catalog, DuckLakeTableEntry &table) {
	DuckLakeUtil::ValidateCanEnableInlining(table.GetColumns(), catalog.SupportsV1_1Metadata(),
	                                        table.name.GetIdentifierName());
}

static void ValidateTablesWithoutOverride(const DuckLakeCatalog &catalog,
                                          const vector<reference<DuckLakeTableEntry>> &tables, bool global_scope) {
	for (auto &table_ref : tables) {
		auto &table = table_ref.get();
		SchemaIndex override_scope_id;
		if (global_scope) {
			override_scope_id = table.ParentSchema().Cast<DuckLakeSchemaEntry>().GetSchemaId();
		}
		string override_val;
		if (catalog.TryGetScopedConfigOption("data_inlining_row_limit", override_val, override_scope_id,
		                                     table.GetTableId(), &table.GetTableOptions()) &&
		    std::stoull(override_val) == 0) {
			continue;
		}
		ValidateCanEnableInlining(catalog, table);
	}
}

static void ValidateNoReservedInliningColumns(ClientContext &context, DuckLakeCatalog &catalog,
                                              optional_ptr<DuckLakeSchemaEntry> schema,
                                              optional_ptr<DuckLakeTableEntry> table) {
	if (table) {
		ValidateCanEnableInlining(catalog, *table);
	} else if (schema) {
		ValidateTablesWithoutOverride(catalog, DuckLakeBaseMetadataFunction::GetTablesInSchema(context, *schema),
		                              false);
	} else {
		ValidateTablesWithoutOverride(catalog, DuckLakeBaseMetadataFunction::GetTablesInScope(context, catalog, "", ""),
		                              true);
	}
}

static string GetScopeName(const TableFunctionBindInput &input, const string &name) {
	auto entry = input.named_parameters.find(Identifier(name));
	if (entry == input.named_parameters.end() || entry->second.IsNull()) {
		return string();
	}
	return StringValue::Get(entry->second);
}

struct DuckLakeSetOptionData : public TableFunctionData {
	DuckLakeSetOptionData(DuckLakeCatalog &catalog, DuckLakeConfigOption option_p)
	    : catalog(catalog), option(std::move(option_p)) {
	}

	DuckLakeCatalog &catalog;
	DuckLakeConfigOption option;
};

static unique_ptr<FunctionData> DuckLakeSetOptionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input);
	DuckLakeConfigOption config_option;
	auto &option = config_option.option.key;
	auto &value = config_option.option.value;

	if (input.inputs[1].IsNull()) {
		throw BinderException("Option name cannot be NULL");
	}
	option = StringUtil::Lower(StringValue::Get(input.inputs[1]));
	auto &val = input.inputs[2];

	value = DuckLakeUtil::ParseConfigOptionValue(context, option, val);
	auto schema = GetScopeName(input, "schema");
	auto table = GetScopeName(input, "table_name");
	DuckLakeUtil::ValidateConfigOptionScope(option, !schema.empty(), !table.empty());

	optional_ptr<DuckLakeTableEntry> table_entry;
	optional_ptr<DuckLakeSchemaEntry> schema_entry;
	if (!table.empty()) {
		table_entry = DuckLakeBaseMetadataFunction::GetTableEntry(context, catalog, schema, table);
	} else if (!schema.empty()) {
		schema_entry = catalog.ResolveSchema(context, schema).Cast<DuckLakeSchemaEntry>();
	}
	if (option == "data_inlining_row_limit" && std::stoull(value) > 0) {
		ValidateNoReservedInliningColumns(context, catalog, schema_entry, table_entry);
	}
	if (table_entry) {
		config_option.table_id = table_entry->GetTableId();
		if (IsTransactionLocal(config_option.table_id)) {
			throw NotImplementedException("Settings cannot be set for transaction-local tables");
		}
		if (option == "skip_stats_columns") {
			value = DuckLakeTableEntry::ResolveSkippedStatsColumns(*table_entry, val);
		}
	} else if (schema_entry) {
		config_option.schema_id = schema_entry->GetSchemaId();
		if (config_option.schema_id.IsTransactionLocal()) {
			throw NotImplementedException("Settings cannot be set for transaction-local schemas");
		}
	}

	return_types.push_back(LogicalType::BOOLEAN);
	names.push_back("Success");
	return make_uniq<DuckLakeSetOptionData>(catalog, std::move(config_option));
}

void DuckLakeSetOptionExecute(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &bind_data = data_p.bind_data->Cast<DuckLakeSetOptionData>();
	auto &transaction = DuckLakeTransaction::Get(context, bind_data.catalog);
	transaction.SetConfigOption(bind_data.option);
}

DuckLakeSetOptionFunction::DuckLakeSetOptionFunction()
    : TableFunction("ducklake_set_option",
                    FunctionSignature()
                        .AddPositionalOnly("catalog", LogicalType::VARCHAR)
                        .AddPositionalOnly("option", LogicalType::VARCHAR)
                        .AddPositionalOnly("value", LogicalType::ANY),
                    DuckLakeSetOptionExecute, DuckLakeSetOptionBind) {
	GetSignature().WithTypedKwargs("options", [&](TypedKwargs &options) {
		options.Add("table_name", LogicalType::VARCHAR).Add("schema", LogicalType::VARCHAR);
	});
}

} // namespace duckdb
