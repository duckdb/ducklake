#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/constant_binder.hpp"

namespace duckdb {
// -------------------------------------------------------------------------//
// Group of functions to validate if it's safe to change the inline option
// ------------------------------------------------------------------------//

static void ValidateTableScope(ClientContext &context, Catalog &catalog, const string &schema_name,
                               const string &table_name) {
	auto table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(
	    context, Identifier(schema_name), Identifier(table_name), OnEntryNotFound::THROW_EXCEPTION);
	auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
	DuckLakeUtil::ValidateNoInlinedSystemColumns(ducklake_table.GetColumns(), ducklake_table.name.GetIdentifierName());
}

static void ValidateTablesInSchema(ClientContext &context, DuckLakeCatalog &duck_catalog,
                                   DuckLakeSchemaEntry &schema_entry, SchemaIndex override_scope_id) {
	schema_entry.Scan(context, CatalogType::TABLE_ENTRY, [&](CatalogEntry &entry) {
		auto &ducklake_table = entry.Cast<DuckLakeTableEntry>();
		string override_val;
		if (duck_catalog.TryGetScopedConfigOption("data_inlining_row_limit", override_val, override_scope_id,
		                                          ducklake_table.GetTableId()) &&
		    std::stoull(override_val) == 0) {
			return;
		}
		DuckLakeUtil::ValidateNoInlinedSystemColumns(ducklake_table.GetColumns(),
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

// ------------------------------------------------------------------------//
// ------------------------------------------------------------------------//

vector<DuckLakeTag> ValidateOptionsInCreateWith(ClientContext &context,
                                                const case_insensitive_map_t<unique_ptr<ParsedExpression>> &options) {
	// Each value must fold to a literal under ConstantBinder; copy before binding since CTAS calls this twice.
	auto binder = Binder::CreateBinder(context);
	ConstantBinder option_binder(*binder, context, "DuckLake WITH option");
	vector<DuckLakeTag> result;
	result.reserve(options.size());
	for (auto &option : options) {
		if (!option.second) {
			throw BinderException("WITH option \"%s\" requires a value", option.first);
		}
		auto expr_copy = option.second->Copy();
		auto bound_expr = option_binder.Bind(expr_copy);
		if (bound_expr->HasParameter()) {
			throw ParameterNotResolvedException();
		}
		auto value = ExpressionExecutor::EvaluateScalar(context, *bound_expr, true);
		result.push_back(ValidateDuckLakeConfigOption(context, option.first, value));
	}
	return result;
}

struct DuckLakeSetOptionData : public TableFunctionData {
	DuckLakeSetOptionData(Catalog &catalog, DuckLakeConfigOption option_p)
	    : catalog(catalog), option(std::move(option_p)) {
	}

	Catalog &catalog;
	DuckLakeConfigOption option;
};

DuckLakeTag ValidateDuckLakeConfigOption(ClientContext &context, const string &option_key, const Value &val) {
	DuckLakeTag result;
	auto option = StringUtil::Lower(option_key);
	result.key = option;
	auto &value = result.value;

	if (option == "parquet_compression") {
		auto codec = val.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
		vector<string> supported_algorithms {"uncompressed", "snappy", "gzip", "zstd", "brotli", "lz4", "lz4_raw"};
		bool found = false;
		for (auto &algorithm : supported_algorithms) {
			if (StringUtil::CIEquals(algorithm, codec)) {
				found = true;
				break;
			}
		}
		if (!found) {
			auto supported = StringUtil::Join(supported_algorithms, ", ");
			throw NotImplementedException("Unsupported codec \"%s\" for parquet, supported options are %s", codec,
			                              supported);
		}
		value = StringUtil::Lower(codec);
	} else if (option == "parquet_version") {
		auto version = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
		if (version != 1 && version != 2) {
			throw NotImplementedException("Only Parquet version 1 and 2 are supported");
		}
		value = "V" + to_string(version);
	} else if (option == "parquet_compression_level") {
		auto compression_level = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
		value = to_string(compression_level);
	} else if (option == "parquet_row_group_size") {
		auto row_group_size = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
		if (row_group_size == 0) {
			throw NotImplementedException("Row group size cannot be 0");
		}
		value = to_string(row_group_size);
	} else if (option == "parquet_row_group_size_bytes") {
		auto row_group_size_bytes = DBConfig::ParseMemoryLimit(val.ToString());
		if (row_group_size_bytes == 0) {
			throw NotImplementedException("Row group size bytes cannot be 0");
		}
		value = to_string(row_group_size_bytes);
	} else if (option == "target_file_size") {
		auto target_file_size_bytes = DBConfig::ParseMemoryLimit(val.ToString());
		value = to_string(target_file_size_bytes);
	} else if (option == "data_inlining_row_limit") {
		auto data_inlining_row_limit = val.DefaultCastAs(LogicalType::UBIGINT).GetValue<idx_t>();
		value = to_string(data_inlining_row_limit);
	} else if (option == "require_commit_message") {
		value = val.GetValue<bool>() ? "true" : "false";
	} else if (option == "rewrite_delete_threshold") {
		double threshold = val.GetValue<double>();
		if (threshold < 0 || threshold > 1) {
			throw BinderException("The rewrite_delete_threshold must be between 0 and 1");
		}
		value = to_string(val.GetValue<double>());
	} else if (option == "hive_file_pattern") {
		value = val.GetValue<bool>() ? "true" : "false";
	} else if (option == "delete_older_than" || option == "expire_older_than") {
		auto interval_value = val.ToString();
		if (!interval_value.empty()) {
			interval_t interval_result;
			if (!Interval::FromString(val.ToString(), interval_result)) {
				throw BinderException("%s is not a valid interval value.", option);
			}
		}
		value = val.ToString();
	} else if (option == "auto_compact") {
		if (val.IsNull()) {
			throw BinderException("The %s option can't be null.", option.c_str());
		}
		value = val.CastAs(context, LogicalType::BOOLEAN).GetValue<bool>() ? "true" : "false";
	} else if (option == "per_thread_output") {
		value = val.CastAs(context, LogicalType::BOOLEAN).GetValue<bool>() ? "true" : "false";
	} else if (option == "write_deletion_vectors") {
		value = val.CastAs(context, LogicalType::BOOLEAN).GetValue<bool>() ? "true" : "false";
	} else if (option == "sort_on_insert") {
		value = val.CastAs(context, LogicalType::BOOLEAN).GetValue<bool>() ? "true" : "false";
	} else {
		throw NotImplementedException("Unsupported option %s", option);
	}
	return result;
}

static unique_ptr<FunctionData> DuckLakeSetOptionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
	auto &catalog = DuckLakeBaseMetadataFunction::GetCatalog(context, input.inputs[0]);
	DuckLakeConfigOption config_option;
	config_option.option = ValidateDuckLakeConfigOption(context, StringValue::Get(input.inputs[1]), input.inputs[2]);
	auto &option = config_option.option.key;
	// enabling inlining for existing tables requires that no table in scope uses reserved column names
	if (option == "data_inlining_row_limit" && std::stoull(config_option.option.value) > 0) {
		ValidateNoReservedInliningColumns(context, catalog, input);
	}

	// read the scope
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
	if ((!table.empty() || !schema.empty()) && (option == "expire_older_than" || option == "delete_older_than")) {
		throw InvalidInputException("The '%s' option can only be set globally, not for a specific schema or table",
		                            option);
	}
	if (!table.empty()) {
		// find the scope
		auto table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(context, Identifier(schema), Identifier(table),
		                                                               OnEntryNotFound::THROW_EXCEPTION);
		auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
		config_option.table_id = ducklake_table.GetTableId();
		if (IsTransactionLocal(config_option.table_id)) {
			throw NotImplementedException("Settings cannot be set for transaction-local tables");
		}
	} else if (!schema.empty()) {
		// find the scope
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
