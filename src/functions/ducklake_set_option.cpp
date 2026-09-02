#include "functions/ducklake_table_functions.hpp"
#include "common/ducklake_util.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"
#include "storage/ducklake_partition_data.hpp"

namespace duckdb {
// -------------------------------------------------------------------------//
// Group of functions to validate if it's safe to change the inline option
// ------------------------------------------------------------------------//

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
		                                          ducklake_table.GetTableId()) &&
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

//! Geometry and variant bounds live outside min/max, so they cannot be skipped
static void ValidateStatsCanBeSkipped(const DuckLakeTableEntry &table, const DuckLakeFieldId &field_id) {
	auto unsupported = FindStatsUnsupportedField(field_id);
	if (unsupported) {
		if (RefersToSameObject(*unsupported, field_id)) {
			throw NotImplementedException("Statistics cannot be skipped for %s columns", field_id.Type().ToString());
		}
		throw NotImplementedException(
		    "Statistics cannot be skipped for column \"%s\" - it contains a %s field (\"%s\")", field_id.Name(),
		    unsupported->Type().ToString(), unsupported->Name());
	}
	auto partition_data = table.GetPartitionData();
	if (!partition_data) {
		return;
	}
	for (auto &field : partition_data->fields) {
		if (field.field_id == field_id.GetFieldIndex()) {
			throw InvalidInputException("Statistics cannot be skipped for partition column \"%s\"", field_id.Name());
		}
	}
}

//! Resolves a column name, or a LIST of them, to the field ids the option stores - ids are used
//! because they survive a rename. An empty value clears the option.
static string ResolveSkippedStatsColumns(DuckLakeTableEntry &table, const Value &val, const string &raw_value) {
	vector<string> column_names;
	if (!val.IsNull() && val.type().id() == LogicalTypeId::LIST) {
		auto &children = ListValue::GetChildren(val);
		column_names.reserve(children.size());
		for (auto &child : children) {
			if (!child.IsNull()) {
				column_names.push_back(child.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>());
			}
		}
	} else if (!raw_value.empty()) {
		column_names.push_back(raw_value);
	}
	vector<string> field_ids;
	field_ids.reserve(column_names.size());
	unordered_set<idx_t> seen;
	seen.reserve(column_names.size());
	for (auto &column_name : column_names) {
		// a VARIANT column only resolves when the lookup can report where its own path begins
		optional_idx name_offset;
		auto field_id = table.TryGetFieldId(StringsToIdentifiers({column_name}), &name_offset);
		if (!field_id) {
			throw BinderException("Column \"%s\" does not exist in table \"%s\"", column_name,
			                      table.name.GetIdentifierName());
		}
		ValidateStatsCanBeSkipped(table, *field_id);
		auto field_index = field_id->GetFieldIndex().index;
		if (seen.insert(field_index).second) {
			field_ids.push_back(to_string(field_index));
		}
	}
	return StringUtil::Join(field_ids, ",");
}

// ------------------------------------------------------------------------//
// ------------------------------------------------------------------------//

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

	// read the option
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
		if (data_inlining_row_limit > 0) {
			ValidateNoReservedInliningColumns(context, catalog, input);
		}
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
			// Let's verify this is actually an interval
			interval_t result;
			if (!Interval::FromString(val.ToString(), result)) {
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
	} else if (option == "skip_stats_columns") {
		// the column names are resolved to field ids below, once the table scope is known
		value = val.IsNull() || val.type().id() == LogicalTypeId::LIST
		            ? string()
		            : val.DefaultCastAs(LogicalType::VARCHAR).GetValue<string>();
	} else {
		throw NotImplementedException("Unsupported option %s", option);
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
	if (option == "skip_stats_columns" && table.empty()) {
		throw InvalidInputException("The '%s' option can only be set for a specific table - pass table_name", option);
	}
	if (!table.empty()) {
		// find the scope
		auto table_catalog_entry = catalog.GetEntry<TableCatalogEntry>(
		    context, QualifiedName(catalog.GetName(), Identifier(schema), Identifier(table)),
		    OnEntryNotFound::THROW_EXCEPTION);
		auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
		config_option.table_id = ducklake_table.GetTableId();
		if (IsTransactionLocal(config_option.table_id)) {
			throw NotImplementedException("Settings cannot be set for transaction-local tables");
		}
		if (option == "skip_stats_columns") {
			value = ResolveSkippedStatsColumns(ducklake_table, val, value);
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
