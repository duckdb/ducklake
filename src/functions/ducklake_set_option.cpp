#include "functions/ducklake_table_functions.hpp"
#include "storage/ducklake_transaction.hpp"
#include "storage/ducklake_catalog.hpp"
#include "storage/ducklake_table_entry.hpp"
#include "storage/ducklake_schema_entry.hpp"

namespace duckdb {

struct DuckLakeSetOptionData : public TableFunctionData {
	DuckLakeSetOptionData(Catalog &catalog, DuckLakeConfigOption option_p)
	    : catalog(catalog), option(std::move(option_p)) {
	}

	Catalog &catalog;
	DuckLakeConfigOption option;
};

static unique_ptr<FunctionData> DuckLakeSetOptionBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {
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
	if (!table.empty()) {
		// find the scope
		auto table_catalog_entry =
		    catalog.GetEntry<TableCatalogEntry>(context, schema, table, OnEntryNotFound::THROW_EXCEPTION);
		auto &ducklake_table = table_catalog_entry->Cast<DuckLakeTableEntry>();
		config_option.table_id = ducklake_table.GetTableId();
		if (config_option.table_id.IsTransactionLocal()) {
			throw NotImplementedException("Settings cannot be set for transaction-local tables");
		}
	} else if (!schema.empty()) {
		// find the scope
		auto schema_catalog_entry = catalog.GetSchema(context, schema, OnEntryNotFound::THROW_EXCEPTION);
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
