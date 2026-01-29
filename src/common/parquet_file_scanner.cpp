#include "common/parquet_file_scanner.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

ParquetFileScanner::ParquetFileScanner(ClientContext &context, const DuckLakeFileData &file)
    : ParquetFileScanner(context, file, nullptr, nullptr) {
}

ParquetFileScanner::ParquetFileScanner(ClientContext &context, const DuckLakeFileData &file,
                                       table_function_get_multi_file_reader_t multi_file_reader_creator_p,
                                       shared_ptr<TableFunctionInfo> function_info_p)
    : context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionLoader loader(instance, "ducklake");
	auto &parquet_scan_entry = loader.GetTableFunction("parquet_scan");
	parquet_scan = parquet_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.push_back(Value(file.path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	if (!file.encryption_key.empty()) {
		child_list_t<Value> encryption_values;
		encryption_values.emplace_back("footer_key_value", Value::BLOB_RAW(file.encryption_key));
		named_params["encryption_config"] = Value::STRUCT(std::move(encryption_values));
	}

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "ParquetFileScanner";

	if (multi_file_reader_creator_p) {
		dummy_table_function.get_multi_file_reader = multi_file_reader_creator_p;
		if (function_info_p) {
			dummy_table_function.function_info = std::move(function_info_p);
		}
	}

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);

	bind_data = parquet_scan.bind(context, bind_input, return_types, return_names);
}

const vector<LogicalType> &ParquetFileScanner::GetTypes() const {
	return return_types;
}

const vector<string> &ParquetFileScanner::GetNames() const {
	return return_names;
}

optional_idx ParquetFileScanner::FindColumn(const string &name) const {
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i] == name) {
			return i;
		}
	}
	return optional_idx();
}

void ParquetFileScanner::SetFilters(unique_ptr<TableFilterSet> filters_p) {
	filters = std::move(filters_p);
}

void ParquetFileScanner::SetColumnIds(vector<column_t> column_ids_p) {
	column_ids = std::move(column_ids_p);
}

void ParquetFileScanner::InitializeScan() {
	if (initialized) {
		return;
	}

	// If no column_ids specified, read all columns
	if (column_ids.empty()) {
		for (idx_t i = 0; i < return_types.size(); i++) {
			column_ids.push_back(i);
		}
	}

	thread_context = make_uniq<ThreadContext>(context);
	execution_context = make_uniq<ExecutionContext>(context, *thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), filters.get());
	global_state = parquet_scan.init_global(context, input);
	local_state = parquet_scan.init_local(*execution_context, input, global_state.get());

	initialized = true;
}

bool ParquetFileScanner::Scan(DataChunk &chunk) {
	if (!initialized) {
		InitializeScan();
	}

	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	chunk.Reset();
	parquet_scan.function(context, function_input, chunk);

	return chunk.size() > 0;
}

} // namespace duckdb
