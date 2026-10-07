#include "common/parquet_file_scanner.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

ParquetFileScanner::ParquetFileScanner(ClientContext &context, const DuckLakeFileData &file, bool use_file_metadata)
    : context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionLoader loader(instance, "ducklake");
	auto &parquet_scan_entry = loader.GetTableFunction("parquet_scan");
	parquet_scan = *parquet_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	if (use_file_metadata) {
		child_list_t<Value> file_entry;
		file_entry.emplace_back(MultiFileReader::FILE_PATH_FIELD, Value(file.path));
		file_entry.emplace_back("file_size", Value::UBIGINT(file.file_size_bytes));
		file_entry.emplace_back("etag", Value(""));
		file_entry.emplace_back("last_modified", Value::TIMESTAMP(timestamp_t(0)));
		if (!file.encryption_key.empty()) {
			file_entry.emplace_back("encryption_key", Value::BLOB_RAW(file.encryption_key));
		}
		children.push_back(Value::STRUCT(std::move(file_entry)));
	} else {
		children.push_back(Value(file.path));
	}
	named_argument_map_t named_params;
	vector<LogicalType> input_types;
	vector<Identifier> input_names;

	// ducklake-managed paths may contain incidental key=value segments
	named_params["hive_partitioning"] = Value::BOOLEAN(false);

	if (!file.encryption_key.empty()) {
		named_params["encryption_config"] = EncryptionConfig(file.encryption_key);
	}

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.SetName("ParquetFileScanner");

	// the bind sees the function as a bound call would
	BoundTableFunction bound_table_function(dummy_table_function);
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  bound_table_function, empty);

	bind_data = parquet_scan.bind(context, bind_input, return_types, return_names);
}

Value ParquetFileScanner::EncryptionConfig(const string &encryption_key) {
	child_list_t<Value> values;
	values.emplace_back("footer_key_value", Value::BLOB_RAW(encryption_key));
	return Value::STRUCT(std::move(values));
}

const vector<LogicalType> &ParquetFileScanner::GetTypes() const {
	return return_types;
}

const vector<Identifier> &ParquetFileScanner::GetNames() const {
	return return_names;
}

optional_idx ParquetFileScanner::FindColumn(const string &name) const {
	auto index = StringUtil::CIFind(return_names, Identifier(name));
	return index == DConstants::INVALID_INDEX ? optional_idx() : optional_idx(index);
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
