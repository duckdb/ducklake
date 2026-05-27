#include "common/vortex_file_scanner.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"

namespace duckdb {

VortexFileScanner::VortexFileScanner(ClientContext &context, const DuckLakeFileData &file) : context(context) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionHelper::TryAutoLoadExtension(instance, "vortex");
	ExtensionLoader loader(instance, "ducklake");
	auto &vortex_scan_entry = loader.GetTableFunction("read_vortex");
	vortex_scan = vortex_scan_entry.functions.functions[0];

	vector<Value> children;
	children.push_back(Value(file.path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "VortexFileScanner";

	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);

	bind_data = vortex_scan.bind(context, bind_input, return_types, return_names);
}

const vector<LogicalType> &VortexFileScanner::GetTypes() const {
	return return_types;
}

const vector<string> &VortexFileScanner::GetNames() const {
	return return_names;
}

optional_idx VortexFileScanner::FindColumn(const string &name) const {
	for (idx_t i = 0; i < return_names.size(); i++) {
		if (return_names[i] == name) {
			return i;
		}
	}
	return optional_idx();
}

void VortexFileScanner::SetFilters(unique_ptr<TableFilterSet> filters_p) {
	filters = std::move(filters_p);
}

void VortexFileScanner::SetColumnIds(vector<column_t> column_ids_p) {
	column_ids = std::move(column_ids_p);
}

void VortexFileScanner::InitializeScan() {
	if (initialized) {
		return;
	}

	if (column_ids.empty()) {
		for (idx_t i = 0; i < return_types.size(); i++) {
			column_ids.push_back(i);
		}
	}

	thread_context = make_uniq<ThreadContext>(context);
	execution_context = make_uniq<ExecutionContext>(context, *thread_context, nullptr);

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), filters.get());
	global_state = vortex_scan.init_global(context, input);
	local_state = vortex_scan.init_local(*execution_context, input, global_state.get());

	initialized = true;
}

bool VortexFileScanner::Scan(DataChunk &chunk) {
	if (!initialized) {
		InitializeScan();
	}

	TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
	chunk.Reset();
	vortex_scan.function(context, function_input, chunk);

	return chunk.size() > 0;
}

} // namespace duckdb
