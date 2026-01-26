#include "storage/ducklake_delete_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {

DuckLakeDeleteFilter::DuckLakeDeleteFilter() : delete_data(make_shared_ptr<DuckLakeDeleteData>()) {
}

idx_t DuckLakeDeleteData::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel,
                                 optional_idx snapshot_filter) const {
	auto entry = std::lower_bound(deleted_rows.begin(), deleted_rows.end(), start_row_index);
	if (entry == deleted_rows.end()) {
		// no filter found for this entry
		return count;
	}
	idx_t end_pos = start_row_index + count;
	auto delete_idx = NumericCast<idx_t>(entry - deleted_rows.begin());
	if (deleted_rows[delete_idx] > end_pos) {
		// nothing in this range is deleted - skip
		return count;
	}
	// we have deletes in this range
	result_sel.Initialize(STANDARD_VECTOR_SIZE);
	idx_t result_count = 0;
	bool check_snapshots = snapshot_filter.IsValid() && !snapshot_ids.empty();
	for (idx_t i = 0; i < count; i++) {
		if (delete_idx < deleted_rows.size() && start_row_index + i == deleted_rows[delete_idx]) {
			bool is_deleted = true;
			if (check_snapshots) {
				// only consider deletions where snapshot_id <= snapshot_filter
				is_deleted = snapshot_ids[delete_idx] <= snapshot_filter.GetIndex();
			}
			delete_idx++;
			if (is_deleted) {
				continue;
			}
		}
		result_sel.set_index(result_count++, i);
	}
	return result_count;
}

bool DuckLakeDeleteData::HasEmbeddedSnapshots() const {
	return !snapshot_ids.empty();
}

optional_idx DuckLakeDeleteData::GetSnapshotForRow(idx_t row_id) const {
	auto it = scan_snapshot_map.find(row_id);
	if (it != scan_snapshot_map.end()) {
		return it->second;
	}
	return optional_idx();
}

idx_t DuckLakeDeleteFilter::Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) {
	// apply max row count (if it is set)
	if (max_row_count.IsValid()) {
		auto max_count = max_row_count.GetIndex();
		if (max_count <= NumericCast<idx_t>(start_row_index)) {
			// no rows to read based on max row count - skip
			return 0;
		}
		count = MinValue<idx_t>(max_count - start_row_index, count);
	}
	return delete_data->Filter(start_row_index, count, result_sel, snapshot_filter);
}

DeleteFileScanResult DuckLakeDeleteFilter::ScanDeleteFile(ClientContext &context, const DuckLakeFileData &delete_file,
                                                          optional_idx snapshot_filter_min,
                                                          optional_idx snapshot_filter_max) {
	auto &instance = DatabaseInstance::GetDatabase(context);
	ExtensionLoader loader(instance, "ducklake");
	auto &parquet_scan_entry = loader.GetTableFunction("parquet_scan");
	auto &parquet_scan = parquet_scan_entry.functions.functions[0];

	// Prepare the inputs for the bind
	vector<Value> children;
	children.reserve(1);
	children.push_back(Value(delete_file.path));
	named_parameter_map_t named_params;
	vector<LogicalType> input_types;
	vector<string> input_names;
	if (!delete_file.encryption_key.empty()) {
		child_list_t<Value> encryption_values;
		encryption_values.emplace_back("footer_key_value", Value::BLOB_RAW(delete_file.encryption_key));
		named_params["encryption_config"] = Value::STRUCT(std::move(encryption_values));
	}

	TableFunctionRef empty;
	TableFunction dummy_table_function;
	dummy_table_function.name = "DuckLakeDeleteScan";
	TableFunctionBindInput bind_input(children, named_params, input_types, input_names, nullptr, nullptr,
	                                  dummy_table_function, empty);
	vector<LogicalType> return_types;
	vector<string> return_names;

	auto bind_data = parquet_scan.bind(context, bind_input, return_types, return_names);

	// Check for valid schema, there are three possibilities:
	// 1. 2 columns: file_path (VARCHAR), pos (BIGINT) -> standard delete file
	// 2. 3 columns: file_path (VARCHAR), pos (BIGINT), _ducklake_internal_snapshot_id (BIGINT) -> delete file with
	// snapshots
	// 3. 3 columns: file_path (VARCHAR), pos (BIGINT), row (?) -> iceberg format (third column ignored)
	bool valid_two_col = return_types.size() == 2 && return_types[0].id() == LogicalTypeId::VARCHAR &&
	                     return_types[1].id() == LogicalTypeId::BIGINT;
	bool valid_three_col = return_types.size() == 3 && return_types[0].id() == LogicalTypeId::VARCHAR &&
	                       return_types[1].id() == LogicalTypeId::BIGINT;

	if (!valid_two_col && !valid_three_col) {
		throw InvalidInputException(
		    "Invalid schema contained in the delete file %s - expected file_name/position/[snapshot_id or row]",
		    delete_file.path);
	}

	// is this from ducklake?
	bool has_snapshot_id = false;
	if (return_types.size() == 3 && return_types[2].id() == LogicalTypeId::BIGINT) {
		// check if the name matches _ducklake_internal_snapshot_id
		if (return_names.size() > 2 && return_names[2] == "_ducklake_internal_snapshot_id") {
			has_snapshot_id = true;
		}
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, return_types);

	ThreadContext thread_context(context);
	ExecutionContext execution_context(context, thread_context, nullptr);

	vector<column_t> column_ids;
	for (idx_t i = 0; i < return_types.size(); i++) {
		column_ids.push_back(i);
	}

	// Create snapshot filters if we have a snapshot column and filter range is specified
	unique_ptr<TableFilterSet> filters;
	if (has_snapshot_id && (snapshot_filter_min.IsValid() || snapshot_filter_max.IsValid())) {
		filters = make_uniq<TableFilterSet>();
		ColumnIndex snapshot_col_idx(2); // snapshot_id is column 2

		if (snapshot_filter_min.IsValid()) {
			auto min_constant = Value::BIGINT(NumericCast<int64_t>(snapshot_filter_min.GetIndex()));
			auto min_filter =
			    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, std::move(min_constant));
			filters->PushFilter(snapshot_col_idx, std::move(min_filter));
		}
		if (snapshot_filter_max.IsValid()) {
			auto max_constant = Value::BIGINT(NumericCast<int64_t>(snapshot_filter_max.GetIndex()));
			auto max_filter =
			    make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, std::move(max_constant));
			filters->PushFilter(snapshot_col_idx, std::move(max_filter));
		}
	}

	TableFunctionInitInput input(bind_data.get(), column_ids, vector<idx_t>(), filters.get());
	auto global_state = parquet_scan.init_global(context, input);
	auto local_state = parquet_scan.init_local(execution_context, input, global_state.get());

	DeleteFileScanResult result;
	result.has_embedded_snapshots = has_snapshot_id;
	int64_t last_delete = -1;
	while (true) {
		TableFunctionInput function_input(bind_data.get(), local_state.get(), global_state.get());
		scan_chunk.Reset();
		parquet_scan.function(context, function_input, scan_chunk);

		idx_t count = scan_chunk.size();
		if (count == 0) {
			break;
		}
		UnifiedVectorFormat pos_data;
		scan_chunk.data[1].ToUnifiedFormat(count, pos_data);
		auto row_ids = UnifiedVectorFormat::GetData<int64_t>(pos_data);

		UnifiedVectorFormat snapshot_data;
		for (idx_t i = 0; i < count; i++) {
			auto pos_idx = pos_data.sel->get_index(i);
			if (!pos_data.validity.RowIsValid(pos_idx)) {
				throw InvalidInputException("Invalid delete data - delete data cannot have NULL values");
			}
			auto &row_id = row_ids[pos_idx];
			if (row_id <= last_delete) {
				throw InvalidInputException(
				    "Invalid delete data - row ids must be sorted and strictly increasing - but found %d after %d",
				    row_id, last_delete);
			}

			result.deleted_rows.push_back(row_id);
			last_delete = row_id;

			if (has_snapshot_id) {
				scan_chunk.data[2].ToUnifiedFormat(count, snapshot_data);
				auto snapshot_ids = UnifiedVectorFormat::GetData<int64_t>(snapshot_data);
				auto snap_idx = snapshot_data.sel->get_index(i);
				if (!snapshot_data.validity.RowIsValid(snap_idx)) {
					throw InvalidInputException("Invalid delete data - snapshot_id cannot be NULL");
				}
				result.snapshot_ids.push_back(NumericCast<idx_t>(snapshot_ids[snap_idx]));
			}
		}
	}
	return result;
}

void DuckLakeDeleteFilter::Initialize(ClientContext &context, const DuckLakeFileData &delete_file) {
	auto scan_result = ScanDeleteFile(context, delete_file);
	delete_data->deleted_rows = std::move(scan_result.deleted_rows);
	delete_data->snapshot_ids = std::move(scan_result.snapshot_ids);
}

void DuckLakeDeleteFilter::Initialize(const DuckLakeInlinedDataDeletes &inlined_deletes) {
	for (auto &idx : inlined_deletes.rows) {
		delete_data->deleted_rows.push_back(idx);
	}
}

void DuckLakeDeleteFilter::Initialize(ClientContext &context, const DuckLakeDeleteScanEntry &delete_scan) {
	// scanning deletes - we need to scan the opposite (i.e. only the rows that were deleted)
	auto rows_to_scan = make_unsafe_uniq_array<bool>(delete_scan.row_count);
	bool has_embedded_snapshots = false;

	// scan the current set of deletes
	if (!delete_scan.delete_file.path.empty()) {
		// we have a delete file - read the delete file from disk
		auto current_deletes =
		    ScanDeleteFile(context, delete_scan.delete_file, delete_scan.start_snapshot, delete_scan.end_snapshot);
		has_embedded_snapshots = current_deletes.has_embedded_snapshots;
		// iterate over the current deletes - these are the rows we need to scan
		memset(rows_to_scan.get(), 0, sizeof(bool) * delete_scan.row_count);
		for (idx_t i = 0; i < current_deletes.deleted_rows.size(); i++) {
			auto delete_idx = current_deletes.deleted_rows[i];
			if (delete_idx >= delete_scan.row_count) {
				throw InvalidInputException(
				    "Invalid delete data - delete index read from file %s is out of range for data file %s",
				    delete_scan.delete_file.path, delete_scan.file.path);
			}
			rows_to_scan[delete_idx] = true;
			if (i < current_deletes.snapshot_ids.size()) {
				delete_data->scan_snapshot_map[delete_idx] = current_deletes.snapshot_ids[i];
			}
		}
	} else {
		// we have no delete file - this means the entire file was deleted
		// set all rows as being scanned
		memset(rows_to_scan.get(), 1, sizeof(bool) * delete_scan.row_count);
		// for full file deletes, store the snapshot_id from the delete_scan entry
		if (delete_scan.snapshot_id.IsValid()) {
			for (idx_t i = 0; i < delete_scan.row_count; i++) {
				delete_data->scan_snapshot_map[i] = delete_scan.snapshot_id.GetIndex();
			}
		}
	}

	if (!delete_scan.previous_delete_file.path.empty() && !has_embedded_snapshots) {
		// if we have a previous delete file - scan that set of deletes
		// This only matters if we do not have a partial deletion file, since thes have all deletes
		auto previous_deletes = ScanDeleteFile(context, delete_scan.previous_delete_file);
		// these deletes are not new - we should not scan them
		for (auto delete_idx : previous_deletes.deleted_rows) {
			if (delete_idx >= delete_scan.row_count) {
				throw InvalidInputException(
				    "Invalid delete data - delete index read from file %s is out of range for data file %s",
				    delete_scan.previous_delete_file.path, delete_scan.file.path);
			}
			rows_to_scan[delete_idx] = false;
		}
	}

	// now construct the delete filter based on the rows we want to scan
	auto &deleted = delete_data->deleted_rows;
	for (idx_t i = 0; i < delete_scan.row_count; i++) {
		if (!rows_to_scan[i]) {
			deleted.push_back(i);
		}
	}
}

void DuckLakeDeleteFilter::SetMaxRowCount(idx_t max_row_count_p) {
	max_row_count = max_row_count_p;
}

void DuckLakeDeleteFilter::SetSnapshotFilter(idx_t snapshot_filter_p) {
	snapshot_filter = snapshot_filter_p;
}

} // namespace duckdb
