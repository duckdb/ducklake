#include "storage/ducklake_delete_filter.hpp"
#include "common/parquet_file_scanner.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/multi_file/multi_file_reader.hpp"

namespace duckdb {

//! FunctionInfo to pass delete file metadata to the MultiFileReader
struct DeleteFileFunctionInfo : public TableFunctionInfo {
	DuckLakeFileData file_data;
};

//! Custom MultiFileReader that creates a SimpleMultiFileList with extended info.
//! This avoids HEAD requests by providing file metadata (size, etag, last_modified) upfront
struct DeleteFileMultiFileReader : public MultiFileReader {
	static unique_ptr<MultiFileReader> CreateInstance(const TableFunction &table_function) {
		auto &info = table_function.function_info->Cast<DeleteFileFunctionInfo>();
		return make_uniq<DeleteFileMultiFileReader>(info.file_data);
	}

	explicit DeleteFileMultiFileReader(const DuckLakeFileData &delete_file) {
		OpenFileInfo file_info(delete_file.path);
		auto extended_info = make_shared_ptr<ExtendedOpenFileInfo>();
		extended_info->options["file_size"] = Value::UBIGINT(delete_file.file_size_bytes);
		extended_info->options["etag"] = Value("");
		extended_info->options["last_modified"] = Value::TIMESTAMP(timestamp_t(0));
		if (!delete_file.encryption_key.empty()) {
			extended_info->options["encryption_key"] = Value::BLOB_RAW(delete_file.encryption_key);
		}
		file_info.extended_info = std::move(extended_info);

		vector<OpenFileInfo> files;
		files.push_back(std::move(file_info));
		file_list = make_shared_ptr<SimpleMultiFileList>(std::move(files));
	}

	shared_ptr<MultiFileList> CreateFileList(ClientContext &context, const vector<string> &paths,
	                                         const FileGlobInput &options) override {
		return file_list;
	}

private:
	shared_ptr<MultiFileList> file_list;
};

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
	// Set up custom MultiFileReader to avoid HEAD requests
	auto function_info = make_shared_ptr<DeleteFileFunctionInfo>();
	function_info->file_data = delete_file;
	ParquetFileScanner scanner(context, delete_file, DeleteFileMultiFileReader::CreateInstance,
	                           std::move(function_info));

	auto &return_types = scanner.GetTypes();
	auto &return_names = scanner.GetNames();

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

	// Create snapshot filters if we have a snapshot column and filter range is specified
	if (has_snapshot_id && (snapshot_filter_min.IsValid() || snapshot_filter_max.IsValid())) {
		auto filters = make_uniq<TableFilterSet>();
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
		scanner.SetFilters(std::move(filters));
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, return_types);

	DeleteFileScanResult result;
	result.has_embedded_snapshots = has_snapshot_id;
	int64_t last_delete = -1;

	while (scanner.Scan(scan_chunk)) {
		idx_t count = scan_chunk.size();

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
	auto scan_result = ScanDeleteFile(context, delete_file, optional_idx(), optional_idx());
	delete_data->deleted_rows = std::move(scan_result.deleted_rows);
	delete_data->snapshot_ids = std::move(scan_result.snapshot_ids);
}

void DuckLakeDeleteFilter::Initialize(const DuckLakeInlinedDataDeletes &inlined_deletes) {
	D_ASSERT(std::is_sorted(delete_data->deleted_rows.begin(), delete_data->deleted_rows.end()));
	auto mid_idx = delete_data->deleted_rows.size();
	for (auto &idx : inlined_deletes.rows) {
		delete_data->deleted_rows.push_back(idx);
	}
	delete_data->snapshot_ids.clear();
	std::inplace_merge(delete_data->deleted_rows.begin(), delete_data->deleted_rows.begin() + mid_idx,
	                   delete_data->deleted_rows.end());
}

unordered_map<idx_t, idx_t> DuckLakeDeleteFilter::ScanDataFileRowIds(ClientContext &context,
                                                                     const DuckLakeFileData &data_file,
                                                                     const unordered_set<idx_t> &file_positions) {
	unordered_map<idx_t, idx_t> result;
	if (file_positions.empty()) {
		return result;
	}

	ParquetFileScanner scanner(context, data_file);

	// Find the _ducklake_internal_row_id column
	auto row_id_col_idx = scanner.FindColumn("_ducklake_internal_row_id");
	if (!row_id_col_idx.IsValid()) {
		// If we don't have a _ducklake_internal_row_id, we can exit
		return result;
	}

	DataChunk scan_chunk;
	scan_chunk.Initialize(context, scanner.GetTypes());

	idx_t current_file_position = 0;
	while (scanner.Scan(scan_chunk)) {
		idx_t count = scan_chunk.size();

		// Access the row_id column at its correct position
		UnifiedVectorFormat row_id_data;
		scan_chunk.data[row_id_col_idx.GetIndex()].ToUnifiedFormat(count, row_id_data);
		auto row_ids = UnifiedVectorFormat::GetData<int64_t>(row_id_data);

		for (idx_t i = 0; i < count; i++) {
			if (file_positions.count(current_file_position) > 0) {
				auto row_id_idx = row_id_data.sel->get_index(i);
				if (row_id_data.validity.RowIsValid(row_id_idx)) {
					result[current_file_position] = NumericCast<idx_t>(row_ids[row_id_idx]);
				}
			}
			current_file_position++;
		}
	}
	return result;
}

void DuckLakeDeleteFilter::PopulateSnapshotMapFromPositions(
    ClientContext &context, const DuckLakeFileData &data_file,
    const unordered_map<idx_t, idx_t> &position_to_snapshot) const {
	if (position_to_snapshot.empty()) {
		return;
	}
	unordered_set<idx_t> positions;
	for (auto &entry : position_to_snapshot) {
		positions.insert(entry.first);
	}
	// Try to get row_ids from the data file
	auto file_pos_to_row_id = ScanDataFileRowIds(context, data_file, positions);
	if (!file_pos_to_row_id.empty()) {
		// File has embedded row_ids, use row_id as key
		delete_data->uses_row_id = true;
		for (auto &entry : position_to_snapshot) {
			auto it = file_pos_to_row_id.find(entry.first);
			if (it != file_pos_to_row_id.end()) {
				delete_data->scan_snapshot_map[it->second] = entry.second;
			}
		}
	} else {
		// No embedded row_ids, use file position as key
		delete_data->uses_row_id = false;
		for (auto &entry : position_to_snapshot) {
			delete_data->scan_snapshot_map[entry.first] = entry.second;
		}
	}
}

void DuckLakeDeleteFilter::Initialize(ClientContext &context, const DuckLakeDeleteScanEntry &delete_scan) {
	// Scanning deletes - we need to scan the opposite (i.e. only the rows that were deleted)
	// rows_to_scan[i] = true means row i was deleted and should be returned
	auto rows_to_scan = make_unsafe_uniq_array<bool>(delete_scan.row_count);
	bool has_embedded_snapshots = false;

	unordered_map<idx_t, idx_t> all_position_to_snapshot;

	// Handle the primary deletion source (delete file OR full file delete)
	if (!delete_scan.delete_file.path.empty()) {
		// Partial delete, read deletions from the delete file
		auto current_deletes =
		    ScanDeleteFile(context, delete_scan.delete_file, delete_scan.start_snapshot, delete_scan.end_snapshot);
		has_embedded_snapshots = current_deletes.has_embedded_snapshots;

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
				all_position_to_snapshot[delete_idx] = current_deletes.snapshot_ids[i];
			}
		}
	} else if (delete_scan.snapshot_id.IsValid() && delete_scan.inlined_file_deletions.empty()) {
		// Full file delete, all rows are being scanned
		memset(rows_to_scan.get(), 1, sizeof(bool) * delete_scan.row_count);
		auto snapshot_id = delete_scan.snapshot_id.GetIndex();
		for (idx_t i = 0; i < delete_scan.row_count; i++) {
			all_position_to_snapshot[i] = snapshot_id;
		}
	} else {
		// No delete file and no full file delete
		memset(rows_to_scan.get(), 0, sizeof(bool) * delete_scan.row_count);
	}

	// Add inlined file deletions to the combined map
	if (!delete_scan.inlined_file_deletions.empty()) {
		for (auto &inlined_delete : delete_scan.inlined_file_deletions) {
			rows_to_scan[inlined_delete.first] = true;
			// Add to combined map (inlined deletions may override or add to existing)
			all_position_to_snapshot[inlined_delete.first] = inlined_delete.second;
		}
	}

	// Scan data file to get row_id mappings for all positions
	if (!all_position_to_snapshot.empty()) {
		PopulateSnapshotMapFromPositions(context, delete_scan.file, all_position_to_snapshot);
	}

	if (!delete_scan.previous_delete_file.path.empty() && !has_embedded_snapshots) {
		// if we have a previous delete file - scan that set of deletes
		// This only matters if we do not have a partial deletion file, since thes have all deletes
		auto previous_deletes =
		    ScanDeleteFile(context, delete_scan.previous_delete_file, optional_idx(), optional_idx());
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
