//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_delete_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/multi_file/multi_file_reader.hpp"
#include "storage/ducklake_metadata_info.hpp"

namespace duckdb {

struct DuckLakeDeleteData {
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
	//! For deletion scans: mapping from row_id to snapshot_id for rows that were deleted
	//! If scan_snapshot_map_uses_row_id is true, this is indexed by global row_id (from _ducklake_internal_row_id)
	//! Otherwise, it's indexed by file position
	unordered_map<idx_t, idx_t> scan_snapshot_map;
	//! Whether scan_snapshot_map is indexed by global row_id (true) or file position (false)
	bool uses_row_id = false;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel,
	             optional_idx snapshot_filter = optional_idx()) const;

	bool HasEmbeddedSnapshots() const;

	//! Look up the snapshot_id for a deleted row (used in deletion scans)
	//! The row_id parameter is the global row_id (from _ducklake_internal_row_id)
	optional_idx GetSnapshotForRow(idx_t row_id) const;
};

struct DeleteFileScanResult {
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
	//! Whether the delete file has embedded snapshot_id column
	bool has_embedded_snapshots = false;
};

class DuckLakeDeleteFilter : public DeleteFilter {
public:
	DuckLakeDeleteFilter();

	shared_ptr<DuckLakeDeleteData> delete_data;
	optional_idx max_row_count;

	optional_idx snapshot_filter;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel) override;
	void Initialize(ClientContext &context, const DuckLakeFileData &delete_file);
	void Initialize(const DuckLakeInlinedDataDeletes &inlined_deletes);
	void Initialize(ClientContext &context, const DuckLakeDeleteScanEntry &delete_scan);
	void SetMaxRowCount(idx_t max_row_count);
	void SetSnapshotFilter(idx_t snapshot_filter);

private:
	static DeleteFileScanResult ScanDeleteFile(ClientContext &context, const DuckLakeFileData &delete_file,
	                                           optional_idx snapshot_filter_min = optional_idx(),
	                                           optional_idx snapshot_filter_max = optional_idx());
	//! Scan the data file to get the global row_ids at specific file positions
	//! Returns a map from file_position to global_row_id
	static unordered_map<idx_t, idx_t> ScanDataFileRowIds(ClientContext &context, const DuckLakeFileData &data_file,
	                                                       const unordered_set<idx_t> &file_positions);
	//! Populate scan_snapshot_map from a position-to-snapshot mapping
	//! Handles conversion from file positions to row_ids if the data file has embedded row_ids
	void PopulateSnapshotMapFromPositions(ClientContext &context, const DuckLakeFileData &data_file,
	                                      const unordered_map<idx_t, idx_t> &position_to_snapshot) const;
};

} // namespace duckdb
