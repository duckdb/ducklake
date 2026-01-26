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
	unordered_map<idx_t, idx_t> scan_snapshot_map;

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel,
	             optional_idx snapshot_filter = optional_idx()) const;

	bool HasEmbeddedSnapshots() const;

	//! Look up the snapshot_id for a deleted row (used in deletion scans)
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
};

} // namespace duckdb
