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

	idx_t Filter(row_t start_row_index, idx_t count, SelectionVector &result_sel,
	             optional_idx snapshot_filter = optional_idx()) const;

	bool HasEmbeddedSnapshots() const;
};

struct DeleteFileScanResult {
	vector<idx_t> deleted_rows;
	vector<idx_t> snapshot_ids;
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
	static DeleteFileScanResult ScanDeleteFile(ClientContext &context, const DuckLakeFileData &delete_file);
};

} // namespace duckdb
