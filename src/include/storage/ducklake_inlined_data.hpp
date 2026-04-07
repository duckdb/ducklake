//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/ducklake_inlined_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "storage/ducklake_stats.hpp"
#include "common/index.hpp"

namespace duckdb {

struct DuckLakeInlinedData {
	unique_ptr<ColumnDataCollection> data;
	map<FieldIndex, DuckLakeColumnStats> column_stats;
	//! Row Ids for update inlining
	vector<int64_t> row_ids;

	bool HasPreservedRowIds() const;
	//! Get the row_id for a given position in the data collection
	idx_t GetRowId(idx_t position) const;
	//! Get the output row_id for a surviving (non-deleted) row
	int64_t GetOutputRowId(idx_t position) const;
	//! Merge preserved row_ids from update inlining
	void MergeRowIds(const DuckLakeInlinedData &new_data, idx_t new_data_count);
};

struct DuckLakeInlinedDataDeletes {
	set<idx_t> rows;
};

//! Stores inlined file deletions for a table
struct DuckLakeInlinedFileDeletes {
	map<idx_t, set<idx_t>> file_deletes;
};

} // namespace duckdb
