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

	bool HasPreservedRowIds() const {
		return !row_ids.empty();
	}
};

struct DuckLakeInlinedDataDeletes {
	set<idx_t> rows;
};

//! Stores inlined file deletions for a table
struct DuckLakeInlinedFileDeletes {
	map<idx_t, set<idx_t>> file_deletes;
};

} // namespace duckdb
