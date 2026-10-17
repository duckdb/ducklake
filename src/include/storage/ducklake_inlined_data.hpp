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
class DuckLakeTableEntry;

struct DuckLakeInlinedData {
	unique_ptr<ColumnDataCollection> data;
	//! Explicit row count used when `data` is null.
	idx_t external_row_count = 0;
	map<FieldIndex, DuckLakeColumnStats> column_stats;
	//! Row Ids for update inlining
	vector<int64_t> row_ids;
	//! Set when rows were deleted, the table to recompute column_stats with before the commit
	optional_ptr<DuckLakeTableEntry> outdated_stats_table;

	//! Row count of the inlined collection.
	idx_t Count() const;
	bool HasPreservedRowIds() const;
	//! Get the output row_id for a surviving (non-deleted) row
	int64_t GetOutputRowId(idx_t position) const;
	//! Merge preserved row_ids from update inlining
	void MergeRowIds(const DuckLakeInlinedData &new_data, idx_t new_data_count);
};

//! The committed rows of an inlined table, read before the file list of a scan
struct DuckLakeCommittedInlinedRows {
	//! The column each inlined data reader selects, in the order of the columns of the rows
	vector<string> columns;
	shared_ptr<DuckLakeInlinedData> rows;
};

struct DuckLakeInlinedDataDeletes {
	set<idx_t> rows;
};

//! Stores inlined file deletions for a table
struct DuckLakeInlinedFileDeletes {
	map<idx_t, set<idx_t>> file_deletes;
};

} // namespace duckdb
